import argparse
import datetime
import logging
import logging.config
import random
import string
import sys

import yaml
from kubeflow.pytorchjob import (PyTorchJobClient, V1PyTorchJob,
                                 V1PyTorchJobSpec, V1ReplicaSpec, utils)
from kubernetes import watch
from kubernetes.client import V1ObjectMeta, V1Pod, V1PodTemplateSpec

with open('./logging.yaml', 'r', encoding='utf-8') as f:
    logging.config.dictConfig(yaml.safe_load(f))

LOGGER = logging.getLogger(__name__)


def yamlOrJsonStr(str):
    if str == "" or str is None:
        return None
    return yaml.safe_load(str)


def _create_pytorchjob(pod_spec: dict,
                       job_name='pytorch-operation-job',
                       namespace='kubeflow',
                       worker_num=0):

    disbale_istio_injection = {
        'sidecar.istio.io/inject': "false"
    }

    replica_spec = {"Master": V1ReplicaSpec(
        replicas=1,
        restart_policy="OnFailure",
        template=V1PodTemplateSpec(
            metadata=V1ObjectMeta(annotations=disbale_istio_injection, namespace=namespace),
            spec=pod_spec
        )
    )}

    if worker_num > 0:
        worker = V1ReplicaSpec(
            replicas=worker_num,
            restart_policy="OnFailure",
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(annotations=disbale_istio_injection, namespace=namespace),
                spec=pod_spec
            )
        )
        replica_spec['Worker'] = worker

    return V1PyTorchJob(
        api_version="kubeflow.org/v1",
        kind="PyTorchJob",
        metadata=V1ObjectMeta(name=job_name, namespace=namespace),
        spec=V1PyTorchJobSpec(
            clean_pod_policy="None",
            pytorch_replica_specs=replica_spec
        )
    )


def launch_job(client: PyTorchJobClient, job: V1PyTorchJob):
    """
    Launch PyTorchJob on kubeflow pipeline

    """

    ret = client.create(job)  # type: V1PyTorchJob
    LOGGER.info('Launch PyTorchJob %s', ret)
    job_name = ret.metadata.name
    namespace = ret.metadata.namespace
    job = client.wait_for_condition(job_name,
                                    ['Created', 'Failed'],
                                    namespace=namespace,
                                    status_callback=lambda x: LOGGER.debug('PyTorchJob Conditions\n %s', x.get("status", {}).get("conditions", ['None Condition'])[-1]))

    if job.get("status", {}).get("conditions", [])[0]['type'] == 'Failed':
        LOGGER.error('Cancel PytorchJob: %s', job_name)
        LOGGER.error('Unexpected condition. Could you confirm below ?')
        LOGGER.error(job)

        sys.exit(1)

    LOGGER.info('PyTorchJob created: %s', job_name)
    for _pname in client.get_pod_names(job_name, namespace=namespace):
        LOGGER.info('Pod name: %s', _pname)

    master_pod_name = list(client.get_pod_names(job_name, namespace=namespace, master=True))[0]
    master_pod = client.core_api.read_namespaced_pod(master_pod_name, namespace, pretty='true')

    LOGGER.debug('master pod spec')
    LOGGER.debug(master_pod)

    labels = utils.get_labels(job_name, master=True)
    LOGGER.info('wait till pod running. target selector: %s', labels)
    w = watch.Watch()
    last_pod_info = None
    for event in w.stream(client.core_api.list_namespaced_pod,
                          namespace,
                          label_selector=utils.to_selector(labels)):
        last_pod_info = event['object']  # type: V1Pod
        LOGGER.debug("Event: %s %s %s %s",
                     event['type'],
                     last_pod_info.metadata.name,
                     last_pod_info.status.phase,
                     last_pod_info.status.conditions[-1] if len(last_pod_info.status.conditions) > 0 else 'none')

        if last_pod_info.status.phase in ['Succeeded', 'Failed', 'Unknown'] or (last_pod_info.status.phase == 'Running' and
                                                                                len(last_pod_info.status.conditions) > 0 and
                                                                                last_pod_info.status.conditions[-1].type == 'PodScheduled'):
            w.stop()

    if last_pod_info.status.phase in ['Failed', 'Unknown']:
        LOGGER.error('Cancel PytorchJob: %s', job_name)
        LOGGER.error('master pod status: %s', last_pod_info.status.phase)
        LOGGER.error('Could you confirm below ?')
        LOGGER.error(last_pod_info)

        sys.exit(1)

    LOGGER.info('start watch PyTorchJob Pods log')
    for line in client.core_api.read_namespaced_pod_log(master_pod_name,
                                                        namespace,
                                                        container='pytorch',
                                                        follow=True,
                                                        _preload_content=False).stream():
        LOGGER.info(line.decode('utf-8')[:-1])

    client.wait_for_job(job_name, namespace=namespace)

    LOGGER.info('Delete PyTorchJob')
    client.delete(job_name, namespace=namespace)

    LOGGER.info('Launched job finished')


if __name__ == "__main__":
    today = datetime.date.today()
    job_name_format = 'pytorchjob-{yyyyMMdd}-{random_str}'
    default_job_name = job_name_format.format(yyyyMMdd=today.strftime('%Y%m%d'),
                                              random_str=''.join(random.choices(string.ascii_lowercase + string.digits, k=8)))

    parser = argparse.ArgumentParser(
        description='PyTorchJob Launcher')
    parser.add_argument('--pod-spec', type=yamlOrJsonStr, required=True,
                        help='Specify V1PodSpec by yaml or json. This spec be passed as is api')

    parser.add_argument('--job_name', type=str, default=default_job_name,
                        help='Specify PyTorchJob name. default format is [{}]'.format(job_name_format))
    parser.add_argument('--worker-num', type=int, default=0, help='Specify worker count. if 0 set, execute on master only.')
    parser.add_argument('--namespace', type=str, default='kubeflow', help='Specify namespace executed')

    parser.add_argument('--log-level', default='DEBUG', choices=['DEBUG', 'INFO'])

    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    args = vars(args)
    del args['log_level']

    LOGGER.debug(args)

    try:
        job = _create_pytorchjob(**args)
        launch_job(PyTorchJobClient(), job)
    except Exception as e:
        LOGGER.exception('Unexpected Error')
        raise e
