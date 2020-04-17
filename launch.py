import argparse
import datetime
import logging
import logging.config
import random
import string

import yaml
from kubeflow.pytorchjob import (PyTorchJobClient, V1PyTorchJob,
                                 V1PyTorchJobSpec, V1ReplicaSpec, constants,
                                 utils)
from kubeflow.pytorchjob.constants import constants

from kubernetes import watch
from kubernetes.client import (V1Container, V1ObjectMeta, V1PodSpec,
                               V1PodTemplateSpec, V1ResourceRequirements,
                               V1VolumeMount, V1Pod)

with open('./logging.yaml', 'r', encoding='utf-8') as f:
    logging.config.dictConfig(yaml.safe_load(f))

LOGGER = logging.getLogger(__name__)


def yamlOrJsonStr(str):
    if str == "" or str == None:
        return None
    return yaml.safe_load(str)


def launch_job(pod_spec: dict,
               job_name='pytorch-operation-job',
               namespace='kubeflow',
               worker_num=0,
               wait_for_startup=1800):
    """
    Launch PyTorchJob on kubeflow pipeline

    """
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

    pytorchjob = V1PyTorchJob(
        api_version="kubeflow.org/v1",
        kind="PyTorchJob",
        metadata=V1ObjectMeta(name=job_name, namespace=namespace),
        spec=V1PyTorchJobSpec(
            clean_pod_policy="None",
            pytorch_replica_specs=replica_spec
        )
    )

    client = PyTorchJobClient()
    ret = client.create(pytorchjob)
    LOGGER.info('Launch PyTorchJob %s', ret)
    client.wait_for_condition(job_name,
                              'Created',
                              namespace=namespace,
                              timeout_seconds=1800,
                              polling_interval=60,
                              status_callback=lambda x: LOGGER.debug('PyTorchJob Conditions: %s', x.get("status", {}).get("conditions", [])))
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
    for event in w.stream(client.core_api.list_namespaced_pod,
                          namespace,
                          label_selector=utils.to_selector(labels)):
        pod = event['object']  # type: V1Pod
        LOGGER.debug("Event: %s %s %s",
                     event['type'],
                     pod.metadata.name,
                     pod.status.phase,
                     pod.status.conditions[-1] if len(pod.status.conditions) > 0 else 'none')

        if pod.status.phase in ['Succeeded', 'Failed', 'Unknown'] or (pod.status.phase == 'Running' and
                                                                      len(pod.status.conditions) > 0 and
                                                                      pod.status.conditions[-1].type == 'PodScheduled'):
            w.stop()

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
        launch_job(**args)
    except Exception as e:
        LOGGER.exception('Unexpected Error')
        raise e
