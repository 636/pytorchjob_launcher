# PyTorchJob Launcher on Kubeflow Pipeline


## How to use on Kubeflow pipeline
```python
import json
import kfp.dsl as dsl
from kubernetes.client import ApiClient, V1Container, V1PodSpec, V1Volume, V1HostPathVolumeSource

@dsl.pipeline(
    name='foo',
    description='descrption')
def hogehoge():

    vop = dsl.VolumeOp(
        name="shared-pvc",
        resource_name="my-pvc",
        modes=dsl.VOLUME_MODE_RWO,
        size=disk_size
    );

    pod_spec = V1PodSpec(
      volumes=[
              vop.volume,
              V1Volume(name='shm', host_path=V1HostPathVolumeSource(path='/dev/shm'))
          ],
      containers=[
        V1Container()
    ])

    job = dsl.ContainerOp(
        name="launchPyTorchJob",
        image="zx636/pytorchjob-launcher",
        command=['python', 'launch.py'],
        arguments=['--pod-spec', json.dumps(ApiClient().sanitize_for_serialization(pod_spec))]
```

## arguments option
```
usage: launch.py [-h] --pod-spec POD_SPEC [--job_name JOB_NAME]
                 [--worker-num WORKER_NUM] [--namespace NAMESPACE]
                 [--log-level {DEBUG,INFO}]

PyTorchJob Launcher

optional arguments:
  -h, --help            show this help message and exit
  --pod-spec POD_SPEC   Specify V1PodSpec by yaml or json. This spec be passed
                        as is api
  --job_name JOB_NAME   Specify PyTorchJob name. default format is
                        [pytorchjob-{yyyyMMdd}-{random_str}]
  --worker-num WORKER_NUM
                        Specify worker count. if 0 set, execute on master
                        only.
  --namespace NAMESPACE
                        Specify namespace executed
  --log-level {DEBUG,INFO}
```