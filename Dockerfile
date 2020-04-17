FROM python:3.7.7

RUN pip install kubeflow-pytorchjob==0.1.3 kubernetes==10.0.1 retrying

ADD launch.py ./
ADD logging.yaml ./

CMD ["python", "launch.py"]
