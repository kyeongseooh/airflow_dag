from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum
from kubernetes.client import models as k8s

volume_mount = k8s.V1VolumeMount(name='airflow-shared-pv',
       mount_path="/airflow/shared",
       sub_path=None,
       read_only=False)

pvc = k8s.V1PersistentVolumeClaimVolumeSource(claim_name="osm-config-pv-claim")

volume = k8s.V1Volume(name="airflow-shared-pv", persistent_volume_claim=pvc)


executor_config={
              "pod_override": k8s.V1Pod(
                  spec=k8s.V1PodSpec(
                      containers=[
                          k8s.V1Container(
                              name="base",
                              volume_mounts=[volume_mount]
                          )
                      ],
                      volumes=[volume],
                  )
              )
          }

dag = DAG(
    dag_id='pythonOp',
    # test용으로 한번만 돌릴 때 @once를 사용
    schedule_interval='@once',
    start_date=datetime(2023,8,21,10,17)
    # schedule=timedelta(minutes=1)
    )


# python Operator
def createFile(location, filename):
    with open(f"{location}/{filename}", 'a') as f:
        f.write('test')

# op_kwargs는 dict, op_args는 list
a = PythonOperator(task_id="t1", python_callable=createFile, op_kwargs={'location': '/airflow/shared','filename': 'touch.txt'}, dag=dag, executor_config=executor_config)

def catFile():
    with open(f"/airflow/shared/touch.txt", 'r') as f:
        text = f.read()
    print(text)
b = PythonOperator(task_id="t2", python_callable=catFile,  dag=dag, executor_config=executor_config)

a >> b
