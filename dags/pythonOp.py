from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum

executor_config = {
    "KubernetesExecutor": {
        "volumes": [
            {
                "name": "airflow-shared-pv",
                "persistentVolumeClaim": {
                    "claimName": "airflow-shared-pvc"
                }
            }
        ],
        "volume_mounts": [
            {
                "name": "airflow-shared-pv",
                "mountPath": "/mnt/shared"
            }
        ]
    }
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
        f.write('')

# op_kwargs는 dict, op_args는 list
PythonOperator(task_id="t1", python_callable=createFile, op_kwargs={'location': '/mnt/shared','filename': 'touch.txt'}, dag=dag, executor_config=executor_config)
