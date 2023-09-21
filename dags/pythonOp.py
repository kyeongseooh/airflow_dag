from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum


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
a = PythonOperator(task_id="t1", python_callable=createFile, op_kwargs={'location': '/mnt/shared','filename': 'touch.txt'}, dag=dag)

def catFile():
    with open(f"/mnt/shared/touch.txt", 'r') as f:
        text = f.read()
    print(text)
b = PythonOperator(task_id="t1", python_callable=catFile,  dag=dag)

a >> b
