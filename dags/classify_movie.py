from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'classify_movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='classify_movie DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['classify movie'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='movie_name',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t3 = BashOperator(
        task_id='genre',
        depends_on_past=False,
        bash_command='date',
    )

    task_end = DummyOperator(task_id='end')
    task_start = DummyOperator(task_id='start')

    t1 >> [t2,t3] >> task_end
    task_start >> t1
    t1 >> t2 >> task_end
    

    t1 >> [t2, t3]
