from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pprint import pprint

from airflow.operators.python import PythonOperator

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)
        from movie.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KEY = {key}")
        YYYYMMDD = kwargs['ds_nodash'] #20240724
        df = save2df(YYYYMMDD)
        print(df.head(5))

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
            task_id="print_the_context", 
            python_callable=print_context)

    task_start = EmptyOperator(
            task_id ='start',
            )

    task_get_data= PythonOperator(
            task_id ='get_data',
            python_callable=get_data
            )

    task_save_data= BashOperator(
            task_id ='save_data',
            bash_command="""
            echo "save_data"
            """
            )

    task_end = EmptyOperator(
            task_id ='end',
            trigger_rule="all_done"
            )


    task_start >> task_get_data >> task_save_data >> task_end
    task_start >> run_this >> task_end
