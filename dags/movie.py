from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pprint import pprint

from airflow.operators.python import (
        PythonOperator, 
        BranchPythonOperator, 
        PythonVirtualenvOperator)

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    
    },
    max_active_tasks=3,
    max_active_runs=1,
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:
        def get_data(ds_nodash):
                from movie.api.call import get_key,save2df
                df=save2df(ds_nodash)
                print(df.head(5))

        def save_data(ds_nodash):
                from movie.api.call import get_key
                key = get_key()
                print( "*" * 33)
                print(key)
                print( "*" * 33)


        def branch_fun(ds_nodash):
                import os
                home_dir = os.path.expanduser("~")
                path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
                if os.path.exists(path):
                        return "rm.dir"
                else:
                        return "get_start", "echo.task"
   
        branch_op = BranchPythonOperator(
                task_id="branch.op", 
                python_callable=branch_fun
                
                )

        get_data= PythonVirtualenvOperator(
                task_id ='get_data',
                python_callable=get_data,
                requirements=["git+https://github.com/HaramSs/movie.git@0.3/api"],
                system_site_packages=False,
                )

        save_data= PythonVirtualenvOperator(
                task_id ='save.data',
                python_callable=save_data,
                system_site_packages=False,
                trigger_rule = "one_success",
                venv_cache_path="/home/haram/tmp2/air_venv/get_data"
                )

        def get_data_with_params(**kwargs):

                params = dict(kwargs.get("url_params"))
                date = kwargs.get("ds")

                if len(params) >= 1:
                        from movie.api.call import save2df
                        df = save2df(load_dt=date, url_param=params)
                        print(df)
                else:
                        print("params안에 값이 없음")
                        sys.exit(1)


        nation_k = PythonVirtualenvOperator(
                task_id='nation.k',
                system_site_packages=False,
                requirements=["git+https://github.com/HaramSs/movie.git@0.3/api"],
                op_kwargs={
                "url_params": {"repNationCd": "K"},
                "ds": "{{ds_nodash}}"
                },
                python_callable=get_data_with_params,
        )

        nation_f = PythonVirtualenvOperator(
                task_id='nation.f',
                system_site_packages=False,
                requirements=["git+https://github.com/HaramSs/movie.git@0.3/api"],
                op_kwargs={
                "url_params": {"repNationCd": "F"},
                "ds": "{{ds_nodash}}"
                },
                python_callable=get_data_with_params,
        )

        multi_y = PythonVirtualenvOperator(
                task_id='multi.y',
                system_site_packages=False,
                requirements=["git+https://github.com/HaramSs/movie.git@0.3/api"],
                op_kwargs={
                "url_params": {"multiMovieYn": "Y"},
                "ds": "{{ds_nodash}}"
                },
                python_callable=get_data_with_params,
        )
        multi_n = PythonVirtualenvOperator(
                task_id='multi.n',
                system_site_packages=False,
                requirements=["git+https://github.com/HaramSs/movie.git@0.3/api"],
                op_kwargs={
                "url_params": {"multiMovieYn": "N"},
                "ds": "{{ds_nodash}}"
                },
                python_callable=get_data_with_params,
        )

        rm_dir= BashOperator(
                task_id ='rm.dir',
                bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
                )

        echo_task = BashOperator(
                task_id='echo.task', 
                bash_command="echo 'task'"
                )

        start= EmptyOperator(task_id ='start')
        end = EmptyOperator(task_id ='end')
        
        throw_err = BashOperator(
                task_id='throw.err',
                bash_command="exit 1",
                trigger_rule="all_done"
                )

        get_start = EmptyOperator(
                task_id='get.start',
                trigger_rule = "all_done"
                )

        get_end = EmptyOperator(task_id='get.end')

        start >> branch_op 
        start >> throw_err >> get_start

        branch_op >> [rm_dir, echo_task] >> get_start
        # branch_op >> echo_task >> get_start
        branch_op >> get_start
        get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end
        get_end >> save_data >> end
        
