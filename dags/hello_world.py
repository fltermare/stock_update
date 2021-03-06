"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta
from myutils import check_stock_day_exist, query_yahoo_finance, insert_new_data
from setting import Setting

default_args = {
    "owner": "Albert",
    "depends_on_past": False,
    "start_date": datetime.today() - timedelta(days=3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def day_check(stock_code, config, **context):
    execution_date = context['execution_date']

    is_existed = check_stock_day_exist(stock_code, execution_date, config)

    if is_existed:
        print('[day_check]', stock_code, execution_date, '[skip]')
        return "done"
    else:
        print('[day_check]', stock_code, execution_date, '[fetch]')
        return "fetch_stock"


def fetch_stock(stock_code, config, **context):
    """ Get data"""

    execution_date = context['execution_date']
    df = query_yahoo_finance(stock_code, 0, execution_date)
    print("[***************]%s" % stock_code, df.shape)

    return df


def update_db(stock_code, config, **context):
    """ """
    df = context['task_instance'].xcom_pull(task_ids='fetch_stock')

    insert_new_data(stock_code, df, config)

    print('[*******update_db********]', df.shape)



def create_dag(dag_id,
               schedule,
               default_args,
               stock_code,
               config):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)
    with dag:

        day_check_operator = BranchPythonOperator(
            task_id='day_check',
            python_callable=day_check,
            provide_context=True,
            op_args=[stock_code, config],
            dag=dag,
        )

        fetch_stock_operator = PythonOperator(
            task_id='fetch_stock',
            python_callable=fetch_stock,
            op_args=[stock_code, config],
            provide_context=True,
            retries=5,
            retry_delay=timedelta(minutes=1),
            dag=dag,
        )

        update_db_operator = PythonOperator(
            task_id='update_db',
            python_callable=update_db,
            op_args=[stock_code, config],
            provide_context=True,
            dag=dag,
        )

        done = DummyOperator(
            task_id='done',
            dag=dag
        )

        day_check_operator >> fetch_stock_operator >> update_db_operator >> done
        day_check_operator >> done
        return dag


# Load setting
config = Setting()

# schedule = "@hourly"
# schedule = "0 */12 * * *"
schedule = "0 1 * * *"

dags = []
for stock_code in config.stock_code_list:
    dag_id = f"Dynamic_DAG_{stock_code}"
    dags.append(dag_id)
    globals()[dag_id] = create_dag(dag_id, schedule, default_args, stock_code, config)

# unpause_airflow_dag(dags)
# docker-compose -f docker-compose-CeleryExecutor.yml up --scale worker=3