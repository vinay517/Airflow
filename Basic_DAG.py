from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
args= {
    'owner': 'Vinay'
    'Start_date': days_ago(2)
    
}

dag = DAG(dag_id = 'initial_dag', default_args= args, schedule_interval = None)

def run_this_func(**context):
    print('hello')

with dag:
    Task1 = PythonOperator(
    task_id = run_this,
    python_callable=run_this_func,
    provide_context = True
    )
    Task2 = PythonOperator(
    task_id = run_this2,
    python_callable=run_this_func,
    provide_context = True
    )
    
    Task1 >> Task2 
