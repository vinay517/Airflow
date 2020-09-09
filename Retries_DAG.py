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

def fail(**context):
    raise Exception('Exception') # this fails always
    
def rand_fail(**context):
    if random.random() > 0.6:
        raise Exception('Exception') # this fails randomly so we can use retry to run it again 
    print('not failed')

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
    Task3 = PythonOperator(
    task_id = run_this3,
    python_callable=Fail,
    provide_context = True
    )
    Task4 = PythonOperator(
    task_id = run_this4,
    python_callable=rand_Fail,
    provide_context = True
    retries = 10
    )
    
    
    Task1 >> Task2 >> Task3 >> Task4
