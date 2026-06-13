from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
 

with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule=None
):
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")
    t4 = EmptyOperator(task_id="t4")
    t5 = EmptyOperator(task_id="t5")


    t1 >> t2 >> t3
    t2 >> t4 >> t5