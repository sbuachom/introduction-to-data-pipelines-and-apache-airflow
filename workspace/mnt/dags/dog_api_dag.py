from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
 

def _get_dog_image_url():
    print("Hello!")

    import requests

    # url = "https://dog.ceo/api/breeds/image/random"
    url = "https://raw.githubusercontent.com/zkan/data-engineering-bootcamp/refs/heads/main/dataset/greenery/addresses.csv"
    response = requests.get(url)
    data = response.json()
    print(data)

with DAG(
    "dog_api_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule="50 15 30 * *"
):
    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url,
    )