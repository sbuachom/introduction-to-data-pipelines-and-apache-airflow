import json
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
 


def _get_air_quality_data(**context):
    print(context)
    
    ds = context["ds"]


    print("Hello!")

    import requests

   
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude=13.870352796078675&longitude=100.55150541726421&hourly=pm2_5&start_date={ds}&end_date={ds}"
    response = requests.get(url)
    data = response.json()
    print(data)

    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _find_average_pm2_5(**context):
    ds = context["ds"]
    

    with open(f"/opt/airflow/dags/{ds}-output.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    pm2_5_values = data["hourly"]["pm2_5"]
    average_pm2_5 = sum(pm2_5_values) / len(pm2_5_values)
    print(f"Average PM2.5 for {ds}: {average_pm2_5}")

    with open(f"/opt/airflow/dags/{ds}-average.json", "w", encoding="utf-8") as f:
        data = {
            "average_pm2_5": average_pm2_5
        }
        json.dump(data, f, ensure_ascii=False, indent=2)


#latitude=13.870352796078675&longitude=100.55150541726421

with DAG(
    "weather_data_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule="@daily"
):
    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    find_average_pm2_5=PythonOperator(
        task_id = "find_average_pm2_5",
        python_callable = _find_average_pm2_5,
    )

    get_air_quality_data>> find_average_pm2_5


    #echo_date = BashOperator(
       # task_id="echo_date",
      #  bash_command="echo {{ ds }}",
  # )