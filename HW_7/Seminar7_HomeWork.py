
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def openwear_get_temp(**kwargs):

    ti = kwargs['ti']
    city_id = 498817
    api_key = "c83d04aa98d5f0ecd90e86d3595008c9"
    res = requests.get('http://api.openweathermap.org/data/2.5/weather?',
                        params={'id': city_id, 'units': 'metric', 'lang': 'ru', 'APPID': api_key})
    data = res.json()
    print(data['main']['temp'])
    return int(data['main']['temp'])


def openwear_check_temp(ti):
    temp = int(ti.xcom_pull(task_ids='SPb_get_temperature'))
    print(f'Temperature now is {temp}')
    if temp >= 15:
        return 'SPb_Temp_warm'
    else:
        return 'SPb_Temp_cold'

with DAG(
        'SPb_check_temperature',
        start_date=datetime(2024, 4, 25),
        catchup=False,
        tags=['seminars'],
) as dag:
    SPb_get_temperature = PythonOperator(
        task_id='SPb_get_temperature',
        python_callable=openwear_get_temp,
    )

    SPb_check_temperature = BranchPythonOperator(
        task_id='SPb_check_temperature',
        python_callable=openwear_check_temp,
    )

    SPb_Temp_warm = BashOperator(
        task_id='SPb_Temp_warm',
        bash_command='echo "В Санкт-Петербурге тепло! :о)"',
    )

    SPb_Temp_cold = BashOperator(
        task_id='SPb_Temp_cold',
        bash_command='echo "В Санкт-Петербурге холодно! :о("',
    )

SPb_get_temperature >> SPb_check_temperature >> [SPb_Temp_warm, SPb_Temp_cold]
