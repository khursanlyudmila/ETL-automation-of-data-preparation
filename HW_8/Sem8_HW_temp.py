"""
Используя материалы семинара и s8dag.py нужно доработать задачу в части записи данных в mysq
по погоде яндекса и open weather (поля - метка текущего времени и температура).
Создать еще одну задачу по отправке данных в телеграм. За основу взять данные таблиц платежей
из 4-го семинара (все 360 периодов), конвертировать их в текстовый формат и отправить их в
telegram. К ДЗ приложите код и скриншоты отрабоданных задач аирфлоу, а также отправленный
слепкок из базы данных в вашем чаботе. Рассмотрите возможность применения разметки html
либо markdown. Нужно выслать одну основную таблицу. Есть есть лимит по сообщениям, можно
ограничить количество строк таблицы. Можете использовать функцию limit в sql запросе.

"""

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine


with DAG('Sem8_HW_temp', start_date=datetime(2024, 6, 18), schedule='@daily', catchup=False, tags=["seminars"]) as dag:  

  @task(task_id="get_openweather")
  def get_openweather(**kwargs):
        city_id = 498817
        appid = "c83d04aa98d5f0ecd90e86d3595008c9"
        try:
            res = requests.get('http://api.openweathermap.org/data/2.5/weather?',
                        params={'id': city_id, 'units': 'metric', 'lang': 'ru', 'APPID': appid})
            data = res.json()
            ti = kwargs['ti']
            ti.xcom_push(key='open_weather', value=int(data['main']['temp']))
            print(data['main']['temp'])
            return int(data['main']['temp'])
        except Exception as e:
            print("Exception (weather):", e)

  @task(task_id="get_yandexweather")
  def get_yandexweather(**kwargs):
    access_key = '03b22142-9561-4bd2-8f8d-e375bbfb80ce'
    headers = {
        'X-Yandex-Weather-Key': access_key
    }
    try:
      response = requests.get('https://api.weather.yandex.ru/v2/forecast?lat=59.8944&lon=30.2642', headers=headers)
      res = response.json()
      ti = kwargs['ti']
      ti.xcom_push(key='weather', value=res)
      print(res)
      return res
    except Exception as e:
      print("Exception (weather):", e)

  sending_to_telegram = TelegramOperator(
        task_id="sending_to_telegram",
        telegram_conn_id='telegram_connect',
        chat_id=1563337265,
        text='Weather in Saint Petersburg\nYandex: ' + "{{ ti.xcom_pull(task_ids=['get_yandexweather'],key='weather')[0]}}" + " degrees" +"\nOpen weather: " + "{{ ti.xcom_pull(task_ids=['get_openweather'],key='open_weather')[0]}}" + " degrees",
        dag=dag,
    )
  @task(task_id='python_weather')
  def python_weather(**kwargs):
        print('Weather in Saint Petersburg\nYandex: ' + "{{ ti.xcom_pull(task_ids=['get_yandexweather'],key='weather')[0]}}" + " degrees" +"\nOpen weather: " + "{{ ti.xcom_pull(task_ids=['get_openweather'],key='open_weather')[0]}}" + " degrees")

  @task(task_id='python_table')
  def get_weather_table(**kwargs):
      con = create_engine("postgresql://postgres:1981@localhost/postgres")
      data = [[str(kwargs['ti'].xcom_pull(task_ids=['get_yandexweather'],key='weather')[0]), str(kwargs['ti'].xcom_pull(task_ids=['get_openweather'],key='open_weather')[0]), datetime.now()]]
      df = pd.DataFrame(data)
      df.to_sql('weather',con,if_exists='replace',index=False)

  get_openweather() >> get_yandexweather() >> python_weather() >> get_weather_table() >> sending_to_telegram
