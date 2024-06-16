"""
1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать рандомное число и печатать его в
консоль.
2. Создайте PythonOperator, который генерирует рандомное число, возводит его в квадрат и выводит
в консоль исходное число и результат.
3. Сделайте оператор, который отправляет запрос к https://goweather.herokuapp.com/weather/"location"
(вместо location используйте ваше местоположение).
4. Задайте последовательный порядок выполнения операторов.
"""

from datetime import datetime
import random
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

dag = DAG( 'Sem6_HW' , description= 'Home work of Seminar 6' ,
	schedule_interval= '0 12 * * *' ,
    tags=["seminars"],
	start_date=datetime( 2023 , 1 , 1), 
	catchup= False ) # наверствование предыдущих запусков не производится, если их не было

random_number_BashOperator = BashOperator(task_id= 'random_number_BashOperator' , bash_command='echo $RANDOM', dag=dag)

def random_number():
    number = random.randint(1, 10)
    return number, number**2

random_number_PythonOperator = PythonOperator(
    task_id = 'random_number_PythonOperator',
    dag = dag,
    python_callable = random_number,
)

http_task1 = SimpleHttpOperator(
    task_id='http_goweather_operator',
    method='GET',
    http_conn_id='http_goweather_herokuapp_com',
    endpoint='weather/saint-petersburg',
    dag=dag,
)

http_task2 = SimpleHttpOperator(
    task_id='http_yandex_pogoda',
    method='GET',
    http_conn_id='http_yandex_pogoda',
    endpoint='pogoda/saint-petersburg/',
    dag=dag,
)

random_number_BashOperator >> random_number_PythonOperator >> http_task1 >> http_task2