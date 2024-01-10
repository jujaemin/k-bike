from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks import snowflake

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import json


work_places = ['강남역', '미아사거리역', '건대입구역', '광화문·덕수궁', 'DDP(동대문디자인플라자)']
life_places = ['뚝섬한강공원', '여의도한강공원', '서울숲공원', '난지한강공원', '홍대입구역(2호선)']


def get_Snowflake_connection(autocommit=True):
    hook = snowflake(snowflake_conn_id = 'snowflake_conn')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    link = context["params"]["url"]
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]

    return (link)

# 따릉이 transform

def sbike_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    sbike_data = {}
    place_list = []

    for place in work_places:
        api_url = str(response) + place
        place_list.append(api_url)

    for res in place_list:
        resp = requests.get(res)
        data = BeautifulSoup(resp.text, "lxml")
        sbikes = data.find('citydata').find('sbike_stts')

        for sbike in sbikes:
            sbike_spot = sbike.find('sbike_spot_nm').text              # 따릉이 대여소 명
            sbike_spot_id = sbike.find('sbike_spot_id').text           # 따릉이 대여소 ID
            sbike_parking_cnt = sbike.find('sbike_parking_cnt').text   # 따릉이 주차 건수
            sbike_rack_cnt = sbike.find('sbike_rack_cnt').text         # 따릉이 거치대 수
            sbike_shared = sbike.find('sbike_shared').text             # 따릉이 거치율

            sbike_data[res[80:]] = {'따릉이 대여소 명' : sbike_spot, '따릉이 대여소 ID' : sbike_spot_id, '따릉이 주차 건수' : sbike_parking_cnt, 
                                    '따릉이 거치대 수' : sbike_rack_cnt, '따릉이 거치율' : sbike_shared}
    
    sbike_json = json.dumps(sbike_data, indent=4, ensure_ascii=False)

    return sbike_json

# 날씨 transform

def weather_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    weather_data = {}
    place_list = []

    for place in life_places:
        api_url = str(response) + place
        place_list.append(api_url)

    for res in place_list:
        resp = requests.get(res)
        data = BeautifulSoup(resp.text, "lxml")
        weathers = data.find('citydata').find('weather_stts')

        for weather in weathers:
            temp = weather.find('temp').text                      # 온도
            sensible_temp = weather.find('sensible_temp').text    # 체감온도
            rain_chance = weather.find('rain_chance').text        # 강수확률
            precipitation = weather.find('precipitation').text    # 강수량
            uv_index_lvl = weather.find('uv_index_lvl').text      # 자외선 지수 단계
            pm10 = weather.find('pm10').text                      # 미세먼지농도
            pm25 = weather.find('pm25').text                      # 초미세먼지농도

            weather_data[res[80:]] = {'온도' : temp, '체감온도' : sensible_temp, '강수확률' : rain_chance, 
                                      '강수량' : precipitation, '자외선 지수 단계' : uv_index_lvl, '미세먼지농도' : pm10, '초미세먼지농도' : pm25}

    weather_json = json.dumps(weather_data, indent=4, ensure_ascii=False)
     
    return weather_json



dag = DAG(
    dag_id = 'Seoul_data',
    start_date = datetime(2023,4,6),
    schedule = '0 0 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url':  Variable.get("url")
    },
    dag = dag)

sbike_transform = PythonOperator(
    task_id = 'sbike_transform',
    python_callable = sbike_transform,
    params = { 
    },  
    dag = dag)

weather_transform = PythonOperator(
    task_id = 'weather_transform',
    python_callable = weather_transform,
    params = { 
    },  
    dag = dag)



extract >> [sbike_transform, weather_transform]