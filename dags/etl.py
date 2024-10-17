from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks import snowflake

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta


places = ['강남역', '미아사거리역', '건대입구역', '광화문·덕수궁', 'DDP(동대문디자인플라자)', '뚝섬한강공원', '여의도한강공원', '서울숲공원', '난지한강공원', '홍대입구역(2호선)']


def get_Snowflake_connection(autocommit=True):
    hook = snowflake.SnowflakeHook(snowflake_conn_id = 'snowflake_conn_raw')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    link = context["params"]["url"]

    return (link)

# 따릉이 transform

def sbike_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    sbike_data = []
    place_list = []

    for place in places:
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

            sbike_data.append([res[80:], sbike_spot, sbike_spot_id, int(sbike_parking_cnt), int(sbike_rack_cnt), int(sbike_shared)])
    
    return sbike_data

# 날씨 transform

def weather_transform(**context):
    response = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    weather_data = []
    place_list = []

    for place in places:
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

            weather_data.append([res[80:], float(temp), float(sensible_temp), int(rain_chance), precipitation, int(uv_index_lvl), int(pm10), int(pm25)])

     
    return weather_data

def sbike_load_func(**context):  
    schema = context["params"]["schema"]
    table = context["params"]["table"]
        # convert timezone UTC -> KST
    tmp_dt = datetime.now() + timedelta(hours=9)
    created_at = tmp_dt.strftime('%Y-%m-%d %H:%M:%S')

    records = context["task_instance"].xcom_pull(key="return_value", task_ids="sbike_transform")    

    cur = get_Snowflake_connection()

    if not records:
        raise Exception('records is empty')

    try:
        cur.execute("BEGIN;")

        for r in records:
            place = r[0]
            sbike_spot = r[1]
            sbike_spot_id = r[2]
            sbike_parking_cnt = r[3]
            sbike_rack_cnt = r[4] 
            sbike_shared = r[5]
            insert_sql = f"INSERT INTO {table} VALUES ('{place}','{created_at}','{sbike_spot}', '{sbike_spot_id}', '{sbike_parking_cnt}', '{sbike_rack_cnt}', '{sbike_shared}')"
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
        
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

def weather_load_func(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    # convert timezone UTC -> KST
    tmp_dt = datetime.now() + timedelta(hours=9)
    created_at = tmp_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="weather_transform")    

    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    cur = get_Snowflake_connection()

    if not records:
        raise Exception('records is empty')

    try:
        cur.execute("BEGIN;")

        for r in records:
            place = r[0]
            temp = r[1]
            sensible_temp = r[2]
            rain_chance = r[3]
            precipitation = r[4]
            uv_index_lvl = r[5]
            pm10 = r[6]
            pm25 = r[7]
            insert_sql = f"INSERT INTO {table} VALUES ('{place}','{created_at}', '{temp}', '{sensible_temp}', '{rain_chance}', '{precipitation}', '{uv_index_lvl}', '{pm10}', '{pm25}')"
            cur.execute(insert_sql)
        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise


dag = DAG(
    dag_id = 'Seoul_data',
    start_date = datetime(2024,1,1),
    schedule = timedelta(minutes = 30),
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

sbike_load = PythonOperator(
    task_id = 'sbike_load',
    python_callable = sbike_load_func,
    params = {
        'schema': 'RAW_DATA',  
        'table': 'SBIKE',
    },
    dag = dag)

weather_load = PythonOperator(
    task_id = 'weather_load',
    python_callable = weather_load_func,
    params = {
        'schema': 'RAW_DATA',  
        'table': 'WEATHER',
    },
    dag = dag)

trigger = TriggerDagRunOperator(
    task_id='trigger_next_dag',
    trigger_dag_id="get_latest_data",  # 트리거하려는 다음 DAG의 ID
    dag=dag,
)

[extract >> sbike_transform >> sbike_load, extract >> weather_transform >> weather_load] >> trigger

