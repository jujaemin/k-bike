import airflow
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import timedelta

default_args = {
    "owner": "Airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="get_latest_data",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
) as dag:

    get_latest_weather_info = SnowflakeOperator(
        task_id="get_latest_weather_info",
        snowflake_conn_id="snowflake_conn_raw",
        sql="""
                BEGIN;
                 
                DROP TABLE IF EXISTS ANALYTICS.latest_weather_info; 
                CREATE OR REPLACE TABLE ANALYTICS.latest_weather_info AS (
                SELECT PLACE, TEMP, SENSIBLE_TEMP, UV_INDEX_LVL, PM10, PM25, CREATED_AT
                FROM WEATHER
                WHERE (PLACE, CREATED_AT) IN (
                    SELECT PLACE, MAX(CREATED_AT)
                    FROM WEATHER
                    GROUP BY PLACE
                    )
                ORDER BY PLACE
                );
                
                COMMIT;
                """,
        autocommit=True
    )

    get_latest_sbike_info = SnowflakeOperator(
        task_id="get_latest_sbike_info",
        snowflake_conn_id="snowflake_conn_raw",
        sql="""
                BEGIN;
                
                DROP TABLE IF EXISTS ANALYTICS.latest_sbike_info; 
                CREATE OR REPLACE TABLE ANALYTICS.latest_sbike_info AS (
                SELECT PLACE, SBIKE_SPOT, SBIKE_SPOT_ID, SBIKE_PARKING_CNT, SBIKE_RACK_CNT, SBIKE_SHARED, CREATED_AT
                FROM SBIKE
                WHERE (PLACE, CREATED_AT) IN (
                    SELECT PLACE, MAX(CREATED_AT)
                    FROM SBIKE    
                    GROUP BY PLACE
                    )
                ORDER BY PLACE
                );
                
                COMMIT;
                """,
        autocommit=True
    )

    get_latest_temp_rain_shared = SnowflakeOperator(
        task_id="get_latest_temp_rain_shared",
        snowflake_conn_id="snowflake_conn_raw",
        sql="""
                    BEGIN;

                    DROP TABLE IF EXISTS ANALYTICS.latest_temp_rain_shared;
                    CREATE OR REPLACE TABLE ANALYTICS.latest_temp_rain_shared AS (
                    SELECT W.PLACE, W.TEMP, W.RAIN_CHANCE, AVG(S.SBIKE_SHARED) AS AVG_SBIKE_SHARED, MAX(W.CREATED_AT) AS MAX_CREATED_AT
                    FROM WEATHER W
                    JOIN SBIKE S on W.PLACE = S.PLACE
                    WHERE (W.PLACE, W.CREATED_AT) IN (
                        SELECT W2.PLACE, MAX(W2.CREATED_AT)
                        FROM WEATHER W2
                        GROUP BY W2.PLACE
                        )
                    GROUP BY W.PLACE, W.TEMP, W.RAIN_CHANCE
                    ORDER BY W.PLACE
                    );

                    COMMIT;
                    """,
        autocommit=True
    )

    [get_latest_weather_info, get_latest_sbike_info, get_latest_temp_rain_shared]