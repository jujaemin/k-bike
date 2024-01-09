import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_rawdata_test_dag",
    default_args=default_args,
    schedule_interval=None     # * * * * *
)

snowflake_query = [
        """
            create  table if not exists source_table( emp_no int,emp_name text,salary int, hra int ,Dept text);
        """,
                    """INSERT INTO source_table VALUES (100, 'A' ,2000, 100,'HR'),
						(101, 'B' ,5000, 300,'HR'),
						(102, 'C' ,6000, 400,'Sales'),
						(103, 'D' ,500, 50,'Sales'),
						(104, 'E' ,15000, 3000,'Tech'),
						(105, 'F' ,150000, 20050,'Tech'),
						(105, 'F' ,150000, 20060,'Tech');
                  """
]



with dag:

	create_table = SnowflakeOperator(
			task_id="create_table",
			sql=snowflake_query[0],
			snowflake_conn_id="snowflake.RAW_DATA_conn"
		)
		
	insert_data = SnowflakeOperator(
		task_id="insert_snowflake_data",
		sql=snowflake_query[1],
		snowflake_conn_id="snowflake.RAW_DATA_conn"
	)


create_table >> insert_data
