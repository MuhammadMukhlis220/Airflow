from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from pyhive import hive

today_year = Variable.get("global_tahun") if bool(int(Variable.get("global_variabel"))) else str(datetime.today().year)
username = "hive"
dir_name = f"/mydir/{today_year}"
hive_table_name_target = f"mydir_{today_year}"
hive_database_source = "konsolidasi"
hive_database_target = "data"
hive_node = "x.x.x.x"

default_args = {
    "start_date":days_ago(1)
}

dag = DAG(
    "parsing_argument_to_spark", # DAG name
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    tags = ["My Tag", "Your Tag"]
)

def query_hive():

    pass # Nothing to do

    '''
    conn = hive.Connection(host = hive_node, port = 10000, username = "hive", database = hive_database_source)
    cursor = conn.cursor()

    cursor.execute("Descrive konsolidasi.konsolidasi_apbdgeser")
    schema = cursor.fetchall()
    df_schema = pd.DataFrame(schema, columns = ["column_name", "data_type", "comment"])
    columns = ",\n    ".join([f"{row['column_name']} {row['data_type']}" for index, row in df_schema.iterrows()])

    drop_query = f"DROP TABLE IF EXISTS {hive_database_source}.{hive_table_name_target}"
    
    create_table_query = f"""
        CREATE EXTERNAL TABLE `{hive_database_source}.{hive_table_name_target}`(
        {columns})
        STORED AS ORC
        LOCATION '{dir_name}'
     """

    cursor.execute(drop_query)
    cursor.execute(create_table_query)
    '''

task_1 = PythonOperator(
    task_id = "query_hive", # task name
    python_callable = query_hive, # python function name to execute for this task
    dag = dag
)

task_2 = SparkSubmitOperator(
    task_id = "run_spark", # task name
    application = "/mydir/spark_parsing_from_airflow.py", # select python file to be executed by spark
    dag = dag,
    application_args=[
        "--tahun", today_year # send argument (today_year) to spark as "tahun"
    ],
    yarn_queue = "default"
)

task_1 >> task_2