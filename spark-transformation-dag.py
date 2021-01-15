from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 14),
    'email': ['test@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

dag = DAG(dag_id = 'spark_transformation_dag', default_args = default_args)

app_folder = Variable.get("APP_FOLDER")
spark_transformation = SparkSubmitOperator(task_id = 'spark_transformation',
                                           conn_id = 'spark_local',
                                           application = '{app_folder}/spark-transformation.py',
                                           packages = 'io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1',
                                           executor_cores = 1,
                                           executor_memory = '1g',
                                           driver_memory = '1g',
                                           name = 'spark_transformation',
                                           dag = dag,
                                           )

spark_transformation

if __name__ == "__main__":
    dag.cli()