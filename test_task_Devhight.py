from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Укажите параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Настройки проекта GCP, названия набора данных и таблиц в BigQuery
GCP_PROJECT_ID = 'development-391510'
DATASET_NAME = 'store123'
TABLE_NAME = 'tbl_current'
HISTORICAL_TABLE_NAME = 'tbl_version'

# Пути к файлам на GCS и SFTP сервере
GCS_BUCKET = 'testbucket'
GCS_FILE_PATH = '/test.csv'
SFTP_REMOTE_PATH = '\file.txt'

# Функция выполнения вызова API 
def execute_api_trigger(**kwargs):
    # Ваш код вызова API
    pass

# DAG
dag = DAG(
    'devhight_test_task',
    default_args=default_args,
    description='DAG',
    schedule_interval=None,  
    catchup=False
)

# Определение операторов DAG
start_task = DummyOperator(task_id='start_task', dag=dag)

# Оператор для создания таблицы в BigQuery
create_bq_table = BigQueryCreateEmptyTableOperator(
    task_id='create_bq_table',
    dataset_id=DATASET_NAME,
    table_id=TABLE_NAME,
    project_id=GCP_PROJECT_ID,
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER'},
        {'name': 'name', 'type': 'STRING'},
        {'name': 'age', 'type': 'INTEGER'}
    ],
    dag=dag
)

# Оператор для выполнения скрипта BigQuery и записи результатов в историческую таблицу
write_to_historical_table = BigQueryExecuteQueryOperator(
    task_id='write_to_historical_table',
    sql=f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{DATASET_NAME}.{HISTORICAL_TABLE_NAME}`
        PARTITION BY DATE(_PARTITIONTIME) AS
        SELECT *
        FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    """,
    use_legacy_sql=False,
    dag=dag
)

# Оператор для выполнения скрипта BigQuery и записи результатов в GCS
export_bq_to_gcs = BigQueryToGCSOperator(
    task_id='export_bq_to_gcs',
    source_project_dataset_table=f"{GCP_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
    destination_cloud_storage_uris=[f"gs://{GCS_BUCKET}/output.csv"],
    dag=dag
)

# Оператор для создания бакета GCS
create_gcs_bucket = GCSCreateBucketOperator(
    task_id='create_gcs_bucket',
    bucket_name=GCS_BUCKET,
    dag=dag
)

# Оператор для создания пустого файла на GCS
create_empty_file = GCSToLocalFilesystemOperator(
    task_id='create_empty_file',
    bucket_name=GCS_BUCKET,
    object_name=GCS_FILE_PATH,
    local_filename='/path/on/airflow_server/empty_file.txt',
    dag=dag
)

# Оператор для копирования файла с GCS на SFTP сервер
copy_file_to_sftp = GCSToSFTPOperator(
    task_id='copy_file_to_sftp',
    source_bucket=GCS_BUCKET,
    source_object=GCS_FILE_PATH,
    destination_path=SFTP_REMOTE_PATH,
    dag=dag
)

# Оператор для ожидания выполнения API 
wait_for_api = PythonOperator(
    task_id='wait_for_api',
    python_callable=execute_api_trigger,
    provide_context=True,
    dag=dag
)

# Оператор для передачи данных с GCS на SFTP сервер
transfer_to_sftp = SFTPOperator(
    task_id='transfer_to_sftp',
    sftp_conn_id='ваш-ид-подключения-sftp',
    local_filepath='путь-к-локальному-файлу/output.csv',
    remote_filepath=SFTP_REMOTE_PATH,
    dag=dag
)

# Установка зависимостей между задачами
start_task >> create_bq_table
create_bq_table >> write_to_historical_table
write_to_historical_table >> export_bq_to_gcs
export_bq_to_gcs >> create_gcs_bucket
create_gcs_bucket >> create_empty_file
create_empty_file >> copy_file_to_sftp
copy_file_to_sftp >> wait_for_api
wait_for_api >> transfer_to_sftp

end_task = DummyOperator(task_id='end_task', dag=dag)
write_to_historical_table >> end_task
