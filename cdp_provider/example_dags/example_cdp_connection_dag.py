from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from cdp_provider.operators.datahub import CDPDataHubOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_cdp_connection_dag',
    default_args=default_args,
    description='Example DAG using CDP connection',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    start_cluster = CDPDataHubOperator(
        task_id='start_cluster',
        cluster_name='nl-load-iceberg',
        environment_name='se-sandboxx-aws',
        operation='start',
        wait_for_cluster=True,
        cluster_wait_timeout=1800,
        cdp_conn_id='cdp_default'  # This should match the connection ID you create in Airflow UI
    )

    stop_cluster = CDPDataHubOperator(
        task_id='stop_cluster',
        cluster_name='nl-load-iceberg',
        environment_name='se-sandboxx-aws',
        operation='stop',
        wait_for_cluster=True,
        cluster_wait_timeout=1800,
        cdp_conn_id='cdp_default'  # Same connection ID as above
    )

    start_cluster >> stop_cluster 