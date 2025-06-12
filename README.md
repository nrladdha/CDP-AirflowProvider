# CDP DataHub Airflow Operator

This package provides an Airflow operator for managing CDP DataHub clusters.

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure you have the CDP CLI installed and configured with your credentials.

## Usage

Here's an example of how to use the operator in your Airflow DAG:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from cdp_datahub_operator import CDPDataHubOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cdp_datahub_operations',
    default_args=default_args,
    description='Manage CDP DataHub cluster',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    start_cluster = CDPDataHubOperator(
        task_id='start_cluster',
        cluster_name='my-cluster',
        environment_name='my-environment',
        operation='start',
        wait_for_cluster=True,
        cluster_wait_timeout=1800,  # 30 minutes
    )

    stop_cluster = CDPDataHubOperator(
        task_id='stop_cluster',
        cluster_name='my-cluster',
        environment_name='my-environment',
        operation='stop',
    )

    start_cluster >> stop_cluster
```

## Parameters

- `cluster_name` (required): Name of the CDP DataHub cluster
- `environment_name` (required): CDP environment name
- `operation` (optional): The operation to perform ('start' or 'stop', default: 'start')
- `wait_for_cluster` (optional): Whether to wait for cluster to be ready before proceeding (default: True)
- `cluster_wait_timeout` (optional): Timeout in seconds for waiting cluster to be ready (default: 1800)

## Features

1. Start / Stop CDP DataHub cluster
2. Wait for cluster to be ready (optional)
3. Configurable timeouts

## Requirements

- Python 3.7+
- Apache Airflow 2.5.0+
- CDP CLI installed and configured
- Proper permissions to manage CDP DataHub clusters 