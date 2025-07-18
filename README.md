# CDP Airflow Provider

This repo provides Airflow provider for managing Cloudera CDP clusters. It can be utilised to orchestrate CDO cluster actions in your airflow dag.

## Features

CDP Airflow provider includes airflow operators and connection type to interact with Cloudera on Cloud (CDP-PC) clusters. 
It includes operator to manage 
(a) CDP datahub clusters
(b) COD database clusters 

## CDP Datahub Operator
Provides CDP datahub operator to utilise in Airflow DAGs.  It supports   
1. Start / Stop CDP DataHub cluster operations
2. Wait for cluster to be ready (optional)
3. Configurable operation timeouts

### Parameters
CDPDataHubOperator accepts following parameters

- `cluster_name` (required): Name of the CDP DataHub cluster
- `environment_name` (required): CDP environment name
- `operation` (required): The operation to perform ('start' or 'stop')
- `wait_for_cluster` (optional): Whether to wait for cluster to be ready before proceeding (default: True)
- `cluster_wait_timeout` (optional): Timeout in seconds for waiting cluster to be ready (default: 1800)


## COD Operator
Provides COD operator to utilise in Airflow DAGs.  It supports   
1. Start / Stop COD database cluster operations
2. Wait for cluster to be ready (optional)
3. Configurable operation timeouts

### Parameters
CDPDataHubOperator accepts following parameters

- `database_name` (required): Name of the COD database cluster
- `environment_name` (required): CDP environment name
- `operation` (required): The operation to perform ('start' or 'stop')
- `wait_for_cluster` (optional): Whether to wait for cluster to be ready before proceeding (default: True)
- `cluster_wait_timeout` (optional): Timeout in seconds for waiting cluster to be ready (default: 1800)



## CDP Connection
CDP Airflow operators utilises cdp cli and requires CDP access key to interact with CDP cluster.  To generate CDP access key, [see the steps provided](https://docs.cloudera.com/cdp-public-cloud/cloud/cli/topics/mc-cli-generating-an-api-access-key.html#pnavId1) from Cloudera documentation. 
Note : cdp-airflow operatior does not require creating .credentials file. Instead installation of cdp-airflow-provider provides "CDP" airflow connection type with ability to supply access-keyid and private-key.  Both are required for interaction with CDP cluster. 
1. New connection type - CDP
2. Accepts credentials - Cloudera access key id & private key. 
3. Define Region to use the correct Cloudera Control Plane region (optional)



## How to use 

1. Install CDP provider:
```bash
pip install cdp-airflow-provider @ git+https://github.com/nrladdha/CDP-AirflowProvider
```

2. Restart Airflow

3. Using Airflow UI, add new connection of connection type as "cdp" using Admin--> Connections--> Add connection menu.
Enter the following information :
Access key ID : Copy and paste the access key ID that generated in the Cloudera Management Console. 
Private key   : Copy and paste the private key that generated in the Cloudera Management Console.
Region        : Optionally define region name to use the correct Cloudera Control Plane region. For example: cdp_region = eu-1

[Click here to read steps to generate CDP access-key](https://docs.cloudera.com/cdp-public-cloud/cloud/cli/topics/mc-cli-generating-an-api-access-key.html#pnavId1)

3. It assume CDP datahub cluster is already created and its cluster-name and environment-name are required while implementing a Airflow dag (workflow). Tasks like start and/or stop can be orchestrated using CDPDataHubOperator. Please see example dag [example_cdp_datahub_dag.py](https://github.com/nrladdha/CDP-AirflowProvider/blob/master/cdp_provider/example_dags/example_cdp_datahub_dag.py), [example_cdp_cod_dag.py](https://github.com/nrladdha/CDP-AirflowProvider/blob/master/cdp_provider/example_dags/example_cdp_cod_dag.py) 

## Requirements

- Python 3.7+
- Apache Airflow 2.5.0+
- CDP CLI installed and configured
- Proper permissions to manage CDP DataHub clusters 

## 🤝 Contributing
We welcome contributions! Whether you're experimenting with the cdp-airflow provider plugin and identified bugs, fixing a bug, adding a feature, or improving documentation, all contributions are welcomed and helps drive this project. Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.
