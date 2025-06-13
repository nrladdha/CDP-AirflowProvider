from airflow.plugins_manager import AirflowPlugin
from hooks.cdp import *
from operators.datahub import *
                    
class cdp_provider(AirflowPlugin):
 """
    This is a plugin class required by the Amazon Managed Workflows for Apache Airflow (AWS MWAA). 
    """
                    
    name = 'cdp-airflow-provider'
                    
    hooks = [CDPConnection]
    operators = [CDPDataHubOperator]
    sensors = []