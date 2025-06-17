from __future__ import annotations
from typing import Optional, List, Dict, Any
import subprocess
import time
import warnings
import json
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from cdp_provider.hooks.cdp import CDPConnection


class CDPEnvironmentOperator(BaseOperator):
    """
    Airflow operator to manage CDP DataHub clusters.
    
    This operator can:
    1. Start CDP Environment
    2. Stop CDP Environment
    
    :param environment_name: CDP environment name
    :type environment_name: str
    :param wait_for_cluster: Whether to wait for cluster to be ready before proceeding
    :type wait_for_cluster: bool
    :param cluster_wait_timeout: Timeout in seconds for waiting cluster to be ready
    :type cluster_wait_timeout: int
    :param operation: The operation to perform ('start' or 'stop')
    :type operation: str
    :param cdp_conn_id: The connection ID to use for credentials
    :type cdp_conn_id: str
    """

    def __init__(
        self,
        environment_name: str,
        operation: str,
        wait_for_environment: bool = True,
        environment_wait_timeout: int = 1800,
        cdp_conn_id: str = 'cdp_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.environment_name = environment_name
        self.operation = operation.lower()
        self.wait_for_cluster = wait_for_environment
        self.cluster_wait_timeout = environment_wait_timeout
        self.cdp_conn_id = cdp_conn_id
        self.start_result = None
        self.stop_result = None
        
        if self.operation not in ['start', 'stop']:
            raise ValueError("Environment Operation must be either 'start' or 'stop'")

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the operator's tasks.
        """
        try:
            # Get credentials 
            self.log.info(f"About to fetch credentials")
            self._use_cdp_credentials()

            # Set environment variables for CDP CLI
            self.log.info(f"Input:environment_name: {self.environment_name}")
            self.log.info(f"Input:operation: {self.operation}")
            self.log.info(f"Input:wait_for_environment: {self.wait_for_cluster}")
            self.log.info(f"Input:environment_wait_timeout: {self.cluster_wait_timeout}")
            self.log.info(f"Input:cdp_conn_id: {self.cdp_conn_id}")

            if self.operation == 'start':
                self.log.info(f"Starting CDP Environment: {self.environment_name}")
                self._start_cluster()
                if self.wait_for_cluster:
                    self._wait_for_cluster_ready()
            elif self.operation == 'stop':
                self.log.info(f"Stopping CDP Environment: {self.environment_name}")
                self._stop_cluster()
                if self.wait_for_cluster:
                    self._wait_for_cluster_ready()
            else:
                self.log.info(f"CDP Environment  - Invalid operation supplied : {self.operation}")
            
        except Exception as e:
            self.log.error(f"Error in CDPEnvironmentOperator: {str(e)}")
            raise AirflowException(f"CDPEnvironmentOperator failed: {str(e)}")

    def _start_cluster(self) -> None:
        """Start the CDP DataHub cluster."""
        cmd = [
            "cdp", "environments", "start-environment",
            "--environment-name", self.environment_name,    
        ]
        self.start_result = self._run_command(cmd, capture_output=True)

    def _stop_cluster(self) -> None:
        """Stop the CDP DataHub cluster."""
        cmd = [
            "cdp", "environments", "stop-environment",
            "--environment-name", self.environment_name,  
        ]
        self.stop_result = self._run_command(cmd, capture_output=True)

    def _wait_for_cluster_ready(self) -> None:
        """Wait for the cluster to be ready."""
        start_time = time.time()
        while time.time() - start_time < self.cluster_wait_timeout:
            cmd = [
                "cdp", "environments", "describe-environment",
                "--environment-name", self.environment_name, 
            ]
            wait_for_result = self._run_command(cmd, capture_output=True)

            # Parse stdout as JSON
            output_json = json.loads(wait_for_result.stdout)
            operation_status = output_json["environment"]["status"]
           
            if self.operation == 'start':
                if "AVAILABLE" in operation_status:
                    self.log.info("Environment is running")
                    return
            elif self.operation == 'stop':
                if "STOPPED" in operation_status:
                    self.log.info("Environment is stopped")
                    return
            
            self.log.info("Waiting for environment operation to finish...")
            time.sleep(30)
        
        raise AirflowException(f"Timeout occurred: {self.cluster_wait_timeout}: Environment operation could not finish")

    def _run_command(self, cmd: List[str], capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a shell command and return the result."""
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=capture_output,
                text=True
            )
            return result
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"Command failed: {e.stderr}") 

    def _use_cdp_credentials(self) -> None:
        # Get credentials from CDP connection
        cdp_conn = CDPConnection(conn_id=self.cdp_conn_id)
        access_key_id = cdp_conn.get_access_key_id()
        private_key = cdp_conn.get_private_key()
        region = cdp_conn.get_region()

        if not access_key_id or not private_key:
            raise AirflowException("CDP Access Key ID and Private Key must be provided in the CDP connection")


        import os
        os.environ['CDP_ACCESS_KEY_ID'] = access_key_id
        os.environ['CDP_PRIVATE_KEY'] = private_key

        if region is not None and region.strip() != "":
            os.environ['CDP_REGION'] = region

        self.log.info(f"CDP credentials are fetched.")
