"""
CDP Provider for Apache Airflow.
This package provides operators, hooks, and connections for interacting with CDP services.
"""

__version__ = "0.1.0" 

def get_provider_info():
    return {
        "package-name": "cdp-airflow-provider",
        "name": "cdp-airflow-provider",
        "description": "Airflow provider for managing CDP clusters.",
        "versions": ["1.0.0"],
        "integrations": [],
        "hook-class-names": [
            "cdp_provider.hooks.cdp.CDPConnection"
        ],
        "operator-class-names": [
            "cdp_provider.operators.datahub.CDPDataHubOperator"
        ],
        "sensor-class-names": [],
        "connection-types": [
            {
                "connection-type": "cdp",
                "hook-class-name": "cdp_provider.hooks.cdp.CDPConnection",
            }
        ],
    }