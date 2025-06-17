from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.utils.session import create_session
from airflow.utils.db import provide_session
from typing import Optional

class CDPConnection(BaseHook):
    """
    CDP connection type for handling cdp access key id, private key credentials and optional field for region.
    """
    conn_name_attr = 'cdp_conn_id'
    default_conn_name = 'cdp_default'
    conn_type = 'cdp'
    hook_name = 'CDP'

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns custom widgets to be added for the hook to handle extra fields."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "access_key_id": StringField(
                lazy_gettext('Access Key ID'),
                widget=BS3TextFieldWidget()
             ),
            "private_key": StringField(
                lazy_gettext('Private Key'),
                widget=BS3TextFieldWidget()
             ),
            "region": StringField(
                lazy_gettext('Region (optional)'),
                widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behavior."""
        return {
            "hidden_fields": ['port', 'schema', 'login', 'password', 'host', 'extra','description'],
            "relabeling": {},
            "placeholders": {
                'access_key_id': 'Enter your access key id (Required)',
                'private_key': 'Enter your private key (Required) ',
                'region': 'Enter your CDP region (Optional, e.g., us-west-1)',
            },
            "required_fields": ['access_key_id', 'private_key'],
        }

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = conn_id
        self.conn = None

    def get_conn(self) -> Connection:
        """Returns the connection object."""
        if not self.conn:
            self.conn = self.get_connection(self.conn_id)
        return self.conn

    def get_access_key_id(self) -> str:
        """Returns the access ID from the connection."""
        return self.get_conn().extra_dejson.get('access_key_id')

    def get_private_key(self) -> str:
        """Returns the access key from the connection."""
        return self.get_conn().extra_dejson.get('private_key') 

    def get_region(self) -> str:
        """Returns the region from the connection."""
        return self.get_conn().extra_dejson.get('region') 