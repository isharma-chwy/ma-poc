from abstract_bi_client import AbstractBIGetter
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from secret_client import SecretClient
from typing import List


class SnowflakeBIGetter(AbstractBIGetter):
    """
    Concrete implementation of BIGetter for Snowflake.
    """

    def __init__(self, secret_client: SecretClient):
        self.secret_client = secret_client
        self.connection = None
        self.creds = None

    def __enter__(self):
        """
        Support for context manager entry.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Support for context manager exit.
        """
        self.disconnect()

    def connect(self):
        """
        Connect to the Snowflake database using credentials from Secrets Manager.
        """
        self.creds = self.secret_client.get_secret()
        # print(self.creds)
        key_string = self.creds["private_key"]
        # key_string = self.creds["extra"]["private_key_content"]
        p_key = serialization.load_pem_private_key(
            key_string.encode(), password=None, backend=default_backend()
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        self.connection = snowflake.connector.connect(
            # user=self.creds["login"],
            user=self.creds["user_name"],
            # account=self.creds["extra"]["account"],
            account=self.creds["account"],
            private_key=pkb,
            # database=self.creds["extra"]["database"],
            database="EDLDB_DEV",
            # warehouse=self.creds["extra"]["warehouse"],
            warehouse="MRCH_SYSTEMS_WH",
            autocommit=True,
        )

    def execute_query(self, query) -> List:
        """
        Execute a query on the Snowflake database.
        """
        if not self.connection:
            raise Exception("Not connected to Snowflake")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def disconnect(self):
        """
        Close the connection to the Snowflake database.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
