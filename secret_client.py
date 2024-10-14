import boto3
import json
from dotenv import load_dotenv
import os

load_dotenv()


class SecretClient:
    """
    A client to interact with AWS Secrets Manager and retrieve secrets.
    """

    def __init__(self, region_name="us-east-1"):
        self.session = boto3.session.Session()
        self.secrets_client = self.session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )
        self.sts_client = self.session.client("sts")

    def get_secret(self):
        """
        Retrieve and return the secret as a dictionary.
        """
        get_secret_value_response = self.secrets_client.get_secret_value(
            SecretId=os.getenv("SNOWFLAKE_SECRET_NAME_NEBULA")
        )
        secret_string = get_secret_value_response["SecretString"]
        return json.loads(secret_string)
