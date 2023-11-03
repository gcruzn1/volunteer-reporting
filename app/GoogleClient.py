import os
from google.oauth2.service_account import Credentials


class GoogleServiceClient:
    def __init__(self) -> None:
        self.creds = {
            "type": "service_account",
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("CLIENT_CERT_URL"),
        }

        if missing_creds := [var for var, val in self.creds.items() if val is None]:
            raise KeyError(
                f"Missing Environment variable(s): {','.join(missing_creds)}"
            )

    @property
    def CredsServiceAcct(self) -> Credentials:
        return Credentials.from_service_account_info(self.creds)
