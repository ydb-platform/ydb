#!/usr/bin/env python
"""Constants related to the GCP auth method and/or secrets engine."""

DEFAULT_MOUNT_POINT = "gcp"
ALLOWED_ROLE_TYPES = ["iam", "gce"]
ALLOWED_SECRETS_TYPES = ["access_token", "service_account_key"]
SERVICE_ACCOUNT_KEY_ALGORITHMS = [
    "KEY_ALG_UNSPECIFIED",
    "KEY_ALG_RSA_1024",
    "KEY_ALG_RSA_2048",
]
SERVICE_ACCOUNT_KEY_TYPES = [
    "TYPE_UNSPECIFIED",
    "TYPE_PKCS12_FILE",
    "TYPE_GOOGLE_CREDENTIALS_FILE",
]
GCP_CERTS_ENDPOINT = "https://www.googleapis.com/oauth2/v3/certs"
