import os
import time
from datetime import datetime
from typing import Dict, Optional, Union

# noinspection PyUnresolvedReferences
# jwt package depends on cryptography
import cryptography  # noqa: F401; pylint: disable=unused-import
import jwt
import requests
import six

from yandex.cloud.iam.v1.iam_token_service_pb2 import CreateIamTokenRequest

_MDS_ADDR = "169.254.169.254"
_MDS_URL = "http://{}/computeMetadata/v1/instance/service-accounts/default/token"
_MDS_HEADERS = {"Metadata-Flavor": "Google"}
_MDS_TIMEOUT = (1.0, 1.0)  # 1sec connect, 1sec read

YC_API_ENDPOINT = "api.cloud.yandex.net"


def set_up_yc_api_endpoint(endpoint: str) -> str:
    # pylint: disable-next=global-statement
    global YC_API_ENDPOINT
    YC_API_ENDPOINT = endpoint
    return YC_API_ENDPOINT


def __validate_service_account_key(sa_key: Optional[dict]) -> bool:
    if not isinstance(sa_key, dict):
        raise RuntimeError(f"Invalid Service Account Key: expecting dictionary, actually got {type(sa_key)}")

    obj_id = sa_key.get("id")
    sa_id = sa_key.get("service_account_id")
    private_key = sa_key.get("private_key")

    if not obj_id:
        raise RuntimeError("Invalid Service Account Key: missing key object id.")

    if not sa_id:
        raise RuntimeError("Invalid Service Account Key: missing service account id.")

    if not private_key:
        raise RuntimeError("Invalid Service Account Key: missing private key.")

    private_key_prefix = "-----BEGIN PRIVATE KEY-----"
    if not isinstance(private_key, six.string_types) or private_key_prefix not in private_key:
        error_message = (
            "Invalid Service Account Key: private key is in incorrect format."
            f"Should start with {private_key_prefix}.\n"
            "To obtain one you can use YC CLI: yc iam key create --output sa.json --service-account-id <id>"
        )
        raise RuntimeError(error_message)
    return True


class MetadataAuth:
    def __init__(self, metadata_addr: str):
        self.__metadata_addr = metadata_addr

    def url(self) -> str:
        return _MDS_URL.format(self.__metadata_addr)

    def get_token(self) -> str:
        r = requests.get(self.url(), headers=_MDS_HEADERS, timeout=_MDS_TIMEOUT)
        r.raise_for_status()
        response = r.json()
        return response["access_token"]


class TokenAuth:
    def __init__(self, token: str):
        self.__oauth_token = token

    def get_token_request(self) -> "CreateIamTokenRequest":
        return CreateIamTokenRequest(yandex_passport_oauth_token=self.__oauth_token)


class ServiceAccountAuth:
    __SECONDS_IN_HOUR = 60.0 * 60.0

    def __init__(self, sa_key: Dict[str, str], endpoint: Optional[str] = None):
        self.__sa_key = sa_key
        self._endpoint = endpoint if endpoint is not None else YC_API_ENDPOINT

    def get_token_request(self) -> "CreateIamTokenRequest":
        return CreateIamTokenRequest(jwt=self.__prepare_request(self._endpoint))

    def __prepare_request(self, endpoint: str) -> str:
        now = time.time()
        now_utc = datetime.utcfromtimestamp(now)
        exp_utc = datetime.utcfromtimestamp(now + self.__SECONDS_IN_HOUR)
        url = f"https://iam.{endpoint}/iam/v1/tokens"
        payload = {
            "iss": self.__sa_key["service_account_id"],
            "aud": url,
            "iat": now_utc,
            "exp": exp_utc,
        }

        headers = {
            "typ": "JWT",
            "alg": "PS256",
            "kid": self.__sa_key["id"],
        }

        return jwt.encode(payload, self.__sa_key["private_key"], algorithm="PS256", headers=headers)


class IamTokenAuth:
    def __init__(self, iam_token: str):
        self.__iam_token = iam_token

    def get_token(self) -> str:
        return self.__iam_token


def get_auth_token_requester(
    token: Optional[str] = None,
    service_account_key: Optional[dict] = None,
    iam_token: Optional[str] = None,
    metadata_addr: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> Union["MetadataAuth", "TokenAuth", "IamTokenAuth", "ServiceAccountAuth"]:
    if endpoint is None:
        endpoint = YC_API_ENDPOINT
    auth_methods = [("token", token), ("service_account_key", service_account_key), ("iam_token", iam_token)]
    auth_methods = [(auth_type, value) for auth_type, value in auth_methods if value is not None]

    if len(auth_methods) == 0:
        metadata_addr = metadata_addr if metadata_addr is not None else os.environ.get("YC_METADATA_ADDR", _MDS_ADDR)
        return MetadataAuth(metadata_addr)

    if len(auth_methods) > 1:
        raise RuntimeError(f"Conflicting API credentials properties are set: {[auth[0] for auth in auth_methods]}.")

    if token is not None:
        return TokenAuth(token=token)
    if iam_token is not None:
        return IamTokenAuth(iam_token)
    if service_account_key is not None and __validate_service_account_key(service_account_key):
        return ServiceAccountAuth(service_account_key, endpoint)

    raise RuntimeError("Unknown auth method")
