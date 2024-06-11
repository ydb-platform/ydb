from typing import Dict, Optional

from yandex.cloud.iam.v1.iam_token_service_pb2_grpc import IamTokenServiceStub
from yandexcloud._auth_fabric import (
    YC_API_ENDPOINT,
    IamTokenAuth,
    MetadataAuth,
    get_auth_token_requester,
)
from yandexcloud._sdk import SDK


def get_auth_token(
    token: Optional[str] = None,
    service_account_key: Optional[Dict[str, str]] = None,
    iam_token: Optional[str] = None,
    metadata_addr: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> str:
    if endpoint is None:
        endpoint = YC_API_ENDPOINT
    requester = get_auth_token_requester(
        token=token,
        service_account_key=service_account_key,
        iam_token=iam_token,
        metadata_addr=metadata_addr,
        endpoint=endpoint,
    )
    if isinstance(requester, (MetadataAuth, IamTokenAuth)):
        return requester.get_token()

    sdk = SDK()
    client = sdk.client(IamTokenServiceStub)
    return client.Create(requester.get_token_request()).iam_token
