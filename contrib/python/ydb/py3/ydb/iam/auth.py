# -*- coding: utf-8 -*-
from ydb import credentials, tracing
import grpc
import time
import abc
from datetime import datetime
import json
import os

try:
    import jwt
except ImportError:
    jwt = None

try:
    from yandex.cloud.iam.v1 import iam_token_service_pb2_grpc
    from yandex.cloud.iam.v1 import iam_token_service_pb2
except ImportError:
    try:
        # This attempt is to enable the IAM auth inside the YDB repository on GitHub
        from ydb.public.api.client.yc_public.iam import iam_token_service_pb2_grpc
        from ydb.public.api.client.yc_public.iam import iam_token_service_pb2
    except ImportError:
        try:
            # This attempt is to enable the IAM auth inside the YDB repository on Arcadia
            from contrib.ydb.public.api.client.yc_public.iam import iam_token_service_pb2_grpc
            from contrib.ydb.public.api.client.yc_public.iam import iam_token_service_pb2
        except ImportError:
            iam_token_service_pb2_grpc = None
            iam_token_service_pb2 = None

try:
    import requests
except ImportError:
    requests = None


DEFAULT_METADATA_URL = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
YANDEX_CLOUD_IAM_TOKEN_SERVICE_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
YANDEX_CLOUD_JWT_ALGORITHM = "PS256"


def get_jwt(account_id, access_key_id, private_key, jwt_expiration_timeout, algorithm, token_service_url, subject=None):
    assert jwt is not None, "Install pyjwt library to use jwt tokens"
    now = time.time()
    now_utc = datetime.utcfromtimestamp(now)
    exp_utc = datetime.utcfromtimestamp(now + jwt_expiration_timeout)
    payload = {
        "iss": account_id,
        "aud": token_service_url,
        "iat": now_utc,
        "exp": exp_utc,
    }
    if subject is not None:
        payload["sub"] = subject
    return jwt.encode(
        key=private_key,
        algorithm=algorithm,
        headers={"typ": "JWT", "alg": algorithm, "kid": access_key_id},
        payload=payload,
    )


class TokenServiceCredentials(credentials.AbstractExpiringTokenCredentials):
    def __init__(self, iam_endpoint=None, iam_channel_credentials=None, tracer=None):
        super(TokenServiceCredentials, self).__init__(tracer)
        assert iam_token_service_pb2_grpc is not None, 'run pip install "ydb[yc]" to use service account credentials'
        self._get_token_request_timeout = 10
        self._iam_token_service_pb2 = iam_token_service_pb2
        self._iam_token_service_pb2_grpc = iam_token_service_pb2_grpc
        self._iam_endpoint = "iam.api.cloud.yandex.net:443" if iam_endpoint is None else iam_endpoint
        self._iam_channel_credentials = {} if iam_channel_credentials is None else iam_channel_credentials

    def _channel_factory(self):
        return grpc.secure_channel(
            self._iam_endpoint,
            grpc.ssl_channel_credentials(**self._iam_channel_credentials),
        )

    @abc.abstractmethod
    def _get_token_request(self):
        pass

    @tracing.with_trace()
    def _make_token_request(self):
        with self._channel_factory() as channel:
            tracing.trace(self.tracer, {"iam_token.from_service": True})
            stub = self._iam_token_service_pb2_grpc.IamTokenServiceStub(channel)
            response = stub.Create(self._get_token_request(), timeout=self._get_token_request_timeout)
            expires_in = max(0, response.expires_at.seconds - int(time.time()))
            return {"access_token": response.iam_token, "expires_in": expires_in}


class BaseJWTCredentials(abc.ABC):
    def __init__(self, account_id, access_key_id, private_key, algorithm, token_service_url, subject=None):
        self._account_id = account_id
        self._jwt_expiration_timeout = 60.0 * 60
        self._token_expiration_timeout = 120
        self._access_key_id = access_key_id
        self._private_key = private_key
        self._algorithm = algorithm
        self._token_service_url = token_service_url
        self._subject = subject

    def set_token_expiration_timeout(self, value):
        self._token_expiration_timeout = value
        return self

    @classmethod
    def from_file(cls, key_file, iam_endpoint=None, iam_channel_credentials=None):
        with open(os.path.expanduser(key_file), "r") as r:
            key = r.read()

        return cls.from_content(key, iam_endpoint=iam_endpoint, iam_channel_credentials=iam_channel_credentials)

    @classmethod
    def from_content(cls, key, iam_endpoint=None, iam_channel_credentials=None):
        key_json = json.loads(key)
        account_id = key_json.get("service_account_id", None)
        if account_id is None:
            account_id = key_json.get("user_account_id", None)
        return cls(
            account_id,
            key_json["id"],
            key_json["private_key"],
            iam_endpoint=iam_endpoint,
            iam_channel_credentials=iam_channel_credentials,
        )

    def _get_jwt(self):
        return get_jwt(
            self._account_id,
            self._access_key_id,
            self._private_key,
            self._jwt_expiration_timeout,
            self._algorithm,
            self._token_service_url,
            self._subject,
        )


class JWTIamCredentials(TokenServiceCredentials, BaseJWTCredentials):
    def __init__(
        self,
        account_id,
        access_key_id,
        private_key,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        TokenServiceCredentials.__init__(self, iam_endpoint, iam_channel_credentials)
        BaseJWTCredentials.__init__(
            self, account_id, access_key_id, private_key, YANDEX_CLOUD_JWT_ALGORITHM, YANDEX_CLOUD_IAM_TOKEN_SERVICE_URL
        )

    def _get_token_request(self):
        return self._iam_token_service_pb2.CreateIamTokenRequest(jwt=self._get_jwt())


class YandexPassportOAuthIamCredentials(TokenServiceCredentials):
    def __init__(
        self,
        yandex_passport_oauth_token,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        self._yandex_passport_oauth_token = yandex_passport_oauth_token
        super(YandexPassportOAuthIamCredentials, self).__init__(iam_endpoint, iam_channel_credentials)

    def _get_token_request(self):
        return iam_token_service_pb2.CreateIamTokenRequest(
            yandex_passport_oauth_token=self._yandex_passport_oauth_token
        )


class MetadataUrlCredentials(credentials.AbstractExpiringTokenCredentials):
    def __init__(self, metadata_url=None, tracer=None):
        """
        :param metadata_url: Metadata url
        :param ydb.Tracer tracer: ydb tracer
        """
        super(MetadataUrlCredentials, self).__init__(tracer)
        assert requests is not None, "Install requests library to use metadata credentials provider"
        self.extra_error_message = (
            "Check that metadata service configured properly since we failed to fetch it from metadata_url."
        )
        self._metadata_url = DEFAULT_METADATA_URL if metadata_url is None else metadata_url
        self._tp.submit(self._refresh)

    @tracing.with_trace()
    def _make_token_request(self):
        response = requests.get(self._metadata_url, headers={"Metadata-Flavor": "Google"}, timeout=3)
        response.raise_for_status()
        return json.loads(response.text)


class ServiceAccountCredentials(JWTIamCredentials):
    def __init__(
        self,
        service_account_id,
        access_key_id,
        private_key,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        super(ServiceAccountCredentials, self).__init__(
            service_account_id,
            access_key_id,
            private_key,
            iam_endpoint,
            iam_channel_credentials,
        )
