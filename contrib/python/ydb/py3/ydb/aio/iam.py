import grpc.aio
import time

import abc
import logging
from ydb.iam import auth
from .credentials import AbstractExpiringTokenCredentials
from ydb import issues

logger = logging.getLogger(__name__)

try:
    import jwt
except ImportError:
    jwt = None

try:
    from yandex.cloud.iam.v1 import iam_token_service_pb2_grpc
    from yandex.cloud.iam.v1 import iam_token_service_pb2
except ImportError:
    iam_token_service_pb2_grpc = None
    iam_token_service_pb2 = None

try:
    import aiohttp
except ImportError:
    aiohttp = None


class TokenServiceCredentials(AbstractExpiringTokenCredentials):
    def __init__(self, iam_endpoint=None, iam_channel_credentials=None):
        super(TokenServiceCredentials, self).__init__()
        assert iam_token_service_pb2_grpc is not None, 'run pip install "ydb[yc]" to use service account credentials'
        self._get_token_request_timeout = 10
        self._iam_endpoint = "iam.api.cloud.yandex.net:443" if iam_endpoint is None else iam_endpoint
        self._iam_channel_credentials = {} if iam_channel_credentials is None else iam_channel_credentials

    def _channel_factory(self):
        return grpc.aio.secure_channel(
            self._iam_endpoint,
            grpc.ssl_channel_credentials(**self._iam_channel_credentials),
        )

    @abc.abstractmethod
    def _get_token_request(self):
        pass

    async def _make_token_request(self):
        async with self._channel_factory() as channel:
            stub = iam_token_service_pb2_grpc.IamTokenServiceStub(channel)
            response = await stub.Create(self._get_token_request(), timeout=self._get_token_request_timeout)
            self.logger.debug(str(response))
            expires_in = max(0, response.expires_at.seconds - int(time.time()))
            return {"access_token": response.iam_token, "expires_in": expires_in}


# IamTokenCredentials need for backward compatibility
# Deprecated
IamTokenCredentials = TokenServiceCredentials


class OAuth2JwtTokenExchangeCredentials(AbstractExpiringTokenCredentials, auth.BaseJWTCredentials):
    def __init__(
        self,
        token_exchange_url,
        account_id,
        access_key_id,
        private_key,
        algorithm,
        token_service_url,
        subject=None,
    ):
        super(OAuth2JwtTokenExchangeCredentials, self).__init__()
        auth.BaseJWTCredentials.__init__(
            self, account_id, access_key_id, private_key, algorithm, token_service_url, subject
        )
        assert aiohttp is not None, "Install aiohttp library to use OAuth 2.0 token exchange credentials provider"
        self._token_exchange_url = token_exchange_url

    async def _make_token_request(self):
        params = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": self._get_jwt(),
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        timeout = aiohttp.ClientTimeout(total=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self._token_exchange_url, data=params, headers=headers) as response:
                if response.status == 403:
                    raise issues.Unauthenticated(await response.text())
                if response.status >= 500:
                    raise issues.Unavailable(await response.text())
                if response.status >= 400:
                    raise issues.BadRequest(await response.text())
                if response.status != 200:
                    raise issues.Error(await response.text())

                response_json = await response.json()
                access_token = response_json["access_token"]
                expires_in = response_json["expires_in"]
                return {"access_token": access_token, "expires_in": expires_in}


class JWTIamCredentials(TokenServiceCredentials, auth.BaseJWTCredentials):
    def __init__(
        self,
        account_id,
        access_key_id,
        private_key,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        TokenServiceCredentials.__init__(self, iam_endpoint, iam_channel_credentials)
        auth.BaseJWTCredentials.__init__(
            self,
            account_id,
            access_key_id,
            private_key,
            auth.YANDEX_CLOUD_JWT_ALGORITHM,
            auth.YANDEX_CLOUD_IAM_TOKEN_SERVICE_URL,
        )

    def _get_token_request(self):
        return iam_token_service_pb2.CreateIamTokenRequest(jwt=self._get_jwt())


class NebiusJWTIamCredentials(OAuth2JwtTokenExchangeCredentials):
    def __init__(
        self,
        account_id,
        access_key_id,
        private_key,
        token_exchange_url=None,
    ):
        url = token_exchange_url
        if url is None:
            url = auth.NEBIUS_CLOUD_IAM_TOKEN_EXCHANGE_URL
        OAuth2JwtTokenExchangeCredentials.__init__(
            self,
            url,
            account_id,
            access_key_id,
            private_key,
            auth.NEBIUS_CLOUD_JWT_ALGORITHM,
            auth.NEBIUS_CLOUD_IAM_TOKEN_SERVICE_AUDIENCE,
            account_id,
        )


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


class MetadataUrlCredentials(AbstractExpiringTokenCredentials):
    def __init__(self, metadata_url=None):
        super(MetadataUrlCredentials, self).__init__()
        assert aiohttp is not None, "Install aiohttp library to use metadata credentials provider"
        self._metadata_url = auth.DEFAULT_METADATA_URL if metadata_url is None else metadata_url
        self._tp.submit(self._refresh)
        self.extra_error_message = "Check that metadata service configured properly and application deployed in VM or function at Yandex.Cloud."

    async def _make_token_request(self):
        timeout = aiohttp.ClientTimeout(total=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self._metadata_url, headers={"Metadata-Flavor": "Google"}) as response:
                if not response.ok:
                    self.logger.error("Error while getting token from metadata: %s" % await response.text())
                response.raise_for_status()
                # response from default metadata credentials provider
                # contains text/plain content type.
                return await response.json(content_type=None)


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


class NebiusServiceAccountCredentials(NebiusJWTIamCredentials):
    def __init__(
        self,
        service_account_id,
        access_key_id,
        private_key,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        super(NebiusServiceAccountCredentials, self).__init__(
            service_account_id,
            access_key_id,
            private_key,
            iam_endpoint,
        )
