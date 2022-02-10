import grpc.aio
import time

import abc
import asyncio
import logging
import six
from ydb import issues, credentials
from ydb.iam import auth

logger = logging.getLogger(__name__)

try:
    from yandex.cloud.iam.v1 import iam_token_service_pb2_grpc
    from yandex.cloud.iam.v1 import iam_token_service_pb2
    import jwt
except ImportError:
    jwt = None
    iam_token_service_pb2_grpc = None
    iam_token_service_pb2 = None

try:
    import aiohttp
except ImportError:
    aiohttp = None


class _OneToManyValue(object):
    def __init__(self):
        self._value = None
        self._condition = asyncio.Condition()

    async def consume(self, timeout=3):
        async with self._condition:
            if self._value is None:
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=timeout)
                except Exception:
                    return self._value
            return self._value

    async def update(self, n_value):
        async with self._condition:
            prev_value = self._value
            self._value = n_value
            if prev_value is None:
                self._condition.notify_all()


class _AtMostOneExecution(object):
    def __init__(self):
        self._can_schedule = True
        self._lock = asyncio.Lock()  # Lock to guarantee only one execution

    async def _wrapped_execution(self, callback):
        await self._lock.acquire()
        try:
            res = callback()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            pass

        finally:
            self._lock.release()
            self._can_schedule = True

    def submit(self, callback):
        if self._can_schedule:
            self._can_schedule = False
            asyncio.ensure_future(self._wrapped_execution(callback))


@six.add_metaclass(abc.ABCMeta)
class IamTokenCredentials(auth.IamTokenCredentials):
    def __init__(self):
        super(IamTokenCredentials, self).__init__()
        self._tp = _AtMostOneExecution()
        self._iam_token = _OneToManyValue()

    @abc.abstractmethod
    async def _get_iam_token(self):
        pass

    async def _refresh(self):
        current_time = time.time()
        self._log_refresh_start(current_time)

        try:
            auth_metadata = await self._get_iam_token()
            await self._iam_token.update(auth_metadata["access_token"])
            self.update_expiration_info(auth_metadata)
            self.logger.info(
                "Token refresh successful. current_time %s, refresh_in %s",
                current_time,
                self._refresh_in,
            )

        except (KeyboardInterrupt, SystemExit):
            return

        except Exception as e:
            self.last_error = str(e)
            await asyncio.sleep(1)
            self._tp.submit(self._refresh)

    async def iam_token(self):
        current_time = time.time()
        if current_time > self._refresh_in:
            self._tp.submit(self._refresh)

        iam_token = await self._iam_token.consume(timeout=3)
        if iam_token is None:
            if self.last_error is None:
                raise issues.ConnectionError(
                    "%s: timeout occurred while waiting for token.\n%s"
                    % self.__class__.__name__,
                    self.extra_error_message,
                )
            raise issues.ConnectionError(
                "%s: %s.\n%s"
                % (self.__class__.__name__, self.last_error, self.extra_error_message)
            )
        return iam_token

    async def auth_metadata(self):
        return [(credentials.YDB_AUTH_TICKET_HEADER, await self.iam_token())]


@six.add_metaclass(abc.ABCMeta)
class TokenServiceCredentials(IamTokenCredentials):
    def __init__(self, iam_endpoint=None, iam_channel_credentials=None):
        super(TokenServiceCredentials, self).__init__()
        self._iam_endpoint = (
            "iam.api.cloud.yandex.net:443" if iam_endpoint is None else iam_endpoint
        )
        self._iam_channel_credentials = (
            {} if iam_channel_credentials is None else iam_channel_credentials
        )
        self._get_token_request_timeout = 10
        if (
            iam_token_service_pb2_grpc is None
            or jwt is None
            or iam_token_service_pb2 is None
        ):
            raise RuntimeError(
                "Install jwt & yandex python cloud library to use service account credentials provider"
            )

    def _channel_factory(self):
        return grpc.aio.secure_channel(
            self._iam_endpoint,
            grpc.ssl_channel_credentials(**self._iam_channel_credentials),
        )

    @abc.abstractmethod
    def _get_token_request(self):
        pass

    async def _get_iam_token(self):
        async with self._channel_factory() as channel:
            stub = iam_token_service_pb2_grpc.IamTokenServiceStub(channel)
            response = await stub.Create(
                self._get_token_request(), timeout=self._get_token_request_timeout
            )
            self.logger.debug(str(response))
            expires_in = max(0, response.expires_at.seconds - int(time.time()))
            return {"access_token": response.iam_token, "expires_in": expires_in}


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
        auth.BaseJWTCredentials.__init__(self, account_id, access_key_id, private_key)

    def _get_token_request(self):
        return iam_token_service_pb2.CreateIamTokenRequest(
            jwt=auth.get_jwt(
                self._account_id,
                self._access_key_id,
                self._private_key,
                self._jwt_expiration_timeout,
            )
        )


class YandexPassportOAuthIamCredentials(TokenServiceCredentials):
    def __init__(
        self,
        yandex_passport_oauth_token,
        iam_endpoint=None,
        iam_channel_credentials=None,
    ):
        self._yandex_passport_oauth_token = yandex_passport_oauth_token
        super(YandexPassportOAuthIamCredentials, self).__init__(
            iam_endpoint, iam_channel_credentials
        )

    def _get_token_request(self):
        return iam_token_service_pb2.CreateIamTokenRequest(
            yandex_passport_oauth_token=self._yandex_passport_oauth_token
        )


class MetadataUrlCredentials(IamTokenCredentials):
    def __init__(self, metadata_url=None):
        super(MetadataUrlCredentials, self).__init__()
        if aiohttp is None:
            raise RuntimeError(
                "Install aiohttp library to use metadata credentials provider"
            )
        self._metadata_url = (
            auth.DEFAULT_METADATA_URL if metadata_url is None else metadata_url
        )
        self._tp.submit(self._refresh)
        self.extra_error_message = "Check that metadata service configured properly and application deployed in VM or function at Yandex.Cloud."

    async def _get_iam_token(self):
        timeout = aiohttp.ClientTimeout(total=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                self._metadata_url, headers={"Metadata-Flavor": "Google"}
            ) as response:
                if not response.ok:
                    self.logger.error(
                        "Error while getting token from metadata: %s" % response.text()
                    )
                response.raise_for_status()
                return await response.json()


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
