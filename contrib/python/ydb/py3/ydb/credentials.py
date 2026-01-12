# -*- coding: utf-8 -*-
import abc
import typing

from . import tracing, issues, connection
from . import settings as settings_impl
from concurrent import futures
import threading
import logging
import time

try:
    from ydb.public.api.protos import ydb_auth_pb2
    from ydb.public.api.grpc import ydb_auth_v1_pb2_grpc
except ImportError:
    from contrib.ydb.public.api.protos import ydb_auth_pb2
    from contrib.ydb.public.api.grpc import ydb_auth_v1_pb2_grpc


YDB_AUTH_TICKET_HEADER = "x-ydb-auth-ticket"
logger = logging.getLogger(__name__)


class AtMostOneExecution(object):
    def __init__(self):
        self._can_schedule = True
        self._lock = threading.Lock()
        self._tp = futures.ThreadPoolExecutor(1)

    def wrapped_execution(self, callback):
        try:
            callback()
        except Exception:
            pass

        finally:
            self.cleanup()

    def submit(self, callback):
        with self._lock:
            if self._can_schedule:
                self._tp.submit(self.wrapped_execution, callback)
                self._can_schedule = False

    def cleanup(self):
        with self._lock:
            self._can_schedule = True


class AbstractCredentials(abc.ABC):
    """
    An abstract class that provides auth metadata
    """


class Credentials(abc.ABC):
    def __init__(self, tracer=None):
        self.tracer = tracer if tracer is not None else tracing.Tracer(None)

    @abc.abstractmethod
    def auth_metadata(self):
        """
        :return: An iterable with auth metadata
        """
        pass

    def get_auth_token(self) -> str:
        for header, token in self.auth_metadata():
            if header == YDB_AUTH_TICKET_HEADER:
                return token
        return ""

    def _update_driver_config(self, driver_config):
        pass


class AbstractExpiringTokenCredentials(Credentials):
    def __init__(self, tracer=None):
        super(AbstractExpiringTokenCredentials, self).__init__(tracer)
        self._refresh_in = 0
        self._expires_in = 0
        self._cached_token = None
        self._token_lock = threading.Lock()
        self.logger = logger.getChild(self.__class__.__name__)
        self.last_error = None
        self.extra_error_message = ""
        self._hour = 60 * 60
        self._tp = AtMostOneExecution()
        self._time_shift_protection_seconds = 30

    @abc.abstractmethod
    def _make_token_request(self):
        pass

    def _is_token_valid(self):
        return self._cached_token is not None and time.time() <= self._expires_in

    def _should_refresh(self):
        return time.time() >= self._refresh_in

    def _update_token_info(self, token_response, current_time):
        self._refresh_in = current_time + min(self._hour / 2, token_response["expires_in"] / 10)
        self._expires_in = current_time + token_response["expires_in"] - self._time_shift_protection_seconds
        self._cached_token = token_response["access_token"]

    def _refresh_token(self, should_raise=False):
        current_time = time.time()

        try:
            self.logger.debug("Refreshing token, current_time: %s, expires_in: %s", current_time, self._expires_in)

            token_response = self._make_token_request()
            self._update_token_info(token_response, current_time)

            self.logger.info("Token refreshed successfully, expires_in: %s", self._expires_in)
            self.last_error = None

        except Exception as e:
            self.last_error = str(e)
            self.logger.error("Failed to refresh token: %s", e)
            if should_raise:
                raise issues.ConnectionError(
                    "%s: %s.\n%s" % (self.__class__.__name__, self.last_error, self.extra_error_message)
                )

    @property
    @tracing.with_trace()
    def token(self):
        if self._is_token_valid():
            if self._should_refresh():
                tracing.trace(self.tracer, {"refresh": True})
                self._tp.submit(self._refresh_token)

            tracing.trace(self.tracer, {"consumed": True})
            return self._cached_token

        with self._token_lock:
            if self._is_token_valid():
                tracing.trace(self.tracer, {"consumed": True})
                return self._cached_token

            tracing.trace(self.tracer, {"refresh": True})
            self._refresh_token(should_raise=True)

        tracing.trace(self.tracer, {"consumed": True})
        return self._cached_token

    def auth_metadata(self):
        return [(YDB_AUTH_TICKET_HEADER, self.token)]


def _wrap_static_credentials_response(rpc_state, response):
    issues._process_response(response.operation)
    result = ydb_auth_pb2.LoginResult()
    response.operation.result.Unpack(result)
    return result


class StaticCredentials(AbstractExpiringTokenCredentials):
    def __init__(self, driver_config, user, password="", tracer=None):
        super(StaticCredentials, self).__init__(tracer)

        from .driver import DriverConfig

        if driver_config is not None:
            self.driver_config = DriverConfig(
                endpoint=driver_config.endpoint,
                database=driver_config.database,
                root_certificates=driver_config.root_certificates,
            )
        self.user = user
        self.password = password
        self.request_timeout = 10

    @classmethod
    def from_user_password(cls, user: str, password: str, tracer=None):
        return cls(None, user, password, tracer)

    def _make_token_request(self):
        conn = connection.Connection.ready_factory(self.driver_config.endpoint, self.driver_config)
        assert conn is not None, "Failed to establish connection in to %s" % self.driver_config.endpoint
        try:
            result = conn(
                ydb_auth_pb2.LoginRequest(user=self.user, password=self.password),
                ydb_auth_v1_pb2_grpc.AuthServiceStub,
                "Login",
                _wrap_static_credentials_response,
                settings_impl.BaseRequestSettings().with_timeout(self.request_timeout).with_need_rpc_auth(False),
            )
        finally:
            conn.close()
        return {"expires_in": 30 * 60, "access_token": result.token}

    def _update_driver_config(self, driver_config):
        from .driver import DriverConfig

        self.driver_config = DriverConfig(
            endpoint=driver_config.endpoint,
            database=driver_config.database,
            root_certificates=driver_config.root_certificates,
        )


class AnonymousCredentials(Credentials):
    @staticmethod
    def auth_metadata():
        return []


class AuthTokenCredentials(Credentials):
    def __init__(self, token):
        self._token = token

    def auth_metadata(self):
        return [(YDB_AUTH_TICKET_HEADER, self._token)]


class AccessTokenCredentials(Credentials):
    def __init__(self, token):
        self._token = token

    def auth_metadata(self):
        return [(YDB_AUTH_TICKET_HEADER, self._token)]
