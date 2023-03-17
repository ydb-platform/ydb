# -*- coding: utf-8 -*-
import abc
import six
from . import tracing, issues, connection
from . import settings as settings_impl
import threading
from concurrent import futures
import logging
import time

# Workaround for good IDE and universal for runtime
# noinspection PyUnreachableCode
if False:
    from ._grpc.v4.protos import ydb_auth_pb2
    from ._grpc.v4 import ydb_auth_v1_pb2_grpc
else:
    from ._grpc.common.protos import ydb_auth_pb2
    from ._grpc.common import ydb_auth_v1_pb2_grpc


YDB_AUTH_TICKET_HEADER = "x-ydb-auth-ticket"
logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class AbstractCredentials(object):
    """
    An abstract class that provides auth metadata
    """


@six.add_metaclass(abc.ABCMeta)
class Credentials(object):
    def __init__(self, tracer=None):
        self.tracer = tracer if tracer is not None else tracing.Tracer(None)

    @abc.abstractmethod
    def auth_metadata(self):
        """
        :return: An iterable with auth metadata
        """
        pass


class OneToManyValue(object):
    def __init__(self):
        self._value = None
        self._condition = threading.Condition()

    def consume(self, timeout=3):
        with self._condition:
            if self._value is None:
                self._condition.wait(timeout=timeout)
            return self._value

    def update(self, n_value):
        with self._condition:
            prev_value = self._value
            self._value = n_value
            if prev_value is None:
                self._condition.notify_all()


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


@six.add_metaclass(abc.ABCMeta)
class AbstractExpiringTokenCredentials(Credentials):
    def __init__(self, tracer=None):
        super(AbstractExpiringTokenCredentials, self).__init__(tracer)
        self._expires_in = 0
        self._refresh_in = 0
        self._hour = 60 * 60
        self._cached_token = OneToManyValue()
        self._tp = AtMostOneExecution()
        self.logger = logger.getChild(self.__class__.__name__)
        self.last_error = None
        self.extra_error_message = ""

    @abc.abstractmethod
    def _make_token_request(self):
        pass

    def _log_refresh_start(self, current_time):
        self.logger.debug("Start refresh token from metadata")
        if current_time > self._refresh_in:
            self.logger.info(
                "Cached token reached refresh_in deadline, current time %s, deadline %s",
                current_time,
                self._refresh_in,
            )

        if current_time > self._expires_in and self._expires_in > 0:
            self.logger.error(
                "Cached token reached expires_in deadline, current time %s, deadline %s",
                current_time,
                self._expires_in,
            )

    def _update_expiration_info(self, auth_metadata):
        self._expires_in = time.time() + min(
            self._hour, auth_metadata["expires_in"] / 2
        )
        self._refresh_in = time.time() + min(
            self._hour / 2, auth_metadata["expires_in"] / 4
        )

    def _refresh(self):
        current_time = time.time()
        self._log_refresh_start(current_time)
        try:
            token_response = self._make_token_request()
            self._cached_token.update(token_response["access_token"])
            self._update_expiration_info(token_response)
            self.logger.info(
                "Token refresh successful. current_time %s, refresh_in %s",
                current_time,
                self._refresh_in,
            )

        except (KeyboardInterrupt, SystemExit):
            return

        except Exception as e:
            self.last_error = str(e)
            time.sleep(1)
            self._tp.submit(self._refresh)

    @property
    @tracing.with_trace()
    def token(self):
        current_time = time.time()
        if current_time > self._refresh_in:
            tracing.trace(self.tracer, {"refresh": True})
            self._tp.submit(self._refresh)
        cached_token = self._cached_token.consume(timeout=3)
        tracing.trace(self.tracer, {"consumed": True})
        if cached_token is None:
            if self.last_error is None:
                raise issues.ConnectionError(
                    "%s: timeout occurred while waiting for token.\n%s"
                    % (
                        self.__class__.__name__,
                        self.extra_error_message,
                    )
                )
            raise issues.ConnectionError(
                "%s: %s.\n%s"
                % (self.__class__.__name__, self.last_error, self.extra_error_message)
            )
        return cached_token

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
        self.driver_config = driver_config
        self.user = user
        self.password = password
        self.request_timeout = 10

    def _make_token_request(self):
        conn = connection.Connection.ready_factory(
            self.driver_config.endpoint, self.driver_config
        )
        assert conn is not None, (
            "Failed to establish connection in to %s" % self.driver_config.endpoint
        )
        try:
            result = conn(
                ydb_auth_pb2.LoginRequest(user=self.user, password=self.password),
                ydb_auth_v1_pb2_grpc.AuthServiceStub,
                "Login",
                _wrap_static_credentials_response,
                settings_impl.BaseRequestSettings()
                .with_timeout(self.request_timeout)
                .with_need_rpc_auth(False),
            )
        finally:
            conn.close()
        return {"expires_in": 30 * 60, "access_token": result.token}


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
