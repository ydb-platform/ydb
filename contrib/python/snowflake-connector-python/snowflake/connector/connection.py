#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
import os
import re
import sys
import uuid
import warnings
import weakref
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from difflib import get_close_matches
from functools import partial
from io import StringIO
from logging import getLogger
from threading import Lock
from time import strptime
from typing import Any, Callable, Generator, Iterable, NamedTuple, Sequence

from . import errors, proxy
from .auth import (
    FIRST_PARTY_AUTHENTICATORS,
    Auth,
    AuthByDefault,
    AuthByKeyPair,
    AuthByOAuth,
    AuthByOkta,
    AuthByPlugin,
    AuthByUsrPwdMfa,
    AuthByWebBrowser,
)
from .auth.idtoken import AuthByIdToken
from .bind_upload_agent import BindUploadError
from .compat import IS_LINUX, IS_WINDOWS, quote, urlencode
from .connection_diagnostic import ConnectionDiagnostic
from .constants import (
    ENV_VAR_PARTNER,
    PARAMETER_AUTOCOMMIT,
    PARAMETER_CLIENT_PREFETCH_THREADS,
    PARAMETER_CLIENT_REQUEST_MFA_TOKEN,
    PARAMETER_CLIENT_SESSION_KEEP_ALIVE,
    PARAMETER_CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY,
    PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL,
    PARAMETER_CLIENT_TELEMETRY_ENABLED,
    PARAMETER_CLIENT_TELEMETRY_OOB_ENABLED,
    PARAMETER_CLIENT_VALIDATE_DEFAULT_PARAMETERS,
    PARAMETER_ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1,
    PARAMETER_SERVICE_NAME,
    PARAMETER_TIMEZONE,
    OCSPMode,
    QueryStatus,
)
from .converter import SnowflakeConverter
from .cursor import LOG_MAX_QUERY_LENGTH, SnowflakeCursor
from .description import (
    CLIENT_NAME,
    CLIENT_VERSION,
    PLATFORM,
    PYTHON_VERSION,
    SNOWFLAKE_CONNECTOR_VERSION,
)
from .errorcode import (
    ER_CONNECTION_IS_CLOSED,
    ER_FAILED_PROCESSING_PYFORMAT,
    ER_FAILED_PROCESSING_QMARK,
    ER_FAILED_TO_CONNECT_TO_DB,
    ER_INVALID_VALUE,
    ER_NO_ACCOUNT_NAME,
    ER_NO_NUMPY,
    ER_NO_PASSWORD,
    ER_NO_USER,
    ER_NOT_IMPLICITY_SNOWFLAKE_DATATYPE,
)
from .errors import DatabaseError, Error, OperationalError, ProgrammingError
from .network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
    REQUEST_ID,
    USR_PWD_MFA_AUTHENTICATOR,
    ReauthenticationRequest,
    SnowflakeRestful,
)
from .sqlstate import SQLSTATE_CONNECTION_NOT_EXISTS, SQLSTATE_FEATURE_NOT_SUPPORTED
from .telemetry import TelemetryClient, TelemetryData, TelemetryField
from .telemetry_oob import TelemetryService
from .time_util import HeartBeatTimer, get_time_millis
from .util_text import construct_hostname, parse_account, split_statements

DEFAULT_CLIENT_PREFETCH_THREADS = 4
MAX_CLIENT_PREFETCH_THREADS = 10


def DefaultConverterClass():
    if IS_WINDOWS:
        from .converter_issue23517 import SnowflakeConverterIssue23517

        return SnowflakeConverterIssue23517
    else:
        from .converter import SnowflakeConverter

        return SnowflakeConverter


SUPPORTED_PARAMSTYLES = {
    "qmark",
    "numeric",
    "format",
    "pyformat",
}
# Default configs, tuple of default variable and accepted types
DEFAULT_CONFIGURATION: dict[str, tuple[Any, type | tuple[type, ...]]] = {
    "dsn": (None, (type(None), str)),  # standard
    "user": ("", str),  # standard
    "password": ("", str),  # standard
    "host": ("127.0.0.1", str),  # standard
    "port": (8080, (int, str)),  # standard
    "database": (None, (type(None), str)),  # standard
    "proxy_host": (None, (type(None), str)),  # snowflake
    "proxy_port": (None, (type(None), str)),  # snowflake
    "proxy_user": (None, (type(None), str)),  # snowflake
    "proxy_password": (None, (type(None), str)),  # snowflake
    "protocol": ("http", str),  # snowflake
    "warehouse": (None, (type(None), str)),  # snowflake
    "region": (None, (type(None), str)),  # snowflake
    "account": (None, (type(None), str)),  # snowflake
    "schema": (None, (type(None), str)),  # snowflake
    "role": (None, (type(None), str)),  # snowflake
    "session_id": (None, (type(None), str)),  # snowflake
    "login_timeout": (120, int),  # login timeout
    "network_timeout": (
        None,
        (type(None), int),
    ),  # network timeout (infinite by default)
    "passcode_in_password": (False, bool),  # Snowflake MFA
    "passcode": (None, (type(None), str)),  # Snowflake MFA
    "private_key": (None, (type(None), str)),
    "token": (None, (type(None), str)),  # OAuth or JWT Token
    "authenticator": (DEFAULT_AUTHENTICATOR, (type(None), str)),
    "mfa_callback": (None, (type(None), Callable)),
    "password_callback": (None, (type(None), Callable)),
    "auth_class": (None, (type(None), AuthByPlugin)),
    "application": (CLIENT_NAME, (type(None), str)),
    "internal_application_name": (CLIENT_NAME, (type(None), str)),
    "internal_application_version": (CLIENT_VERSION, (type(None), str)),
    "insecure_mode": (False, bool),  # Error security fix requirement
    "ocsp_fail_open": (True, bool),  # fail open on ocsp issues, default true
    "inject_client_pause": (0, int),  # snowflake internal
    "session_parameters": (None, (type(None), dict)),  # snowflake session parameters
    "autocommit": (None, (type(None), bool)),  # snowflake
    "client_session_keep_alive": (None, (type(None), bool)),  # snowflake
    "client_session_keep_alive_heartbeat_frequency": (
        None,
        (type(None), int),
    ),  # snowflake
    "client_prefetch_threads": (4, int),  # snowflake
    "numpy": (False, bool),  # snowflake
    "ocsp_response_cache_filename": (None, (type(None), str)),  # snowflake internal
    "converter_class": (DefaultConverterClass(), SnowflakeConverter),
    "validate_default_parameters": (False, bool),  # snowflake
    "probe_connection": (False, bool),  # snowflake
    "paramstyle": (None, (type(None), str)),  # standard/snowflake
    "timezone": (None, (type(None), str)),  # snowflake
    "consent_cache_id_token": (True, bool),  # snowflake
    "service_name": (None, (type(None), str)),  # snowflake
    "support_negative_year": (True, bool),  # snowflake
    "log_max_query_length": (LOG_MAX_QUERY_LENGTH, int),  # snowflake
    "disable_request_pooling": (False, bool),  # snowflake
    # enable temporary credential file for Linux, default false. Mac/Win will overlook this
    "client_store_temporary_credential": (False, bool),
    "client_request_mfa_token": (False, bool),
    "use_openssl_only": (
        False,
        bool,
    ),  # only use openssl instead of python only crypto modules
    # whether to convert Arrow number values to decimal instead of doubles
    "arrow_number_to_decimal": (False, bool),
    "enable_stage_s3_privatelink_for_us_east_1": (
        False,
        bool,
    ),  # only use regional url when the param is set
    # Allows cursors to be re-iterable
    "reuse_results": (False, bool),
    # parameter protecting behavior change of SNOW-501058
    "interpolate_empty_sequences": (False, bool),
    "enable_connection_diag": (False, bool),  # Generate SnowCD like report
    "connection_diag_log_path": (
        None,
        (type(None), str),
    ),  # Path to connection diag report
    "connection_diag_whitelist_path": (
        None,
        (type(None), str),
    ),  # Path to connection diag whitelist json
    "log_imported_packages_in_telemetry": (
        True,
        bool,
    ),  # Whether to log imported packages in telemetry
}

APPLICATION_RE = re.compile(r"[\w\d_]+")

# adding the exception class to Connection class
for m in [method for method in dir(errors) if callable(getattr(errors, method))]:
    setattr(sys.modules[__name__], m, getattr(errors, m))

# Workaround for https://bugs.python.org/issue7980
strptime("20150102030405", "%Y%m%d%H%M%S")

logger = getLogger(__name__)


class TypeAndBinding(NamedTuple):
    """Stores the type name and the Snowflake binding."""

    type: str
    binding: str | None


class SnowflakeConnection:
    """Implementation of the connection object for the Snowflake Database.

    Use connect(..) to get the object.

    Attributes:
        insecure_mode: Whether or not the connection is in insecure mode. Insecure mode means that the connection
            validates the TLS certificate but doesn't check revocation status.
        ocsp_fail_open: Whether or not the connection is in fail open mode. Fail open mode decides if TLS certificates
            continue to be validated. Revoked certificates are blocked. Any other exceptions are disregarded.
        session_id: The session ID of the connection.
        user: The user name used in the connection.
        host: The host name the connection attempts to connect to.
        port: The port to communicate with on the host.
        region: Region name if not the default Snowflake Database deployment.
        proxy_host: The hostname used proxy server.
        proxy_port: Port on proxy server to communicate with.
        proxy_user: User name to login with on the proxy sever.
        proxy_password: Password to be used to authenticate with proxy server.
        account: Account name to be used to authenticate with Snowflake.
        database: Database to use on Snowflake.
        schema: Schema in use on Snowflake.
        warehouse: Warehouse to be used on Snowflake.
        role: Role in use on Snowflake.
        login_timeout: Login timeout in seconds. Used while authenticating.
        network_timeout: Network timeout. Used for general purpose.
        client_session_keep_alive_heartbeat_frequency: Heartbeat frequency to keep connection alive in seconds.
        client_prefetch_threads: Number of threads to download the result set.
        rest: Snowflake REST API object. Internal use only. Maybe removed in a later release.
        application: Application name to communicate with Snowflake as. By default, this is "PythonConnector".
        errorhandler: Handler used with errors. By default, an exception will be raised on error.
        converter_class: Handler used to convert data to Python native objects.
        validate_default_parameters: Validate database, schema, role and warehouse used on Snowflake.
        is_pyformat: Whether the current argument binding is pyformat or format.
        consent_cache_id_token: Consented cache ID token.
        use_openssl_only: Use OpenSSL instead of pure Python libraries for signature verification and encryption.
        enable_stage_s3_privatelink_for_us_east_1: when true, clients use regional s3 url to upload files.
        enable_connection_diag: when true, clients will generate a connectivity diagnostic report.
        connection_diag_log_path: path to location to create diag report with enable_connection_diag.
        connection_diag_whitelist_path: path to a whitelist.json file to test with enable_connection_diag.
    """

    OCSP_ENV_LOCK = Lock()

    def __init__(self, **kwargs):
        self._lock_sequence_counter = Lock()
        self.sequence_counter = 0
        self._errorhandler = Error.default_errorhandler
        self._lock_converter = Lock()
        self.messages = []
        self._async_sfqids: dict[str, None] = {}
        self._done_async_sfqids: dict[str, None] = {}
        self.telemetry_enabled = False
        self._session_parameters: dict[str, str | int | bool] = {}
        logger.info(
            "Snowflake Connector for Python Version: %s, "
            "Python Version: %s, Platform: %s",
            SNOWFLAKE_CONNECTOR_VERSION,
            PYTHON_VERSION,
            PLATFORM,
        )

        self._rest = None
        for name, (value, _) in DEFAULT_CONFIGURATION.items():
            setattr(self, f"_{name}", value)

        self.heartbeat_thread = None

        if "application" not in kwargs:
            if ENV_VAR_PARTNER in os.environ.keys():
                kwargs["application"] = os.environ[ENV_VAR_PARTNER]
            elif "streamlit" in sys.modules:
                kwargs["application"] = "streamlit"

        self.converter = None
        self.__set_error_attributes()
        self.connect(**kwargs)
        self._telemetry = TelemetryClient(self._rest)

        # get the imported modules from sys.modules
        self._log_telemetry_imported_packages()

    def __del__(self):  # pragma: no cover
        try:
            self.close(retry=False)
        except Exception:
            pass

    @property
    def insecure_mode(self) -> bool:
        return self._insecure_mode

    @property
    def ocsp_fail_open(self) -> bool:
        return self._ocsp_fail_open

    def _ocsp_mode(self) -> OCSPMode:
        """OCSP mode. INSECURE, FAIL_OPEN or FAIL_CLOSED."""
        if self.insecure_mode:
            return OCSPMode.INSECURE
        elif self.ocsp_fail_open:
            return OCSPMode.FAIL_OPEN
        else:
            return OCSPMode.FAIL_CLOSED

    @property
    def session_id(self):
        return self._session_id

    @property
    def user(self):
        return self._user

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def region(self):
        warnings.warn(
            "Region has been deprecated and will be removed in the near future",
            PendingDeprecationWarning,
        )
        return self._region

    @property
    def proxy_host(self):
        return self._proxy_host

    @property
    def proxy_port(self):
        return self._proxy_port

    @property
    def proxy_user(self):
        return self._proxy_user

    @property
    def proxy_password(self):
        return self._proxy_password

    @property
    def account(self):
        return self._account

    @property
    def database(self):
        return self._database

    @property
    def schema(self):
        return self._schema

    @property
    def warehouse(self):
        return self._warehouse

    @property
    def role(self):
        return self._role

    @property
    def login_timeout(self):
        return int(self._login_timeout) if self._login_timeout is not None else None

    @property
    def network_timeout(self):
        return int(self._network_timeout) if self._network_timeout is not None else None

    @property
    def client_session_keep_alive(self):
        return self._client_session_keep_alive

    @client_session_keep_alive.setter
    def client_session_keep_alive(self, value):
        self._client_session_keep_alive = value

    @property
    def client_session_keep_alive_heartbeat_frequency(self):
        return self._client_session_keep_alive_heartbeat_frequency

    @client_session_keep_alive_heartbeat_frequency.setter
    def client_session_keep_alive_heartbeat_frequency(self, value):
        self._client_session_keep_alive_heartbeat_frequency = value
        self._validate_client_session_keep_alive_heartbeat_frequency()

    @property
    def client_prefetch_threads(self):
        return (
            self._client_prefetch_threads
            if self._client_prefetch_threads
            else DEFAULT_CLIENT_PREFETCH_THREADS
        )

    @client_prefetch_threads.setter
    def client_prefetch_threads(self, value):
        self._client_prefetch_threads = value
        self._validate_client_prefetch_threads()

    @property
    def rest(self):
        return self._rest

    @property
    def application(self):
        return self._application

    @property
    def errorhandler(self):
        return self._errorhandler

    @errorhandler.setter
    def errorhandler(self, value):
        if value is None:
            raise ProgrammingError("None errorhandler is specified")
        self._errorhandler = value

    @property
    def converter_class(self):
        return self._converter_class

    @property
    def validate_default_parameters(self):
        return self._validate_default_parameters

    @property
    def is_pyformat(self):
        return self._paramstyle in ("pyformat", "format")

    @property
    def consent_cache_id_token(self):
        return self._consent_cache_id_token

    @property
    def telemetry_enabled(self):
        return self._telemetry_enabled

    @telemetry_enabled.setter
    def telemetry_enabled(self, value):
        self._telemetry_enabled = True if value else False

    @property
    def service_name(self):
        return self._service_name

    @service_name.setter
    def service_name(self, value):
        self._service_name = value

    @property
    def log_max_query_length(self):
        return self._log_max_query_length

    @property
    def disable_request_pooling(self):
        return self._disable_request_pooling

    @disable_request_pooling.setter
    def disable_request_pooling(self, value):
        self._disable_request_pooling = True if value else False

    @property
    def use_openssl_only(self):
        return self._use_openssl_only

    @property
    def arrow_number_to_decimal(self):
        return self._arrow_number_to_decimal

    @property
    def enable_stage_s3_privatelink_for_us_east_1(self):
        return self._enable_stage_s3_privatelink_for_us_east_1

    @enable_stage_s3_privatelink_for_us_east_1.setter
    def enable_stage_s3_privatelink_for_us_east_1(self, value):
        self._enable_stage_s3_privatelink_for_us_east_1 = True if value else False

    @property
    def enable_connection_diag(self):
        return self._enable_connection_diag

    @property
    def connection_diag_log_path(self):
        return self._connection_diag_log_path

    @property
    def connection_diag_whitelist_path(self):
        return self._connection_diag_whitelist_path

    @arrow_number_to_decimal.setter
    def arrow_number_to_decimal_setter(self, value: bool):
        self._arrow_number_to_decimal = value

    @property
    def auth_class(self) -> AuthByPlugin | None:
        return self._auth_class

    @auth_class.setter
    def auth_class(self, value: AuthByPlugin) -> None:
        if isinstance(value, AuthByPlugin):
            self._auth_class = value
        else:
            raise TypeError("auth_class must subclass AuthByPlugin")

    def connect(self, **kwargs):
        """Establishes connection to Snowflake."""
        logger.debug("connect")
        if len(kwargs) > 0:
            self.__config(**kwargs)
            TelemetryService.get_instance().update_context(kwargs)

        if self.enable_connection_diag:
            connection_diag = ConnectionDiagnostic(
                account=self.account,
                host=self.host,
                connection_diag_log_path=self.connection_diag_log_path,
                connection_diag_whitelist_path=self.connection_diag_whitelist_path,
                proxy_host=self.proxy_host,
                proxy_port=self.proxy_port,
                proxy_user=self.proxy_user,
                proxy_password=self.proxy_password,
            )
            try:
                connection_diag.run_test()
                self.__open_connection()
                connection_diag.cursor = self.cursor()
            except Exception as e:
                logger.warning(f"Exception during connection test: {e}")

            try:
                connection_diag.run_post_test()
            except Exception as e:
                logger.warning(f"Exception during post connection test: {e}")
            finally:
                connection_diag.generate_report()
        else:
            self.__open_connection()

    def close(self, retry=True):
        """Closes the connection."""
        try:
            if not self.rest:
                logger.debug("Rest object has been destroyed, cannot close session")
                return

            # will hang if the application doesn't close the connection and
            # CLIENT_SESSION_KEEP_ALIVE is set, because the heartbeat runs on
            # a separate thread.
            self._cancel_heartbeat()

            # close telemetry first, since it needs rest to send remaining data
            logger.info("closed")
            self._telemetry.close(send_on_close=retry)
            if self._all_async_queries_finished():
                logger.info("No async queries seem to be running, deleting session")
                self.rest.delete_session(retry=retry)
            else:
                logger.info(
                    "There are {} async queries still running, not deleting session".format(
                        len(self._async_sfqids)
                    )
                )
            self.rest.close()
            self._rest = None
            del self.messages[:]
            logger.debug("Session is closed")
        except Exception as e:
            logger.debug(
                "Exception encountered in closing connection. ignoring...: %s", e
            )

    def is_closed(self):
        """Checks whether the connection has been closed."""
        return self.rest is None

    def autocommit(self, mode):
        """Sets autocommit mode to True, or False. Defaults to True."""
        if not self.rest:
            Error.errorhandler_wrapper(
                self,
                None,
                DatabaseError,
                {
                    "msg": "Connection is closed",
                    "errno": ER_CONNECTION_IS_CLOSED,
                    "sqlstate": SQLSTATE_CONNECTION_NOT_EXISTS,
                },
            )
        if not isinstance(mode, bool):
            Error.errorhandler_wrapper(
                self,
                None,
                ProgrammingError,
                {
                    "msg": f"Invalid parameter: {mode}",
                    "errno": ER_INVALID_VALUE,
                },
            )
        try:
            self.cursor().execute(f"ALTER SESSION SET autocommit={mode}")
        except Error as e:
            if e.sqlstate == SQLSTATE_FEATURE_NOT_SUPPORTED:
                logger.debug(
                    "Autocommit feature is not enabled for this " "connection. Ignored"
                )

    def commit(self):
        """Commits the current transaction."""
        self.cursor().execute("COMMIT")

    def rollback(self):
        """Rolls back the current transaction."""
        self.cursor().execute("ROLLBACK")

    def cursor(
        self, cursor_class: type[SnowflakeCursor] = SnowflakeCursor
    ) -> SnowflakeCursor:
        """Creates a cursor object. Each statement will be executed in a new cursor object."""
        logger.debug("cursor")
        if not self.rest:
            Error.errorhandler_wrapper(
                self,
                None,
                DatabaseError,
                {
                    "msg": "Connection is closed",
                    "errno": ER_CONNECTION_IS_CLOSED,
                    "sqlstate": SQLSTATE_CONNECTION_NOT_EXISTS,
                },
            )
        return cursor_class(self)

    def execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Iterable[SnowflakeCursor]:
        """Executes a SQL text including multiple statements. This is a non-standard convenience method."""
        stream = StringIO(sql_text)
        stream_generator = self.execute_stream(
            stream, remove_comments=remove_comments, cursor_class=cursor_class, **kwargs
        )
        ret = list(stream_generator)
        return ret if return_cursors else list()

    def execute_stream(
        self,
        stream: StringIO,
        remove_comments: bool = False,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Generator[SnowflakeCursor, None, None]:
        """Executes a stream of SQL statements. This is a non-standard convenient method."""
        split_statements_list = split_statements(
            stream, remove_comments=remove_comments
        )
        # Note: split_statements_list is a list of tuples of sql statements and whether they are put/get
        non_empty_statements = [e for e in split_statements_list if e[0]]
        for sql, is_put_or_get in non_empty_statements:
            cur = self.cursor(cursor_class=cursor_class)
            cur.execute(sql, _is_put_get=is_put_or_get, **kwargs)
            yield cur

    def __set_error_attributes(self):
        for m in [
            method for method in dir(errors) if callable(getattr(errors, method))
        ]:
            # If name starts with _ then ignore that
            name = m if not m.startswith("_") else m[1:]
            setattr(self, name, getattr(errors, m))

    @staticmethod
    def setup_ocsp_privatelink(app, hostname):
        SnowflakeConnection.OCSP_ENV_LOCK.acquire()
        ocsp_cache_server = f"http://ocsp.{hostname}/ocsp_response_cache.json"
        os.environ["SF_OCSP_RESPONSE_CACHE_SERVER_URL"] = ocsp_cache_server
        logger.debug("OCSP Cache Server is updated: %s", ocsp_cache_server)
        SnowflakeConnection.OCSP_ENV_LOCK.release()

    def __open_connection(self):
        """Opens a new network connection."""
        self.converter = self._converter_class(
            use_numpy=self._numpy, support_negative_year=self._support_negative_year
        )

        proxy.set_proxies(
            self.proxy_host, self.proxy_port, self.proxy_user, self.proxy_password
        )

        self._rest = SnowflakeRestful(
            host=self.host,
            port=self.port,
            protocol=self._protocol,
            inject_client_pause=self._inject_client_pause,
            connection=self,
        )
        logger.debug("REST API object was created: %s:%s", self.host, self.port)

        if "SF_OCSP_RESPONSE_CACHE_SERVER_URL" in os.environ:
            logger.debug(
                "Custom OCSP Cache Server URL found in environment - %s",
                os.environ["SF_OCSP_RESPONSE_CACHE_SERVER_URL"],
            )

        if self.host.endswith(".privatelink.snowflakecomputing.com"):
            SnowflakeConnection.setup_ocsp_privatelink(self.application, self.host)
        else:
            if "SF_OCSP_RESPONSE_CACHE_SERVER_URL" in os.environ:
                del os.environ["SF_OCSP_RESPONSE_CACHE_SERVER_URL"]

        if self._session_parameters is None:
            self._session_parameters = {}
        if self._autocommit is not None:
            self._session_parameters[PARAMETER_AUTOCOMMIT] = self._autocommit

        if self._timezone is not None:
            self._session_parameters[PARAMETER_TIMEZONE] = self._timezone

        if self._validate_default_parameters:
            # Snowflake will validate the requested database, schema, and warehouse
            self._session_parameters[
                PARAMETER_CLIENT_VALIDATE_DEFAULT_PARAMETERS
            ] = True

        if self.client_session_keep_alive is not None:
            self._session_parameters[
                PARAMETER_CLIENT_SESSION_KEEP_ALIVE
            ] = self._client_session_keep_alive

        if self.client_session_keep_alive_heartbeat_frequency is not None:
            self._session_parameters[
                PARAMETER_CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY
            ] = self._validate_client_session_keep_alive_heartbeat_frequency()

        if self.client_prefetch_threads:
            self._session_parameters[
                PARAMETER_CLIENT_PREFETCH_THREADS
            ] = self._validate_client_prefetch_threads()

        # Setup authenticator
        auth = Auth(self.rest)
        if self.auth_class is not None:
            if type(
                self.auth_class
            ) not in FIRST_PARTY_AUTHENTICATORS and not issubclass(
                type(self.auth_class), AuthByKeyPair
            ):
                raise TypeError("auth_class must be a child class of AuthByKeyPair")
                # TODO: add telemetry for custom auth
            self.auth_class = self.auth_class
        elif self._authenticator == DEFAULT_AUTHENTICATOR:
            self.auth_class = AuthByDefault(password=self._password)
        elif self._authenticator == EXTERNAL_BROWSER_AUTHENTICATOR:
            self._session_parameters[PARAMETER_CLIENT_STORE_TEMPORARY_CREDENTIAL] = (
                self._client_store_temporary_credential if IS_LINUX else True
            )
            auth.read_temporary_credentials(
                self.host,
                self.user,
                self._session_parameters,
            )
            # Depending on whether self._rest.id_token is available we do different
            #  auth_instance
            if self._rest.id_token is None:
                self.auth_class = AuthByWebBrowser(
                    application=self.application,
                    protocol=self._protocol,
                    host=self.host,
                    port=self.port,
                )
            else:
                self.auth_class = AuthByIdToken(id_token=self._rest.id_token)

        elif self._authenticator == KEY_PAIR_AUTHENTICATOR:
            self.auth_class = AuthByKeyPair(private_key=self._private_key)
        elif self._authenticator == OAUTH_AUTHENTICATOR:
            self.auth_class = AuthByOAuth(oauth_token=self._token)
        elif self._authenticator == USR_PWD_MFA_AUTHENTICATOR:
            self.auth_class = AuthByUsrPwdMfa(password=self._password)
            self._session_parameters[PARAMETER_CLIENT_REQUEST_MFA_TOKEN] = (
                self._client_request_mfa_token if IS_LINUX else True
            )
        else:
            # okta URL, e.g., https://<account>.okta.com/
            self.auth_class = AuthByOkta(application=self.application)

        self.authenticate_with_retry(self.auth_class)

        self._password = None  # ensure password won't persist
        self.auth_class.reset_secrets()

        if self.client_session_keep_alive:
            # This will be called after the heartbeat frequency has actually been set.
            # By this point it should have been decided if the heartbeat has to be enabled
            # and what would the heartbeat frequency be
            self._add_heartbeat()

    def __config(self, **kwargs):
        """Sets up parameters in the connection object."""
        logger.debug("__config")
        # Handle special cases first
        if "sequence_counter" in kwargs:
            self.sequence_counter = kwargs["sequence_counter"]
        if "application" in kwargs:
            value = kwargs["application"]
            if not APPLICATION_RE.match(value):
                msg = f"Invalid application name: {value}"
                raise ProgrammingError(msg=msg, errno=0)
            else:
                self._application = value
        if "validate_default_parameters" in kwargs:
            self._validate_default_parameters = kwargs["validate_default_parameters"]
        # Handle rest of arguments
        skip_list = ["validate_default_parameters", "sequence_counter", "application"]
        for name, value in filter(lambda e: e[0] not in skip_list, kwargs.items()):
            if self.validate_default_parameters:
                if name not in DEFAULT_CONFIGURATION.keys():
                    close_matches = get_close_matches(
                        name, DEFAULT_CONFIGURATION.keys(), n=1, cutoff=0.8
                    )
                    guess = close_matches[0] if len(close_matches) > 0 else None
                    warnings.warn(
                        "'{}' is an unknown connection parameter{}".format(
                            name, f", did you mean '{guess}'?" if guess else ""
                        )
                    )
                elif not isinstance(value, DEFAULT_CONFIGURATION[name][1]):
                    accepted_types = DEFAULT_CONFIGURATION[name][1]
                    warnings.warn(
                        "'{}' connection parameter should be of type '{}', but is a '{}'".format(
                            name,
                            str(tuple(e.__name__ for e in accepted_types)).replace(
                                "'", ""
                            )
                            if isinstance(accepted_types, tuple)
                            else accepted_types.__name__,
                            type(value).__name__,
                        )
                    )
            setattr(self, "_" + name, value)

        if self._numpy:
            try:
                import numpy  # noqa: F401
            except ModuleNotFoundError:  # pragma: no cover
                Error.errorhandler_wrapper(
                    self,
                    None,
                    ProgrammingError,
                    {
                        "msg": "Numpy module is not installed. Cannot fetch data as numpy",
                        "errno": ER_NO_NUMPY,
                    },
                )

        if self._paramstyle is None:
            import snowflake.connector

            self._paramstyle = snowflake.connector.paramstyle
        elif self._paramstyle not in SUPPORTED_PARAMSTYLES:
            raise ProgrammingError(
                msg="Invalid paramstyle is specified", errno=ER_INVALID_VALUE
            )

        if self._auth_class and not isinstance(self._auth_class, AuthByPlugin):
            raise TypeError("auth_class must subclass AuthByPlugin")

        if "account" in kwargs:
            if "host" not in kwargs:
                self._host = construct_hostname(kwargs.get("region"), self._account)
            if "port" not in kwargs:
                self._port = "443"
            if "protocol" not in kwargs:
                self._protocol = "https"

        # If using a custom auth class, we should set the authenticator
        # type to be the same as the custom auth class
        if self._auth_class:
            self._authenticator = self._auth_class.type_.value

        if self._authenticator:
            # Only upper self._authenticator if it is a non-okta link
            auth_tmp = self._authenticator.upper()
            if auth_tmp in [  # Non-okta authenticators
                DEFAULT_AUTHENTICATOR,
                EXTERNAL_BROWSER_AUTHENTICATOR,
                KEY_PAIR_AUTHENTICATOR,
                OAUTH_AUTHENTICATOR,
                USR_PWD_MFA_AUTHENTICATOR,
            ]:
                self._authenticator = auth_tmp

        if not self.user and self._authenticator != OAUTH_AUTHENTICATOR:
            # OAuth Authentication does not require a username
            Error.errorhandler_wrapper(
                self,
                None,
                ProgrammingError,
                {"msg": "User is empty", "errno": ER_NO_USER},
            )

        if self._private_key:
            self._authenticator = KEY_PAIR_AUTHENTICATOR

        if (
            self.auth_class is None
            and self._authenticator
            not in [
                EXTERNAL_BROWSER_AUTHENTICATOR,
                OAUTH_AUTHENTICATOR,
                KEY_PAIR_AUTHENTICATOR,
            ]
            and not self._password
        ):
            Error.errorhandler_wrapper(
                self,
                None,
                ProgrammingError,
                {"msg": "Password is empty", "errno": ER_NO_PASSWORD},
            )

        if not self._account:
            Error.errorhandler_wrapper(
                self,
                None,
                ProgrammingError,
                {"msg": "Account must be specified", "errno": ER_NO_ACCOUNT_NAME},
            )
        if "." in self._account:
            self._account = parse_account(self._account)

        if self.ocsp_fail_open:
            logger.info(
                "This connection is in OCSP Fail Open Mode. "
                "TLS Certificates would be checked for validity "
                "and revocation status. Any other Certificate "
                "Revocation related exceptions or OCSP Responder "
                "failures would be disregarded in favor of "
                "connectivity."
            )

        if self.insecure_mode:
            logger.info(
                "THIS CONNECTION IS IN INSECURE MODE. IT "
                "MEANS THE CERTIFICATE WILL BE VALIDATED BUT THE "
                "CERTIFICATE REVOCATION STATUS WILL NOT BE "
                "CHECKED."
            )

        if "SF_USE_OPENSSL_ONLY" not in os.environ:
            logger.info("Setting use_openssl_only mode to %s", self.use_openssl_only)
            os.environ["SF_USE_OPENSSL_ONLY"] = str(self.use_openssl_only)
        elif (
            os.environ.get("SF_USE_OPENSSL_ONLY", "False") == "True"
        ) != self.use_openssl_only:
            logger.warning(
                "Mode use_openssl_only is already set to: %s, ignoring set request to: %s",
                os.environ["SF_USE_OPENSSL_ONLY"],
                self.use_openssl_only,
            )
            self._use_openssl_only = os.environ["SF_USE_OPENSSL_ONLY"] == "True"

    def cmd_query(
        self,
        sql: str,
        sequence_counter: int,
        request_id: uuid.UUID,
        binding_params: None | tuple | dict[str, dict[str, str]] = None,
        binding_stage: str | None = None,
        is_file_transfer: bool = False,
        statement_params: dict[str, str] | None = None,
        is_internal: bool = False,
        describe_only: bool = False,
        _no_results: bool = False,
        _update_current_object: bool = True,
        _no_retry: bool = False,
    ):
        """Executes a query with a sequence counter."""
        logger.debug("_cmd_query")
        data = {
            "sqlText": sql,
            "asyncExec": _no_results,
            "sequenceId": sequence_counter,
            "querySubmissionTime": get_time_millis(),
        }
        if statement_params is not None:
            data["parameters"] = statement_params
        if is_internal:
            data["isInternal"] = is_internal
        if describe_only:
            data["describeOnly"] = describe_only
        if binding_stage is not None:
            # binding stage for bulk array binding
            data["bindStage"] = binding_stage
        if binding_params is not None:
            # binding parameters. This is for qmarks paramstyle.
            data["bindings"] = binding_params

        client = "sfsql_file_transfer" if is_file_transfer else "sfsql"

        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(
                "sql=[%s], sequence_id=[%s], is_file_transfer=[%s]",
                self._format_query_for_log(data["sqlText"]),
                data["sequenceId"],
                is_file_transfer,
            )

        url_parameters = {REQUEST_ID: request_id}

        ret = self.rest.request(
            "/queries/v1/query-request?" + urlencode(url_parameters),
            data,
            client=client,
            _no_results=_no_results,
            _include_retry_params=True,
            _no_retry=_no_retry,
        )

        if ret is None:
            ret = {"data": {}}
        if ret.get("data") is None:
            ret["data"] = {}
        if _update_current_object:
            data = ret["data"]
            if "finalDatabaseName" in data:
                self._database = data["finalDatabaseName"]
            if "finalSchemaName" in data:
                self._schema = data["finalSchemaName"]
            if "finalWarehouseName" in data:
                self._warehouse = data["finalWarehouseName"]
            if "finalRoleName" in data:
                self._role = data["finalRoleName"]

        return ret

    def _reauthenticate(self):
        return self._auth_class.reauthenticate(conn=self)

    def authenticate_with_retry(self, auth_instance):
        # make some changes if needed before real __authenticate
        try:
            self._authenticate(auth_instance)
        except ReauthenticationRequest as ex:
            # cached id_token expiration error, we have cleaned id_token and try to authenticate again
            logger.debug("ID token expired. Reauthenticating...: %s", ex)
            self._authenticate(auth_instance)

    def _authenticate(self, auth_instance: AuthByPlugin):
        auth_instance.prepare(
            conn=self,
            authenticator=self._authenticator,
            service_name=self.service_name,
            account=self.account,
            user=self.user,
            password=self._password,
        )
        self._consent_cache_id_token = getattr(
            auth_instance, "consent_cache_id_token", True
        )

        auth = Auth(self.rest)
        try:
            auth.authenticate(
                auth_instance=auth_instance,
                account=self.account,
                user=self.user,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
                passcode=self._passcode,
                passcode_in_password=self._passcode_in_password,
                mfa_callback=self._mfa_callback,
                password_callback=self._password_callback,
                session_parameters=self._session_parameters,
            )
        except OperationalError as e:
            logger.debug(
                "Operational Error raised at authentication"
                f"for authenticator: {type(auth_instance).__name__}"
            )

            while True:
                try:
                    auth_instance.handle_timeout(
                        authenticator=self._authenticator,
                        service_name=self.service_name,
                        account=self.account,
                        user=self.user,
                        password=self._password,
                    )
                    auth.authenticate(
                        auth_instance=auth_instance,
                        account=self.account,
                        user=self.user,
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                        role=self.role,
                        passcode=self._passcode,
                        passcode_in_password=self._passcode_in_password,
                        mfa_callback=self._mfa_callback,
                        password_callback=self._password_callback,
                        session_parameters=self._session_parameters,
                    )
                except OperationalError as auth_op:
                    if auth_op.errno == ER_FAILED_TO_CONNECT_TO_DB:
                        raise auth_op from e
                    logger.debug("Continuing authenticator specific timeout handling")
                    continue
                break

    def _write_params_to_byte_rows(
        self, params: list[tuple[Any | tuple]]
    ) -> list[bytes]:
        """Write csv-format rows of binding values as list of bytes string.

        Args:
            params: Binding parameters to bulk array insertion query with qmark/numeric format.
            cursor: SnowflakeCursor.

        Returns:
            List of bytes string corresponding to rows

        """
        res = []
        try:
            for row in params:
                temp = map(self.converter.to_csv_bindings, row)
                res.append((",".join(temp) + "\n").encode("utf-8"))
        except (ProgrammingError, AttributeError) as exc:
            raise BindUploadError from exc
        return res

    def _get_snowflake_type_and_binding(
        self,
        cursor: SnowflakeCursor | None,
        v: tuple[str, Any] | Any,
    ) -> TypeAndBinding:
        if isinstance(v, tuple):
            if len(v) != 2:
                Error.errorhandler_wrapper(
                    self,
                    cursor,
                    ProgrammingError,
                    {
                        "msg": "Binding parameters must be a list "
                        "where one element is a single value or "
                        "a pair of Snowflake datatype and a value",
                        "errno": ER_FAILED_PROCESSING_QMARK,
                    },
                )
            snowflake_type, v = v
        else:
            snowflake_type = self.converter.snowflake_type(v)
            if snowflake_type is None:
                Error.errorhandler_wrapper(
                    self,
                    cursor,
                    ProgrammingError,
                    {
                        "msg": "Python data type [{}] cannot be "
                        "automatically mapped to Snowflake data "
                        "type. Specify the snowflake data type "
                        "explicitly.".format(v.__class__.__name__.lower()),
                        "errno": ER_NOT_IMPLICITY_SNOWFLAKE_DATATYPE,
                    },
                )
        return TypeAndBinding(
            snowflake_type,
            self.converter.to_snowflake_bindings(snowflake_type, v),
        )

    # TODO we could probably rework this to not make dicts like this: {'1': 'value', '2': '13'}
    def _process_params_qmarks(
        self,
        params: Sequence | None,
        cursor: SnowflakeCursor | None = None,
    ) -> dict[str, dict[str, str]] | None:
        if not params:
            return None
        processed_params = {}

        get_type_and_binding = partial(self._get_snowflake_type_and_binding, cursor)

        for idx, v in enumerate(params):
            if isinstance(v, list):
                snowflake_type = self.converter.snowflake_type(v)
                all_param_data = list(map(get_type_and_binding, v))
                first_type = all_param_data[0].type
                # if all elements have the same snowflake type, update snowflake_type
                if all(param_data.type == first_type for param_data in all_param_data):
                    snowflake_type = first_type
                processed_params[str(idx + 1)] = {
                    "type": snowflake_type,
                    "value": [param_data.binding for param_data in all_param_data],
                }
            else:
                snowflake_type, snowflake_binding = get_type_and_binding(v)
                processed_params[str(idx + 1)] = {
                    "type": snowflake_type,
                    "value": snowflake_binding,
                }
        if logger.getEffectiveLevel() <= logging.DEBUG:
            for k, v in processed_params.items():
                logger.debug("idx: %s, type: %s", k, v.get("type"))
        return processed_params

    def _process_params_pyformat(
        self,
        params: Any | Sequence[Any] | dict[Any, Any] | None,
        cursor: SnowflakeCursor | None = None,
    ) -> tuple[Any] | dict[str, Any] | None:
        """Process parameters for client-side parameter binding.

        Args:
            params: Either a sequence, or a dictionary of parameters, if anything else
                is given then it will be put into a list and processed that way.
            cursor: The SnowflakeCursor used to report errors if necessary.
        """
        if params is None:
            if self._interpolate_empty_sequences:
                return None
            return {}
        if isinstance(params, dict):
            return self._process_params_dict(params)

        # TODO: remove this, callers should send in what's in the signature
        if not isinstance(params, (tuple, list)):
            params = [
                params,
            ]

        try:
            res = map(self._process_single_param, params)
            ret = tuple(res)
            logger.debug(f"parameters: {ret}")
            return ret
        except Exception as e:
            Error.errorhandler_wrapper(
                self,
                cursor,
                ProgrammingError,
                {
                    "msg": f"Failed processing pyformat-parameters; {e}",
                    "errno": ER_FAILED_PROCESSING_PYFORMAT,
                },
            )

    def _process_params_dict(
        self, params: dict[Any, Any], cursor: SnowflakeCursor | None = None
    ) -> dict:
        try:
            res = {k: self._process_single_param(v) for k, v in params.items()}
            logger.debug(f"parameters: {res}")
            return res
        except Exception as e:
            Error.errorhandler_wrapper(
                self,
                cursor,
                ProgrammingError,
                {
                    "msg": f"Failed processing pyformat-parameters: {e}",
                    "errno": ER_FAILED_PROCESSING_PYFORMAT,
                },
            )

    def _process_single_param(self, param: Any) -> Any:
        """Process a single parameter to Snowflake understandable form.

        This is a convenience function to replace repeated multiple calls with a single
        function call.

        It calls the following underlying functions in this order:
            1. self.converter.to_snowflake
            2. self.converter.escape
            3. self.converter.quote
        """
        to_snowflake = self.converter.to_snowflake
        escape = self.converter.escape
        _quote = self.converter.quote
        return _quote(escape(to_snowflake(param)))

    def _cancel_query(self, sql, request_id):
        """Cancels the query with the exact SQL query and requestId."""
        logger.debug("_cancel_query sql=[%s], request_id=[%s]", sql, request_id)
        url_parameters = {REQUEST_ID: str(uuid.uuid4())}

        return self.rest.request(
            "/queries/v1/abort-request?" + urlencode(url_parameters),
            {
                "sqlText": sql,
                REQUEST_ID: str(request_id),
            },
        )

    def _next_sequence_counter(self):
        """Gets next sequence counter. Used internally."""
        with self._lock_sequence_counter:
            self.sequence_counter += 1
            logger.debug("sequence counter: %s", self.sequence_counter)
            return self.sequence_counter

    def _log_telemetry(self, telemetry_data):
        """Logs data to telemetry."""
        if self.telemetry_enabled:
            self._telemetry.try_add_log_to_batch(telemetry_data)

    def _add_heartbeat(self):
        """Add an hourly heartbeat query in order to keep connection alive."""
        if not self.heartbeat_thread:
            self._validate_client_session_keep_alive_heartbeat_frequency()
            heartbeat_wref = weakref.WeakMethod(self._heartbeat_tick)

            def beat_if_possible() -> None:
                heartbeat_fn = heartbeat_wref()
                if heartbeat_fn:
                    heartbeat_fn()

            self.heartbeat_thread = HeartBeatTimer(
                self.client_session_keep_alive_heartbeat_frequency,
                beat_if_possible,
            )
            self.heartbeat_thread.start()
            logger.debug("started heartbeat")

    def _cancel_heartbeat(self):
        """Cancel a heartbeat thread."""
        if self.heartbeat_thread:
            self.heartbeat_thread.cancel()
            self.heartbeat_thread.join()
            self.heartbeat_thread = None
            logger.debug("stopped heartbeat")

    def _heartbeat_tick(self):
        """Execute a hearbeat if connection isn't closed yet."""
        if not self.is_closed():
            logger.debug("heartbeating!")
            self.rest._heartbeat()

    def _validate_client_session_keep_alive_heartbeat_frequency(self) -> int:
        """Validate and return heartbeat frequency in seconds."""
        real_max = int(self.rest.master_validity_in_seconds / 4)
        real_min = int(real_max / 4)
        if self.client_session_keep_alive_heartbeat_frequency is None:
            # This is an unlikely scenario but covering it just in case.
            self._client_session_keep_alive_heartbeat_frequency = real_min
        elif self.client_session_keep_alive_heartbeat_frequency > real_max:
            self._client_session_keep_alive_heartbeat_frequency = real_max
        elif self.client_session_keep_alive_heartbeat_frequency < real_min:
            self._client_session_keep_alive_heartbeat_frequency = real_min

        # ensure the type is integer
        self._client_session_keep_alive_heartbeat_frequency = int(
            self.client_session_keep_alive_heartbeat_frequency
        )
        return self.client_session_keep_alive_heartbeat_frequency

    def _validate_client_prefetch_threads(self):
        if self.client_prefetch_threads <= 0:
            self._client_prefetch_threads = 1
        elif self.client_prefetch_threads > MAX_CLIENT_PREFETCH_THREADS:
            self._client_prefetch_threads = MAX_CLIENT_PREFETCH_THREADS
        self._client_prefetch_threads = int(self.client_prefetch_threads)
        return self.client_prefetch_threads

    def _update_parameters(
        self,
        parameters: dict[str, str | int | bool],
    ) -> None:
        """Update session parameters."""
        with self._lock_converter:
            self.converter.set_parameters(parameters)
        for name, value in parameters.items():
            self._session_parameters[name] = value
            if PARAMETER_CLIENT_TELEMETRY_ENABLED == name:
                self.telemetry_enabled = value
            elif PARAMETER_CLIENT_TELEMETRY_OOB_ENABLED == name:
                if value:
                    TelemetryService.get_instance().enable()
                else:
                    TelemetryService.get_instance().disable()
            elif PARAMETER_CLIENT_SESSION_KEEP_ALIVE == name:
                # Only set if the local config is None.
                # Always give preference to user config.
                if self.client_session_keep_alive is None:
                    self.client_session_keep_alive = value
            elif (
                PARAMETER_CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY == name
                and self.client_session_keep_alive_heartbeat_frequency is None
            ):
                # Only set if local value hasn't been set already.
                self.client_session_keep_alive_heartbeat_frequency = value
            elif PARAMETER_SERVICE_NAME == name:
                self.service_name = value
            elif PARAMETER_CLIENT_PREFETCH_THREADS == name:
                self.client_prefetch_threads = value
            elif PARAMETER_ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1 == name:
                self.enable_stage_s3_privatelink_for_us_east_1 = value

    def _format_query_for_log(self, query):
        ret = " ".join(line.strip() for line in query.split("\n"))
        return (
            ret
            if len(ret) < self.log_max_query_length
            else ret[0 : self.log_max_query_length] + "..."
        )

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager with commit or rollback teardown."""
        if not self._session_parameters.get("AUTOCOMMIT", False):
            # Either AUTOCOMMIT is turned off, or is not set so we default to old behavior
            if exc_tb is None:
                self.commit()
            else:
                self.rollback()
        self.close()

    def _get_query_status(self, sf_qid: str) -> tuple[QueryStatus, dict[str, Any]]:
        """Retrieves the status of query with sf_qid and returns it with the raw response.

        This is the underlying function used by the public get_status functions.

        Args:
            sf_qid: Snowflake query id of interest.

        Raises:
            ValueError: if sf_qid is not a valid UUID string.
        """
        try:
            uuid.UUID(sf_qid)
        except ValueError:
            raise ValueError(f"Invalid UUID: '{sf_qid}'")
        logger.debug(f"get_query_status sf_qid='{sf_qid}'")

        status = "NO_DATA"
        if self.is_closed():
            return QueryStatus.DISCONNECTED, {"data": {"queries": []}}
        status_resp = self.rest.request(
            "/monitoring/queries/" + quote(sf_qid), method="get", client="rest"
        )
        if "queries" not in status_resp["data"]:
            return QueryStatus.FAILED_WITH_ERROR, status_resp
        queries = status_resp["data"]["queries"]
        if len(queries) > 0:
            status = queries[0]["status"]
        status_ret = QueryStatus[status]
        return status_ret, status_resp

    def _cache_query_status(self, sf_qid: str, status_ret: QueryStatus) -> None:
        # If query was started by us and it has finished let's cache this info
        if sf_qid in self._async_sfqids and not self.is_still_running(status_ret):
            self._async_sfqids.pop(
                sf_qid, None
            )  # Prevent KeyError when multiple threads try to remove the same query id
            self._done_async_sfqids[sf_qid] = None

    def get_query_status(self, sf_qid: str) -> QueryStatus:
        """Retrieves the status of query with sf_qid.

        Query status is returned as a QueryStatus.

        Args:
            sf_qid: Snowflake query id of interest.

        Raises:
            ValueError: if sf_qid is not a valid UUID string.
        """
        status, _ = self._get_query_status(sf_qid)
        self._cache_query_status(sf_qid, status)
        return status

    def get_query_status_throw_if_error(self, sf_qid: str) -> QueryStatus:
        """Retrieves the status of query with sf_qid as a QueryStatus and raises an exception if the query terminated with an error.

        Query status is returned as a QueryStatus.

        Args:
            sf_qid: Snowflake query id of interest.

        Raises:
            ValueError: if sf_qid is not a valid UUID string.
        """
        status, status_resp = self._get_query_status(sf_qid)
        self._cache_query_status(sf_qid, status)
        queries = status_resp["data"]["queries"]
        if self.is_an_error(status):
            if sf_qid in self._async_sfqids:
                self._async_sfqids.pop(sf_qid, None)
            message = status_resp.get("message")
            if message is None:
                message = ""
            code = queries[0].get("errorCode", -1)
            sql_state = None
            if "data" in status_resp:
                message += (
                    queries[0].get("errorMessage", "") if len(queries) > 0 else ""
                )
                sql_state = status_resp["data"].get("sqlState")
            Error.errorhandler_wrapper(
                self,
                None,
                ProgrammingError,
                {
                    "msg": message,
                    "errno": int(code),
                    "sqlstate": sql_state,
                    "sfqid": sf_qid,
                },
            )
        return status

    @staticmethod
    def is_still_running(status: QueryStatus) -> bool:
        """Checks whether given status is currently running."""
        return status in (
            QueryStatus.RUNNING,
            QueryStatus.QUEUED,
            QueryStatus.RESUMING_WAREHOUSE,
            QueryStatus.QUEUED_REPARING_WAREHOUSE,
            QueryStatus.BLOCKED,
            QueryStatus.NO_DATA,
        )

    @staticmethod
    def is_an_error(status: QueryStatus) -> bool:
        """Checks whether given status means that there has been an error."""
        return status in (
            QueryStatus.ABORTING,
            QueryStatus.FAILED_WITH_ERROR,
            QueryStatus.ABORTED,
            QueryStatus.FAILED_WITH_INCIDENT,
            QueryStatus.DISCONNECTED,
        )

    def _all_async_queries_finished(self) -> bool:
        """Checks whether all async queries started by this Connection have finished executing."""

        if not self._async_sfqids:
            return True

        if sys.version_info >= (3, 8):
            queries = list(reversed(self._async_sfqids.keys()))
        else:
            queries = list(reversed(list(self._async_sfqids.keys())))

        num_workers = min(self.client_prefetch_threads, len(queries))
        found_unfinished_query = False

        def async_query_check_helper(
            sfq_id: str,
        ) -> bool:
            nonlocal found_unfinished_query
            return found_unfinished_query or self.is_still_running(
                self.get_query_status(sfq_id)
            )

        with ThreadPoolExecutor(
            max_workers=num_workers, thread_name_prefix="async_query_check_"
        ) as tpe:  # We should upgrade to using cancel_futures=True once supporting 3.9+

            futures = (tpe.submit(async_query_check_helper, sfqid) for sfqid in queries)
            for f in as_completed(futures):
                if f.result():
                    found_unfinished_query = True
                    break
            for f in futures:
                f.cancel()

        return not found_unfinished_query

    def _log_telemetry_imported_packages(self) -> None:
        if self._log_imported_packages_in_telemetry:
            # filter out duplicates caused by submodules
            # and internal modules with names starting with an underscore
            imported_modules = {
                k.split(".", maxsplit=1)[0]
                for k in sys.modules.keys()
                if not k.startswith("_")
            }
            ts = get_time_millis()
            self._log_telemetry(
                TelemetryData.from_telemetry_data_dict(
                    from_dict={
                        TelemetryField.KEY_TYPE.value: TelemetryField.IMPORTED_PACKAGES.value,
                        TelemetryField.KEY_VALUE.value: str(imported_modules),
                    },
                    timestamp=ts,
                    connection=self,
                )
            )
