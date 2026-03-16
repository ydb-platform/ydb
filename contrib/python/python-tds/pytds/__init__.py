"""DB-SIG compliant module for communicating with MS SQL servers"""
from __future__ import annotations

from collections import deque
import datetime
import os
import socket
import time
import uuid
import warnings
from typing import Any, Callable

from pytds.tds_types import TzInfoFactoryType
from . import lcid
from . import connection_pool
import pytds.tz
from .connection import MarsConnection, NonMarsConnection, Connection
from .cursor import Cursor  # noqa: F401 # export for backward compatibility
from .login import KerberosAuth, SspiAuth, AuthProtocol  # noqa: F401 # export for backward compatibility
from .row_strategies import (
    tuple_row_strategy,
    list_row_strategy,  # noqa: F401 # export for backward compatibility
    dict_row_strategy,
    namedtuple_row_strategy,  # noqa: F401 # export for backward compatibility
    recordtype_row_strategy,  # noqa: F401 # export for backward compatibility
    RowStrategy,
)
from .tds_socket import _TdsSocket
from . import instance_browser_client
from . import tds_base
from . import utils
from . import login as pytds_login
from .tds_base import (
    Error,  # noqa: F401 # export for backward compatibility
    LoginError,  # noqa: F401 # export for backward compatibility
    DatabaseError,  # noqa: F401 # export for backward compatibility
    ProgrammingError,  # noqa: F401 # export for backward compatibility
    IntegrityError,  # noqa: F401 # export for backward compatibility
    DataError,  # noqa: F401 # export for backward compatibility
    InternalError,  # noqa: F401 # export for backward compatibility
    InterfaceError,  # noqa: F401 # export for backward compatibility
    TimeoutError,  # noqa: F401 # export for backward compatibility
    OperationalError,  # noqa: F401 # export for backward compatibility
    NotSupportedError,  # noqa: F401 # export for backward compatibility
    Warning,  # noqa: F401 # export for backward compatibility
    ClosedConnectionError,  # noqa: F401 # export for backward compatibility
    Column,  # noqa: F401 # export for backward compatibility
    PreLoginEnc,  # noqa: F401 # export for backward compatibility
)

from .tds_types import TableValuedParam, Binary  # noqa: F401 # export for backward compatibility

from .tds_base import (
    ROWID,  # noqa: F401 # export for backward compatibility
    DECIMAL,  # noqa: F401 # export for backward compatibility
    STRING,  # noqa: F401 # export for backward compatibility
    BINARY,  # noqa: F401 # export for backward compatibility
    NUMBER,  # noqa: F401 # export for backward compatibility
    DATETIME,  # noqa: F401 # export for backward compatibility
    INTEGER,  # noqa: F401 # export for backward compatibility
    REAL,  # noqa: F401 # export for backward compatibility
    XML,  # noqa: F401 # export for backward compatibility
    output,  # noqa: F401 # export for backward compatibility
    default,  # noqa: F401 # export for backward compatibility
)


from . import tls
from .tds_base import logger

__author__ = "Mikhail Denisenko <denisenkom@gmail.com>"

intversion = utils.ver_to_int("1.0.0")

#: Compliant with DB SIG 2.0
apilevel = "2.0"

#: Module may be shared, but not connections
threadsafety = 1

#: This module uses extended python format codes
paramstyle = "pyformat"


# map to servers deques, used to store active/passive servers
# between calls to connect function
# deques are used because they can be rotated
_servers_deques: dict[
    tuple[tuple[tuple[str, int | None, str], ...], str | None],
    deque[tuple[Any, int | None, str]],
] = {}


def _get_servers_deque(
    servers: tuple[tuple[str, int | None, str], ...], database: str | None
) -> deque[tuple[Any, int | None, str]]:
    """Returns deque of servers for given tuple of servers and
    database name.
    This deque have active server at the begining, if first server
    is not accessible at the moment the deque will be rotated,
    second server will be moved to the first position, thirt to the
    second position etc, and previously first server will be moved
    to the last position.
    This allows to remember last successful server between calls
    to connect function.
    """
    key = (servers, database)
    if key not in _servers_deques:
        _servers_deques[key] = deque(servers)
    return _servers_deques[key]


def connect(
    dsn: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    timeout: float | None = None,
    login_timeout: float = 15,
    as_dict: bool | None = None,
    appname: str | None = None,
    port: int | None = None,
    tds_version: int = tds_base.TDS74,
    autocommit: bool = False,
    blocksize: int = 4096,
    use_mars: bool = False,
    auth: AuthProtocol | None = None,
    readonly: bool = False,
    load_balancer: tds_base.LoadBalancer | None = None,
    use_tz: datetime.tzinfo | None = None,
    bytes_to_unicode: bool = True,
    row_strategy: RowStrategy | None = None,
    failover_partner: str | None = None,
    server: str | None = None,
    cafile: str | None = None,
    sock: socket.socket | None = None,
    validate_host: bool = True,
    enc_login_only: bool = False,
    disable_connect_retry: bool = False,
    pooling: bool = False,
    use_sso: bool = False,
    isolation_level: int = 0,
    access_token_callable: Callable[[], str] | None = None,
):
    """
    Opens connection to the database

    :keyword dsn: SQL server host and instance: <host>[\\<instance>]
    :type dsn: string
    :keyword failover_partner: secondary database host, used if primary is not accessible
    :type failover_partner: string
    :keyword database: the database to initially connect to
    :type database: string
    :keyword user: database user to connect as
    :type user: string
    :keyword password: user's password
    :type password: string
    :keyword timeout: query timeout in seconds, default 0 (no timeout)
    :type timeout: int
    :keyword login_timeout: timeout for connection and login in seconds, default 15
    :type login_timeout: int
    :keyword as_dict: whether rows should be returned as dictionaries instead of tuples.
    :type as_dict: boolean
    :keyword appname: Set the application name to use for the connection
    :type appname: string
    :keyword port: the TCP port to use to connect to the server
    :type port: int
    :keyword tds_version: Maximum TDS version to use, should only be used for testing
    :type tds_version: int
    :keyword autocommit: Enable or disable database level autocommit
    :type autocommit: bool
    :keyword blocksize: Size of block for the TDS protocol, usually should not be used
    :type blocksize: int
    :keyword use_mars: Enable or disable MARS
    :type use_mars: bool
    :keyword auth: An instance of authentication method class, e.g. Ntlm or Sspi
    :keyword readonly: Allows to enable read-only mode for connection, only supported by MSSQL 2012,
      earlier versions will ignore this parameter
    :type readonly: bool
    :keyword load_balancer: An instance of load balancer class to use, if not provided will not use load balancer
    :keyword use_tz: Provides timezone for naive database times, if not provided date and time will be returned
      in naive format
    :keyword bytes_to_unicode: If true single byte database strings will be converted to unicode Python strings,
      otherwise will return strings as ``bytes`` without conversion.
    :type bytes_to_unicode: bool
    :keyword row_strategy: strategy used to create rows, determines type of returned rows, can be custom or one of:
      :func:`tuple_row_strategy`, :func:`list_row_strategy`, :func:`dict_row_strategy`,
      :func:`namedtuple_row_strategy`, :func:`recordtype_row_strategy`
    :type row_strategy: function of list of column names returning row factory
    :keyword cafile: Name of the file containing trusted CAs in PEM format, if provided will enable TLS
    :type cafile: str
    :keyword validate_host: Host name validation during TLS connection is enabled by default, if you disable it you
      will be vulnerable to MitM type of attack.
    :type validate_host: bool
    :keyword enc_login_only: Allows you to scope TLS encryption only to an authentication portion.  This means that
      anyone who can observe traffic on your network will be able to see all your SQL requests and potentially modify
      them.
    :type enc_login_only: bool
    :keyword use_sso: Enables SSO login, e.g. Kerberos using SSPI on Windows and kerberos package on other platforms.
             Cannot be used together with auth parameter.
    :keyword access_token_callable: Callable that returns a Federated Authentication Token
    :type access_token_callable: Callable[[], str]
    :returns: An instance of :class:`Connection`
    """
    if use_sso and auth:
        raise ValueError("use_sso cannot be used with auth parameter defined")

    if (user or password) and access_token_callable:
        raise ValueError("user/password cannot be used with access_token_callable")

    login = tds_base._TdsLogin()
    login.client_host_name = socket.gethostname()[:128]
    login.library = "Python TDS Library"
    login.user_name = user or ""
    login.password = password or ""
    login.app_name = appname or "pytds"
    login.port = port
    login.language = ""  # use database default
    login.attach_db_file = ""
    login.tds_version = tds_version
    if tds_version < tds_base.TDS70:
        raise ValueError("This TDS version is not supported")
    login.database = database or ""
    login.bulk_copy = False
    login.client_lcid = lcid.LANGID_ENGLISH_US
    login.use_mars = use_mars
    login.pid = os.getpid()
    login.change_password = ""
    login.client_id = uuid.getnode()  # client mac address
    login.cafile = cafile
    login.validate_host = validate_host
    login.enc_login_only = enc_login_only
    login.access_token_callable = access_token_callable

    if cafile:
        if not tls.OPENSSL_AVAILABLE:
            raise ValueError(
                "You are trying to use encryption but pyOpenSSL does not work, you probably "
                "need to install it first"
            )
        login.tls_ctx = tls.create_context(cafile)
        if login.enc_login_only:
            login.enc_flag = PreLoginEnc.ENCRYPT_OFF
        else:
            login.enc_flag = PreLoginEnc.ENCRYPT_ON
    else:
        login.tls_ctx = None
        login.enc_flag = PreLoginEnc.ENCRYPT_NOT_SUP

    if use_tz:
        login.client_tz = use_tz
    else:
        login.client_tz = pytds.tz.local

    # that will set:
    # ANSI_DEFAULTS to ON,
    # IMPLICIT_TRANSACTIONS to OFF,
    # TEXTSIZE to 0x7FFFFFFF (2GB) (TDS 7.2 and below), TEXTSIZE to infinite (introduced in TDS 7.3),
    # and ROWCOUNT to infinite
    login.option_flag2 = tds_base.TDS_ODBC_ON

    login.connect_timeout = login_timeout
    login.query_timeout = timeout
    login.blocksize = blocksize
    login.readonly = readonly
    login.load_balancer = load_balancer
    login.bytes_to_unicode = bytes_to_unicode

    if server and dsn:
        raise ValueError("Both server and dsn shouldn't be specified")

    if server:
        warnings.warn(
            "server parameter is deprecated, use dsn instead", DeprecationWarning
        )
        dsn = server

    if load_balancer and failover_partner:
        raise ValueError(
            "Both load_balancer and failover_partner shoudln't be specified"
        )
    servers: list[tuple[str, int | None]] = []
    if load_balancer:
        servers += ((srv, None) for srv in load_balancer.choose())
    else:
        servers += [(dsn or "localhost", port)]
        if failover_partner:
            servers.append((failover_partner, port))

    parsed_servers: list[tuple[str, int | None, str]] = []
    for srv, instance_port in servers:
        host, instance = utils.parse_server(srv)
        if instance and instance_port:
            raise ValueError("Both instance and port shouldn't be specified")
        parsed_servers.append((host, instance_port, instance))

    if use_sso:
        spn = f"MSSQLSvc@{parsed_servers[0][0]}:{parsed_servers[0][1]}"
        try:
            login.auth = pytds_login.SspiAuth(spn=spn)
        except ImportError:
            login.auth = pytds_login.KerberosAuth(spn)
    else:
        login.auth = auth

    login.servers = _get_servers_deque(tuple(parsed_servers), database)

    # unique connection identifier used to pool connection
    key = (
        dsn,
        login.user_name,
        login.app_name,
        login.tds_version,
        login.database,
        login.client_lcid,
        login.use_mars,
        login.cafile,
        login.blocksize,
        login.readonly,
        login.bytes_to_unicode,
        login.auth,
        login.client_tz,
        autocommit,
    )
    tzinfo_factory = None if use_tz is None else pytds.tz.FixedOffsetTimezone
    assert (
        row_strategy is None or as_dict is None
    ), "Both row_startegy and as_dict were specified, you should use either one or another"
    if as_dict:
        row_strategy = dict_row_strategy
    elif row_strategy is not None:
        row_strategy = row_strategy
    else:
        row_strategy = tuple_row_strategy  # default row strategy

    if disable_connect_retry:
        first_try_time = login.connect_timeout
    else:
        first_try_time = login.connect_timeout * 0.08

    def attempt(attempt_timeout: float) -> Connection:
        if pooling:
            res = connection_pool.connection_pool.take(key)
            if res is not None:
                tds_socket, sess = res
                sess.callproc("sp_reset_connection", [])
                tds_socket._row_strategy = row_strategy
                if tds_socket.mars_enabled:
                    return MarsConnection(
                        pooling=pooling,
                        key=key,
                        tds_socket=tds_socket,
                    )
                else:
                    return NonMarsConnection(
                        pooling=pooling,
                        key=key,
                        tds_socket=tds_socket,
                    )
        host, port, instance = login.servers[0]
        login.servers.rotate(1)
        return _connect(
            login=login,
            host=host,
            port=port,
            instance=instance,
            timeout=attempt_timeout,
            pooling=pooling,
            key=key,
            autocommit=autocommit,
            isolation_level=isolation_level,
            tzinfo_factory=tzinfo_factory,
            sock=sock,
            use_tz=use_tz,
            row_strategy=row_strategy,
        )

    def ex_handler(ex: Exception) -> None:
        if isinstance(ex, LoginError):
            raise ex
        elif isinstance(ex, BrokenPipeError) or isinstance(ex, ConnectionError) or isinstance(ex, socket.timeout):
            # Allow to retry when various connection errors occur
            pass
        elif isinstance(ex, OperationalError):
            # if there are more than one message this means
            # that the login was successful, like in the
            # case when database is not accessible
            # mssql returns 2 messages:
            # 1) Cannot open database "<dbname>" requested by the login. The login failed.
            # 2) Login failed for user '<username>'
            # in this case we want to retry
            if ex.msg_no in (
                18456,  # login failed
                18486,  # account is locked
                18487,  # password expired
                18488,  # password should be changed
                18452,  # login from untrusted domain
            ):
                raise ex
        else:
            raise ex

    return utils.exponential_backoff(
        work=attempt,
        ex_handler=ex_handler,
        max_time_sec=login.connect_timeout,
        first_attempt_time_sec=first_try_time,
    )


def _connect(
    login: tds_base._TdsLogin,
    host: str,
    port: int | None,
    instance: str,
    timeout: float,
    pooling: bool,
    key: connection_pool.PoolKeyType,
    autocommit: bool,
    isolation_level: int,
    tzinfo_factory: TzInfoFactoryType | None,
    sock: socket.socket | None,
    use_tz: datetime.tzinfo | None,
    row_strategy: RowStrategy,
) -> Connection:
    """
    Establish physical connection and login.
    """
    login.server_name = host
    login.instance_name = instance
    resolved_port = instance_browser_client.resolve_instance_port(
        server=host, port=port, instance=instance, timeout=timeout
    )

    if login.access_token_callable is not None:
        login.access_token = login.access_token_callable()

    if not sock:
        logger.info("Opening socket to %s:%d", host, resolved_port)
        sock = socket.create_connection((host, resolved_port), timeout)
    try:
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

        # default keep alive should be 30 seconds according to spec:
        # https://msdn.microsoft.com/en-us/library/dd341108.aspx
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 30)

        sock.settimeout(timeout)
        tds_socket = _TdsSocket(
            sock=sock,
            tzinfo_factory=tzinfo_factory,
            use_tz=use_tz,
            row_strategy=row_strategy,
            autocommit=autocommit,
            login=login,
            isolation_level=isolation_level,
        )
        logger.info("Performing login on the connection")
        route = tds_socket.login()
        if route is not None:
            logger.info(
                "Connection was rerouted to %s:%d", route["server"], route["port"]
            )
            sock.close()
            ###  Change SPN once route exists

            if isinstance(login.auth, pytds_login.SspiAuth):
                route_spn = f"MSSQLSvc@{host}:{port}"
                login.auth = pytds_login.SspiAuth(
                    user_name=login.user_name,
                    password=login.password,
                    server_name=host,
                    port=port,
                    spn=route_spn,
                )

            return _connect(
                login=login,
                host=route["server"],
                port=route["port"],
                instance=instance,
                timeout=timeout,
                pooling=pooling,
                key=key,
                autocommit=autocommit,
                isolation_level=isolation_level,
                tzinfo_factory=tzinfo_factory,
                use_tz=use_tz,
                row_strategy=row_strategy,
                sock=None,
            )
        if not autocommit:
            tds_socket.main_session.begin_tran()
        sock.settimeout(login.query_timeout)
        if tds_socket.mars_enabled:
            return MarsConnection(
                pooling=pooling,
                key=key,
                tds_socket=tds_socket,
            )
        else:
            return NonMarsConnection(
                pooling=pooling,
                key=key,
                tds_socket=tds_socket,
            )
    except Exception:
        sock.close()
        raise


def Date(year: int, month: int, day: int) -> datetime.date:
    return datetime.date(year, month, day)


def DateFromTicks(ticks: float) -> datetime.date:
    return datetime.date.fromtimestamp(ticks)


def Time(
    hour: int,
    minute: int,
    second: int,
    microsecond: int = 0,
    tzinfo: datetime.tzinfo | None = None,
) -> datetime.time:
    return datetime.time(hour, minute, second, microsecond, tzinfo)


def TimeFromTicks(ticks: float) -> datetime.time:
    return Time(*time.localtime(ticks)[3:6])


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    microseconds: int = 0,
    tzinfo: datetime.tzinfo | None = None,
) -> datetime.datetime:
    return datetime.datetime(
        year, month, day, hour, minute, second, microseconds, tzinfo
    )


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ticks)
