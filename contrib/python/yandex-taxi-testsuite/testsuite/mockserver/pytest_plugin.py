import asyncio
import contextlib
import logging
import warnings

import pytest

from testsuite import types
from testsuite.tracing import TraceidManager
from testsuite.utils import net

from . import classes, exceptions, server

MOCKSERVER_DEFAULT_PORT = 9999
MOCKSERVER_SSL_DEFAULT_PORT = 9998

_SSL_KEY_FILE_INI_KEY = 'mockserver-ssl-key-file'
_SSL_CERT_FILE_INI_KEY = 'mockserver-ssl-cert-file'

MOCKSERVER_PORT_HELP = """
{proto} mockserver port for default worker.
Random port is used by default. If testsuite is started with
--service-wait or --service-disabled default is forced to {default}.
"""

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    group = parser.getgroup('mockserver')
    group.addoption(
        '--mockserver-nofail',
        action='store_true',
        help='Do not fail if no handler is set.',
    )
    group.addoption(
        '--mockserver-host',
        default='localhost',
        help='Default host for http mockserver.',
    )
    group.addoption(
        '--mockserver-port',
        type=int,
        default=0,
        help=MOCKSERVER_PORT_HELP.format(
            proto='HTTP',
            default=MOCKSERVER_DEFAULT_PORT,
        ),
    )
    group.addoption(
        '--mockserver-ssl-host',
        default='localhost',
        help='Default host for https mockserver.',
    )
    group.addoption(
        '--mockserver-ssl-port',
        type=int,
        default=0,
        help=MOCKSERVER_PORT_HELP.format(
            proto='HTTPS',
            default=MOCKSERVER_SSL_DEFAULT_PORT,
        ),
    )
    group.addoption(
        '--mockserver-unix-socket',
        type=str,
        help='Bind server to unix socket instead of tcp',
    )
    group.addoption(
        '--mockserver-debug',
        action='store_true',
        help='Enable debugging logs.',
    )
    parser.addini(
        'mockserver-tracing-enabled',
        type='bool',
        default=False,
        help=(
            'When request trace-id header not from testsuite:\n'
            '  True: handle, if handler missing return http status 500\n'
            '  False: handle, if handler missing raise '
            'HandlerNotFoundError\n'
            'When request trace-id header from other test:\n'
            '  True: do not handle, return http status 500\n'
            '  False: handle, if handler missing raise HandlerNotFoundError'
        ),
    )
    parser.addini(
        'mockserver-trace-id-header',
        default=server.DEFAULT_TRACE_ID_HEADER,
        help=(
            'name of tracing http header, value changes from test to test and '
            'is constant within test'
        ),
    )
    parser.addini(
        'mockserver-span-id-header',
        default=server.DEFAULT_SPAN_ID_HEADER,
        help='name of tracing http header, value is unique for each request',
    )
    parser.addini(
        'mockserver-ssl-cert-file',
        type='pathlist',
        help='path to ssl certificate file to setup mockserver_ssl',
    )
    parser.addini(
        'mockserver-ssl-key-file',
        type='pathlist',
        help='path to ssl key file to setup mockserver_ssl',
    )
    parser.addini(
        'mockserver-http-proxy-enabled',
        type='bool',
        default=False,
        help='If enabled mockserver acts as http proxy',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'mockserver_assert_lost_calls: assert that all calls to mockservers are checked',
    )


def pytest_register_object_hooks():
    return {
        '$mockserver': {'$fixture': '_mockserver_hook'},
        '$mockserver_https': {'$fixture': '_mockserver_https_hook'},
    }


@pytest.fixture(name='mockserver_strict_default')
def fixture_mockserver_strict_default():
    return False


@pytest.fixture(name='mockserver_create_session')
def fixture_mockserver_create_session(
    request,
    asyncexc_append,
    testsuite_traceid_manager: TraceidManager,
    mockserver_strict_default: bool,
):
    assert_lost_calls = request.node.get_closest_marker(
        'mockserver_assert_lost_calls'
    )

    @contextlib.contextmanager
    def create_session(mockserver):
        with mockserver.new_session(
            asyncexc_append=asyncexc_append,
            traceid_manager=testsuite_traceid_manager,
        ) as session:
            yield server.MockserverFixture(
                mockserver,
                session,
                strict_default=mockserver_strict_default,
            )

            calls = session.collect_calls()
            if assert_lost_calls:
                if not calls:
                    raise exceptions.MockServerError(
                        f'mockserver is expected to have lost calls, but it doesnt'
                    )
            else:
                if calls:
                    raise exceptions.MockServerError(
                        f'mockserver handler with strict=True has skipped calls: {calls}'
                    )

    return create_session


@pytest.fixture(name='_mockserver_create_session')
def legacy_fixture_mockserver_create_session(
    mockserver_create_session,
):
    def create_session(*args, **kwargs):
        warnings.warn(
            'Use mockserver_create_session() fixture instead',
            DeprecationWarning,
        )
        return mockserver_create_session(*args, **kwargs)

    return create_session


@pytest.fixture
def mockserver(
    _mockserver: server.Server,
    mockserver_create_session,
) -> types.YieldFixture[server.MockserverFixture]:
    with mockserver_create_session(_mockserver) as fixture:
        yield fixture


@pytest.fixture
def mockserver_ssl(
    _mockserver_ssl: server.Server | None,
    mockserver_create_session,
) -> types.AsyncYieldFixture[server.MockserverSslFixture]:
    if _mockserver_ssl is None:
        raise exceptions.MockServerError(
            f'mockserver_ssl is not configured. {_SSL_KEY_FILE_INI_KEY} and '
            f'{_SSL_CERT_FILE_INI_KEY} must be specified in pytest.ini',
        )
    with mockserver_create_session(_mockserver_ssl) as fixture:
        yield fixture


@pytest.fixture(scope='session')
def mockserver_info(
    _mockserver_socket: classes.MockserverSocket,
) -> classes.MockserverInfo:
    """Returns mockserver information object."""
    return _mockserver_socket.info


@pytest.fixture(scope='session')
def mockserver_ssl_info(
    _mockserver_ssl_socket: classes.MockserverSocket | None,
) -> classes.MockserverInfo | None:
    if _mockserver_ssl_socket is None:
        return None
    return _mockserver_ssl_socket.info


@pytest.fixture(scope='session')
def mockserver_ssl_cert(pytestconfig) -> classes.SslCertInfo | None:
    def _get_ini_path(name):
        values = pytestconfig.getini(name)
        if not values:
            return None
        if len(values) > 1:
            raise exceptions.MockServerError(
                f'{name} ini setting has multiple values',
            )
        return str(values[0])

    cert_path = _get_ini_path(_SSL_CERT_FILE_INI_KEY)
    key_path = _get_ini_path(_SSL_KEY_FILE_INI_KEY)
    if cert_path and key_path:
        return classes.SslCertInfo(
            cert_path=cert_path,
            private_key_path=key_path,
        )
    return None


@pytest.fixture(scope='session')
async def mockserver_create(
    _mockserver_config,
):
    @contextlib.asynccontextmanager
    async def create(
        *,
        host='localhost',
        port=0,
        socket_path=None,
        ssl_cert: classes.SslCertInfo | None = None,
        config: classes.MockserverConfig | None = None,
    ):
        socket_info = server._create_mockserver_socket(
            host=host,
            port=port,
            socket_path=socket_path,
            https=bool(ssl_cert),
        )
        async with server._create_server_from_socket(
            socket_info,
            config or _mockserver_config,
            ssl_cert=ssl_cert,
        ) as result:
            yield result

    return create


@pytest.fixture(scope='session')
async def _mockserver(
    pytestconfig,
    _mockserver_socket: classes.MockserverSocket,
    _mockserver_config: classes.MockserverConfig,
) -> types.AsyncYieldFixture[server.Server]:
    async with server._create_server_from_socket(
        _mockserver_socket, _mockserver_config
    ) as result:
        yield result


@pytest.fixture(scope='session')
async def _mockserver_ssl(
    pytestconfig,
    _mockserver_ssl_socket: classes.MockserverSocket,
    _mockserver_config: classes.MockserverConfig,
    mockserver_ssl_cert,
) -> types.AsyncYieldFixture[server.Server]:
    if mockserver_ssl_cert:
        async with server._create_server_from_socket(
            _mockserver_ssl_socket,
            _mockserver_config,
            ssl_cert=mockserver_ssl_cert,
        ) as result:
            yield result
    else:
        yield None


@pytest.fixture(scope='session')
def _mockserver_hook(mockserver_info):
    def wrapper(doc: dict):
        return _mockserver_info_hook(doc, '$mockserver', mockserver_info)

    return wrapper


@pytest.fixture(scope='session')
def _mockserver_https_hook(mockserver_ssl_info):
    def wrapper(doc: dict):
        return _mockserver_info_hook(
            doc,
            '$mockserver_https',
            mockserver_ssl_info,
        )

    return wrapper


@pytest.fixture(scope='session')
def _mockserver_socket(
    pytestconfig, _mockserver_config
) -> classes.MockserverSocket:
    port = _mockserver_getport(
        pytestconfig,
        pytestconfig.option.mockserver_port,
        default_port=MOCKSERVER_DEFAULT_PORT,
    )
    mockserver_socket = server._create_mockserver_socket(
        socket_path=pytestconfig.option.mockserver_unix_socket,
        host=pytestconfig.option.mockserver_host,
        port=port,
    )
    with net.closing_sockets(mockserver_socket.sockets):
        info = []
        for sock in mockserver_socket.sockets:
            info.append(sock.getsockname())
        logger.debug('Mockserver bound to %r', info)
        yield mockserver_socket


@pytest.fixture(scope='session')
def _mockserver_ssl_socket(
    pytestconfig,
) -> classes.MockserverSocket | None:
    port = _mockserver_getport(
        pytestconfig,
        pytestconfig.option.mockserver_ssl_port,
        default_port=MOCKSERVER_SSL_DEFAULT_PORT,
    )
    mockserver_socket = server._create_mockserver_socket(
        host=pytestconfig.option.mockserver_ssl_host,
        port=port,
        https=True,
    )
    with net.closing_sockets(mockserver_socket.sockets):
        info = []
        for sock in mockserver_socket.sockets:
            info.append(sock.getsockname())
        logger.debug('Mockserver HTTPS bound to %r', info)
        yield mockserver_socket


@pytest.fixture(scope='session')
async def mockserver_set_debug(_mockserver, _mockserver_ssl):
    def set_debug(enabled: bool):
        loop = asyncio.get_running_loop()
        for obj in (loop, _mockserver, _mockserver_ssl):
            if obj is not None:
                obj.set_debug(enabled)

    return set_debug


@pytest.fixture(scope='session')
def _mockserver_config(pytestconfig) -> classes.MockserverConfig:
    return classes.MockserverConfig(
        nofail=pytestconfig.option.mockserver_nofail,
        debug=pytestconfig.option.mockserver_debug,
        tracing_enabled=pytestconfig.getini('mockserver-tracing-enabled'),
        trace_id_header=pytestconfig.getini('mockserver-trace-id-header'),
        span_id_header=pytestconfig.getini('mockserver-span-id-header'),
        http_proxy_enabled=pytestconfig.getini('mockserver-http-proxy-enabled'),
    )


def _mockserver_info_hook(
    doc: dict, key=None, mockserver_info: classes.MockserverInfo | None = None
):
    if mockserver_info is None:
        raise RuntimeError(f'Missing {key} argument')
    if not doc.get('$schema', True):
        schema = ''
    elif mockserver_info.https:
        schema = 'https://'
    else:
        schema = 'http://'
    return '%s%s:%d%s' % (
        schema,
        mockserver_info.host,
        mockserver_info.port,
        doc[key],
    )


def _mockserver_getport(config, option_port, default_port):
    # If service is started outside of testsuite use constant
    # port by default.
    if config.option.service_wait or config.option.service_disable:
        if option_port == 0:
            return default_port
    return option_port
