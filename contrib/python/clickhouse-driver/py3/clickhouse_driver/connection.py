import logging
import socket
import ssl
from collections import deque
from contextlib import contextmanager
from sys import platform
from time import time
from urllib.parse import urlparse

try:
    import certifi
except ImportError:
    certifi = None

from . import defines
from . import errors
from .block import RowOrientedBlock
from .blockstreamprofileinfo import BlockStreamProfileInfo
from .bufferedreader import BufferedSocketReader
from .bufferedwriter import BufferedSocketWriter
from .clientinfo import ClientInfo
from .compression import get_compressor_cls
from .context import Context
from .log import log_block
from .progress import Progress
from .protocol import Compression, ClientPacketTypes, ServerPacketTypes
from .queryprocessingstage import QueryProcessingStage
from .reader import read_binary_str, read_binary_uint64
from .readhelpers import read_exception
from .settings.writer import write_settings, SettingsFlags
from .streams.native import BlockInputStream, BlockOutputStream
from .util.compat import threading
from .util.escape import escape_params
from .varint import write_varint, read_varint
from .writer import write_binary_str

logger = logging.getLogger(__name__)


class Packet(object):
    def __init__(self):
        self.type = None
        self.block = None
        self.exception = None
        self.progress = None
        self.profile_info = None
        self.multistring_message = None

        super(Packet, self).__init__()


class ServerInfo(object):
    def __init__(self, name, version_major, version_minor, version_patch,
                 revision, timezone, display_name, used_revision):
        self.name = name
        self.version_major = version_major
        self.version_minor = version_minor
        self.version_patch = version_patch
        self.revision = revision
        self.timezone = timezone
        self.session_timezone = None
        self.display_name = display_name
        self.used_revision = used_revision

        super(ServerInfo, self).__init__()

    def get_timezone(self):
        return self.session_timezone or self.timezone

    def version_tuple(self):
        return self.version_major, self.version_minor, self.version_patch

    def __repr__(self):
        version = '%s.%s.%s' % (
            self.version_major, self.version_minor, self.version_patch
        )
        items = [
            ('name', self.name),
            ('version', version),
            ('revision', self.revision),
            ('used revision', self.used_revision),
            ('timezone', self.timezone),
            ('display_name', self.display_name)
        ]

        params = ', '.join('{}={}'.format(key, value) for key, value in items)
        return '<ServerInfo(%s)>' % (params)


class Connection(object):
    """
    Represents connection between client and ClickHouse server.

    :param host: host with running ClickHouse server.
    :param port: port ClickHouse server is bound to.
                 Defaults to ``9000`` if connection is not secured and
                 to ``9440`` if connection is secured.
    :param database: database connect to. Defaults to ``'default'``.
    :param user: database user. Defaults to ``'default'``.
    :param password: user's password. Defaults to ``''`` (no password).
    :param client_name: this name will appear in server logs.
                        Defaults to ``'python-driver'``.
    :param connect_timeout: timeout for establishing connection.
                            Defaults to ``10`` seconds.
    :param send_receive_timeout: timeout for sending and receiving data.
                                 Defaults to ``300`` seconds.
    :param sync_request_timeout: timeout for server ping.
                                 Defaults to ``5`` seconds.
    :param compress_block_size: size of compressed block to send.
                                Defaults to ``1048576``.
    :param compression: specifies whether or not use compression.
                        Defaults to ``False``. Possible choices:

                            * ``True`` is equivalent to ``'lz4'``.
                            * ``'lz4'``.
                            * ``'lz4hc'`` high-compression variant of
                              ``'lz4'``.
                            * ``'zstd'``.

    :param secure: establish secure connection. Defaults to ``False``.
    :param verify: specifies whether a certificate is required and whether it
                   will be validated after connection.
                   Defaults to ``True``.
    :param ssl_version: see :func:`ssl.wrap_socket` docs.
    :param ca_certs: see :func:`ssl.wrap_socket` docs.
    :param ciphers: see :func:`ssl.wrap_socket` docs.
    :param keyfile: see :func:`ssl.wrap_socket` docs.
    :param keypass: see :func:`ssl.wrap_socket` docs.
    :param certfile: see :func:`ssl.wrap_socket` docs.
    :param check_hostname: see :func:`ssl.wrap_socket` docs.
                           Defaults to ``True``.
    :param server_hostname: Hostname to use in SSL Wrapper construction.
                            Defaults to `None` which will send the passed
                            host param during SSL initialization. This param
                            may be used when connecting over an SSH tunnel
                            to correctly identify the desired server via SNI.
    :param alt_hosts: list of alternative hosts for connection.
                      Example: alt_hosts=host1:port1,host2:port2.
    :param settings_is_important: ``False`` means unknown settings will be
                                  ignored, ``True`` means that the query will
                                  fail with UNKNOWN_SETTING error.
                                  Defaults to ``False``.
    :param tcp_keepalive: enables `TCP keepalive <https://tldp.org/HOWTO/
                          TCP-Keepalive-HOWTO/overview.html>`_ on established
                          connection. If is set to ``True``` system keepalive
                          settings are used. You can also specify custom
                          keepalive setting with tuple:
                          ``(idle_time_sec, interval_sec, probes)``.
                          Defaults to ``False``.
    :param client_revision: can be used for client version downgrading.
                          Defaults to ``None``.
    :param disable_reconnect: disable automatic reconnect in case of
                              failed ``ping``, helpful when every reconnect
                              need to be caught in calling code.
                              Defaults to ``False``.
    """

    def __init__(
            self, host, port=None,
            database=defines.DEFAULT_DATABASE,
            user=defines.DEFAULT_USER, password=defines.DEFAULT_PASSWORD,
            client_name=defines.CLIENT_NAME,
            connect_timeout=defines.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
            send_receive_timeout=defines.DBMS_DEFAULT_TIMEOUT_SEC,
            sync_request_timeout=defines.DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
            compress_block_size=defines.DEFAULT_COMPRESS_BLOCK_SIZE,
            compression=False,
            secure=False,
            # Secure socket parameters.
            verify=True, ssl_version=None, ca_certs=None, ciphers=None,
            keyfile=None, keypass=None, certfile=None, check_hostname=True,
            server_hostname=None,
            alt_hosts=None,
            settings_is_important=False,
            tcp_keepalive=False,
            client_revision=None,
            disable_reconnect=False,
    ):
        if secure:
            default_port = defines.DEFAULT_SECURE_PORT
        else:
            default_port = defines.DEFAULT_PORT

        self.hosts = deque([(host, port or default_port)])

        if alt_hosts:
            for host in alt_hosts.split(','):
                url = urlparse('clickhouse://' + host)
                self.hosts.append((url.hostname, url.port or default_port))

        self.database = database
        self.user = user
        self.password = password
        self.client_name = defines.DBMS_NAME + ' ' + client_name
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.sync_request_timeout = sync_request_timeout
        self.settings_is_important = settings_is_important
        self.tcp_keepalive = tcp_keepalive
        self.client_revision = min(
            client_revision or defines.CLIENT_REVISION, defines.CLIENT_REVISION
        )
        self.disable_reconnect = disable_reconnect

        self.secure_socket = secure
        self.verify_cert = verify

        if certifi is not None:
            ca_certs = ca_certs or certifi.where()

        ssl_options = {}
        if ssl_version is not None:
            ssl_options['ssl_version'] = ssl_version
        if ca_certs is not None:
            ssl_options['ca_certs'] = ca_certs
        if ciphers is not None:
            ssl_options['ciphers'] = ciphers
        if keyfile is not None:
            ssl_options['keyfile'] = keyfile
        if keypass is not None:
            ssl_options['keypass'] = keypass
        if certfile is not None:
            ssl_options['certfile'] = certfile

        self.ssl_options = ssl_options

        self.check_hostname = check_hostname if self.verify_cert else False
        self.server_hostname = server_hostname

        # Use LZ4 compression by default.
        if compression is True:
            compression = 'lz4'

        if compression is False:
            self.compression = Compression.DISABLED
            self.compressor_cls = None
            self.compress_block_size = None
        else:
            self.compression = Compression.ENABLED
            self.compressor_cls = get_compressor_cls(compression)
            self.compress_block_size = compress_block_size

        self.socket = None
        self.fin = None
        self.fout = None

        self.connected = False

        self.client_trace_context = None
        self.server_info = None
        self.context = Context()

        # Block writer/reader
        self.block_in = None
        self.block_out = None
        self.block_in_raw = None  # log blocks are always not compressed

        self._lock = threading.Lock()
        self.is_query_executing = False

        super(Connection, self).__init__()

    def __repr__(self):
        dsn = '%s://%s:***@%s:%s/%s' % (
            'clickhouses' if self.secure_socket else 'clickhouse',
            self.user, self.host, self.port, self.database
        ) if self.connected else '(not connected)'

        return '<Connection(dsn=%s, compression=%s)>' % (dsn, self.compression)

    def get_description(self):
        return '{}:{}'.format(self.host, self.port)

    def force_connect(self):
        self.check_query_execution()

        if not self.connected:
            self.connect()

        elif not self.ping():
            if self.disable_reconnect:
                raise errors.NetworkError(
                    "Connection was closed, reconnect is disabled."
                )

            logger.warning('Connection was closed, reconnecting.')
            self.connect()

    def _create_socket(self, host, port):
        """
        Acts like socket.create_connection, but wraps socket with SSL
        if connection is secure.
        """
        ssl_options = {}
        if self.secure_socket:
            if self.verify_cert:
                cert_reqs = ssl.CERT_REQUIRED
            else:
                cert_reqs = ssl.CERT_NONE

            ssl_options = self.ssl_options.copy()
            ssl_options['cert_reqs'] = cert_reqs

        err = None
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.settimeout(self.connect_timeout)

                if self.secure_socket:
                    ssl_context = self._create_ssl_context(ssl_options)
                    sock = ssl_context.wrap_socket(
                        sock, server_hostname=self.server_hostname or host)

                sock.connect(sa)
                return sock

            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        else:
            raise socket.error("getaddrinfo returns an empty list")

    def _create_ssl_context(self, ssl_options):
        purpose = ssl.Purpose.SERVER_AUTH

        version = ssl_options.get('ssl_version', ssl.PROTOCOL_TLS_CLIENT)
        context = ssl.SSLContext(version)
        context.check_hostname = self.check_hostname

        if 'ca_certs' in ssl_options:
            context.load_verify_locations(ssl_options['ca_certs'])
        elif ssl_options.get('cert_reqs') != ssl.CERT_NONE:
            context.load_default_certs(purpose)
        if 'ciphers' in ssl_options:
            context.set_ciphers(ssl_options['ciphers'])

        if 'cert_reqs' in ssl_options:
            context.verify_mode = ssl_options['cert_reqs']

        if 'certfile' in ssl_options:
            keyfile = ssl_options.get('keyfile')
            keypass = ssl_options.get('keypass')
            context.load_cert_chain(
                ssl_options['certfile'],
                keyfile=keyfile, password=keypass
            )

        return context

    def _init_connection(self, host, port):
        self.socket = self._create_socket(host, port)
        self.connected = True
        self.host, self.port = host, port
        self.socket.settimeout(self.send_receive_timeout)

        # performance tweak
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self.tcp_keepalive:
            self._set_keepalive()

        self.fin = BufferedSocketReader(self.socket, defines.BUFFER_SIZE)
        self.fout = BufferedSocketWriter(self.socket, defines.BUFFER_SIZE)

        self.send_hello()
        self.receive_hello()

        revision = self.server_info.used_revision
        if revision >= defines.DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM:
            self.send_addendum()

        self.block_in = self.get_block_in_stream()
        self.block_in_raw = BlockInputStream(self.fin, self.context)
        self.block_out = self.get_block_out_stream()

    def _set_keepalive(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if not isinstance(self.tcp_keepalive, tuple):
            return

        idle_time_sec, interval_sec, probes = self.tcp_keepalive

        if platform == 'linux' or platform == 'win32':
            # This should also work for Windows
            # starting with Windows 10, version 1709.
            self.socket.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle_time_sec
            )
            self.socket.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec
            )
            self.socket.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_KEEPCNT, probes
            )

        elif platform == 'darwin':
            TCP_KEEPALIVE = 0x10
            # Only interval is available in mac os.
            self.socket.setsockopt(
                socket.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec
            )

    def _format_connection_error(self, e, host, port):
        err = (e.strerror + ' ') if e.strerror else ''
        return err + '({}:{})'.format(host, port)

    def connect(self):
        if self.connected:
            self.disconnect()

        logger.debug(
            'Connecting. Database: %s. User: %s', self.database, self.user
        )

        err = None
        for i in range(len(self.hosts)):
            host, port = self.hosts[0]
            logger.debug('Connecting to %s:%s', host, port)

            try:
                return self._init_connection(host, port)

            except socket.timeout as e:
                self.disconnect()
                logger.warning(
                    'Failed to connect to %s:%s', host, port, exc_info=True
                )
                err_str = self._format_connection_error(e, host, port)
                err = errors.SocketTimeoutError(err_str)

            except socket.error as e:
                self.disconnect()
                logger.warning(
                    'Failed to connect to %s:%s', host, port, exc_info=True
                )
                err_str = self._format_connection_error(e, host, port)
                err = errors.NetworkError(err_str)

            self.hosts.rotate(-1)

        if err is not None:
            raise err

    def reset_state(self):
        self.host = None
        self.port = None
        self.socket = None
        self.fin = None
        self.fout = None

        self.connected = False

        self.client_trace_context = None
        self.server_info = None

        self.block_in = None
        self.block_in_raw = None
        self.block_out = None

        self.is_query_executing = False

    def disconnect(self):
        """
        Closes connection between server and client.
        Frees resources: e.g. closes socket.
        """

        if self.connected:
            # There can be errors on shutdown.
            # We need to close socket and reset state even if it happens.
            try:
                self.socket.shutdown(socket.SHUT_RDWR)

            except socket.error as e:
                logger.warning('Error on socket shutdown: %s', e)

            self.socket.close()

        # Socket can be constructed but not connected.
        elif self.socket:
            self.socket.close()

        self.reset_state()

    def send_hello(self):
        write_varint(ClientPacketTypes.HELLO, self.fout)
        write_binary_str(self.client_name, self.fout)
        write_varint(defines.CLIENT_VERSION_MAJOR, self.fout)
        write_varint(defines.CLIENT_VERSION_MINOR, self.fout)
        # NOTE For backward compatibility of the protocol,
        # client cannot send its version_patch.
        write_varint(self.client_revision, self.fout)
        write_binary_str(self.database, self.fout)
        write_binary_str(self.user, self.fout)
        write_binary_str(self.password, self.fout)

        self.fout.flush()

    def receive_hello(self):
        packet_type = read_varint(self.fin)

        if packet_type == ServerPacketTypes.HELLO:
            server_name = read_binary_str(self.fin)
            server_version_major = read_varint(self.fin)
            server_version_minor = read_varint(self.fin)
            server_revision = read_varint(self.fin)

            used_revision = min(self.client_revision, server_revision)

            server_timezone = None
            if used_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                server_timezone = read_binary_str(self.fin)

            server_display_name = ''
            if used_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME:
                server_display_name = read_binary_str(self.fin)

            server_version_patch = server_revision
            if used_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_VERSION_PATCH:
                server_version_patch = read_varint(self.fin)

            if used_revision >= defines. \
                    DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES:
                rules_size = read_varint(self.fin)
                for _i in range(rules_size):
                    read_binary_str(self.fin)  # original_pattern
                    read_binary_str(self.fin)  # exception_message

            if used_revision >= defines. \
                    DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2:
                read_binary_uint64(self.fin)  # read_nonce

            self.server_info = ServerInfo(
                server_name, server_version_major, server_version_minor,
                server_version_patch, server_revision,
                server_timezone, server_display_name, used_revision
            )
            self.context.server_info = self.server_info

            logger.debug(
                'Connected to %s server version %s.%s.%s, revision: %s',
                server_name, server_version_major, server_version_minor,
                server_version_patch, server_revision
            )

        elif packet_type == ServerPacketTypes.EXCEPTION:
            raise self.receive_exception()

        else:
            message = self.unexpected_packet_message('Hello or Exception',
                                                     packet_type)
            self.disconnect()
            raise errors.UnexpectedPacketFromServerError(message)

    def send_addendum(self):
        revision = self.server_info.used_revision

        if revision >= defines.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY:
            write_binary_str(
                self.context.client_settings['quota_key'], self.fout
            )

    def ping(self):
        if not self.socket:
            return None

        timeout = self.sync_request_timeout

        with self.timeout_setter(timeout):
            try:
                write_varint(ClientPacketTypes.PING, self.fout)
                self.fout.flush()

                packet_type = read_varint(self.fin)
                while packet_type == ServerPacketTypes.PROGRESS:
                    self.receive_progress()
                    packet_type = read_varint(self.fin)

                if packet_type != ServerPacketTypes.PONG:
                    msg = self.unexpected_packet_message('Pong', packet_type)
                    raise errors.UnexpectedPacketFromServerError(msg)

            except errors.Error:
                raise

            except (socket.error, EOFError) as e:
                # It's just a warning now.
                # Current connection will be closed, new will be established.
                logger.warning(
                    'Error on %s ping: %s', self.get_description(), e
                )
                return False

        return True

    def receive_packet(self):
        packet = Packet()

        packet.type = packet_type = read_varint(self.fin)

        if packet_type == ServerPacketTypes.DATA:
            packet.block = self.receive_data(may_be_use_numpy=True)

        elif packet_type == ServerPacketTypes.EXCEPTION:
            packet.exception = self.receive_exception()

        elif packet_type == ServerPacketTypes.PROGRESS:
            packet.progress = self.receive_progress()

        elif packet_type == ServerPacketTypes.PROFILE_INFO:
            packet.profile_info = self.receive_profile_info()

        elif packet_type == ServerPacketTypes.TOTALS:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.EXTREMES:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.LOG:
            packet.block = self.receive_data(may_be_compressed=False)
            log_block(packet.block)

        elif packet_type == ServerPacketTypes.END_OF_STREAM:
            self.is_query_executing = False
            pass

        elif packet_type == ServerPacketTypes.TABLE_COLUMNS:
            packet.multistring_message = self.receive_multistring_message(
                packet_type
            )

        elif packet_type == ServerPacketTypes.PART_UUIDS:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.READ_TASK_REQUEST:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.PROFILE_EVENTS:
            packet.block = self.receive_data(may_be_compressed=False)

        elif packet_type == ServerPacketTypes.TIMEZONE_UPDATE:
            timezone = read_binary_str(self.fin)
            if timezone:
                logger.info('Server timezone changed to %s', timezone)
                self.server_info.session_timezone = timezone

        else:
            message = 'Unknown packet {} from server {}'.format(
                packet_type, self.get_description()
            )
            self.disconnect()
            raise errors.UnknownPacketFromServerError(message)

        return packet

    def get_block_in_stream(self):
        if self.compression:
            from .streams.compressed import CompressedBlockInputStream

            return CompressedBlockInputStream(self.fin, self.context)
        else:
            return BlockInputStream(self.fin, self.context)

    def get_block_out_stream(self):
        if self.compression:
            from .streams.compressed import CompressedBlockOutputStream

            return CompressedBlockOutputStream(
                self.compressor_cls, self.compress_block_size,
                self.fout, self.context
            )
        else:
            return BlockOutputStream(self.fout, self.context)

    def receive_data(self, may_be_compressed=True, may_be_use_numpy=False):
        revision = self.server_info.used_revision

        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            read_binary_str(self.fin)

        reader = self.block_in if may_be_compressed else self.block_in_raw
        use_numpy = False if not may_be_use_numpy else None
        return reader.read(use_numpy=use_numpy)

    def receive_exception(self):
        return read_exception(self.fin)

    def receive_progress(self):
        progress = Progress()
        progress.read(self.server_info, self.fin)
        return progress

    def receive_profile_info(self):
        profile_info = BlockStreamProfileInfo()
        profile_info.read(self.fin)
        return profile_info

    def receive_multistring_message(self, packet_type):
        num = ServerPacketTypes.strings_in_message(packet_type)
        return [read_binary_str(self.fin) for _i in range(num)]

    def send_data(self, block, table_name=''):
        start = time()
        write_varint(ClientPacketTypes.DATA, self.fout)

        revision = self.server_info.used_revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            write_binary_str(table_name, self.fout)

        self.block_out.write(block)
        logger.debug('Block "%s" send time: %f', table_name, time() - start)

    def send_query(self, query, query_id=None, params=None):
        if not self.connected:
            self.connect()

        write_varint(ClientPacketTypes.QUERY, self.fout)

        write_binary_str(query_id or '', self.fout)

        revision = self.server_info.used_revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name, self.context,
                                     client_revision=self.client_revision)
            client_info.query_kind = ClientInfo.QueryKind.INITIAL_QUERY

            client_info.write(revision, self.fout)

        settings_as_strings = (
            revision >= defines
            .DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        )
        settings_flags = 0
        if self.settings_is_important:
            settings_flags |= SettingsFlags.IMPORTANT
        write_settings(self.context.settings, self.fout, settings_as_strings,
                       settings_flags)

        if revision >= defines.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET:
            write_binary_str('', self.fout)

        write_varint(QueryProcessingStage.COMPLETE, self.fout)
        write_varint(self.compression, self.fout)

        write_binary_str(query, self.fout)

        if revision >= defines.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS:
            if self.context.client_settings['server_side_params']:
                # Always settings_as_strings = True
                escaped = escape_params(
                    params or {}, self.context, for_server=True
                )
            else:
                escaped = {}
            write_settings(escaped, self.fout, True, SettingsFlags.CUSTOM)

        logger.debug('Query: %s', query)

        self.fout.flush()

    def send_cancel(self):
        write_varint(ClientPacketTypes.CANCEL, self.fout)

        self.fout.flush()

    def send_external_tables(self, tables, types_check=False):
        for table in tables or []:
            if not table['structure']:
                raise ValueError(
                    'Empty table "{}" structure'.format(table['name'])
                )

            data = table['data']
            block_cls = RowOrientedBlock

            if self.context.client_settings['use_numpy']:
                from .numpy.block import NumpyColumnOrientedBlock

                columns = [x[0] for x in table['structure']]
                data = [data[column].values for column in columns]

                block_cls = NumpyColumnOrientedBlock

            block = block_cls(table['structure'], data,
                              types_check=types_check)
            self.send_data(block, table_name=table['name'])

        # Empty block, end of data transfer.
        self.send_data(RowOrientedBlock())

    @contextmanager
    def timeout_setter(self, new_timeout):
        old_timeout = self.socket.gettimeout()
        self.socket.settimeout(new_timeout)

        yield

        self.socket.settimeout(old_timeout)

    def unexpected_packet_message(self, expected, packet_type):
        packet_type = ServerPacketTypes.to_str(packet_type)

        return (
            'Unexpected packet from server {} (expected {}, got {})'
            .format(self.get_description(), expected, packet_type)
        )

    def check_query_execution(self):
        self._lock.acquire(blocking=False)

        if self.is_query_executing:
            raise errors.PartiallyConsumedQueryError()

        self.is_query_executing = True
        self._lock.release()
