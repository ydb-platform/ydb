import logging
import socket
import ssl
from collections import deque
from contextlib import contextmanager
from time import time

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
from .reader import read_binary_str
from .readhelpers import read_exception
from .settings.writer import write_settings
from .util.compat import urlparse
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
                 revision, timezone, display_name):
        self.name = name
        self.version_major = version_major
        self.version_minor = version_minor
        self.version_patch = version_patch
        self.revision = revision
        self.timezone = timezone
        self.display_name = display_name

        super(ServerInfo, self).__init__()

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
    :param alt_hosts: list of alternative hosts for connection.
                      Example: alt_hosts=host1:port1,host2:port2.
    :param settings_is_important: ``False`` means unknown settings will be
                                  ignored, ``True`` means that the query will
                                  fail with UNKNOWN_SETTING error.
                                  Defaults to ``False``.
    """

    def __init__(
            self, host, port=None,
            database='default', user='default', password='',
            client_name=defines.CLIENT_NAME,
            connect_timeout=defines.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
            send_receive_timeout=defines.DBMS_DEFAULT_TIMEOUT_SEC,
            sync_request_timeout=defines.DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
            compress_block_size=defines.DEFAULT_COMPRESS_BLOCK_SIZE,
            compression=False,
            secure=False,
            # Secure socket parameters.
            verify=True, ssl_version=None, ca_certs=None, ciphers=None,
            alt_hosts=None,
            settings_is_important=False,
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

        self.secure_socket = secure
        self.verify_cert = verify

        ssl_options = {}
        if ssl_version is not None:
            ssl_options['ssl_version'] = ssl_version
        if ca_certs is not None:
            ssl_options['ca_certs'] = ca_certs
        if ciphers is not None:
            ssl_options['ciphers'] = ciphers

        self.ssl_options = ssl_options

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

        self.server_info = None
        self.context = Context()

        # Block writer/reader
        self.block_in = None
        self.block_out = None

        super(Connection, self).__init__()

    def get_description(self):
        return '{}:{}'.format(self.host, self.port)

    def force_connect(self):
        if not self.connected:
            self.connect()

        elif not self.ping():
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
                    sock = ssl_context.wrap_socket(sock, server_hostname=host)

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

        version = ssl_options.get('ssl_version', ssl.PROTOCOL_TLS)
        context = ssl.SSLContext(version)

        if 'ca_certs' in ssl_options:
            context.load_verify_locations(ssl_options['ca_certs'])
        elif ssl_options.get('cert_reqs') != ssl.CERT_NONE:
            context.load_default_certs(purpose
                                       )
        if 'ciphers' in ssl_options:
            context.set_ciphers(ssl_options['ciphers'])

        if 'cert_reqs' in ssl_options:
            context.options = ssl_options['cert_reqs']

        return context

    def _init_connection(self, host, port):
        self.socket = self._create_socket(host, port)
        self.connected = True
        self.host, self.port = host, port
        self.socket.settimeout(self.send_receive_timeout)

        # performance tweak
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.fin = BufferedSocketReader(self.socket, defines.BUFFER_SIZE)
        self.fout = BufferedSocketWriter(self.socket, defines.BUFFER_SIZE)

        self.send_hello()
        self.receive_hello()

        self.block_in = self.get_block_in_stream()
        self.block_out = self.get_block_out_stream()

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

        self.server_info = None

        self.block_in = None
        self.block_out = None

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
        write_varint(defines.CLIENT_REVISION, self.fout)
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

            server_timezone = None
            if server_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                server_timezone = read_binary_str(self.fin)

            server_display_name = ''
            if server_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME:
                server_display_name = read_binary_str(self.fin)

            server_version_patch = server_revision
            if server_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_VERSION_PATCH:
                server_version_patch = read_varint(self.fin)

            self.server_info = ServerInfo(
                server_name, server_version_major, server_version_minor,
                server_version_patch, server_revision,
                server_timezone, server_display_name
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
            self.disconnect()
            message = self.unexpected_packet_message('Hello or Exception',
                                                     packet_type)
            raise errors.UnexpectedPacketFromServerError(message)

    def ping(self):
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
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.EXCEPTION:
            packet.exception = self.receive_exception()

        elif packet.type == ServerPacketTypes.PROGRESS:
            packet.progress = self.receive_progress()

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            packet.profile_info = self.receive_profile_info()

        elif packet_type == ServerPacketTypes.TOTALS:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.EXTREMES:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.LOG:
            block = self.receive_data()
            log_block(block)

        elif packet_type == ServerPacketTypes.END_OF_STREAM:
            pass

        elif packet_type == ServerPacketTypes.TABLE_COLUMNS:
            packet.multistring_message = self.receive_multistring_message(
                packet_type
            )

        else:
            self.disconnect()
            raise errors.UnknownPacketFromServerError(
                'Unknown packet {} from server {}'.format(
                    packet_type, self.get_description()
                )
            )

        return packet

    def get_block_in_stream(self):
        if self.compression:
            from .streams.compressed import CompressedBlockInputStream

            return CompressedBlockInputStream(self.fin, self.context)
        else:
            from .streams.native import BlockInputStream

            return BlockInputStream(self.fin, self.context)

    def get_block_out_stream(self):
        if self.compression:
            from .streams.compressed import CompressedBlockOutputStream

            return CompressedBlockOutputStream(
                self.compressor_cls, self.compress_block_size,
                self.fout, self.context
            )
        else:
            from .streams.native import BlockOutputStream

            return BlockOutputStream(self.fout, self.context)

    def receive_data(self):
        revision = self.server_info.revision

        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            read_binary_str(self.fin)

        return self.block_in.read()

    def receive_exception(self):
        return read_exception(self.fin)

    def receive_progress(self):
        progress = Progress()
        progress.read(self.server_info.revision, self.fin)
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

        revision = self.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            write_binary_str(table_name, self.fout)

        self.block_out.write(block)
        logger.debug('Block "%s" send time: %f', table_name, time() - start)

    def send_query(self, query, query_id=None):
        if not self.connected:
            self.connect()

        write_varint(ClientPacketTypes.QUERY, self.fout)

        write_binary_str(query_id or '', self.fout)

        revision = self.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name)
            client_info.query_kind = ClientInfo.QueryKind.INITIAL_QUERY

            client_info.write(revision, self.fout)

        settings_as_strings = (
            revision >= defines
            .DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        )
        write_settings(self.context.settings, self.fout, settings_as_strings,
                       self.settings_is_important)

        if revision >= defines.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET:
            write_binary_str('', self.fout)

        write_varint(QueryProcessingStage.COMPLETE, self.fout)
        write_varint(self.compression, self.fout)

        write_binary_str(query, self.fout)

        logger.debug('Query: %s', query)

        self.fout.flush()

    def send_cancel(self):
        write_varint(ClientPacketTypes.CANCEL, self.fout)

        self.fout.flush()

    def send_external_tables(self, tables, types_check=False):
        for table in tables or []:
            block = RowOrientedBlock(table['structure'], table['data'],
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
