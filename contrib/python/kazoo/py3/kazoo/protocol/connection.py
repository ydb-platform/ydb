"""Zookeeper Protocol Connection Handler"""
from binascii import hexlify
from contextlib import contextmanager
import copy
import logging
import random
import select
import socket
import ssl
import sys
import time

from kazoo.exceptions import (
    AuthFailedError,
    ConnectionDropped,
    EXCEPTIONS,
    SessionExpiredError,
    NoNodeError,
    SASLException,
)
from kazoo.loggingsupport import BLATHER
from kazoo.protocol.serialization import (
    Auth,
    Close,
    Connect,
    Exists,
    GetChildren,
    GetChildren2,
    Ping,
    PingInstance,
    ReplyHeader,
    SASL,
    Transaction,
    Watch,
    int_struct,
)
from kazoo.protocol.states import (
    Callback,
    KeeperState,
    WatchedEvent,
    EVENT_TYPE_MAP,
)
from kazoo.retry import (
    ForceRetryError,
    RetryFailedError,
)

try:
    import puresasl
    import puresasl.client

    PURESASL_AVAILABLE = True
except ImportError:
    PURESASL_AVAILABLE = False


log = logging.getLogger(__name__)


# Special testing hook objects used to force a session expired error as
# if it came from the server
_SESSION_EXPIRED = object()
_CONNECTION_DROP = object()

STOP_CONNECTING = object()

CREATED_EVENT = 1
DELETED_EVENT = 2
CHANGED_EVENT = 3
CHILD_EVENT = 4

WATCH_XID = -1
PING_XID = -2
AUTH_XID = -4

CLOSE_RESPONSE = Close.type

if sys.version_info > (3,):  # pragma: nocover

    def buffer(obj, offset=0):
        return memoryview(obj)[offset:]

    advance_iterator = next
else:  # pragma: nocover

    def advance_iterator(it):
        return it.next()


class RWPinger(object):
    """A Read/Write Server Pinger Iterable

    This object is initialized with the hosts iterator object and the
    socket creation function. Anytime `next` is called on its iterator
    it yields either False, or a host, port tuple if it found a r/w
    capable Zookeeper node.

    After the first run-through of hosts, an exponential back-off delay
    is added before the next run. This delay is tracked internally and
    the iterator will yield False if called too soon.

    """

    def __init__(self, hosts, connection_func, socket_handling):
        self.hosts = hosts
        self.connection = connection_func
        self.last_attempt = None
        self.socket_handling = socket_handling

    def __iter__(self):
        if not self.last_attempt:
            self.last_attempt = time.time()
        delay = 0.5
        while True:
            yield self._next_server(delay)

    def _next_server(self, delay):
        jitter = random.randint(0, 100) / 100.0
        while time.time() < self.last_attempt + delay + jitter:
            # Skip rw ping checks if its too soon
            return False
        for host, port in self.hosts:
            log.debug("Pinging server for r/w: %s:%s", host, port)
            self.last_attempt = time.time()
            try:
                with self.socket_handling():
                    sock = self.connection((host, port))
                    sock.sendall(b"isro")
                    result = sock.recv(8192)
                    sock.close()
                    if result == b"rw":
                        return (host, port)
                    else:
                        return False
            except ConnectionDropped:
                return False

            # Add some jitter between host pings
            while time.time() < self.last_attempt + jitter:
                return False
        delay *= 2


class RWServerAvailable(Exception):
    """Thrown if a RW Server becomes available"""


class ConnectionHandler(object):
    """Zookeeper connection handler"""

    def __init__(self, client, retry_sleeper, logger=None, sasl_options=None):
        self.client = client
        self.handler = client.handler
        self.retry_sleeper = retry_sleeper
        self.logger = logger or log

        # Our event objects
        self.connection_closed = client.handler.event_object()
        self.connection_closed.set()
        self.connection_stopped = client.handler.event_object()
        self.connection_stopped.set()
        self.ping_outstanding = client.handler.event_object()

        self._read_sock = None
        self._write_sock = None

        self._socket = None
        self._xid = None
        self._rw_server = None
        self._ro_mode = False

        self._connection_routine = None

        self.sasl_options = sasl_options
        self.sasl_cli = None

    # This is instance specific to avoid odd thread bug issues in Python
    # during shutdown global cleanup
    @contextmanager
    def _socket_error_handling(self):
        try:
            yield
        except (socket.error, select.error) as e:
            err = getattr(e, "strerror", e)
            raise ConnectionDropped("socket connection error: %s" % (err,))

    def start(self):
        """Start the connection up"""
        if self.connection_closed.is_set():
            rw_sockets = self.handler.create_socket_pair()
            self._read_sock, self._write_sock = rw_sockets
            self.connection_closed.clear()
        if self._connection_routine:
            raise Exception(
                "Unable to start, connection routine already " "active."
            )
        self._connection_routine = self.handler.spawn(self.zk_loop)

    def stop(self, timeout=None):
        """Ensure the writer has stopped, wait to see if it does."""
        self.connection_stopped.wait(timeout)
        if self._connection_routine:
            self._connection_routine.join()
            self._connection_routine = None
        return self.connection_stopped.is_set()

    def close(self):
        """Release resources held by the connection

        The connection can be restarted afterwards.
        """
        if not self.connection_stopped.is_set():
            raise Exception("Cannot close connection until it is stopped")
        self.connection_closed.set()
        ws, rs = self._write_sock, self._read_sock
        self._write_sock = self._read_sock = None
        if ws is not None:
            ws.close()
        if rs is not None:
            rs.close()

    def _server_pinger(self):
        """Returns a server pinger iterable, that will ping the next
        server in the list, and apply a back-off between attempts."""
        return RWPinger(
            self.client.hosts,
            self.handler.create_connection,
            self._socket_error_handling,
        )

    def _read_header(self, timeout):
        b = self._read(4, timeout)
        length = int_struct.unpack(b)[0]
        b = self._read(length, timeout)
        header, offset = ReplyHeader.deserialize(b, 0)
        return header, b, offset

    def _read(self, length, timeout):
        msgparts = []
        remaining = length
        with self._socket_error_handling():
            while remaining > 0:
                # Because of SSL framing, a select may not return when using
                # an SSL socket because the underlying physical socket may not
                # have anything to select, but the wrapped object may still
                # have something to read as it has previously gotten enough
                # data from the underlying socket.
                if (
                    hasattr(self._socket, "pending")
                    and self._socket.pending() > 0
                ):
                    pass
                else:
                    s = self.handler.select([self._socket], [], [], timeout)[0]
                    if not s:  # pragma: nocover
                        # If the read list is empty, we got a timeout. We don't
                        # have to check wlist and xlist as we don't set any
                        raise self.handler.timeout_exception(
                            "socket time-out during read"
                        )
                try:
                    chunk = self._socket.recv(remaining)
                except ssl.SSLError as e:
                    if e.errno in (
                        ssl.SSL_ERROR_WANT_READ,
                        ssl.SSL_ERROR_WANT_WRITE,
                    ):
                        continue
                    else:
                        raise
                if chunk == b"":
                    raise ConnectionDropped("socket connection broken")
                msgparts.append(chunk)
                remaining -= len(chunk)
            return b"".join(msgparts)

    def _invoke(self, timeout, request, xid=None):
        """A special writer used during connection establishment
        only"""
        self._submit(request, timeout, xid)
        zxid = None
        if xid:
            header, buffer, offset = self._read_header(timeout)
            if header.xid != xid:
                raise RuntimeError(
                    "xids do not match, expected %r " "received %r",
                    xid,
                    header.xid,
                )
            if header.zxid > 0:
                zxid = header.zxid
            if header.err:
                callback_exception = EXCEPTIONS[header.err]()
                self.logger.debug(
                    "Received error(xid=%s) %r", xid, callback_exception
                )
                raise callback_exception
            return zxid

        msg = self._read(4, timeout)
        length = int_struct.unpack(msg)[0]
        msg = self._read(length, timeout)

        if hasattr(request, "deserialize"):
            try:
                obj, _ = request.deserialize(msg, 0)
            except Exception:
                self.logger.exception(
                    "Exception raised during deserialization "
                    "of request: %s",
                    request,
                )

                # raise ConnectionDropped so connect loop will retry
                raise ConnectionDropped("invalid server response")
            self.logger.log(BLATHER, "Read response %s", obj)
            return obj, zxid

        return zxid

    def _submit(self, request, timeout, xid=None):
        """Submit a request object with a timeout value and optional
        xid"""
        b = bytearray()
        if xid:
            b.extend(int_struct.pack(xid))
        if request.type:
            b.extend(int_struct.pack(request.type))
        b += request.serialize()
        self.logger.log(
            (BLATHER if isinstance(request, Ping) else logging.DEBUG),
            "Sending request(xid=%s): %s",
            xid,
            request,
        )
        self._write(int_struct.pack(len(b)) + b, timeout)

    def _write(self, msg, timeout):
        """Write a raw msg to the socket"""
        sent = 0
        msg_length = len(msg)
        with self._socket_error_handling():
            while sent < msg_length:
                s = self.handler.select([], [self._socket], [], timeout)[1]
                if not s:  # pragma: nocover
                    # If the write list is empty, we got a timeout. We don't
                    # have to check rlist and xlist as we don't set any
                    raise self.handler.timeout_exception(
                        "socket time-out" " during write"
                    )
                msg_slice = buffer(msg, sent)
                try:
                    bytes_sent = self._socket.send(msg_slice)
                except ssl.SSLError as e:
                    if e.errno in (
                        ssl.SSL_ERROR_WANT_READ,
                        ssl.SSL_ERROR_WANT_WRITE,
                    ):
                        continue
                    else:
                        raise
                if not bytes_sent:
                    raise ConnectionDropped("socket connection broken")
                sent += bytes_sent

    def _read_watch_event(self, buffer, offset):
        client = self.client
        watch, offset = Watch.deserialize(buffer, offset)
        path = watch.path

        self.logger.debug("Received EVENT: %s", watch)

        watchers = []

        if watch.type in (CREATED_EVENT, CHANGED_EVENT):
            watchers.extend(client._data_watchers.pop(path, []))
        elif watch.type == DELETED_EVENT:
            watchers.extend(client._data_watchers.pop(path, []))
            watchers.extend(client._child_watchers.pop(path, []))
        elif watch.type == CHILD_EVENT:
            watchers.extend(client._child_watchers.pop(path, []))
        else:
            self.logger.warn("Received unknown event %r", watch.type)
            return

        # Strip the chroot if needed
        path = client.unchroot(path)
        ev = WatchedEvent(EVENT_TYPE_MAP[watch.type], client._state, path)

        # Last check to ignore watches if we've been stopped
        if client._stopped.is_set():
            return

        # Dump the watchers to the watch thread
        for watch in watchers:
            client.handler.dispatch_callback(Callback("watch", watch, (ev,)))

    def _read_response(self, header, buffer, offset):
        client = self.client
        request, async_object, xid = client._pending.popleft()
        if header.zxid and header.zxid > 0:
            client.last_zxid = header.zxid
        if header.xid != xid:
            exc = RuntimeError(
                "xids do not match, expected %r " "received %r",
                xid,
                header.xid,
            )
            async_object.set_exception(exc)
            raise exc

        # Determine if its an exists request and a no node error
        exists_error = (
            header.err == NoNodeError.code and request.type == Exists.type
        )

        # Set the exception if its not an exists error
        if header.err and not exists_error:
            callback_exception = EXCEPTIONS[header.err]()
            self.logger.debug(
                "Received error(xid=%s) %r", xid, callback_exception
            )
            if async_object:
                async_object.set_exception(callback_exception)
        elif request and async_object:
            if exists_error:
                # It's a NoNodeError, which is fine for an exists
                # request
                async_object.set(None)
            else:
                try:
                    response = request.deserialize(buffer, offset)
                except Exception as exc:
                    self.logger.exception(
                        "Exception raised during deserialization "
                        "of request: %s",
                        request,
                    )
                    async_object.set_exception(exc)
                    return
                self.logger.debug(
                    "Received response(xid=%s): %r", xid, response
                )

                # We special case a Transaction as we have to unchroot things
                if request.type == Transaction.type:
                    response = Transaction.unchroot(client, response)

                async_object.set(response)

            # Determine if watchers should be registered
            watcher = getattr(request, "watcher", None)
            if not client._stopped.is_set() and watcher:
                if isinstance(request, (GetChildren, GetChildren2)):
                    client._child_watchers[request.path].add(watcher)
                else:
                    client._data_watchers[request.path].add(watcher)

        if isinstance(request, Close):
            self.logger.log(BLATHER, "Read close response")
            return CLOSE_RESPONSE

    def _read_socket(self, read_timeout):
        """Called when there's something to read on the socket"""
        client = self.client

        header, buffer, offset = self._read_header(read_timeout)
        if header.xid == PING_XID:
            self.logger.log(BLATHER, "Received Ping")
            self.ping_outstanding.clear()
        elif header.xid == AUTH_XID:
            self.logger.log(BLATHER, "Received AUTH")

            request, async_object, xid = client._pending.popleft()
            if header.err:
                async_object.set_exception(AuthFailedError())
                client._session_callback(KeeperState.AUTH_FAILED)
            else:
                async_object.set(True)
        elif header.xid == WATCH_XID:
            self._read_watch_event(buffer, offset)
        else:
            self.logger.log(BLATHER, "Reading for header %r", header)

            return self._read_response(header, buffer, offset)

    def _send_request(self, read_timeout, connect_timeout):
        """Called when we have something to send out on the socket"""
        client = self.client
        try:
            request, async_object = client._queue[0]
        except IndexError:
            # Not actually something on the queue, this can occur if
            # something happens to cancel the request such that we
            # don't clear the socket below after sending
            try:
                # Clear possible inconsistence (no request in the queue
                # but have data in the read socket), which causes cpu to spin.
                self._read_sock.recv(1)
            except OSError:
                pass
            return

        # Special case for testing, if this is a _SessionExpire object
        # then throw a SessionExpiration error as if we were dropped
        if request is _SESSION_EXPIRED:
            raise SessionExpiredError("Session expired: Testing")
        if request is _CONNECTION_DROP:
            raise ConnectionDropped("Connection dropped: Testing")

        # Special case for auth packets
        if request.type == Auth.type:
            xid = AUTH_XID
        else:
            self._xid = (self._xid % 2147483647) + 1
            xid = self._xid

        self._submit(request, connect_timeout, xid)
        client._queue.popleft()
        self._read_sock.recv(1)
        client._pending.append((request, async_object, xid))

    def _send_ping(self, connect_timeout):
        self.ping_outstanding.set()
        self._submit(PingInstance, connect_timeout, PING_XID)

        # Determine if we need to check for a r/w server
        if self._ro_mode:
            result = advance_iterator(self._ro_mode)
            if result:
                self._rw_server = result
                raise RWServerAvailable()

    def zk_loop(self):
        """Main Zookeeper handling loop"""
        self.logger.log(BLATHER, "ZK loop started")

        self.connection_stopped.clear()

        retry = self.retry_sleeper.copy()
        try:
            while not self.client._stopped.is_set():
                # If the connect_loop returns STOP_CONNECTING, stop retrying
                if retry(self._connect_loop, retry) is STOP_CONNECTING:
                    break
        except RetryFailedError:
            self.logger.warning(
                "Failed connecting to Zookeeper "
                "within the connection retry policy."
            )
        finally:
            self.connection_stopped.set()
            self.client._session_callback(KeeperState.CLOSED)
            self.logger.log(BLATHER, "Connection stopped")

    def _expand_client_hosts(self):
        # Expand the entire list in advance so we can randomize it if needed
        host_ports = []
        for host, port in self.client.hosts:
            try:
                host = host.strip()
                for rhost in socket.getaddrinfo(
                    host, port, 0, 0, socket.IPPROTO_TCP
                ):
                    host_ports.append((host, rhost[4][0], rhost[4][1]))
            except socket.gaierror as e:
                # Skip hosts that don't resolve
                self.logger.warning("Cannot resolve %s: %s", host, e)
                pass
        if self.client.randomize_hosts:
            random.shuffle(host_ports)
        return host_ports

    def _connect_loop(self, retry):
        # Iterate through the hosts a full cycle before starting over
        status = None
        host_ports = self._expand_client_hosts()

        # Check for an empty hostlist, indicating none resolved
        if len(host_ports) == 0:
            raise ForceRetryError("No host resolved. Reconnecting")

        for host, hostip, port in host_ports:
            if self.client._stopped.is_set():
                status = STOP_CONNECTING
                break
            status = self._connect_attempt(host, hostip, port, retry)
            if status is STOP_CONNECTING:
                break

        if status is STOP_CONNECTING:
            return STOP_CONNECTING
        else:
            raise ForceRetryError("Reconnecting")

    def _connect_attempt(self, host, hostip, port, retry):
        client = self.client
        KazooTimeoutError = self.handler.timeout_exception

        self._socket = None

        # Were we given a r/w server? If so, use that instead
        if self._rw_server:
            self.logger.log(
                BLATHER, "Found r/w server to use, %s:%s", host, port
            )
            host, port = self._rw_server
            self._rw_server = None

        if client._state != KeeperState.CONNECTING:
            client._session_callback(KeeperState.CONNECTING)

        try:
            self._xid = 0
            read_timeout, connect_timeout = self._connect(host, hostip, port)
            read_timeout = read_timeout / 1000.0
            connect_timeout = connect_timeout / 1000.0
            retry.reset()
            self.ping_outstanding.clear()
            last_send = time.time()
            with self._socket_error_handling():
                while not self.client._stopped.is_set():
                    # Watch for something to read or send
                    jitter_time = random.randint(1, 40) / 100.0
                    deadline = last_send + read_timeout / 2.0 - jitter_time
                    # Ensure our timeout is positive
                    timeout = max([deadline - time.time(), jitter_time])
                    s = self.handler.select(
                        [self._socket, self._read_sock], [], [], timeout
                    )[0]

                    if not s:
                        if self.ping_outstanding.is_set():
                            self.ping_outstanding.clear()
                            raise ConnectionDropped(
                                "outstanding heartbeat ping not received"
                            )
                    else:
                        if self._socket in s:
                            response = self._read_socket(read_timeout)
                            if response == CLOSE_RESPONSE:
                                break
                        # Check if any requests need sending before proceeding
                        # to process more responses.  Otherwise the responses
                        # may choke out the requests.  See PR#633.
                        if self._read_sock in s:
                            self._send_request(read_timeout, connect_timeout)
                            # Requests act as implicit pings.
                            last_send = time.time()
                            continue

                    if time.time() >= deadline:
                        self._send_ping(connect_timeout)
                        last_send = time.time()
            self.logger.info("Closing connection to %s:%s", host, port)
            client._session_callback(KeeperState.CLOSED)
            return STOP_CONNECTING
        except (ConnectionDropped, KazooTimeoutError) as e:
            if isinstance(e, ConnectionDropped):
                self.logger.warning("Connection dropped: %s", e)
            else:
                self.logger.warning("Connection time-out: %s", e)
            if client._state != KeeperState.CONNECTING:
                self.logger.warning("Transition to CONNECTING")
                client._session_callback(KeeperState.CONNECTING)
        except AuthFailedError as err:
            retry.reset()
            self.logger.warning("AUTH_FAILED closing: %s", err)
            client._session_callback(KeeperState.AUTH_FAILED)
            return STOP_CONNECTING
        except SessionExpiredError:
            retry.reset()
            self.logger.warning("Session has expired")
            client._session_callback(KeeperState.EXPIRED_SESSION)
        except RWServerAvailable:
            retry.reset()
            self.logger.warning("Found a RW server, dropping connection")
            client._session_callback(KeeperState.CONNECTING)
        except Exception:
            self.logger.exception("Unhandled exception in connection loop")
            raise
        finally:
            if self._socket is not None:
                self._socket.close()

    def _connect(self, host, hostip, port):
        client = self.client
        self.logger.info(
            "Connecting to %s(%s):%s, use_ssl: %r",
            host,
            hostip,
            port,
            self.client.use_ssl,
        )

        self.logger.log(
            BLATHER,
            "    Using session_id: %r session_passwd: %s",
            client._session_id,
            hexlify(client._session_passwd),
        )

        with self._socket_error_handling():
            self._socket = self.handler.create_connection(
                address=(hostip, port),
                timeout=client._session_timeout / 1000.0,
                use_ssl=self.client.use_ssl,
                keyfile=self.client.keyfile,
                certfile=self.client.certfile,
                ca=self.client.ca,
                keyfile_password=self.client.keyfile_password,
                verify_certs=self.client.verify_certs,
            )

        self._socket.setblocking(0)

        connect = Connect(
            0,
            client.last_zxid,
            client._session_timeout,
            client._session_id or 0,
            client._session_passwd,
            client.read_only,
        )

        connect_result, zxid = self._invoke(
            client._session_timeout / 1000.0 / len(client.hosts), connect
        )

        if connect_result.time_out <= 0:
            raise SessionExpiredError("Session has expired")

        if zxid:
            client.last_zxid = zxid

        # Load return values
        client._session_id = connect_result.session_id
        client._protocol_version = connect_result.protocol_version
        negotiated_session_timeout = connect_result.time_out
        connect_timeout = negotiated_session_timeout / len(client.hosts)
        read_timeout = negotiated_session_timeout * 2.0 / 3.0
        client._session_passwd = connect_result.passwd

        self.logger.log(
            BLATHER,
            "Session created, session_id: %r session_passwd: %s\n"
            "    negotiated session timeout: %s\n"
            "    connect timeout: %s\n"
            "    read timeout: %s",
            client._session_id,
            hexlify(client._session_passwd),
            negotiated_session_timeout,
            connect_timeout,
            read_timeout,
        )

        if connect_result.read_only:
            client._session_callback(KeeperState.CONNECTED_RO)
            self._ro_mode = iter(self._server_pinger())
        else:
            client._session_callback(KeeperState.CONNECTED)
            self._ro_mode = None

        if self.sasl_options is not None:
            self._authenticate_with_sasl(host, connect_timeout / 1000.0)

        # Get a copy of the auth data before iterating, in case it is
        # changed.
        client_auth_data_copy = copy.copy(client.auth_data)

        for scheme, auth in client_auth_data_copy:
            ap = Auth(0, scheme, auth)
            zxid = self._invoke(connect_timeout / 1000.0, ap, xid=AUTH_XID)
            if zxid:
                client.last_zxid = zxid

        return read_timeout, connect_timeout

    def _authenticate_with_sasl(self, host, timeout):
        """Establish a SASL authenticated connection to the server."""
        if not PURESASL_AVAILABLE:
            raise SASLException("Missing SASL support")

        if "service" not in self.sasl_options:
            self.sasl_options["service"] = "zookeeper"

        # NOTE: Zookeeper hardcoded the domain for Digest authentication
        # instead of using the hostname. See
        # zookeeper/util/SecurityUtils.java#L74 and Server/Client
        # initializations.
        if self.sasl_options["mechanism"] == "DIGEST-MD5":
            host = "zk-sasl-md5"

        sasl_cli = self.client.sasl_cli = puresasl.client.SASLClient(
            host=host, **self.sasl_options
        )

        # Inititalize the process with an empty challenge token
        challenge = None
        xid = 0

        while True:
            if sasl_cli.complete:
                break

            try:
                response = sasl_cli.process(challenge=challenge)
            except puresasl.SASLError as err:
                raise SASLException("library error") from err
            except puresasl.SASLProtocolException as exc:
                raise AuthFailedError("protocol error") from exc
            except Exception as exc:
                raise AuthFailedError("Unknown error") from exc

            if sasl_cli.complete and not response:
                break
            elif response is None:
                response = b""

            xid = (xid % 2147483647) + 1

            request = SASL(response)
            self._submit(request, timeout, xid)

            try:
                header, buffer, offset = self._read_header(timeout)
            except ConnectionDropped as exc:
                # Zookeeper simply drops connections with failed authentication
                raise AuthFailedError("Connection dropped in SASL") from exc

            if header.xid != xid:
                raise RuntimeError(
                    "xids do not match, expected %r " "received %r",
                    xid,
                    header.xid,
                )

            if header.zxid > 0:
                self.client.last_zxid = header.zxid

            if header.err:
                callback_exception = EXCEPTIONS[header.err]()
                self.logger.debug(
                    "Received error(xid=%s) %r", xid, callback_exception
                )
                raise callback_exception

            challenge, _ = SASL.deserialize(buffer, offset)

        # If we made it here, authentication is ok, and we are connected.
        # Remove sensible information from the object.
        sasl_cli.dispose()
