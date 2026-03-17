import base64
import hashlib
import logging
import os
import socket
import threading
from io import BytesIO as StringIO
from typing import Any, Callable

from ncclient.capabilities import Capabilities
from ncclient.logging_ import SessionLoggerAdapter
from ncclient.transport.errors import AuthenticationError, SSHError, SSHUnknownHostError
from ncclient.transport.parser import DefaultXMLParser
from ncclient.transport.session import Session
from ssh.channel import Channel
from ssh.exceptions import AuthenticationDenied, ChannelOpenFailure, KeyImportError
from ssh.exceptions import SSHError as LibSSHError
from ssh.key import import_privkey_file, import_privkey_base64
from ssh.options import HOST, KNOWNHOSTS, USER
from ssh.session import Session as LSession

logger = logging.getLogger("ncclient.transport.libssh")

PORT_NETCONF_DEFAULT = 830
BUF_SIZE = 4096


def default_unknown_host_cb(host: str, fingerprint: str) -> bool:
    """
    An unknown host callback returns True if it finds the key acceptable, and False if not. This default
    callback always returns False, which would lead to connect() raising a SSHUnknownHost exception. Supply
    another valid callback if you need to verify the host key programmatically.

    host - the hostname or IP address of the host
    fingerprint - hex string representing the host key fingerprint,
        colon-delimited e.g. "4b:69:6c:72:6f:79:20:77:61:73:20:68:65:72:65:21"
    """
    return False


class LibSSHSession(Session):

    _buffer: StringIO
    _channel: Channel | None
    _closing: threading.Event
    _connected: bool
    _device_handler: Any
    _host: str | None
    _logger: SessionLoggerAdapter
    _receiver_thread: threading.Thread | None
    _session: LSession | None
    _socket: socket.socket | None
    _socket_r: socket.socket
    _socket_w: socket.socket
    parser: DefaultXMLParser

    def __init__(self, device_handler):
        capabilities = Capabilities(device_handler.get_capabilities())
        Session.__init__(self, capabilities)
        self._host = None
        self._connected = False
        self._socket = None
        self._channel = None
        self._session = None
        self._buffer = StringIO()
        self._device_handler = device_handler
        self._message_list = []
        self._closing = threading.Event()
        self.parser = DefaultXMLParser(self)
        self.logger = SessionLoggerAdapter(logger, {"session": self})
        self._socket_r, self._socket_w = socket.socketpair()
        self._receiver_thread = None

    def _receiver_loop(self, socket_w: socket.socket, channel):
        """
        The receiver loop reads data from the SSH channel and writes it to the transport socket.

        ### Parameters:
        - socket_w: The writable end of the socket pair used for communication.
        - channel: The SSH channel to read data from.
        """
        while True:
            try:
                data = channel.read()
            except LibSSHError:
                logger.error("Channel read error")
                break
            if data[0] == 0:
                # EOF received
                self._closing.set()
                break
            socket_w.send(data[1])
        socket_w.close()

    def connect(
        self,
        host: str,
        port: int = PORT_NETCONF_DEFAULT,
        username: str | None = None,
        password: str | None = None,
        key_filename: str | None = None,
        key_base64: bytes | None = None,
        key_passphrase: bytes | None = None,
        allow_agent: bool = False,
        hostkey_verify: bool = True,
        hostkey_b64: str | None = None,
        timeout: int | None = None,
        unknown_host_cb: Callable = default_unknown_host_cb,
        bind_addr: str | None = None,
    ):
        """
        Connect to the SSH server and establish a NETCONF session.

        ### Parameters:
        - host: The hostname or IP address of the SSH server.
        - port: The port number to connect to (default is 830).
        - username: The username for authentication.
        - password: The password for authentication.
        - key_filename: Path to the private key file for public key authentication.
        - key_base64: Base64-encoded private key for public key authentication.
        - key_passphrase: Passphrase for the private key, if required.
        - allow_agent: Whether to use the SSH agent for authentication.
        - hostkey_verify: Whether to verify the server's host key.
        - hostkey_b64: Base64-encoded public key of the server for host key verification.
        - timeout: Timeout for the connection attempt.
        - unknown_host_cb: Callback function for handling unknown hosts.
        - bind_addr: Local address to bind the socket to.
        """
        for i in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, _, sa = i
            try:
                sock = socket.socket(af, socktype, proto)
            except socket.error:
                continue
            try:
                if bind_addr:
                    sock.bind((bind_addr, 0))
                sock.settimeout(timeout)
                sock.connect(sa)
            except socket.error:
                sock.close()
                continue
            break

        else:
            raise SSHError(f"Could not open socket to {host}:{port}")

        if not username:
            username = os.getenv("USER") or os.getenv("LOGNAME") or "root"
        if not username:
            raise AuthenticationError(
                "Username must be provided or set in environment variables"
            )

        self._session = LSession()
        assert self._session is not None

        self._session.options_set(HOST, host)
        self._session.options_set(USER, username)
        self._session.options_set(KNOWNHOSTS, os.path.expanduser("~/.ssh/known_hosts"))
        self._session.options_set_port(port)
        self._session.set_socket(sock)
        self._session.connect()

        if hostkey_verify:
            is_known_host = False
            server_pubkey = self._session.get_server_publickey()
            server_pubkey_base64 = server_pubkey.export_pubkey_base64().decode()
            if hostkey_b64 is not None:
                # Check if the provided hostkey matches the server's public key
                if server_pubkey_base64 == hostkey_b64:
                    is_known_host = True
            else:
                # check known hosts file
                if self._session.is_server_known():
                    is_known_host = True

            def openssh_fingerprint_md5(base64_key: str) -> str:
                """
                Calculate md5 fingerprint of the base64-encoded public key.

                ### Parameters:
                - base64_key: Base64-encoded public key string.

                ### Returns:
                - Fingerprint as a colon-separated hex string.
                """
                key_bytes = base64.b64decode(base64_key)
                digest = hashlib.md5(key_bytes).hexdigest()
                fingerprint = ":".join(
                    digest[i : i + 2] for i in range(0, len(digest), 2)
                )
                return fingerprint

            if not is_known_host:
                fingerprint = openssh_fingerprint_md5(server_pubkey_base64)
                if not unknown_host_cb(host, fingerprint):
                    # If the host key is not known, raise an error
                    raise SSHUnknownHostError(host, fingerprint)

        def auth(
            sess: LSession,
            user: str,
            passwd: str | None,
            keyfile: str | None,
            keybase64: bytes | None,
            agent: bool,
            passphrase: bytes | None,
        ):
            """
            Authenticate the session using the provided credentials.

            ### Parameters:
            - sess: The SSH session object.
            - user: Username.
            - passwd: Password.
            - keyfile: Path to the private key file.
            - agent: Whether to use the SSH agent for authentication.
            """
            if passphrase is None:
                passphrase = b""

            if user and passwd:
                try:
                    sess.userauth_password(user, passwd)
                    self.logger.debug(
                        "Password authentication successful for user %s", user
                    )
                    return
                except AuthenticationDenied:
                    self.logger.debug(
                        "Password authentication failed for user %s", user
                    )

            if keyfile:
                try:
                    privkey = import_privkey_file(keyfile)
                except KeyImportError:
                    self.logger.debug(
                        "Could not import private key from file %s", keyfile
                    )
                else:
                    try:
                        sess.userauth_publickey(privkey)
                        self.logger.debug(
                            "Public key authentication successful for user %s", user
                        )
                        return
                    except AuthenticationDenied:
                        self.logger.debug(
                            "Public key authentication failed for user %s", user
                        )

            if keybase64:
                try:
                    privkey = import_privkey_base64(keybase64, passphrase)
                except KeyImportError:
                    self.logger.debug("Could not import public key from base64 string")
                else:
                    try:
                        sess.userauth_publickey(privkey)
                        self.logger.debug(
                            "Public key authentication successful for user %s", user
                        )
                        return
                    except AuthenticationDenied:
                        self.logger.debug(
                            "Public key authentication failed for user %s", user
                        )

            if agent:
                try:
                    sess.userauth_agent(user)
                    self.logger.debug(
                        "Agent authentication successful for user %s", user
                    )
                    return
                except AuthenticationDenied:
                    self.logger.debug("Agent authentication failed for user %s", user)

            raise AuthenticationError(
                f"Authentication failed for user {user} on host {host}:{port}"
            )

        auth(
            self._session,
            username,
            password,
            key_filename,
            key_base64,
            allow_agent,
            key_passphrase,
        )

        try:
            self._channel = self._session.channel_new()
        except ChannelOpenFailure as e:
            raise SSHError(f"Could not open channel to {host}:{port}") from e

        assert self._channel is not None
        try:
            self._channel.open_session()
            self._channel.request_subsystem("netconf")
        except LibSSHError as e:
            raise SSHError(
                f"Could not open NETCONF session on channel to {host}:{port}"
            ) from e

        self._receiver_thread = threading.Thread(
            target=self._receiver_loop,
            args=(self._socket_w, self._channel),
            daemon=True,
        )
        self._receiver_thread.start()

        self._host = host
        self._socket = sock
        self._connected = True
        self._post_connect()

    def _send_ready(self):
        """
        Check if the session is ready to send data.

        ### Returns:
        - True if the session is connected and thus the channel is open, False otherwise.
        """
        assert self._channel is not None
        return self._channel.is_open()

    def _transport_read(self):
        """
        Read data from the transport layer.

        ### Returns:
        - Data read from the transport layer.
        """
        return self._socket_r.recv(BUF_SIZE)

    def _transport_write(self, data: bytes) -> int:
        """
        Write data to the transport layer.

        ### Parameters:
        - data: The data to be sent over the transport layer.

        ### Returns:
        - The number of bytes written to the transport layer.
        """
        assert self._channel is not None
        res = self._channel.write(data)
        return res[0]

    def _transport_register(self, selector, event):
        """
        Register the socket with the selector for the specified event.

        ### Parameters:
        - selector: The selector to register the socket with.
        - event: The event type to register for (e.g., read, write).
        """
        selector.register(self._socket_r, event)

    def close(self):
        """
        Close the SSH session and clean up resources.
        """
        assert self._channel is not None
        assert self._socket is not None
        self._closing.set()
        if not self._channel.is_closed():
            self._channel.close()
        # wait for the transport thread to close.
        if self.is_alive() and (self is not threading.current_thread()):
            self.join()
        self._channel = None
        self._socket.close()
        self._connected = False

    @property
    def host(self):
        """
        Host this session is connected to, or None if not connected.
        """
        if self._connected:
            return self._host
        return None
