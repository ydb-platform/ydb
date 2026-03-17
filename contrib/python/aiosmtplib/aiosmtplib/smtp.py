"""
Main SMTP client class.

Implements SMTP, ESMTP & Auth methods.
"""

import asyncio
import email.message
import socket
import ssl
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import Any, Literal
from .auth import (
    auth_crammd5_verify,
    auth_login_encode,
    auth_plain_encode,
    auth_xoauth2_encode,
)
from .email import (
    extract_recipients,
    extract_sender,
    flatten_message,
    parse_address,
    quote_address,
)
from .errors import (
    SMTPAuthenticationError,
    SMTPConnectError,
    SMTPConnectTimeoutError,
    SMTPException,
    SMTPHeloError,
    SMTPNotSupported,
    SMTPRecipientRefused,
    SMTPRecipientsRefused,
    SMTPResponseException,
    SMTPSenderRefused,
    SMTPServerDisconnected,
    SMTPTimeoutError,
    SMTPConnectResponseError,
)
from .esmtp import parse_esmtp_extensions
from .protocol import SMTPProtocol
from .response import SMTPResponse
from .typing import Default, SMTPStatus, SMTPTokenGenerator, SocketPathType


__all__ = ("SMTP", "SMTP_PORT", "SMTP_TLS_PORT", "SMTP_STARTTLS_PORT")

SMTP_PORT = 25
SMTP_TLS_PORT = 465
SMTP_STARTTLS_PORT = 587
DEFAULT_TIMEOUT = 60


class SMTP:
    """
    Main SMTP client class.

    Basic usage:

        >>> smtp = aiosmtplib.SMTP(hostname="127.0.0.1", port=1025)
        >>> sender = "root@localhost"
        >>> recipients = ["somebody@localhost"]
        >>> async def connect_and_send():
        ...     await smtp.connect()
        ...     return await smtp.sendmail(sender, recipients, "Hello")
        >>> asyncio.run(connect_and_send())
        ({}, 'OK')

    Keyword arguments can be provided either on :meth:`__init__` or when
    calling the :meth:`connect` method. Note that in both cases these options,
    except for ``timeout``, are saved for later use; subsequent calls to
    :meth:`connect` will use the same options, unless new ones are provided.
    ``timeout`` is saved for later use when provided on :meth:`__init__`, but
    not when calling the :meth:`connect` method.
    """

    # Preferred methods first
    AUTH_METHODS: tuple[str, ...] = (
        "cram-md5",
        "plain",
        "login",
    )

    def __init__(
        self,
        *,
        hostname: str | None = None,
        port: int | None = None,
        username: str | bytes | None = None,
        password: str | bytes | None = None,
        oauth_token_generator: SMTPTokenGenerator | None = None,
        local_hostname: str | None = None,
        source_address: tuple[str, int] | None = None,
        timeout: float | None = DEFAULT_TIMEOUT,
        use_tls: bool = False,
        start_tls: bool | None = None,
        validate_certs: bool = True,
        client_cert: str | None = None,
        client_key: str | None = None,
        tls_context: ssl.SSLContext | None = None,
        cert_bundle: str | None = None,
        socket_path: SocketPathType | None = None,
        sock: socket.socket | None = None,
    ) -> None:
        """
        :keyword hostname:  Server name (or IP) to connect to. defaults to "localhost".
        :keyword port: Server port. defaults ``465`` if ``use_tls`` is ``True``,
            ``587`` if ``start_tls`` is ``True``, or ``25`` otherwise.
        :keyword username:  Username to login as after connect.
        :keyword password:  Password for login after connect. Mutually exclusive
            with ``oauth_token_generator``.
        :keyword oauth_token_generator: An async callable that returns an OAuth2
            access token for XOAUTH2 authentication. Mutually exclusive with
            ``password``.
        :keyword local_hostname: The hostname of the client.  If specified, used as the
            FQDN of the local host in the HELO/EHLO command. Otherwise, the result of
            :func:`socket.getfqdn`.
        :keyword source_address: Takes a 2-tuple (host, port) for the socket to bind to
            as its source address before connecting. If the host is '' and port is 0,
            the OS default behavior will be used.
        :keyword timeout: Default timeout value for the connection, in seconds.
            defaults to 60.
        :keyword use_tls: If True, make the initial connection to the server
            over TLS/SSL. Mutually exclusive with ``start_tls``; if the server uses
            STARTTLS, ``use_tls`` should be ``False``.
        :keyword start_tls: Flag to initiate a STARTTLS upgrade on connect.
            If ``None`` (the default), upgrade will be initiated if supported by the
            server.
            If ``True``, and upgrade will be initiated regardless of server support.
            If ``False``, no upgrade will occur.
            Mutually exclusive with ``use_tls``.
        :keyword validate_certs: Determines if server certificates are
            validated. defaults to ``True``.
        :keyword client_cert: Path to client side certificate, for TLS.
        :keyword client_key: Path to client side key, for TLS.
        :keyword tls_context: An existing :py:class:`ssl.SSLContext`, for TLS.
            Mutually exclusive with ``client_cert``/``client_key``.
        :keyword cert_bundle: Path to certificate bundle, for TLS verification.
        :keyword socket_path: Path to a Unix domain socket. Not compatible with
            hostname or port. Accepts str, bytes, or a pathlike object.
        :keyword sock: An existing, connected socket object. If given, none of
            hostname, port, or socket_path should be provided.

        :raises ValueError: mutually exclusive options provided
        """
        self.protocol: SMTPProtocol | None = None
        self.transport: asyncio.BaseTransport | None = None

        # Kwarg defaults are provided here, and saved for connect.
        self.hostname = hostname
        self.port = port
        self._login_username = username
        self._login_password = password
        self._oauth_token_generator = oauth_token_generator
        self.local_hostname = local_hostname
        self.timeout = timeout
        self.use_tls = use_tls
        self._start_tls_on_connect = start_tls
        self.validate_certs = validate_certs
        self.client_cert = client_cert
        self.client_key = client_key
        self.tls_context = tls_context
        self.cert_bundle = cert_bundle
        self.socket_path = socket_path
        self.sock = sock
        self.source_address = source_address

        self.loop: asyncio.AbstractEventLoop | None = None
        self._connect_lock: asyncio.Lock | None = None
        self.last_helo_response: SMTPResponse | None = None
        self._last_ehlo_response: SMTPResponse | None = None
        self.esmtp_extensions: dict[str, str] = {}
        self.supports_esmtp = False
        self.server_auth_methods: list[str] = []
        self._sendmail_lock: asyncio.Lock | None = None

        self._validate_config()

    async def __aenter__(self) -> "SMTP":
        if not self.is_connected:
            await self.connect()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if isinstance(exc, (ConnectionError, TimeoutError)):
            self.close()
            return

        try:
            await self.quit()
        except (SMTPServerDisconnected, SMTPResponseException, SMTPTimeoutError):
            pass

    @property
    def is_connected(self) -> bool:
        """
        Check if our transport is still connected.
        """
        return bool(self.protocol is not None and self.protocol.is_connected)

    @property
    def last_ehlo_response(self) -> SMTPResponse | None:
        return self._last_ehlo_response

    @last_ehlo_response.setter
    def last_ehlo_response(self, response: SMTPResponse) -> None:
        """
        When setting the last EHLO response, parse the message for supported
        extensions and auth methods.
        """
        extensions, auth_methods = parse_esmtp_extensions(response.message)
        self._last_ehlo_response = response
        self.esmtp_extensions = extensions
        self.server_auth_methods = auth_methods
        self.supports_esmtp = True

    @property
    def is_ehlo_or_helo_needed(self) -> bool:
        """
        Check if we've already received a response to an EHLO or HELO command.
        """
        return self.last_ehlo_response is None and self.last_helo_response is None

    @property
    def supported_auth_methods(self) -> list[str]:
        """
        Get all AUTH methods supported by the both server and by us.
        """
        return [auth for auth in self.AUTH_METHODS if auth in self.server_auth_methods]

    def _update_settings_from_kwargs(
        self,
        hostname: str | Literal[Default.token] | None = Default.token,
        port: int | Literal[Default.token] | None = Default.token,
        username: str | bytes | Literal[Default.token] | None = Default.token,
        password: str | bytes | Literal[Default.token] | None = Default.token,
        oauth_token_generator: SMTPTokenGenerator
        | Literal[Default.token]
        | None = Default.token,
        local_hostname: str | Literal[Default.token] | None = Default.token,
        source_address: tuple[str, int] | Literal[Default.token] | None = Default.token,
        use_tls: bool | None = None,
        start_tls: bool | Literal[Default.token] | None = Default.token,
        validate_certs: bool | None = None,
        client_cert: str | Literal[Default.token] | None = Default.token,
        client_key: str | Literal[Default.token] | None = Default.token,
        tls_context: ssl.SSLContext | Literal[Default.token] | None = Default.token,
        cert_bundle: str | Literal[Default.token] | None = Default.token,
        socket_path: SocketPathType | Literal[Default.token] | None = Default.token,
        sock: socket.socket | Literal[Default.token] | None = Default.token,
    ) -> None:
        """Update our configuration from the kwargs provided.

        This method can be called multiple times.
        """
        if hostname is not Default.token:
            self.hostname = hostname
        if use_tls is not None:
            self.use_tls = use_tls
        if start_tls is not Default.token:
            self._start_tls_on_connect = start_tls
        if validate_certs is not None:
            self.validate_certs = validate_certs
        if port is not Default.token:
            self.port = port
        if username is not Default.token:
            self._login_username = username
        if password is not Default.token:
            self._login_password = password
        if oauth_token_generator is not Default.token:
            self._oauth_token_generator = oauth_token_generator

        if local_hostname is not Default.token:
            self.local_hostname = local_hostname
        if source_address is not Default.token:
            self.source_address = source_address
        if client_cert is not Default.token:
            self.client_cert = client_cert
        if client_key is not Default.token:
            self.client_key = client_key
        if tls_context is not Default.token:
            self.tls_context = tls_context
        if cert_bundle is not Default.token:
            self.cert_bundle = cert_bundle
        if socket_path is not Default.token:
            self.socket_path = socket_path
        if sock is not Default.token:
            self.sock = sock

    def _validate_config(self) -> None:
        if self._login_password is not None and self._oauth_token_generator is not None:
            raise ValueError(
                "password and oauth_token_generator are mutually exclusive"
            )

        if self._start_tls_on_connect and self.use_tls:
            raise ValueError("The start_tls and use_tls options are not compatible.")

        if self.tls_context is not None and self.client_cert is not None:
            raise ValueError(
                "Either a TLS context or a certificate/key must be provided"
            )

        if self.sock is not None and self.socket_path is not None:
            raise ValueError(
                "Either a socket or a socket path must be provided, not both"
            )

        if (self.sock or self.socket_path) and self.port is not None:
            raise ValueError("If using a socket, port is not required")

        if (self.sock or self.socket_path) and self.use_tls and self.hostname is None:
            raise ValueError("If using a socket with TLS, hostname is required")

        if self.local_hostname is not None and (
            "\r" in self.local_hostname or "\n" in self.local_hostname
        ):
            raise ValueError(
                "The local_hostname param contains prohibited newline characters"
            )

        if self.hostname is not None and (
            "\r" in self.hostname or "\n" in self.hostname
        ):
            raise ValueError(
                "The hostname param contains prohibited newline characters"
            )

    def _get_default_port(self) -> int:
        """
        Return an appropriate default port, based on options selected.
        """
        if self.use_tls:
            return SMTP_TLS_PORT
        elif self._start_tls_on_connect:
            return SMTP_STARTTLS_PORT

        return SMTP_PORT

    async def _get_default_local_hostname(self) -> str:
        return await asyncio.to_thread(socket.getfqdn)

    async def connect(
        self,
        *,
        hostname: str | Literal[Default.token] | None = Default.token,
        port: int | Literal[Default.token] | None = Default.token,
        username: str | bytes | Literal[Default.token] | None = Default.token,
        password: str | bytes | Literal[Default.token] | None = Default.token,
        oauth_token_generator: SMTPTokenGenerator
        | Literal[Default.token]
        | None = Default.token,
        local_hostname: str | Literal[Default.token] | None = Default.token,
        source_address: tuple[str, int] | Literal[Default.token] | None = Default.token,
        timeout: float | Literal[Default.token] | None = Default.token,
        use_tls: bool | None = None,
        start_tls: bool | Literal[Default.token] | None = Default.token,
        validate_certs: bool | None = None,
        client_cert: str | Literal[Default.token] | None = Default.token,
        client_key: str | Literal[Default.token] | None = Default.token,
        tls_context: ssl.SSLContext | Literal[Default.token] | None = Default.token,
        cert_bundle: str | Literal[Default.token] | None = Default.token,
        socket_path: SocketPathType | Literal[Default.token] | None = Default.token,
        sock: socket.socket | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Initialize a connection to the server. Options provided to
        :meth:`.connect` take precedence over those used to initialize the
        class.

        :keyword hostname:  Server name (or IP) to connect to. defaults to "localhost".
        :keyword port: Server port. defaults ``465`` if ``use_tls`` is ``True``,
            ``587`` if ``start_tls`` is ``True``, or ``25`` otherwise.
        :keyword username:  Username to login as after connect.
        :keyword password:  Password for login after connect. Mutually exclusive
            with ``oauth_token_generator``.
        :keyword oauth_token_generator: An async callable that returns an OAuth2
            access token for XOAUTH2 authentication. Mutually exclusive with
            ``password``.
        :keyword local_hostname: The hostname of the client.  If specified, used as the
            FQDN of the local host in the HELO/EHLO command. Otherwise, the result of
            :func:`socket.getfqdn`.
        :keyword source_address: Takes a 2-tuple (host, port) for the socket to bind to
            as its source address before connecting. If the host is '' and port is 0,
            the OS default behavior will be used.
        :keyword timeout: timeout value for the connection, in seconds. Defaults to 60.
        :keyword use_tls: If True, make the initial connection to the server
            over TLS/SSL. Mutually exclusive with ``start_tls``; if the server uses
            STARTTLS, ``use_tls`` should be ``False``.
        :keyword start_tls: Flag to initiate a STARTTLS upgrade on connect.
            If ``None`` (the default), upgrade will be initiated if supported by the
            server.
            If ``True``, and upgrade will be initiated regardless of server support.
            If ``False``, no upgrade will occur.
            Mutually exclusive with ``use_tls``.
        :keyword validate_certs: Determines if server certificates are
            validated. defaults to ``True``.
        :keyword client_cert: Path to client side certificate, for TLS.
        :keyword client_key: Path to client side key, for TLS.
        :keyword tls_context: An existing :py:class:`ssl.SSLContext`, for TLS.
            Mutually exclusive with ``client_cert``/``client_key``.
        :keyword cert_bundle: Path to certificate bundle, for TLS verification.
        :keyword socket_path: Path to a Unix domain socket. Not compatible with
            `port` or `sock`. Accepts str, bytes, or a pathlike object.
        :keyword sock: An existing, connected socket object. Not compatible with
            `port`, or `socket_path`. Passing a socket object will transfer
            control of it to the asyncio connection, and it will be closed when
            the client disconnects.

        :raises ValueError: mutually exclusive options provided
        """
        self._update_settings_from_kwargs(
            hostname=hostname,
            port=port,
            local_hostname=local_hostname,
            source_address=source_address,
            use_tls=use_tls,
            start_tls=start_tls,
            validate_certs=validate_certs,
            client_cert=client_cert,
            client_key=client_key,
            tls_context=tls_context,
            cert_bundle=cert_bundle,
            socket_path=socket_path,
            sock=sock,
            username=username,
            password=password,
            oauth_token_generator=oauth_token_generator,
        )
        self._validate_config()

        self.loop = asyncio.get_running_loop()
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        await self._connect_lock.acquire()

        # If we're not using a socket, default to port and hostname
        if self.sock is None and self.socket_path is None:
            if self.hostname is None:
                self.hostname = "localhost"

            if self.port is None and self.sock is None and self.socket_path is None:
                self.port = self._get_default_port()

        if self.local_hostname is None:
            self.local_hostname = await self._get_default_local_hostname()

        try:
            response = await self._create_connection(
                timeout=self.timeout if timeout is Default.token else timeout
            )
            await self._maybe_start_tls_on_connect()
            await self._maybe_login_on_connect()
        except Exception as exc:
            self.close()  # Reset our state to disconnected
            raise exc

        return response

    async def _create_connection(self, timeout: float | None) -> SMTPResponse:
        if self.loop is None:
            raise RuntimeError("No event loop set")

        protocol = SMTPProtocol(loop=self.loop, connection_lost_callback=self.close)

        tls_context: ssl.SSLContext | None = None
        ssl_handshake_timeout: float | None = None
        server_hostname: str | None = None
        if self.use_tls:
            tls_context = self._get_tls_context()
            ssl_handshake_timeout = timeout
            server_hostname = self.hostname

        if self.sock is not None:
            connect_coro = self.loop.create_connection(
                lambda: protocol,
                sock=self.sock,
                ssl=tls_context,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
            )
        elif self.socket_path is not None:
            connect_coro = self.loop.create_unix_connection(
                lambda: protocol,
                path=self.socket_path,  # type: ignore
                ssl=tls_context,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
            )
        else:
            if self.hostname is None:
                raise RuntimeError("No hostname provided; default should have been set")
            if self.port is None:
                raise RuntimeError("No port provided; default should have been set")

            connect_coro = self.loop.create_connection(
                lambda: protocol,
                host=self.hostname,
                port=self.port,
                ssl=tls_context,
                ssl_handshake_timeout=ssl_handshake_timeout,
                local_addr=self.source_address,
            )

        try:
            transport, _ = await asyncio.wait_for(connect_coro, timeout=timeout)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise SMTPConnectTimeoutError(
                f"Timed out connecting to {self.hostname} on port {self.port}"
            ) from exc
        except OSError as exc:
            raise SMTPConnectError(
                f"Error connecting to {self.hostname} on port {self.port}: {exc}"
            ) from exc

        self.protocol = protocol
        self.transport = transport

        try:
            response = await protocol.read_response(timeout=timeout)
        except SMTPServerDisconnected as exc:
            raise SMTPConnectError(
                f"Error connecting to {self.hostname} on port {self.port}: {exc}"
            ) from exc
        except SMTPTimeoutError as exc:
            raise SMTPConnectTimeoutError(
                "Timed out waiting for server ready message"
            ) from exc

        if response.code != SMTPStatus.ready:
            raise SMTPConnectResponseError(response.code, response.message)

        return response

    async def _maybe_start_tls_on_connect(self) -> None:
        """
        Depending on config, upgrade the connection via STARTTLS.
        """
        if self._start_tls_on_connect is True:
            await self.starttls()
        # If _start_tls_on_connect hasn't been set either way,
        # try to STARTTLS if supported, with graceful failure handling
        elif self._start_tls_on_connect is None:
            already_using_tls = self.get_transport_info("sslcontext") is not None
            if not (self.use_tls or already_using_tls):
                await self._ehlo_or_helo_if_needed()
                if self.supports_extension("starttls"):
                    await self.starttls()

    async def _maybe_login_on_connect(self) -> None:
        """
        Depending on config, login after connecting.
        """
        if self._oauth_token_generator is not None:
            if self._login_username is None:
                raise SMTPException(
                    "username is required when using oauth_token_generator"
                )
            await self._ehlo_or_helo_if_needed()
            if "xoauth2" not in self.server_auth_methods:
                raise SMTPException(
                    "oauth_token_generator provided but server does not support XOAUTH2"
                )
            token = await self._oauth_token_generator()
            await self.auth_xoauth2(self._login_username, token)
        elif self._login_username is not None:
            login_password = (
                self._login_password if self._login_password is not None else ""
            )
            await self.login(self._login_username, login_password)

    async def execute_command(
        self,
        *args: bytes,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Check that we're connected, if we got a timeout value, and then
        pass the command to the protocol.

        :raises SMTPServerDisconnected: connection lost
        """
        if self.protocol is None:
            raise SMTPServerDisconnected("Server not connected")

        try:
            response = await self.protocol.execute_command(
                *args, timeout=self.timeout if timeout is Default.token else timeout
            )
        except SMTPServerDisconnected:
            self.close()
            raise

        # If the server is unavailable, be nice and close the connection
        if response.code == SMTPStatus.domain_unavailable:
            self.close()

        return response

    def _get_tls_context(self) -> ssl.SSLContext:
        """
        Build an SSLContext object from the options we've been given.
        """
        if self.tls_context is not None:
            context = self.tls_context
        else:
            # SERVER_AUTH is what we want for a client side socket
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.check_hostname = bool(self.validate_certs)
            if self.validate_certs:
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_NONE

            if self.cert_bundle is not None:
                context.load_verify_locations(cafile=self.cert_bundle)

            if self.client_cert is not None:
                context.load_cert_chain(self.client_cert, keyfile=self.client_key)

        return context

    def close(self) -> None:
        """
        Closes the connection.
        """
        if self.transport is not None and not self.transport.is_closing():
            self.transport.close()

        if self._connect_lock is not None and self._connect_lock.locked():
            self._connect_lock.release()

        self.protocol = None
        self.transport = None

        # Reset ESMTP state
        self._reset_server_state()

    def get_transport_info(self, key: str) -> Any:
        """
        Get extra info from the transport.
        Supported keys:

            - ``peername``
            - ``socket``
            - ``sockname``
            - ``compression``
            - ``cipher``
            - ``peercert``
            - ``sslcontext``
            - ``sslobject``

        :raises SMTPServerDisconnected: connection lost
        """
        if not (self.is_connected and self.transport):
            raise SMTPServerDisconnected("Server not connected")

        return self.transport.get_extra_info(key)

    # Base SMTP commands #

    async def helo(
        self,
        *,
        hostname: str | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send the SMTP HELO command.
        Hostname to send for this command defaults to the FQDN of the local
        host.

        :raises SMTPHeloError: on unexpected server response code
        """
        if hostname is None:
            # Should already be set on connect
            if self.local_hostname is None:
                self.local_hostname = await self._get_default_local_hostname()

            hostname = self.local_hostname

        response = self.last_helo_response = await self.execute_command(
            b"HELO", hostname.encode("ascii"), timeout=timeout
        )

        if response.code != SMTPStatus.completed:
            raise SMTPHeloError(response.code, response.message)

        return response

    async def help(
        self, *, timeout: float | Literal[Default.token] | None = Default.token
    ) -> str:
        """
        Send the SMTP HELP command, which responds with help text.

        :raises SMTPResponseException: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        response = await self.execute_command(b"HELP", timeout=timeout)
        if response.code not in (
            SMTPStatus.system_status_ok,
            SMTPStatus.help_message,
            SMTPStatus.completed,
        ):
            raise SMTPResponseException(response.code, response.message)

        return response.message

    async def rset(
        self, *, timeout: float | Literal[Default.token] | None = Default.token
    ) -> SMTPResponse:
        """
        Send an SMTP RSET command, which resets the server's envelope
        (the envelope contains the sender, recipient, and mail data).

        :raises SMTPResponseException: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        response = await self.execute_command(b"RSET", timeout=timeout)
        if response.code != SMTPStatus.completed:
            raise SMTPResponseException(response.code, response.message)

        return response

    async def noop(
        self, *, timeout: float | Literal[Default.token] | None = Default.token
    ) -> SMTPResponse:
        """
        Send an SMTP NOOP command, which does nothing.

        :raises SMTPResponseException: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        response = await self.execute_command(b"NOOP", timeout=timeout)
        if response.code != SMTPStatus.completed:
            raise SMTPResponseException(response.code, response.message)

        return response

    async def vrfy(
        self,
        address: str,
        /,
        *,
        options: Iterable[str] | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send an SMTP VRFY command, which tests an address for validity.
        Not many servers support this command.

        :raises SMTPResponseException: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        if options is None:
            options = []

        parsed_address = parse_address(address)
        if any(option.lower() == "smtputf8" for option in options):
            if not self.supports_extension("smtputf8"):
                raise SMTPNotSupported("SMTPUTF8 is not supported by this server")
            addr_bytes = parsed_address.encode("utf-8")
        else:
            addr_bytes = parsed_address.encode("ascii")
        options_bytes = [option.encode("ascii") for option in options]

        response = await self.execute_command(
            b"VRFY", addr_bytes, *options_bytes, timeout=timeout
        )

        if response.code not in (
            SMTPStatus.completed,
            SMTPStatus.will_forward,
            SMTPStatus.cannot_vrfy,
        ):
            raise SMTPResponseException(response.code, response.message)

        return response

    async def expn(
        self,
        address: str,
        /,
        *,
        options: Iterable[str] | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send an SMTP EXPN command, which expands a mailing list.
        Not many servers support this command.

        :raises SMTPResponseException: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        if options is None:
            options = []

        parsed_address = parse_address(address)
        if any(option.lower() == "smtputf8" for option in options):
            if not self.supports_extension("smtputf8"):
                raise SMTPNotSupported("SMTPUTF8 is not supported by this server")
            addr_bytes = parsed_address.encode("utf-8")
        else:
            addr_bytes = parsed_address.encode("ascii")
        options_bytes = [option.encode("ascii") for option in options]

        response = await self.execute_command(
            b"EXPN", addr_bytes, *options_bytes, timeout=timeout
        )

        if response.code != SMTPStatus.completed:
            raise SMTPResponseException(response.code, response.message)

        return response

    async def quit(
        self, *, timeout: float | Literal[Default.token] | None = Default.token
    ) -> SMTPResponse:
        """
        Send the SMTP QUIT command, which closes the connection.
        Also closes the connection from our side after a response is received.

        :raises SMTPResponseException: on unexpected server response code
        """
        response = await self.execute_command(b"QUIT", timeout=timeout)
        if response.code != SMTPStatus.closing:
            raise SMTPResponseException(response.code, response.message)

        self.close()

        return response

    async def mail(
        self,
        sender: str,
        /,
        *,
        options: Iterable[str] | None = None,
        encoding: str = "ascii",
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send an SMTP MAIL command, which specifies the message sender and
        begins a new mail transfer session ("envelope").

        :raises SMTPSenderRefused: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        if options is None:
            options = []

        quoted_sender = quote_address(sender)
        addr_bytes = quoted_sender.encode(encoding)
        options_bytes = [option.encode("ascii") for option in options]

        response = await self.execute_command(
            b"MAIL", b"FROM:" + addr_bytes, *options_bytes, timeout=timeout
        )

        if response.code != SMTPStatus.completed:
            raise SMTPSenderRefused(response.code, response.message, sender)

        return response

    async def rcpt(
        self,
        recipient: str,
        /,
        *,
        options: Iterable[str] | None = None,
        encoding: str = "ascii",
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send an SMTP RCPT command, which specifies a single recipient for
        the message. This command is sent once per recipient and must be
        preceded by 'MAIL'.

        :raises SMTPRecipientRefused: on unexpected server response code
        """
        await self._ehlo_or_helo_if_needed()

        if options is None:
            options = []

        quoted_recipient = quote_address(recipient)
        addr_bytes = quoted_recipient.encode(encoding)
        options_bytes = [option.encode("ascii") for option in options]

        response = await self.execute_command(
            b"RCPT", b"TO:" + addr_bytes, *options_bytes, timeout=timeout
        )

        if response.code not in (SMTPStatus.completed, SMTPStatus.will_forward):
            raise SMTPRecipientRefused(response.code, response.message, recipient)

        return response

    async def data(
        self,
        message: str | bytes,
        /,
        *,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send an SMTP DATA command, followed by the message given.
        This method transfers the actual email content to the server.

        :raises SMTPDataError: on unexpected server response code
        :raises SMTPServerDisconnected: connection lost
        """
        if self.protocol is None:
            raise SMTPServerDisconnected("Connection lost")

        await self._ehlo_or_helo_if_needed()

        if timeout is Default.token:
            timeout = self.timeout

        if isinstance(message, str):
            message = message.encode("ascii")

        try:
            return await self.protocol.execute_data_command(message, timeout=timeout)
        except SMTPServerDisconnected:
            self.close()
            raise

    # ESMTP commands #

    async def ehlo(
        self,
        *,
        hostname: str | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Send the SMTP EHLO command.
        Hostname to send for this command defaults to the FQDN of the local
        host.

        :raises SMTPHeloError: on unexpected server response code
        """
        if hostname is None:
            # Should already be set on connect
            if self.local_hostname is None:
                self.local_hostname = await self._get_default_local_hostname()

            hostname = self.local_hostname

        response = await self.execute_command(
            b"EHLO", hostname.encode("ascii"), timeout=timeout
        )
        self.last_ehlo_response = response

        if response.code != SMTPStatus.completed:
            raise SMTPHeloError(response.code, response.message)

        return response

    def supports_extension(self, extension: str, /) -> bool:
        """
        Tests if the server supports the ESMTP service extension given.
        """
        return extension.lower() in self.esmtp_extensions

    async def _ehlo_or_helo_if_needed(self) -> None:
        """
        Call self.ehlo() and/or self.helo() if needed.

        If there has been no previous EHLO or HELO command this session, this
        method tries ESMTP EHLO first.
        """
        if self.is_ehlo_or_helo_needed:
            try:
                await self.ehlo()
            except SMTPHeloError as exc:
                if self.is_connected:
                    await self.helo()
                else:
                    raise exc

    def _reset_server_state(self) -> None:
        """
        Clear stored information about the server.
        """
        self.last_helo_response = None
        self._last_ehlo_response = None
        self.esmtp_extensions = {}
        self.supports_esmtp = False
        self.server_auth_methods = []

    async def starttls(
        self,
        *,
        server_hostname: str | None = None,
        validate_certs: bool | None = None,
        client_cert: str | Literal[Default.token] | None = Default.token,
        client_key: str | Literal[Default.token] | None = Default.token,
        cert_bundle: str | Literal[Default.token] | None = Default.token,
        tls_context: ssl.SSLContext | Literal[Default.token] | None = Default.token,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Puts the connection to the SMTP server into TLS mode.

        If there has been no previous EHLO or HELO command this session, this
        method tries ESMTP EHLO first.

        If the server supports TLS, this will encrypt the rest of the SMTP
        session. If you provide the keyfile and certfile parameters,
        the identity of the SMTP server and client can be checked (if
        validate_certs is True). You can also provide a custom SSLContext
        object. If no certs or SSLContext is given, and TLS config was
        provided when initializing the class, STARTTLS will use to that,
        otherwise it will use the Python defaults.

        :raises SMTPException: server does not support STARTTLS
        :raises SMTPServerDisconnected: connection lost
        :raises ValueError: invalid options provided
        """
        if self.protocol is None:
            raise SMTPServerDisconnected("Server not connected")

        if self.get_transport_info("sslcontext") is not None:
            raise SMTPException("Connection already using TLS")

        await self._ehlo_or_helo_if_needed()

        self._update_settings_from_kwargs(
            validate_certs=validate_certs,
            client_cert=client_cert,
            client_key=client_key,
            cert_bundle=cert_bundle,
            tls_context=tls_context,
        )
        self._validate_config()

        if server_hostname is None:
            server_hostname = self.hostname

        if timeout is Default.token:
            timeout = self.timeout

        tls_context = self._get_tls_context()

        if not self.supports_extension("starttls"):
            raise SMTPException("SMTP STARTTLS extension not supported by server.")

        try:
            response = await self.protocol.start_tls(
                tls_context, server_hostname=server_hostname, timeout=timeout
            )
        except SMTPServerDisconnected:
            self.close()
            raise

        # Update our transport reference
        self.transport = self.protocol.transport

        # RFC 3207 part 4.2:
        # The client MUST discard any knowledge obtained from the server, such
        # as the list of SMTP service extensions, which was not obtained from
        # the TLS negotiation itself.
        self._reset_server_state()

        return response

    # Auth commands

    async def login(
        self,
        username: str | bytes,
        password: str | bytes,
        /,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        Tries to login with supported auth methods.

        Some servers advertise authentication methods they don't really
        support, so if authentication fails, we continue until we've tried
        all methods.
        """
        await self._ehlo_or_helo_if_needed()

        if not self.supports_extension("auth"):
            if self.is_connected and self.get_transport_info("sslcontext") is None:
                raise SMTPException(
                    "The SMTP AUTH extension is not supported by this server. Try "
                    "connecting via TLS (or STARTTLS)."
                )
            raise SMTPException(
                "The SMTP AUTH extension is not supported by this server."
            )

        response: SMTPResponse | None = None
        exception: SMTPAuthenticationError | None = None
        for auth_name in self.supported_auth_methods:
            method_name = f"auth_{auth_name.replace('-', '')}"
            try:
                auth_method = getattr(self, method_name)
            except AttributeError as err:
                raise RuntimeError(
                    f"Missing handler for auth method {auth_name}"
                ) from err
            try:
                response = await auth_method(username, password, timeout=timeout)
            except SMTPAuthenticationError as exc:
                exception = exc
            else:
                # No exception means we're good
                break

        if response is None:
            raise exception or SMTPException("No suitable authentication method found.")

        return response

    async def auth_crammd5(
        self,
        username: str | bytes,
        password: str | bytes,
        /,
        *,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        CRAM-MD5 auth uses the password as a shared secret to MD5 the server's
        response.

        Example::

            250 AUTH CRAM-MD5
            auth cram-md5
            334 PDI0NjA5LjEwNDc5MTQwNDZAcG9wbWFpbC5TcGFjZS5OZXQ+
            dGltIGI5MTNhNjAyYzdlZGE3YTQ5NWI0ZTZlNzMzNGQzODkw

        """
        initial_response = await self.execute_command(
            b"AUTH", b"CRAM-MD5", timeout=timeout
        )

        if initial_response.code != SMTPStatus.auth_continue:
            raise SMTPAuthenticationError(
                initial_response.code, initial_response.message
            )

        verification_bytes = auth_crammd5_verify(
            username, password, initial_response.message
        )
        response = await self.execute_command(verification_bytes)

        if response.code != SMTPStatus.auth_successful:
            raise SMTPAuthenticationError(response.code, response.message)

        return response

    async def auth_plain(
        self,
        username: str | bytes,
        password: str | bytes,
        /,
        *,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        PLAIN auth encodes the username and password in one Base64 encoded
        string. No verification message is required.

        Example::

            220-esmtp.example.com
            AUTH PLAIN dGVzdAB0ZXN0AHRlc3RwYXNz
            235 ok, go ahead (#2.0.0)

        """
        encoded = auth_plain_encode(username, password)
        response = await self.execute_command(
            b"AUTH", b"PLAIN", encoded, timeout=timeout
        )

        if response.code != SMTPStatus.auth_successful:
            raise SMTPAuthenticationError(response.code, response.message)

        return response

    async def auth_login(
        self,
        username: str | bytes,
        password: str | bytes,
        /,
        *,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        LOGIN auth sends the Base64 encoded username and password in sequence.

        Example::

            250 AUTH LOGIN PLAIN CRAM-MD5
            auth login avlsdkfj
            334 UGFzc3dvcmQ6
            avlsdkfj

        Note that there is an alternate version sends the username
        as a separate command::

            250 AUTH LOGIN PLAIN CRAM-MD5
            auth login
            334 VXNlcm5hbWU6
            avlsdkfj
            334 UGFzc3dvcmQ6
            avlsdkfj

        However, since most servers seem to support both, we send the username
        with the initial request.
        """
        encoded_username, encoded_password = auth_login_encode(username, password)
        initial_response = await self.execute_command(
            b"AUTH", b"LOGIN", encoded_username, timeout=timeout
        )

        if initial_response.code != SMTPStatus.auth_continue:
            raise SMTPAuthenticationError(
                initial_response.code, initial_response.message
            )

        response = await self.execute_command(encoded_password, timeout=timeout)

        if response.code != SMTPStatus.auth_successful:
            raise SMTPAuthenticationError(response.code, response.message)

        return response

    async def auth_xoauth2(
        self,
        username: str | bytes,
        access_token: str | bytes,
        /,
        *,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> SMTPResponse:
        """
        XOAUTH2 auth for OAuth2 access tokens (e.g., Gmail, Outlook).

        See https://developers.google.com/gmail/imap/xoauth2-protocol

        Example::

            250 AUTH XOAUTH2
            AUTH XOAUTH2 dXNlcj11c2VyQGV4YW1wbGUuY29tAWF1dGg9QmVhcmVyIHRva2VuAQE=
            235 2.7.0 Accepted

        On failure, the server sends a base64-encoded JSON error as a challenge.
        We respond with an empty line to get the final error response.
        """
        await self._ehlo_or_helo_if_needed()

        encoded = auth_xoauth2_encode(username, access_token)
        response = await self.execute_command(
            b"AUTH", b"XOAUTH2", encoded, timeout=timeout
        )

        if response.code == SMTPStatus.auth_continue:
            # Server sent an error challenge; send empty response to get final error
            response = await self.execute_command(b"", timeout=timeout)

        if response.code != SMTPStatus.auth_successful:
            raise SMTPAuthenticationError(response.code, response.message)

        return response

    async def sendmail(
        self,
        sender: str,
        recipients: str | Sequence[str],
        message: str | bytes,
        /,
        *,
        mail_options: Iterable[str] | None = None,
        rcpt_options: Iterable[str] | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> tuple[dict[str, SMTPResponse], str]:
        """
        This command performs an entire mail transaction.

        The arguments are:
            - sender: The address sending this mail.
            - recipients: A list of addresses to send this mail to.  A bare
                string will be treated as a list with 1 address.
            - message: The message string to send.
            - mail_options: List of options (such as ESMTP 8bitmime) for the
                MAIL command.
            - rcpt_options: List of options (such as DSN commands) for all the
                RCPT commands.

        message must be a string containing characters in the ASCII range.
        The string is encoded to bytes using the ascii codec, and lone \\\\r
        and \\\\n characters are converted to \\\\r\\\\n characters.

        If there has been no previous HELO or EHLO command this session, this
        method tries EHLO first.

        This method will return normally if the mail is accepted for at least
        one recipient.  It returns a tuple consisting of:

            - an error dictionary, with one entry for each recipient that was
                refused.  Each entry contains a tuple of the SMTP error code
                and the accompanying error message sent by the server.
            - the message sent by the server in response to the DATA command
                (often containing a message id)

        Example:

            >>> smtp = aiosmtplib.SMTP(hostname="127.0.0.1", port=1025)
            >>> recipients = ["one@one.org", "two@two.org", "3@three.org"]
            >>> message = "From: Me@my.org\\nSubject: testing\\nHello World"
            >>> async def connect_and_send():
            ...     await smtp.connect()
            ...     await smtp.sendmail("me@my.org", recipients, message)
            ...     return await smtp.quit()
            >>> asyncio.run(connect_and_send())
            (221, Bye)

        In the above example, the message was accepted for delivery for all
        three addresses. If delivery had been only successful to two
        of the three addresses, and one was rejected, the response would look
        something like::

            (
                {"nobody@three.org": (550, "User unknown")},
                "Written safely to disk. #902487694.289148.12219.",
            )


        If delivery is not successful to any addresses,
        :exc:`.SMTPRecipientsRefused` is raised.

        If :exc:`.SMTPResponseException` is raised by this method, we try to
        send an RSET command to reset the server envelope automatically for
        the next attempt.

        :raises SMTPRecipientsRefused: delivery to all recipients failed
        :raises SMTPResponseException: on invalid response
        """
        if isinstance(recipients, str):
            recipients = [recipients]
        if mail_options is None:
            mail_options = []
        else:
            mail_options = list(mail_options)
        if rcpt_options is None:
            rcpt_options = []
        else:
            rcpt_options = list(rcpt_options)

        if any(option.lower() == "smtputf8" for option in mail_options):
            mailbox_encoding = "utf-8"
        else:
            mailbox_encoding = "ascii"

        if self._sendmail_lock is None:
            self._sendmail_lock = asyncio.Lock()

        async with self._sendmail_lock:
            # Make sure we've done an EHLO for extension checks
            await self._ehlo_or_helo_if_needed()

            if mailbox_encoding == "utf-8" and not self.supports_extension("smtputf8"):
                raise SMTPNotSupported("SMTPUTF8 is not supported by this server")

            if self.supports_extension("size"):
                message_len = len(message)
                size_option = f"size={message_len}"
                mail_options.insert(0, size_option)

            try:
                await self.mail(
                    sender,
                    options=mail_options,
                    encoding=mailbox_encoding,
                    timeout=timeout,
                )
                recipient_errors = await self._send_recipients(
                    recipients, rcpt_options, encoding=mailbox_encoding, timeout=timeout
                )
                response = await self.data(message, timeout=timeout)
            except (SMTPResponseException, SMTPRecipientsRefused) as exc:
                # If we got an error, reset the envelope.
                try:
                    await self.rset(timeout=timeout)
                except (ConnectionError, SMTPResponseException):
                    # If we're disconnected on the reset, or we get a bad
                    # status, don't raise that as it's confusing
                    pass
                raise exc

        return recipient_errors, response.message

    async def _send_recipients(
        self,
        recipients: Sequence[str],
        options: Iterable[str],
        encoding: str = "ascii",
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> dict[str, SMTPResponse]:
        """
        Send the recipients given to the server. Used as part of
        :meth:`.sendmail`.
        """
        recipient_errors: list[SMTPRecipientRefused] = []
        for address in recipients:
            try:
                await self.rcpt(
                    address, options=options, encoding=encoding, timeout=timeout
                )
            except SMTPRecipientRefused as exc:
                recipient_errors.append(exc)

        if len(recipient_errors) == len(recipients):
            raise SMTPRecipientsRefused(recipient_errors)

        formatted_errors = {
            err.recipient: SMTPResponse(err.code, err.message)
            for err in recipient_errors
        }

        return formatted_errors

    async def send_message(
        self,
        message: email.message.EmailMessage | email.message.Message,
        /,
        *,
        sender: str | None = None,
        recipients: str | Sequence[str] | None = None,
        mail_options: Iterable[str] | None = None,
        rcpt_options: Iterable[str] | None = None,
        timeout: float | Literal[Default.token] | None = Default.token,
    ) -> tuple[dict[str, SMTPResponse], str]:
        r"""
        Sends an :py:class:`email.message.EmailMessage` object.

        Arguments are as for :meth:`.sendmail`, except that message is an
        :py:class:`email.message.EmailMessage` object.  If sender is None or
        recipients is None, these arguments are taken from the headers of the
        EmailMessage as described in RFC 2822.  Regardless of the values of sender
        and recipients, any Bcc field (or Resent-Bcc field, when the message is a
        resent) of the EmailMessage object will not be transmitted.  The EmailMessage
        object is then serialized using :py:class:`email.generator.Generator` and
        :meth:`.sendmail` is called to transmit the message.

        'Resent-Date' is a mandatory field if the message is resent (RFC 2822
        Section 3.6.6). In such a case, we use the 'Resent-\*' fields.
        However, if there is more than one 'Resent-' block there's no way to
        unambiguously determine which one is the most recent in all cases,
        so rather than guess we raise a ``ValueError`` in that case.

        :raises ValueError:
            on more than one Resent header block
            on no sender kwarg or From header in message
            on no recipients kwarg or To, Cc or Bcc header in message
        :raises SMTPRecipientsRefused: delivery to all recipients failed
        :raises SMTPResponseException: on invalid response
        """
        if mail_options is None:
            mail_options = []
        else:
            mail_options = list(mail_options)

        if sender is None:
            sender = extract_sender(message)
        if sender is None:
            raise ValueError("No From header provided in message")

        if isinstance(recipients, str):
            recipients = [recipients]
        elif recipients is None:
            recipients = extract_recipients(message)
        if not recipients:
            raise ValueError("No recipient headers provided in message")

        # Make sure we've done an EHLO for extension checks
        await self._ehlo_or_helo_if_needed()

        try:
            sender.encode("ascii")
            "".join(recipients).encode("ascii")
        except UnicodeEncodeError:
            utf8_required = True
        else:
            utf8_required = False

        if utf8_required:
            if not self.supports_extension("smtputf8"):
                raise SMTPNotSupported(
                    "An address containing non-ASCII characters was provided, but "
                    "SMTPUTF8 is not supported by this server"
                )
            elif "smtputf8" not in [option.lower() for option in mail_options]:
                mail_options.append("SMTPUTF8")

        if self.supports_extension("8BITMIME"):
            if "body=8bitmime" not in [option.lower() for option in mail_options]:
                mail_options.append("BODY=8BITMIME")
            cte_type = "8bit"
        else:
            cte_type = "7bit"

        flat_message = flatten_message(message, utf8=utf8_required, cte_type=cte_type)

        return await self.sendmail(
            sender,
            recipients,
            flat_message,
            mail_options=mail_options,
            rcpt_options=rcpt_options,
            timeout=timeout,
        )

    def sendmail_sync(
        self, *args: Any, **kwargs: Any
    ) -> tuple[dict[str, SMTPResponse], str]:
        """
        Synchronous version of :meth:`.sendmail`. This method starts
        an event loop to connect, send the message, and disconnect.
        """

        async def sendmail_coroutine() -> tuple[dict[str, SMTPResponse], str]:
            async with self:
                return await self.sendmail(*args, **kwargs)

        return asyncio.run(sendmail_coroutine())

    def send_message_sync(
        self, *args: Any, **kwargs: Any
    ) -> tuple[dict[str, SMTPResponse], str]:
        """
        Synchronous version of :meth:`.send_message`. This method
        starts an event loop to connect, send the message, and disconnect.
        """

        async def send_message_coroutine() -> tuple[dict[str, SMTPResponse], str]:
            async with self:
                return await self.send_message(*args, **kwargs)

        return asyncio.run(send_message_coroutine())
