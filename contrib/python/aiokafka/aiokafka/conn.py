import asyncio
import base64
import collections
import functools
import hashlib
import hmac
import io
import logging
import random
import socket
import struct
import sys
import time
import traceback
import uuid
import warnings
import weakref
from enum import IntEnum

import async_timeout

import aiokafka.errors as Errors
from aiokafka.abc import AbstractTokenProvider
from aiokafka.protocol.admin import (
    ApiVersionRequest,
    SaslAuthenticateRequest,
    SaslHandShakeRequest,
)
from aiokafka.protocol.coordination import FindCoordinatorResponse_v0
from aiokafka.util import create_future, create_task, get_running_loop, wait_for

try:
    import gssapi
except ImportError:
    gssapi = None

__all__ = ["AIOKafkaConnection", "create_conn"]

log = logging.getLogger(__name__)

DEFAULT_KAFKA_PORT = 9092

READER_LIMIT = 2**16
SASL_QOP_AUTH = 1


class CloseReason(IntEnum):
    CONNECTION_BROKEN = 0
    CONNECTION_TIMEOUT = 1
    OUT_OF_SYNC = 2
    IDLE_DROP = 3
    SHUTDOWN = 4
    AUTH_FAILURE = 5


async def create_conn(
    host,
    port,
    *,
    client_id="aiokafka",
    request_timeout_ms=40000,
    ssl_context=None,
    security_protocol="PLAINTEXT",
    max_idle_ms=None,
    on_close=None,
    sasl_mechanism=None,
    sasl_plain_username=None,
    sasl_plain_password=None,
    sasl_kerberos_service_name="kafka",
    sasl_kerberos_domain_name=None,
    sasl_oauth_token_provider=None,
):
    conn = AIOKafkaConnection(
        host,
        port,
        client_id=client_id,
        request_timeout_ms=request_timeout_ms,
        ssl_context=ssl_context,
        security_protocol=security_protocol,
        max_idle_ms=max_idle_ms,
        on_close=on_close,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        sasl_kerberos_service_name=sasl_kerberos_service_name,
        sasl_kerberos_domain_name=sasl_kerberos_domain_name,
        sasl_oauth_token_provider=sasl_oauth_token_provider,
    )
    await conn.connect()
    return conn


class AIOKafkaProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, closed_fut, *args, loop, **kw):
        self._closed_fut = closed_fut
        super().__init__(*args, loop=loop, **kw)

    def connection_lost(self, exc):
        super().connection_lost(exc)
        if not self._closed_fut.cancelled():
            self._closed_fut.set_result(None)


class AIOKafkaConnection:
    """Class for manage connection to Kafka node"""

    _reader = None  # For __del__ to work properly, just in case
    _source_traceback = None

    def __init__(
        self,
        host,
        port,
        *,
        client_id="aiokafka",
        request_timeout_ms=40000,
        ssl_context=None,
        security_protocol="PLAINTEXT",
        max_idle_ms=None,
        on_close=None,
        sasl_mechanism=None,
        sasl_plain_password=None,
        sasl_plain_username=None,
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name=None,
        sasl_oauth_token_provider=None,
    ):
        loop = get_running_loop()

        if sasl_mechanism == "GSSAPI":
            assert gssapi is not None, "gssapi library required"

        if sasl_mechanism == "OAUTHBEARER":
            if sasl_oauth_token_provider is None or not isinstance(
                sasl_oauth_token_provider, AbstractTokenProvider
            ):
                raise ValueError(
                    "sasl_oauth_token_provider needs to be"
                    " provided implementing aiokafka.abc.AbstractTokenProvider"
                )
            assert callable(getattr(sasl_oauth_token_provider, "token", None)), (
                "sasl_oauth_token_provider must implement method #token()"
            )

        self._loop = loop
        self._host = host
        self._port = port
        self._request_timeout = request_timeout_ms / 1000
        self._client_id = client_id
        self._ssl_context = ssl_context
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._sasl_kerberos_service_name = sasl_kerberos_service_name
        self._sasl_kerberos_domain_name = sasl_kerberos_domain_name
        self._sasl_oauth_token_provider = sasl_oauth_token_provider

        self._versions = {}

        self._reader = self._writer = self._protocol = None
        # Even on small size seems to be a bit faster than list.
        # ~2x on size of 2 in Python3.6
        self._requests = collections.deque()
        self._read_task = None
        self._correlation_id = 0
        self._closed_fut = None

        self._max_idle_ms = max_idle_ms
        self._last_action = time.monotonic()
        self._idle_handle = None

        self._on_close_cb = on_close

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    # Warn and try to close. We can close synchronously, so will attempt
    # that
    def __del__(self, _warnings=warnings):
        if self.connected():
            _warnings.warn(
                f"Unclosed AIOKafkaConnection {self!r}",
                ResourceWarning,
                source=self,
            )
            if self._loop.is_closed():
                return

            # We don't need to call callback in this case. Just release
            # sockets and stop connections.
            self._on_close_cb = None
            self.close()

            context = {
                "conn": self,
                "message": "Unclosed AIOKafkaConnection",
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def connect(self):
        loop = self._loop
        self._closed_fut = create_future()
        if self._security_protocol in ["PLAINTEXT", "SASL_PLAINTEXT"]:
            ssl = None
        else:
            assert self._security_protocol in ["SSL", "SASL_SSL"]
            assert self._ssl_context is not None
            ssl = self._ssl_context
        # Create streams same as `open_connection`, but using custom protocol
        reader = asyncio.StreamReader(limit=READER_LIMIT, loop=loop)
        protocol = AIOKafkaProtocol(self._closed_fut, reader, loop=loop)
        async with async_timeout.timeout(self._request_timeout):
            transport, _ = await loop.create_connection(
                lambda: protocol, self.host, self.port, ssl=ssl
            )
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        self._reader, self._writer, self._protocol = reader, writer, protocol

        # Start reader task.
        self._read_task = self._create_reader_task()

        # Start idle checker
        if self._max_idle_ms is not None:
            self._idle_handle = loop.call_soon(self._idle_check, weakref.ref(self))

        try:
            await self._do_version_lookup()

            if self._security_protocol in ["SASL_SSL", "SASL_PLAINTEXT"]:
                await self._do_sasl_handshake()
        except:
            self.close()
            raise

        return reader, writer

    async def _do_version_lookup(self):
        response = await self.send(ApiVersionRequest())
        versions = {}
        for api_key, min_version, max_version in response.api_versions:
            assert min_version <= max_version, (
                f"{min_version} should be less than"
                f" or equal to {max_version} for {api_key}"
            )
            versions[api_key] = (min_version, max_version)
        self._versions = versions

    async def _do_sasl_handshake(self):
        handshake_response = await self.send(SaslHandShakeRequest(self._sasl_mechanism))
        error_type = Errors.for_code(handshake_response.error_code)
        if error_type is not Errors.NoError:
            error = error_type(self)
            self.close(reason=CloseReason.AUTH_FAILURE, exc=error)
            raise error

        if self._sasl_mechanism not in handshake_response.enabled_mechanisms:
            exc = Errors.UnsupportedSaslMechanismError(
                f"Kafka broker does not support {self._sasl_mechanism} sasl "
                "mechanism. Enabled mechanisms are: "
                f"{handshake_response.enabled_mechanisms}"
            )
            self.close(reason=CloseReason.AUTH_FAILURE, exc=exc)
            raise exc

        assert self._sasl_mechanism in (
            "PLAIN",
            "GSSAPI",
            "SCRAM-SHA-256",
            "SCRAM-SHA-512",
            "OAUTHBEARER",
        )
        if (
            self._security_protocol == "SASL_PLAINTEXT"
            and self._sasl_mechanism == "PLAIN"
        ):
            log.warning("Sending username and password in the clear")

        if self._sasl_mechanism == "GSSAPI":
            authenticator = self.authenticator_gssapi()
        elif self._sasl_mechanism.startswith("SCRAM-SHA-"):
            authenticator = self.authenticator_scram()
        elif self._sasl_mechanism == "OAUTHBEARER":
            authenticator = self.authenticator_oauth()
        else:
            authenticator = self.authenticator_plain()

        auth_bytes = None
        expect_response = True

        while True:
            res = await authenticator.step(auth_bytes)
            if res is None:
                break
            payload, expect_response = res

            # Before Kafka 1.0.0 Authentication bytes for SASL were send
            # without a Kafka Header, only with Length. This made error
            # handling hard, so they made SaslAuthenticateRequest to properly
            # pass error messages to clients on source of error.
            if handshake_response is not None and handshake_response.API_VERSION > 0:
                resp = await self.send(SaslAuthenticateRequest(payload))
                error_type = Errors.for_code(resp.error_code)
                if error_type is not Errors.NoError:
                    exc = error_type(resp.error_message)
                    self.close(reason=CloseReason.AUTH_FAILURE, exc=exc)
                    raise exc
                auth_bytes = resp.sasl_auth_bytes
            else:
                auth_bytes = await self._send_sasl_token(payload, expect_response)

        if self._sasl_mechanism == "GSSAPI":
            log.info("Authenticated as %s via GSSAPI", self.sasl_principal)
        elif self._sasl_mechanism == "OAUTHBEARER":
            log.info("Authenticated via OAUTHBEARER")
        else:
            log.info(
                "Authenticated as %s via %s",
                self._sasl_plain_username,
                self._sasl_mechanism,
            )

    def authenticator_plain(self):
        return SaslPlainAuthenticator(
            loop=self._loop,
            sasl_plain_password=self._sasl_plain_password,
            sasl_plain_username=self._sasl_plain_username,
        )

    def authenticator_gssapi(self):
        return SaslGSSAPIAuthenticator(
            loop=self._loop,
            principal=self.sasl_principal,
        )

    def authenticator_scram(self):
        return ScramAuthenticator(
            loop=self._loop,
            sasl_plain_password=self._sasl_plain_password,
            sasl_plain_username=self._sasl_plain_username,
            sasl_mechanism=self._sasl_mechanism,
        )

    def authenticator_oauth(self):
        return OAuthAuthenticator(
            sasl_oauth_token_provider=self._sasl_oauth_token_provider,
        )

    @property
    def sasl_principal(self):
        service = self._sasl_kerberos_service_name
        domain = self._sasl_kerberos_domain_name or self.host

        return f"{service}@{domain}"

    @classmethod
    def _on_read_task_error(cls, self_ref, read_task):
        # We don't want to react to cancelled errors
        if read_task.cancelled():
            return

        try:
            read_task.result()
        except Exception as exc:
            if not isinstance(exc, OSError | EOFError | ConnectionError):
                log.exception("Unexpected exception in AIOKafkaConnection")

            self = self_ref()
            if self is not None:
                self.close(reason=CloseReason.CONNECTION_BROKEN, exc=exc)

    @staticmethod
    def _idle_check(self_ref):
        self = self_ref()
        if self is None:
            return

        idle_for = time.monotonic() - self._last_action
        timeout = self._max_idle_ms / 1000
        # If we have any pending requests, we are assumed to be not idle.
        # it's up to `request_timeout_ms` to break those.
        if (idle_for >= timeout) and not self._requests:
            self.close(CloseReason.IDLE_DROP)
        else:
            if self._requests:
                # We must wait at least max_idle_ms anyway. Mostly this setting
                # is quite high so we shouldn't spend many CPU on this
                wake_up_in = timeout
            else:
                wake_up_in = timeout - idle_for
            self._idle_handle = self._loop.call_later(
                wake_up_in, self._idle_check, self_ref
            )

    def __repr__(self):
        return f"<AIOKafkaConnection host={self.host} port={self.port}>"

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def send(self, request, expect_response=True):
        if self._writer is None:
            raise Errors.KafkaConnectionError(
                f"No connection to broker at {self._host}:{self._port}"
            )

        request_struct = request.prepare(self._versions)

        correlation_id = self._next_correlation_id()
        header = request_struct.build_request_header(
            correlation_id=correlation_id, client_id=self._client_id
        )
        message = header.encode() + request_struct.encode()
        size = struct.pack(">i", len(message))
        try:
            self._writer.write(size + message)
        except OSError as err:
            self.close(reason=CloseReason.CONNECTION_BROKEN)
            raise Errors.KafkaConnectionError(
                f"Connection at {self._host}:{self._port} broken: {err}"
            ) from err

        log.debug(
            "Request to %s:%d %d: %s",
            self._host,
            self._port,
            correlation_id,
            request_struct,
        )

        if not expect_response:
            return self._writer.drain()
        fut = self._loop.create_future()
        self._requests.append(
            (correlation_id, request_struct, fut),
        )
        return wait_for(fut, self._request_timeout)

    def _send_sasl_token(self, payload, expect_response=True):
        if self._writer is None:
            raise Errors.KafkaConnectionError(
                f"No connection to broker at {self._host}:{self._port}"
            )
        size = struct.pack(">i", len(payload))
        try:
            self._writer.write(size + payload)
        except OSError as err:
            self.close(reason=CloseReason.CONNECTION_BROKEN)
            raise Errors.KafkaConnectionError(
                f"Connection at {self._host}:{self._port} broken: {err}"
            ) from err

        if not expect_response:
            return self._writer.drain()

        fut = self._loop.create_future()
        self._requests.append((None, None, fut))
        return wait_for(fut, self._request_timeout)

    def connected(self):
        return bool(self._reader is not None and not self._reader.at_eof())

    def close(self, reason=None, exc=None):
        log.debug(
            "Closing connection at %s:%s (%s, %s)", self._host, self._port, reason, exc
        )
        if self._reader is not None:
            self._writer.close()
            self._writer = self._reader = None
            if not self._read_task.done():
                self._read_task.cancel()
                self._read_task = None
            for _, _, fut in self._requests:
                if not fut.done():
                    error = Errors.KafkaConnectionError(
                        f"Connection at {self._host}:{self._port} closed"
                    )
                    if exc is not None:
                        error.__cause__ = exc
                        error.__context__ = exc
                    fut.set_exception(error)
            self._requests = collections.deque()
            if self._on_close_cb is not None:
                self._on_close_cb(self, reason)
                self._on_close_cb = None
        if self._idle_handle is not None:
            self._idle_handle.cancel()

        # transport.close() will close socket, but not right ahead.
        # Return a future in case we need to wait on it.
        return self._closed_fut

    def _create_reader_task(self):
        self_ref = weakref.ref(self)
        read_task = create_task(self._read(self_ref))
        read_task.add_done_callback(
            functools.partial(self._on_read_task_error, self_ref)
        )
        return read_task

    @staticmethod
    async def _read(self_ref):
        # XXX: I know that it become a bit more ugly once cyclic references
        # were removed, but it's needed to allow connections to properly
        # release resources if leaked.
        # NOTE: all errors will be handled by done callback
        self = self_ref()
        if self is None:
            return
        reader = self._reader
        del self

        while True:
            resp = await reader.readexactly(4)
            (size,) = struct.unpack(">i", resp)

            resp = await reader.readexactly(size)

            self = self_ref()
            if self is None:
                return
            self._handle_frame(resp)
            del self

    def _handle_frame(self, resp):
        correlation_id, request, fut = self._requests[0]

        if correlation_id is None:  # Is a SASL packet, just pass it though
            if not fut.done():
                fut.set_result(resp)
        else:
            resp = io.BytesIO(resp)
            response_header = request.parse_response_header(resp)
            resp_type = request.RESPONSE_TYPE

            if (
                resp_type is FindCoordinatorResponse_v0
                and correlation_id != 0
                and response_header.correlation_id == 0
            ):
                log.warning(
                    "Kafka 0.8.2 quirk -- FindCoordinatorResponse"
                    " correlation id does not match request. This"
                    " should go away once at least one topic has been"
                    " initialized on the broker"
                )

            elif response_header.correlation_id != correlation_id:
                error = Errors.CorrelationIdError(
                    f"Correlation ids do not match: sent {correlation_id},"
                    f" recv {response_header.correlation_id}"
                )
                if not fut.done():
                    fut.set_exception(error)
                self.close(reason=CloseReason.OUT_OF_SYNC)
                return

            if not fut.done():
                response = resp_type.decode(resp)
                log.debug(
                    "Response from %s:%d %d: %s",
                    self._host,
                    self._port,
                    correlation_id,
                    response,
                )
                fut.set_result(response)

        # Update idle timer.
        self._last_action = time.monotonic()
        # We should clear the request future only after all code is done and
        # future is resolved. If any fails it's up to close() method to fail
        # this future.
        self._requests.popleft()

    def _next_correlation_id(self):
        self._correlation_id = (self._correlation_id + 1) % 2**31
        return self._correlation_id


class BaseSaslAuthenticator:
    def step(self, payload):
        return self._loop.run_in_executor(None, self._step, payload)

    def _step(self, payload):
        """Process next token in sequence and return with:
        ``None`` if it was the last needed exchange
        ``tuple`` tuple with new token and a boolean whether it requires an
            answer token
        """
        try:
            data = self._authenticator.send(payload)
        except StopIteration:
            return None
        else:
            return data


class SaslPlainAuthenticator(BaseSaslAuthenticator):
    def __init__(self, *, loop, sasl_plain_password, sasl_plain_username):
        self._loop = loop
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._authenticator = self.authenticator_plain()

    def authenticator_plain(self):
        """Automaton to authenticate with SASL tokens"""
        # Send PLAIN credentials per RFC-4616
        data = "\0".join(
            [
                self._sasl_plain_username,
                self._sasl_plain_username,
                self._sasl_plain_password,
            ]
        ).encode("utf-8")

        resp = yield data, True

        assert resp == b"", "Server should either close or send an empty response"


class SaslGSSAPIAuthenticator(BaseSaslAuthenticator):
    def __init__(self, *, loop, principal):
        self._loop = loop
        self._principal = principal
        self._authenticator = self.authenticator_gssapi()

    def authenticator_gssapi(self):
        name = gssapi.Name(
            self._principal,
            name_type=gssapi.NameType.hostbased_service,
        )
        cname = name.canonicalize(gssapi.MechType.kerberos)

        client_ctx = gssapi.SecurityContext(name=cname, usage="initiate")

        server_token = None
        while not client_ctx.complete:
            client_token = client_ctx.step(server_token)
            client_token = client_token or b""

            server_token = yield client_token, True

        msg = client_ctx.unwrap(server_token).message

        qop = struct.pack("b", SASL_QOP_AUTH & msg[0])
        msg = qop + msg[1:]
        msg = client_ctx.wrap(msg + self._principal.encode(), False).message

        yield (msg, False)


class ScramAuthenticator(BaseSaslAuthenticator):
    MECHANISMS = {
        "SCRAM-SHA-256": hashlib.sha256,
        "SCRAM-SHA-512": hashlib.sha512,
    }

    def __init__(
        self,
        *,
        loop,
        sasl_plain_password,
        sasl_plain_username,
        sasl_mechanism,
    ):
        self._loop = loop
        self._nonce = str(uuid.uuid4()).replace("-", "")
        self._auth_message = ""
        self._salted_password = None
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password.encode("utf-8")
        self._hashfunc = self.MECHANISMS[sasl_mechanism]
        self._hashname = "".join(sasl_mechanism.lower().split("-")[1:3])
        self._stored_key = None
        self._client_key = None
        self._client_signature = None
        self._client_proof = None
        self._server_key = None
        self._server_signature = None
        self._authenticator = self.authenticator_scram()

    def first_message(self):
        # The characters ',' or '=' in usernames are sent as '=2C' and
        # '=3D' respectively.
        # https://datatracker.ietf.org/doc/html/rfc5802#section-5.1
        quoted_username = (
            self._sasl_plain_username.replace("=", "=3D").replace( ",", "=2C")
        )  # fmt: skip
        client_first_bare = f"n={quoted_username},r={self._nonce}"
        self._auth_message += client_first_bare
        return "n,," + client_first_bare

    def process_server_first_message(self, server_first):
        self._auth_message += "," + server_first
        params = dict(pair.split("=", 1) for pair in server_first.split(","))
        server_nonce = params["r"]
        if not server_nonce.startswith(self._nonce):
            raise ValueError("Server nonce, did not start with client nonce!")
        self._nonce = server_nonce
        self._auth_message += ",c=biws,r=" + self._nonce

        salt = base64.b64decode(params["s"].encode("utf-8"))
        iterations = int(params["i"])
        self.create_salted_password(salt, iterations)

        self._client_key = self.hmac(self._salted_password, b"Client Key")
        self._stored_key = self._hashfunc(self._client_key).digest()
        self._client_signature = self.hmac(
            self._stored_key, self._auth_message.encode("utf-8")
        )
        self._client_proof = ScramAuthenticator._xor_bytes(
            self._client_key, self._client_signature
        )
        self._server_key = self.hmac(self._salted_password, b"Server Key")
        self._server_signature = self.hmac(
            self._server_key, self._auth_message.encode("utf-8")
        )

    def final_message(self):
        client_proof = base64.b64encode(self._client_proof).decode("utf-8")
        return f"c=biws,r={self._nonce},p={client_proof}"

    def process_server_final_message(self, server_final):
        params = dict(pair.split("=", 1) for pair in server_final.split(","))
        if self._server_signature != base64.b64decode(params["v"].encode("utf-8")):
            raise ValueError("Server sent wrong signature!")

    def authenticator_scram(self):
        client_first = self.first_message().encode("utf-8")
        server_first = yield client_first, True
        self.process_server_first_message(server_first.decode("utf-8"))
        client_final = self.final_message().encode("utf-8")
        server_final = yield client_final, True
        self.process_server_final_message(server_final.decode("utf-8"))

    def hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=self._hashfunc).digest()

    def create_salted_password(self, salt, iterations):
        self._salted_password = hashlib.pbkdf2_hmac(
            self._hashname, self._sasl_plain_password, salt, iterations
        )

    @staticmethod
    def _xor_bytes(left, right):
        return bytes(lb ^ rb for lb, rb in zip(left, right, strict=False))


class OAuthAuthenticator(BaseSaslAuthenticator):
    def __init__(self, *, sasl_oauth_token_provider):
        self._sasl_oauth_token_provider = sasl_oauth_token_provider
        self._token_sent = False

    async def step(self, payload):
        if self._token_sent:
            return None
        token = await self._sasl_oauth_token_provider.token()
        token_extensions = self._token_extensions()
        self._token_sent = True
        return (
            self._build_oauth_client_request(token, token_extensions).encode("utf-8"),
            True,
        )

    def _build_oauth_client_request(self, token, token_extensions):
        return f"n,,\x01auth=Bearer {token}{token_extensions}\x01\x01"

    def _token_extensions(self):
        """
        Return a string representation of the OPTIONAL key-value pairs
        that can be sent with an OAUTHBEARER initial request.
        """
        # Only run if the #extensions() method is implemented
        # by the clients Token Provider class
        # Builds up a string separated by \x01 via a dict of key value pairs
        if callable(getattr(self._sasl_oauth_token_provider, "extensions", None)):
            extensions = self._sasl_oauth_token_provider.extensions()
            if len(extensions) > 0:
                msg = "\x01".join([f"{k}={v}" for k, v in extensions.items()])
                return "\x01" + msg

        return ""


def _address_family(address):
    """
    Attempt to determine the family of an address (or hostname)

    :return: either socket.AF_INET or socket.AF_INET6 or socket.AF_UNSPEC
    if the address family could not be determined
    """
    if address.startswith("[") and address.endswith("]"):
        return socket.AF_INET6
    for af in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(af, address)
        except (OSError, ValueError, AttributeError):  # noqa: PERF203
            continue
        else:
            return af
    return socket.AF_UNSPEC


def get_ip_port_afi(host_and_port_str):
    """
    Parse the IP and port from a string in the format of:

        * host_or_ip          <- Can be either IPv4 address literal or hostname/fqdn
        * host_or_ipv4:port   <- Can be either IPv4 address literal or hostname/fqdn
        * [host_or_ip]        <- IPv6 address literal
        * [host_or_ip]:port.  <- IPv6 address literal

    .. note:: IPv6 address literals with ports *must* be enclosed in brackets

    .. note:: If the port is not specified, default will be returned.

    :return: tuple (host, port, afi), afi will be socket.AF_INET or
    socket.AF_INET6 or socket.AF_UNSPEC
    """
    host_and_port_str = host_and_port_str.strip()
    if host_and_port_str.startswith("["):
        af = socket.AF_INET6
        host, rest = host_and_port_str[1:].split("]")
        if rest:
            port = int(rest[1:])
        else:
            port = DEFAULT_KAFKA_PORT
        return host, port, af
    elif ":" not in host_and_port_str:
        af = _address_family(host_and_port_str)
        return host_and_port_str, DEFAULT_KAFKA_PORT, af
    else:
        # now we have something with a colon in it and no square brackets. It could
        # be either an IPv6 address literal (e.g., "::1") or an IP:port pair or a
        # host:port pair
        try:
            # if it decodes as an IPv6 address, use that
            socket.inet_pton(socket.AF_INET6, host_and_port_str)
        except AttributeError:
            log.warning(
                "socket.inet_pton not available on this platform."
                " consider `pip install win_inet_pton`"
            )
        except (OSError, ValueError):
            # it's a host:port pair
            pass
        else:
            return host_and_port_str, DEFAULT_KAFKA_PORT, socket.AF_INET6
        host, port = host_and_port_str.rsplit(":", 1)
        port = int(port)

        af = _address_family(host)
        return host, port, af


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """

    if isinstance(hosts, str):
        hosts = hosts.strip().split(",")

    result = []
    afi = socket.AF_INET
    for host_port in hosts:
        host, port, afi = get_ip_port_afi(host_port)

        if port < 0:
            port = DEFAULT_KAFKA_PORT

        result.append((host, port, afi))

    if randomize:
        random.shuffle(result)

    return result
