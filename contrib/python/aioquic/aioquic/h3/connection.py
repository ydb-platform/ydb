import contextlib
import logging
import re
from collections.abc import Generator
from enum import Enum, IntEnum
from typing import FrozenSet, Optional, Set

import pylsqpack
from aioquic.buffer import UINT_VAR_MAX_SIZE, Buffer, BufferReadError, encode_uint_var
from aioquic.h3.events import (
    DatagramReceived,
    DataReceived,
    H3Event,
    Headers,
    HeadersReceived,
    PushPromiseReceived,
    WebTransportStreamDataReceived,
)
from aioquic.h3.exceptions import InvalidStreamTypeError, NoAvailablePushIDError
from aioquic.quic.connection import QuicConnection, stream_is_unidirectional
from aioquic.quic.events import DatagramFrameReceived, QuicEvent, StreamDataReceived
from aioquic.quic.logger import QuicLoggerTrace

logger = logging.getLogger("http3")

H3_ALPN = ["h3"]
RESERVED_SETTINGS = (0x0, 0x2, 0x3, 0x4, 0x5)
UPPERCASE = re.compile(b"[A-Z]")
COLON = 0x3A
NUL = 0x00
LF = 0x0A
CR = 0x0D
SP = 0x20
HTAB = 0x09
WHITESPACE = (SP, HTAB)


class ErrorCode(IntEnum):
    H3_DATAGRAM_ERROR = 0x33
    H3_NO_ERROR = 0x100
    H3_GENERAL_PROTOCOL_ERROR = 0x101
    H3_INTERNAL_ERROR = 0x102
    H3_STREAM_CREATION_ERROR = 0x103
    H3_CLOSED_CRITICAL_STREAM = 0x104
    H3_FRAME_UNEXPECTED = 0x105
    H3_FRAME_ERROR = 0x106
    H3_EXCESSIVE_LOAD = 0x107
    H3_ID_ERROR = 0x108
    H3_SETTINGS_ERROR = 0x109
    H3_MISSING_SETTINGS = 0x10A
    H3_REQUEST_REJECTED = 0x10B
    H3_REQUEST_CANCELLED = 0x10C
    H3_REQUEST_INCOMPLETE = 0x10D
    H3_MESSAGE_ERROR = 0x10E
    H3_CONNECT_ERROR = 0x10F
    H3_VERSION_FALLBACK = 0x110
    QPACK_DECOMPRESSION_FAILED = 0x200
    QPACK_ENCODER_STREAM_ERROR = 0x201
    QPACK_DECODER_STREAM_ERROR = 0x202


class FrameType(IntEnum):
    DATA = 0x0
    HEADERS = 0x1
    PRIORITY = 0x2
    CANCEL_PUSH = 0x3
    SETTINGS = 0x4
    PUSH_PROMISE = 0x5
    GOAWAY = 0x7
    MAX_PUSH_ID = 0xD
    DUPLICATE_PUSH = 0xE
    WEBTRANSPORT_STREAM = 0x41


class HeadersState(Enum):
    INITIAL = 0
    AFTER_HEADERS = 1
    AFTER_TRAILERS = 2


class Setting(IntEnum):
    QPACK_MAX_TABLE_CAPACITY = 0x1
    MAX_FIELD_SECTION_SIZE = 0x6
    QPACK_BLOCKED_STREAMS = 0x7

    # https://datatracker.ietf.org/doc/html/rfc9220#section-5
    ENABLE_CONNECT_PROTOCOL = 0x8
    # https://datatracker.ietf.org/doc/html/rfc9297#section-5.1
    H3_DATAGRAM = 0x33
    # https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-02#section-10.1
    ENABLE_WEBTRANSPORT = 0x2B603742

    # Dummy setting to check it is correctly ignored by the peer.
    # https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.4.1
    DUMMY = 0x21


class StreamType(IntEnum):
    CONTROL = 0
    PUSH = 1
    QPACK_ENCODER = 2
    QPACK_DECODER = 3
    WEBTRANSPORT = 0x54


class ProtocolError(Exception):
    """
    Base class for protocol errors.

    These errors are not exposed to the API user, they are handled
    in :meth:`H3Connection.handle_event`.
    """

    error_code = ErrorCode.H3_GENERAL_PROTOCOL_ERROR

    def __init__(self, reason_phrase: str = ""):
        self.reason_phrase = reason_phrase


class QpackDecompressionFailed(ProtocolError):
    error_code = ErrorCode.QPACK_DECOMPRESSION_FAILED


class QpackDecoderStreamError(ProtocolError):
    error_code = ErrorCode.QPACK_DECODER_STREAM_ERROR


class QpackEncoderStreamError(ProtocolError):
    error_code = ErrorCode.QPACK_ENCODER_STREAM_ERROR


class ClosedCriticalStream(ProtocolError):
    error_code = ErrorCode.H3_CLOSED_CRITICAL_STREAM


class DatagramError(ProtocolError):
    error_code = ErrorCode.H3_DATAGRAM_ERROR


class FrameUnexpected(ProtocolError):
    error_code = ErrorCode.H3_FRAME_UNEXPECTED


class MessageError(ProtocolError):
    error_code = ErrorCode.H3_MESSAGE_ERROR


class MissingSettingsError(ProtocolError):
    error_code = ErrorCode.H3_MISSING_SETTINGS


class SettingsError(ProtocolError):
    error_code = ErrorCode.H3_SETTINGS_ERROR


class StreamCreationError(ProtocolError):
    error_code = ErrorCode.H3_STREAM_CREATION_ERROR


def encode_frame(frame_type: int, frame_data: bytes) -> bytes:
    frame_length = len(frame_data)
    buf = Buffer(capacity=frame_length + 2 * UINT_VAR_MAX_SIZE)
    buf.push_uint_var(frame_type)
    buf.push_uint_var(frame_length)
    buf.push_bytes(frame_data)
    return buf.data


def encode_settings(settings: dict[int, int]) -> bytes:
    buf = Buffer(capacity=1024)
    for setting, value in settings.items():
        buf.push_uint_var(setting)
        buf.push_uint_var(value)
    return buf.data


def parse_max_push_id(data: bytes) -> int:
    buf = Buffer(data=data)
    max_push_id = buf.pull_uint_var()
    assert buf.eof()
    return max_push_id


def parse_settings(data: bytes) -> dict[int, int]:
    buf = Buffer(data=data)
    settings: dict[int, int] = {}
    while not buf.eof():
        setting = buf.pull_uint_var()
        value = buf.pull_uint_var()
        if setting in RESERVED_SETTINGS:
            raise SettingsError("Setting identifier 0x%x is reserved" % setting)
        if setting in settings:
            raise SettingsError("Setting identifier 0x%x is included twice" % setting)
        settings[setting] = value
    return dict(settings)


def stream_is_request_response(stream_id: int):
    """
    Returns True if the stream is a client-initiated bidirectional stream.
    """
    return stream_id % 4 == 0


def validate_header_name(key: bytes) -> None:
    """
    Validate a header name as specified by RFC 9113 section 8.2.1.
    """
    for i, c in enumerate(key):
        if c <= 0x20 or (c >= 0x41 and c <= 0x5A) or c >= 0x7F:
            raise MessageError("Header %r contains invalid characters" % key)
        if c == COLON and i != 0:
            # Colon not at start, definitely bad.  Keys starting with a colon
            # will be checked in pseudo-header validation code.
            raise MessageError("Header %r contains a non-initial colon" % key)


def validate_header_value(key: bytes, value: bytes):
    """
    Validate a header value as specified by RFC 9113 section 8.2.1.
    """
    for c in value:
        if c == NUL or c == LF or c == CR:
            raise MessageError("Header %r value has forbidden characters" % key)
    if len(value) > 0:
        first = value[0]
        if first in WHITESPACE:
            raise MessageError("Header %r value starts with whitespace" % key)
        if len(value) > 1:
            last = value[-1]
            if last in WHITESPACE:
                raise MessageError("Header %r value ends with whitespace" % key)


def validate_headers(
    headers: Headers,
    allowed_pseudo_headers: FrozenSet[bytes],
    required_pseudo_headers: FrozenSet[bytes],
    stream: Optional["H3Stream"] = None,
) -> None:
    after_pseudo_headers = False
    authority: Optional[bytes] = None
    path: Optional[bytes] = None
    scheme: Optional[bytes] = None
    seen_pseudo_headers: Set[bytes] = set()
    for key, value in headers:
        validate_header_name(key)
        validate_header_value(key, value)

        if key.startswith(b":"):
            # pseudo-headers
            if after_pseudo_headers:
                raise MessageError(
                    "Pseudo-header %r is not allowed after regular headers" % key
                )
            if key not in allowed_pseudo_headers:
                raise MessageError("Pseudo-header %r is not valid" % key)
            if key in seen_pseudo_headers:
                raise MessageError("Pseudo-header %r is included twice" % key)
            seen_pseudo_headers.add(key)

            # store value
            if key == b":authority":
                authority = value
            elif key == b":path":
                path = value
            elif key == b":scheme":
                scheme = value
            elif key == b":method" and stream is not None:
                stream.request_method = value
            elif key == b":status" and stream is not None:
                try:
                    if int(value) >= 200:  # skip interim responses
                        stream.response_status = int(value)
                except ValueError:
                    raise MessageError(":status is not an integer")
        else:
            # regular headers
            after_pseudo_headers = True
            # a few more semantic checks
            if key == b"content-length":
                try:
                    content_length = int(value)
                    if content_length < 0:
                        raise ValueError
                except ValueError:
                    raise MessageError("content-length is not a non-negative integer")
                if stream:
                    stream.expected_content_length = content_length
            elif key == b"transfer-encoding" and value != b"trailers":
                raise MessageError(
                    "The only valid value for transfer-encoding is trailers"
                )

    # check required pseudo-headers are present
    missing = required_pseudo_headers.difference(seen_pseudo_headers)
    if missing:
        raise MessageError("Pseudo-headers %s are missing" % sorted(missing))

    if scheme in (b"http", b"https"):
        if not authority:
            raise MessageError("Pseudo-header b':authority' cannot be empty")
        if not path:
            raise MessageError("Pseudo-header b':path' cannot be empty")


def validate_push_promise_headers(headers: Headers) -> None:
    validate_headers(
        headers,
        allowed_pseudo_headers=frozenset(
            (b":method", b":scheme", b":authority", b":path")
        ),
        required_pseudo_headers=frozenset(
            (b":method", b":scheme", b":authority", b":path")
        ),
    )


def validate_request_headers(
    headers: Headers, stream: Optional["H3Stream"] = None
) -> None:
    validate_headers(
        headers,
        allowed_pseudo_headers=frozenset(
            # FIXME: The pseudo-header :protocol is not actually defined, but
            # we use it for the WebSocket demo.
            (b":method", b":scheme", b":authority", b":path", b":protocol")
        ),
        required_pseudo_headers=frozenset((b":method", b":authority")),
        stream=stream,
    )


def validate_response_headers(
    headers: Headers, stream: Optional["H3Stream"] = None
) -> None:
    validate_headers(
        headers,
        allowed_pseudo_headers=frozenset((b":status",)),
        required_pseudo_headers=frozenset((b":status",)),
        stream=stream,
    )


def validate_trailers(headers: Headers) -> None:
    validate_headers(
        headers,
        allowed_pseudo_headers=frozenset(),
        required_pseudo_headers=frozenset(),
    )


class H3Stream:
    def __init__(self, stream_id: int) -> None:
        self.blocked = False
        self.blocked_frame_size: Optional[int] = None
        self.buffer = b""
        self.receiving_ended = False
        self.sending_ended = False
        self.frame_size: Optional[int] = None
        self.frame_type: Optional[int] = None
        self.headers_recv_state: HeadersState = HeadersState.INITIAL
        self.headers_send_state: HeadersState = HeadersState.INITIAL
        self.push_id: Optional[int] = None
        self.session_id: Optional[int] = None
        self.stream_id = stream_id
        self.stream_type: Optional[int] = None
        self.expected_content_length: Optional[int] = None
        self.content_length: int = 0
        self.request_method: Optional[bytes] = None
        self.response_status: Optional[int] = None

    def is_ended(self) -> bool:
        return self.sending_ended and self.receiving_ended and not self.blocked

    def finish_sending(self) -> None:
        if self.sending_ended:
            raise FrameUnexpected("stream was already ended")
        self.sending_ended = True


class H3Connection:
    """
    A low-level HTTP/3 connection object.

    :param quic: A :class:`~aioquic.quic.connection.QuicConnection` instance.
    """

    def __init__(self, quic: QuicConnection, enable_webtransport: bool = False) -> None:
        # settings
        self._max_table_capacity = 4096
        self._blocked_streams = 16
        self._enable_webtransport = enable_webtransport

        self._is_client = quic.configuration.is_client
        self._is_done = False
        self._quic = quic
        self._quic_logger: Optional[QuicLoggerTrace] = quic._quic_logger
        self._decoder = pylsqpack.Decoder(
            self._max_table_capacity, self._blocked_streams
        )
        self._decoder_bytes_received = 0
        self._decoder_bytes_sent = 0
        self._encoder = pylsqpack.Encoder()
        self._encoder_bytes_received = 0
        self._encoder_bytes_sent = 0
        self._settings_received = False
        self._stream: dict[int, H3Stream] = {}

        self._max_push_id: Optional[int] = 8 if self._is_client else None
        self._next_push_id: int = 0

        self._local_control_stream_id: Optional[int] = None
        self._local_decoder_stream_id: Optional[int] = None
        self._local_encoder_stream_id: Optional[int] = None

        self._peer_control_stream_id: Optional[int] = None
        self._peer_decoder_stream_id: Optional[int] = None
        self._peer_encoder_stream_id: Optional[int] = None
        self._received_settings: Optional[dict[int, int]] = None
        self._sent_settings: Optional[dict[int, int]] = None

        self._init_connection()

    def create_webtransport_stream(
        self, session_id: int, is_unidirectional: bool = False
    ) -> int:
        """
        Create a WebTransport stream and return the stream ID.

        .. aioquic_transmit::

        :param session_id: The WebTransport session identifier.
        :param is_unidirectional: Whether to create a unidirectional stream.
        """
        if is_unidirectional:
            stream_id = self._create_uni_stream(StreamType.WEBTRANSPORT)
            self._quic.send_stream_data(stream_id, encode_uint_var(session_id))
        else:
            stream_id = self._quic.get_next_available_stream_id()
            self._log_stream_type(
                stream_id=stream_id, stream_type=StreamType.WEBTRANSPORT
            )
            self._quic.send_stream_data(
                stream_id,
                encode_uint_var(FrameType.WEBTRANSPORT_STREAM)
                + encode_uint_var(session_id),
            )
        return stream_id

    def handle_event(self, event: QuicEvent) -> list[H3Event]:
        """
        Handle a QUIC event and return a list of HTTP events.

        :param event: The QUIC event to handle.
        """

        if not self._is_done:
            try:
                if isinstance(event, StreamDataReceived):
                    return self._receive_stream_data(event)
                elif isinstance(event, DatagramFrameReceived):
                    return self._receive_datagram(event.data)
            except ProtocolError as exc:
                self._is_done = True
                self._quic.close(
                    error_code=exc.error_code, reason_phrase=exc.reason_phrase
                )

        return []

    def send_datagram(self, stream_id: int, data: bytes) -> None:
        """
        Send a datagram for the specified stream.

        If the stream ID is not a client-initiated bidirectional stream, an
        :class:`~aioquic.h3.exceptions.InvalidStreamTypeError` exception is raised.

        .. aioquic_transmit::

        :param stream_id: The stream ID.
        :param data: The HTTP/3 datagram payload.
        """

        # check stream ID is valid
        if not stream_is_request_response(stream_id):
            raise InvalidStreamTypeError(
                "Datagrams can only be sent for client-initiated bidirectional streams"
            )

        self._quic.send_datagram_frame(encode_uint_var(stream_id // 4) + data)

    def send_push_promise(self, stream_id: int, headers: Headers) -> int:
        """
        Send a push promise related to the specified stream.

        Returns the stream ID on which headers and data can be sent.

        If the stream ID is not a client-initiated bidirectional stream, an
        :class:`~aioquic.h3.exceptions.InvalidStreamTypeError` exception is raised.

        If there are not available push IDs, an
        :class:`~aioquic.h3.exceptions.NoAvailablePushIDError` exception is raised.

        .. aioquic_transmit::

        :param stream_id: The stream ID on which to send the data.
        :param headers: The HTTP request headers for this push.
        """
        assert not self._is_client, "Only servers may send a push promise."

        # check stream ID is valid
        if not stream_is_request_response(stream_id):
            raise InvalidStreamTypeError(
                "Push promises can only be sent for client-initiated bidirectional "
                "streams"
            )

        # check a push ID is available
        if self._max_push_id is None or self._next_push_id >= self._max_push_id:
            raise NoAvailablePushIDError

        # send push promise
        push_id = self._next_push_id
        self._next_push_id += 1
        self._quic.send_stream_data(
            stream_id,
            encode_frame(
                FrameType.PUSH_PROMISE,
                encode_uint_var(push_id) + self._encode_headers(stream_id, headers),
            ),
        )

        #  create push stream
        push_stream_id = self._create_uni_stream(StreamType.PUSH, push_id=push_id)
        self._quic.send_stream_data(push_stream_id, encode_uint_var(push_id))
        with self._get_or_create_stream(push_stream_id) as stream:
            for key, value in headers:
                if key == b":method":
                    stream.request_method = value
                    break

        return push_stream_id

    def send_data(self, stream_id: int, data: bytes, end_stream: bool) -> None:
        """
        Send data on the given stream.

        .. aioquic_transmit::

        :param stream_id: The stream ID on which to send the data.
        :param data: The data to send.
        :param end_stream: Whether to end the stream.
        """
        # check DATA frame is allowed
        with self._get_or_create_stream(stream_id) as stream:
            if stream.headers_send_state != HeadersState.AFTER_HEADERS:
                raise FrameUnexpected("DATA frame is not allowed in this state")
            if end_stream:
                stream.finish_sending()

        # log frame
        if self._quic_logger is not None:
            self._quic_logger.log_event(
                category="http",
                event="frame_created",
                data=self._quic_logger.encode_http3_data_frame(
                    length=len(data), stream_id=stream_id
                ),
            )

        self._quic.send_stream_data(
            stream_id, encode_frame(FrameType.DATA, data), end_stream
        )

    def send_headers(
        self, stream_id: int, headers: Headers, end_stream: bool = False
    ) -> None:
        """
        Send headers on the given stream.

        .. aioquic_transmit::

        :param stream_id: The stream ID on which to send the headers.
        :param headers: The HTTP headers to send.
        :param end_stream: Whether to end the stream.
        """
        # check HEADERS frame is allowed
        with self._get_or_create_stream(stream_id) as stream:
            if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                raise FrameUnexpected("HEADERS frame is not allowed in this state")
            if end_stream:
                stream.finish_sending()

            for key, value in headers:
                if self._is_client and key == b":method":
                    stream.request_method = value
                    break
                if not self._is_client and key == b":status":
                    try:
                        if int(value) >= 200:  # skip interim responses
                            stream.response_status = int(value)
                    except ValueError:
                        pass
                    break

            frame_data = self._encode_headers(stream_id, headers)

            # log frame
            if self._quic_logger is not None:
                self._quic_logger.log_event(
                    category="http",
                    event="frame_created",
                    data=self._quic_logger.encode_http3_headers_frame(
                        length=len(frame_data), headers=headers, stream_id=stream_id
                    ),
                )

            # update state and send headers
            if stream.headers_send_state == HeadersState.INITIAL:
                stream.headers_send_state = HeadersState.AFTER_HEADERS
            else:
                stream.headers_send_state = HeadersState.AFTER_TRAILERS
            self._quic.send_stream_data(
                stream_id, encode_frame(FrameType.HEADERS, frame_data), end_stream
            )

    @property
    def received_settings(self) -> Optional[dict[int, int]]:
        """
        Return the received SETTINGS frame, or None.
        """
        return self._received_settings

    @property
    def sent_settings(self) -> Optional[dict[int, int]]:
        """
        Return the sent SETTINGS frame, or None.
        """
        return self._sent_settings

    def _create_uni_stream(
        self, stream_type: int, push_id: Optional[int] = None
    ) -> int:
        """
        Create an unidirectional stream of the given type.
        """
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=True)
        self._log_stream_type(
            push_id=push_id, stream_id=stream_id, stream_type=stream_type
        )
        self._quic.send_stream_data(stream_id, encode_uint_var(stream_type))
        return stream_id

    def _decode_headers(self, stream_id: int, frame_data: Optional[bytes]) -> Headers:
        """
        Decode a HEADERS block and send decoder updates on the decoder stream.

        This is called with frame_data=None when a stream becomes unblocked.
        """
        try:
            if frame_data is None:
                decoder, headers = self._decoder.resume_header(stream_id)
            else:
                decoder, headers = self._decoder.feed_header(stream_id, frame_data)
            self._decoder_bytes_sent += len(decoder)
            self._quic.send_stream_data(self._local_decoder_stream_id, decoder)
        except pylsqpack.DecompressionFailed as exc:
            raise QpackDecompressionFailed() from exc

        return headers

    def _encode_headers(self, stream_id: int, headers: Headers) -> bytes:
        """
        Encode a HEADERS block and send encoder updates on the encoder stream.
        """
        encoder, frame_data = self._encoder.encode(stream_id, headers)
        self._encoder_bytes_sent += len(encoder)
        self._quic.send_stream_data(self._local_encoder_stream_id, encoder)
        return frame_data

    @contextlib.contextmanager
    def _get_or_create_stream(self, stream_id: int) -> Generator[H3Stream]:
        if stream_id not in self._stream:
            self._stream[stream_id] = H3Stream(stream_id)
        stream = self._stream[stream_id]
        try:
            yield stream
        finally:
            # Don't forget to delete stream objects when they are done
            if stream.is_ended():
                self._stream.pop(stream_id)

    def _get_local_settings(self) -> dict[int, int]:
        """
        Return the local HTTP/3 settings.
        """
        settings: dict[int, int] = {
            Setting.QPACK_MAX_TABLE_CAPACITY: self._max_table_capacity,
            Setting.QPACK_BLOCKED_STREAMS: self._blocked_streams,
            Setting.ENABLE_CONNECT_PROTOCOL: 1,
            Setting.DUMMY: 1,
        }
        if self._enable_webtransport:
            settings[Setting.H3_DATAGRAM] = 1
            settings[Setting.ENABLE_WEBTRANSPORT] = 1
        return settings

    def _handle_control_frame(self, frame_type: int, frame_data: bytes) -> None:
        """
        Handle a frame received on the peer's control stream.
        """
        if frame_type != FrameType.SETTINGS and not self._settings_received:
            raise MissingSettingsError

        if frame_type == FrameType.SETTINGS:
            if self._settings_received:
                raise FrameUnexpected("SETTINGS have already been received")
            settings = parse_settings(frame_data)
            self._validate_settings(settings)
            self._received_settings = settings
            encoder = self._encoder.apply_settings(
                max_table_capacity=settings.get(Setting.QPACK_MAX_TABLE_CAPACITY, 0),
                blocked_streams=settings.get(Setting.QPACK_BLOCKED_STREAMS, 0),
            )
            self._quic.send_stream_data(self._local_encoder_stream_id, encoder)
            self._settings_received = True
        elif frame_type == FrameType.MAX_PUSH_ID:
            if self._is_client:
                raise FrameUnexpected("Servers must not send MAX_PUSH_ID")
            self._max_push_id = parse_max_push_id(frame_data)
        elif frame_type in (
            FrameType.DATA,
            FrameType.HEADERS,
            FrameType.PUSH_PROMISE,
            FrameType.DUPLICATE_PUSH,
        ):
            raise FrameUnexpected("Invalid frame type on control stream")

    def _check_content_length(self, stream: H3Stream):
        if (
            self._is_client
            and (stream.request_method == b"HEAD" or stream.response_status == 304)
        ):
            return

        if (
            stream.expected_content_length is not None
            and stream.content_length != stream.expected_content_length
        ):
            raise MessageError("content-length does not match data size")

    def _handle_request_or_push_frame(
        self,
        frame_type: int,
        frame_data: Optional[bytes],
        stream: H3Stream,
        stream_ended: bool,
    ) -> list[H3Event]:
        """
        Handle a frame received on a request or push stream.
        """
        http_events: list[H3Event] = []

        if frame_type == FrameType.DATA:
            # check DATA frame is allowed
            if stream.headers_recv_state != HeadersState.AFTER_HEADERS:
                raise FrameUnexpected("DATA frame is not allowed in this state")

            if frame_data is not None:
                stream.content_length += len(frame_data)

            if stream_ended:
                self._check_content_length(stream)

            if stream_ended or frame_data:
                http_events.append(
                    DataReceived(
                        data=frame_data,
                        push_id=stream.push_id,
                        stream_ended=stream_ended,
                        stream_id=stream.stream_id,
                    )
                )
        elif frame_type == FrameType.HEADERS:
            # check HEADERS frame is allowed
            if stream.headers_recv_state == HeadersState.AFTER_TRAILERS:
                raise FrameUnexpected("HEADERS frame is not allowed in this state")

            # try to decode HEADERS, may raise pylsqpack.StreamBlocked
            headers = self._decode_headers(stream.stream_id, frame_data)

            # validate headers
            if stream.headers_recv_state == HeadersState.INITIAL:
                if self._is_client:
                    validate_response_headers(headers, stream)
                else:
                    validate_request_headers(headers, stream)
            else:
                validate_trailers(headers)

            # content-length needs checking even when there is no data
            if stream_ended:
                self._check_content_length(stream)

            # log frame
            if self._quic_logger is not None:
                self._quic_logger.log_event(
                    category="http",
                    event="frame_parsed",
                    data=self._quic_logger.encode_http3_headers_frame(
                        length=(
                            stream.blocked_frame_size
                            if frame_data is None
                            else len(frame_data)
                        ),
                        headers=headers,
                        stream_id=stream.stream_id,
                    ),
                )

            # FIXME: do not simply ignore interim responses here?
            if stream.response_status is not None:  # skip interim responses
                # update state and emit headers
                if stream.headers_recv_state == HeadersState.INITIAL:
                    stream.headers_recv_state = HeadersState.AFTER_HEADERS
                else:
                    stream.headers_recv_state = HeadersState.AFTER_TRAILERS
                http_events.append(
                    HeadersReceived(
                        headers=headers,
                        push_id=stream.push_id,
                        stream_id=stream.stream_id,
                        stream_ended=stream_ended,
                    )
                )
        elif frame_type == FrameType.PUSH_PROMISE and stream.push_id is None:
            if not self._is_client:
                raise FrameUnexpected("Clients must not send PUSH_PROMISE")
            frame_buf = Buffer(data=frame_data)
            push_id = frame_buf.pull_uint_var()
            headers = self._decode_headers(
                stream.stream_id, frame_data[frame_buf.tell() :]
            )

            # validate headers
            validate_push_promise_headers(headers)

            # log frame
            if self._quic_logger is not None:
                self._quic_logger.log_event(
                    category="http",
                    event="frame_parsed",
                    data=self._quic_logger.encode_http3_push_promise_frame(
                        length=len(frame_data),
                        headers=headers,
                        push_id=push_id,
                        stream_id=stream.stream_id,
                    ),
                )

            # emit event
            http_events.append(
                PushPromiseReceived(
                    headers=headers, push_id=push_id, stream_id=stream.stream_id
                )
            )
        elif frame_type in (
            FrameType.PRIORITY,
            FrameType.CANCEL_PUSH,
            FrameType.SETTINGS,
            FrameType.PUSH_PROMISE,
            FrameType.GOAWAY,
            FrameType.MAX_PUSH_ID,
            FrameType.DUPLICATE_PUSH,
        ):
            raise FrameUnexpected(
                "Invalid frame type on request stream"
                if stream.push_id is None
                else "Invalid frame type on push stream"
            )

        return http_events

    def _init_connection(self) -> None:
        # send our settings
        self._local_control_stream_id = self._create_uni_stream(StreamType.CONTROL)
        self._sent_settings = self._get_local_settings()
        self._quic.send_stream_data(
            self._local_control_stream_id,
            encode_frame(FrameType.SETTINGS, encode_settings(self._sent_settings)),
        )
        if self._is_client and self._max_push_id is not None:
            self._quic.send_stream_data(
                self._local_control_stream_id,
                encode_frame(FrameType.MAX_PUSH_ID, encode_uint_var(self._max_push_id)),
            )

        # create encoder and decoder streams
        self._local_encoder_stream_id = self._create_uni_stream(
            StreamType.QPACK_ENCODER
        )
        self._local_decoder_stream_id = self._create_uni_stream(
            StreamType.QPACK_DECODER
        )

    def _log_stream_type(
        self, stream_id: int, stream_type: int, push_id: Optional[int] = None
    ) -> None:
        if self._quic_logger is not None:
            type_name = {
                0: "control",
                1: "push",
                2: "qpack_encoder",
                3: "qpack_decoder",
                0x54: "webtransport",  # NOTE: not standardized yet
            }.get(stream_type, "unknown")

            data = {"new": type_name, "stream_id": stream_id}
            if push_id is not None:
                data["associated_push_id"] = push_id

            self._quic_logger.log_event(
                category="http",
                event="stream_type_set",
                data=data,
            )

    def _receive_datagram(self, data: bytes) -> list[H3Event]:
        """
        Handle a datagram.
        """
        buf = Buffer(data=data)
        try:
            quarter_stream_id = buf.pull_uint_var()
        except BufferReadError:
            raise DatagramError("Could not parse quarter stream ID")
        return [
            DatagramReceived(data=data[buf.tell() :], stream_id=quarter_stream_id * 4)
        ]

    def _receive_stream_data(self, event: StreamDataReceived) -> list[H3Event]:
        stream_id = event.stream_id
        with self._get_or_create_stream(stream_id) as stream:
            if stream_is_unidirectional(stream_id):
                return self._receive_stream_data_uni(
                    stream, event.data, event.end_stream
                )
            else:
                return self._receive_request_or_push_data(
                    stream, event.data, event.end_stream
                )

    def _receive_request_or_push_data(
        self, stream: H3Stream, data: bytes, stream_ended: bool
    ) -> list[H3Event]:
        """
        Handle data received on a request or push stream.
        """
        http_events: list[H3Event] = []

        stream.buffer += data
        if stream_ended:
            stream.receiving_ended = True
        if stream.blocked:
            return http_events

        # shortcut for WEBTRANSPORT_STREAM frame fragments
        if (
            stream.frame_type == FrameType.WEBTRANSPORT_STREAM
            and stream.session_id is not None
        ):
            http_events.append(
                WebTransportStreamDataReceived(
                    data=stream.buffer,
                    session_id=stream.session_id,
                    stream_id=stream.stream_id,
                    stream_ended=stream_ended,
                )
            )
            stream.buffer = b""
            return http_events

        # shortcut for DATA frame fragments
        if (
            stream.frame_type == FrameType.DATA
            and stream.frame_size is not None
            and len(stream.buffer) < stream.frame_size
        ):
            stream.content_length += len(stream.buffer)
            http_events.append(
                DataReceived(
                    data=stream.buffer,
                    push_id=stream.push_id,
                    stream_id=stream.stream_id,
                    stream_ended=False,
                )
            )
            stream.frame_size -= len(stream.buffer)
            stream.buffer = b""
            return http_events

        # handle lone FIN
        if stream_ended and not stream.buffer:
            self._check_content_length(stream)

            http_events.append(
                DataReceived(
                    data=b"",
                    push_id=stream.push_id,
                    stream_id=stream.stream_id,
                    stream_ended=True,
                )
            )
            return http_events

        buf = Buffer(data=stream.buffer)
        consumed = 0

        while not buf.eof():
            # fetch next frame header
            if stream.frame_size is None:
                try:
                    stream.frame_type = buf.pull_uint_var()
                    stream.frame_size = buf.pull_uint_var()
                except BufferReadError:
                    break
                consumed = buf.tell()

                # WEBTRANSPORT_STREAM frames last until the end of the stream
                if stream.frame_type == FrameType.WEBTRANSPORT_STREAM:
                    stream.session_id = stream.frame_size
                    stream.frame_size = None

                    frame_data = stream.buffer[consumed:]
                    stream.buffer = b""

                    self._log_stream_type(
                        stream_id=stream.stream_id, stream_type=StreamType.WEBTRANSPORT
                    )

                    if frame_data or stream_ended:
                        http_events.append(
                            WebTransportStreamDataReceived(
                                data=frame_data,
                                session_id=stream.session_id,
                                stream_id=stream.stream_id,
                                stream_ended=stream_ended,
                            )
                        )
                    return http_events

                # log frame
                if (
                    self._quic_logger is not None
                    and stream.frame_type == FrameType.DATA
                ):
                    self._quic_logger.log_event(
                        category="http",
                        event="frame_parsed",
                        data=self._quic_logger.encode_http3_data_frame(
                            length=stream.frame_size, stream_id=stream.stream_id
                        ),
                    )

            # check how much data is available
            chunk_size = min(stream.frame_size, buf.capacity - consumed)
            if stream.frame_type != FrameType.DATA and chunk_size < stream.frame_size:
                break

            # read available data
            frame_data = buf.pull_bytes(chunk_size)
            frame_type = stream.frame_type
            consumed = buf.tell()

            # detect end of frame
            stream.frame_size -= chunk_size
            if not stream.frame_size:
                stream.frame_size = None
                stream.frame_type = None

            try:
                http_events.extend(
                    self._handle_request_or_push_frame(
                        frame_type=frame_type,
                        frame_data=frame_data,
                        stream=stream,
                        stream_ended=stream.receiving_ended and buf.eof(),
                    )
                )
            except pylsqpack.StreamBlocked:
                stream.blocked = True
                stream.blocked_frame_size = len(frame_data)
                break

        # remove processed data from buffer
        stream.buffer = stream.buffer[consumed:]

        return http_events

    def _receive_stream_data_uni(
        self, stream: H3Stream, data: bytes, stream_ended: bool
    ) -> list[H3Event]:
        http_events: list[H3Event] = []

        stream.buffer += data
        if stream_ended:
            stream.receiving_ended = True

        buf = Buffer(data=stream.buffer)
        consumed = 0
        unblocked_streams: Set[int] = set()

        while (
            stream.stream_type
            in (StreamType.PUSH, StreamType.CONTROL, StreamType.WEBTRANSPORT)
            or not buf.eof()
        ):
            # fetch stream type for unidirectional streams
            if stream.stream_type is None:
                try:
                    stream.stream_type = buf.pull_uint_var()
                except BufferReadError:
                    break
                consumed = buf.tell()

                # check unicity
                if stream.stream_type == StreamType.CONTROL:
                    if self._peer_control_stream_id is not None:
                        raise StreamCreationError("Only one control stream is allowed")
                    self._peer_control_stream_id = stream.stream_id
                elif stream.stream_type == StreamType.QPACK_DECODER:
                    if self._peer_decoder_stream_id is not None:
                        raise StreamCreationError(
                            "Only one QPACK decoder stream is allowed"
                        )
                    self._peer_decoder_stream_id = stream.stream_id
                elif stream.stream_type == StreamType.QPACK_ENCODER:
                    if self._peer_encoder_stream_id is not None:
                        raise StreamCreationError(
                            "Only one QPACK encoder stream is allowed"
                        )
                    self._peer_encoder_stream_id = stream.stream_id

                # for PUSH, logging is performed once the push_id is known
                if stream.stream_type != StreamType.PUSH:
                    self._log_stream_type(
                        stream_id=stream.stream_id, stream_type=stream.stream_type
                    )

            if stream.stream_type == StreamType.CONTROL:
                if stream_ended:
                    raise ClosedCriticalStream("Closing control stream is not allowed")

                # fetch next frame
                try:
                    frame_type = buf.pull_uint_var()
                    frame_length = buf.pull_uint_var()
                    frame_data = buf.pull_bytes(frame_length)
                except BufferReadError:
                    break
                consumed = buf.tell()

                self._handle_control_frame(frame_type, frame_data)
            elif stream.stream_type == StreamType.PUSH:
                # fetch push id
                if stream.push_id is None:
                    try:
                        stream.push_id = buf.pull_uint_var()
                    except BufferReadError:
                        break
                    consumed = buf.tell()

                    self._log_stream_type(
                        push_id=stream.push_id,
                        stream_id=stream.stream_id,
                        stream_type=stream.stream_type,
                    )

                # remove processed data from buffer
                stream.buffer = stream.buffer[consumed:]

                return self._receive_request_or_push_data(stream, b"", stream_ended)
            elif stream.stream_type == StreamType.WEBTRANSPORT:
                # fetch session id
                if stream.session_id is None:
                    try:
                        stream.session_id = buf.pull_uint_var()
                    except BufferReadError:
                        break
                    consumed = buf.tell()

                frame_data = stream.buffer[consumed:]
                stream.buffer = b""

                if frame_data or stream_ended:
                    http_events.append(
                        WebTransportStreamDataReceived(
                            data=frame_data,
                            session_id=stream.session_id,
                            stream_ended=stream.receiving_ended,
                            stream_id=stream.stream_id,
                        )
                    )
                return http_events
            elif stream.stream_type == StreamType.QPACK_DECODER:
                # feed unframed data to decoder
                data = buf.pull_bytes(buf.capacity - buf.tell())
                consumed = buf.tell()
                try:
                    self._encoder.feed_decoder(data)
                except pylsqpack.DecoderStreamError as exc:
                    raise QpackDecoderStreamError() from exc
                self._decoder_bytes_received += len(data)
            elif stream.stream_type == StreamType.QPACK_ENCODER:
                # feed unframed data to encoder
                data = buf.pull_bytes(buf.capacity - buf.tell())
                consumed = buf.tell()
                try:
                    unblocked_streams.update(self._decoder.feed_encoder(data))
                except pylsqpack.EncoderStreamError as exc:
                    raise QpackEncoderStreamError() from exc
                self._encoder_bytes_received += len(data)
            else:
                # unknown stream type, discard data
                buf.seek(buf.capacity)
                consumed = buf.tell()

        # remove processed data from buffer
        stream.buffer = stream.buffer[consumed:]

        # process unblocked streams
        for stream_id in unblocked_streams:
            stream = self._stream[stream_id]

            # resume headers
            http_events.extend(
                self._handle_request_or_push_frame(
                    frame_type=FrameType.HEADERS,
                    frame_data=None,
                    stream=stream,
                    stream_ended=stream.receiving_ended and not stream.buffer,
                )
            )
            stream.blocked = False
            stream.blocked_frame_size = None

            # resume processing
            if stream.buffer:
                http_events.extend(
                    self._receive_request_or_push_data(
                        stream, b"", stream.receiving_ended
                    )
                )

        return http_events

    def _validate_settings(self, settings: dict[int, int]) -> None:
        for setting in [
            Setting.ENABLE_CONNECT_PROTOCOL,
            Setting.ENABLE_WEBTRANSPORT,
            Setting.H3_DATAGRAM,
        ]:
            if setting in settings and settings[setting] not in (0, 1):
                raise SettingsError(f"{setting.name} setting must be 0 or 1")

        if (
            settings.get(Setting.H3_DATAGRAM) == 1
            and self._quic._remote_max_datagram_frame_size is None
        ):
            raise SettingsError(
                "H3_DATAGRAM requires max_datagram_frame_size transport parameter"
            )

        if (
            settings.get(Setting.ENABLE_WEBTRANSPORT) == 1
            and settings.get(Setting.H3_DATAGRAM) != 1
        ):
            raise SettingsError("ENABLE_WEBTRANSPORT requires H3_DATAGRAM")
