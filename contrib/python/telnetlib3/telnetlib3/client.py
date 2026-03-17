#!/usr/bin/env python3
"""Telnet Client API for the 'telnetlib3' python package."""

from __future__ import annotations

# std imports
import os
import ssl as ssl_module
import sys
import codecs
import struct
import asyncio
import argparse
import functools
from typing import Any, Dict, List, Tuple, Union, Callable, Optional, Sequence

# local
from telnetlib3 import accessories, client_base
from telnetlib3._types import ShellCallback
from telnetlib3.stream_reader import TelnetReader, TelnetReaderUnicode
from telnetlib3.stream_writer import TelnetWriter, TelnetWriterUnicode

__all__ = ("TelnetClient", "TelnetTerminalClient", "open_connection")

#: Default GMCP modules requested via ``Core.Supports.Set``.
_DEFAULT_GMCP_MODULES = [
    "Char 1",
    "Char.Vitals 1",
    "Char.Items 1",
    "Room 1",
    "Room.Info 1",
    "Comm 1",
    "Comm.Channel 1",
]


class TelnetClient(client_base.BaseClient):
    """
    Telnet client that supports all common options.

    Useful for automation, appearing as a virtual terminal to the remote end without requiring an
    interactive terminal to run.
    """

    #: On :meth:`send_env`, the value of 'LANG' will be 'C' for binary
    #: transmission.  When encoding is specified (utf8 by default), the LANG
    #: variable must also contain a locale, this value is used, providing a
    #: full default LANG value of 'en_US.utf8'
    DEFAULT_LOCALE = "en_US"

    #: Default environment variables to send via NEW_ENVIRON
    DEFAULT_SEND_ENVIRON = ("TERM", "LANG", "COLUMNS", "LINES", "COLORTERM")

    def __init__(
        self,
        term: str = "unknown",
        cols: int = 80,
        rows: int = 25,
        tspeed: Tuple[int, int] = (38400, 38400),
        xdisploc: str = "",
        send_environ: Optional[Sequence[str]] = None,
        shell: Optional[ShellCallback] = None,
        encoding: Union[str, bool] = "utf8",
        encoding_errors: str = "strict",
        force_binary: bool = False,
        connect_minwait: float = 0,
        connect_maxwait: float = 4.0,
        limit: Optional[int] = None,
        waiter_closed: Optional[asyncio.Future[None]] = None,
        _waiter_connected: Optional[asyncio.Future[None]] = None,
        gmcp_modules: Optional[List[str]] = None,
        gmcp_log: bool = False,
    ) -> None:
        """Initialize TelnetClient with terminal parameters."""
        super().__init__(
            shell=shell,
            encoding=encoding,
            encoding_errors=encoding_errors,
            force_binary=force_binary,
            connect_minwait=connect_minwait,
            connect_maxwait=connect_maxwait,
            limit=limit,
            waiter_closed=waiter_closed,
            _waiter_connected=_waiter_connected,
        )
        self._gmcp_modules = gmcp_modules or list(_DEFAULT_GMCP_MODULES)
        self._gmcp_log = gmcp_log
        self._gmcp_hello_sent = False
        self._send_environ = set(send_environ or self.DEFAULT_SEND_ENVIRON)
        self._extra.update(
            {
                "charset": encoding or "",
                # for our purposes, we only send the second part (encoding) of our
                # 'lang' variable, CHARSET negotiation does not provide locale
                # negotiation; this is better left to the real LANG variable
                # negotiated as-is by send_env().
                #
                # So which locale should we represent? Rather than using the
                # locale.getpreferredencoding() method, we provide a deterministic
                # class value DEFAULT_LOCALE (en_US), derive and modify as needed.
                "lang": ("C" if not encoding else self.DEFAULT_LOCALE + "." + str(encoding)),
                "cols": cols,
                "rows": rows,
                "term": term,
                "tspeed": f"{tspeed[0]},{tspeed[1]}",
                "xdisploc": xdisploc,
            }
        )

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """
        Handle connection made to server.

        Wire up telnet option callbacks for terminal type, speed, display, environment, window size,
        and character set negotiation.
        """
        from telnetlib3.telopt import NAWS, TTYPE, TSPEED, CHARSET, XDISPLOC, NEW_ENVIRON

        super().connection_made(transport)

        # Wire extended rfc callbacks for requests of
        # terminal attributes, environment values, etc.
        for opt, func in (
            (TTYPE, self.send_ttype),
            (TSPEED, self.send_tspeed),
            (XDISPLOC, self.send_xdisploc),
            (NEW_ENVIRON, self.send_env),
            (NAWS, self.send_naws),
            (CHARSET, self.send_charset),
        ):
            self.writer.set_ext_send_callback(opt, func)

        # Override the default handle_will method to detect when both sides support CHARSET
        # Store the original only on first connection to prevent chain growth on reconnect.
        if not hasattr(self.writer, "_original_handle_will"):
            self.writer._original_handle_will = self.writer.handle_will
        else:
            self.writer.handle_will = (  # type: ignore[method-assign]
                self.writer._original_handle_will
            )
        original_handle_will = self.writer.handle_will
        writer = self.writer

        def enhanced_handle_will(opt: bytes) -> None:
            original_handle_will(opt)

            # If this was a WILL CHARSET from the server, and we also have WILL CHARSET enabled,
            # log that both sides support CHARSET. The server should initiate the actual REQUEST.
            if (
                opt == CHARSET
                and writer.remote_option.enabled(CHARSET)
                and writer.local_option.enabled(CHARSET)
            ):
                self.log.debug("Both sides support CHARSET, ready for server to initiate REQUEST")

        self.writer.handle_will = enhanced_handle_will  # type: ignore[method-assign]

        self._setup_gmcp()

    def _setup_gmcp(self) -> None:
        """Wire GMCP callback and WILL-detection for Core.Hello handshake."""
        from telnetlib3.telopt import GMCP

        self.writer.set_ext_callback(GMCP, self._on_gmcp)

        # Capture current handle_will (already includes CHARSET wrapper).
        # On reconnect, _original_handle_will was already restored in connection_made,
        # so this always wraps exactly once.
        original_handle_will_gmcp = self.writer.handle_will

        def _detect_gmcp_will(opt: bytes) -> None:
            original_handle_will_gmcp(opt)
            if opt == GMCP and self.writer.remote_option.enabled(GMCP):
                self._send_gmcp_hello()

        self.writer.handle_will = _detect_gmcp_will  # type: ignore[method-assign]

    def _send_gmcp_hello(self) -> None:
        """Send ``Core.Hello`` and ``Core.Supports.Set`` after GMCP negotiation."""
        if self._gmcp_hello_sent:
            return
        self._gmcp_hello_sent = True
        from telnetlib3.accessories import get_version

        self.writer.send_gmcp("Core.Hello", {"client": "telnetlib3", "version": get_version()})
        self.writer.send_gmcp("Core.Supports.Set", self._gmcp_modules)
        self.log.info("GMCP handshake: Core.Hello + Core.Supports.Set %s", self._gmcp_modules)

    def _on_gmcp(self, package: str, data: Any) -> None:
        """Store incoming GMCP data on ``writer.ctx``, merging dict updates."""
        gmcp = self.writer.ctx.gmcp_data
        if isinstance(data, dict) and isinstance(gmcp.get(package), dict):
            gmcp[package].update(data)
        else:
            gmcp[package] = data
        if self._gmcp_log:
            self.log.info("GMCP: %s %r", package, data)
        else:
            self.log.debug("GMCP: %s %r", package, data)

    def send_ttype(self) -> str:
        """Callback for responding to TTYPE requests."""
        result: str = self._extra["term"]
        return result

    def send_tspeed(self) -> Tuple[int, int]:
        """Callback for responding to TSPEED requests."""
        parts = self._extra["tspeed"].split(",")
        return (int(parts[0]), int(parts[1]))

    def send_xdisploc(self) -> str:
        """Callback for responding to XDISPLOC requests."""
        result: str = self._extra["xdisploc"]
        return result

    def send_env(self, keys: Sequence[str]) -> Dict[str, Any]:
        """
        Callback for responding to NEW_ENVIRON requests.

        Only sends variables listed in ``_send_environ`` (set via ``send_environ``
        parameter or ``--send-environ`` CLI option).

        :param keys: Values are requested for the keys specified. When empty, all environment
            values that wish to be volunteered should be returned.
        :returns: Environment values requested, or an empty string for keys not
            available. A return value must be given for each key requested.
        """
        # All available values
        all_env = {
            # Terminal info from connection parameters
            "LANG": self._extra["lang"],
            "TERM": self._extra["term"],
            "LINES": self._extra["rows"],
            "COLUMNS": self._extra["cols"],
            # Environment variables from os.environ
            "COLORTERM": os.environ.get("COLORTERM", ""),
            "USER": os.environ.get("USER", ""),
            "HOME": os.environ.get("HOME", ""),
            "SHELL": os.environ.get("SHELL", ""),
            # Note: DISPLAY intentionally not available (security)
        }
        # Filter to only allowed variables
        env = {k: v for k, v in all_env.items() if k in self._send_environ}
        return {key: env.get(key, "") for key in keys} or env

    @staticmethod
    def _normalize_charset_name(name: str) -> str:
        """
        Normalize server-advertised charset names for :func:`codecs.lookup`.

        Servers sometimes advertise non-standard encoding names that Python's
        codec registry does not recognise.  This tries progressively simpler
        variations until one resolves:

        1. Original name (spaces -> hyphens)
        2. Leading zeros stripped from numeric parts (``iso-8859-02`` -> ``iso-8859-2``)
        3. Hyphens removed entirely (``cp-1250`` -> ``cp1250``)
        4. Hyphens removed from all but the first segment (``iso-8859-2`` kept)

        :param name: Raw charset name from the server.
        :returns: Normalized name suitable for :func:`codecs.lookup`.
        """
        import re

        base = name.strip().replace(" ", "-")
        # Strip leading zeros from numeric segments: iso-8859-02 -> iso-8859-2
        no_leading_zeros = re.sub(r"-0+(\d)", r"-\1", base)
        # All hyphens removed: cp-1250 -> cp1250
        no_hyphens = base.replace("-", "")
        # Keep first hyphen-segment, collapse the rest: iso-8859-2 stays
        parts = no_leading_zeros.split("-")
        if len(parts) > 2:
            partial = parts[0] + "-" + "".join(parts[1:])
        else:
            partial = no_leading_zeros
        for candidate in (base, no_leading_zeros, no_hyphens, partial):
            try:
                codecs.lookup(candidate)
                return candidate
            except LookupError:
                continue
        return base

    def send_charset(self, offered: List[str]) -> str:
        """
        Callback for responding to CHARSET requests.

        Simplified policy:

        - If client has explicit encoding that matches an offered charset, use it

        - If client has explicit encoding that isn't offered,

           - For Latin-1 (weak default), accept first viable offered encoding

           - For other explicit encodings, reject (keep client's choice)

        - If no explicit encoding preference, accept first viable offered encoding

        - If no viable encodings found, reject

        :param offered: CHARSET options offered by server.
        :returns: Character encoding agreed to be used, or empty string to reject.
        """
        # Get client's desired encoding canonical name
        desired_name = None
        if self.default_encoding and isinstance(self.default_encoding, str):
            try:
                desired_name = codecs.lookup(self.default_encoding).name
            except LookupError:
                # Unknown encoding, treat as no explicit preference
                pass

        # Find first viable offered encoding and check for exact match
        first_viable = None
        matched_offer = None

        for offer in offered:
            try:
                canon = codecs.lookup(self._normalize_charset_name(offer)).name

                # Record first viable encoding
                if first_viable is None:
                    first_viable = (offer, canon)

                # Check for exact match with desired encoding
                if desired_name and canon == desired_name:
                    matched_offer = (offer, canon)
                    break

            except LookupError:
                self.log.info("LookupError: encoding %s not available", offer)
                continue

        # Decision logic:

        # Case 1: Found exact match for desired encoding
        if matched_offer:
            offer, canon = matched_offer
            self._extra["charset"] = canon
            self._extra["lang"] = self.DEFAULT_LOCALE + "." + canon
            self.log.debug("encoding negotiated: %s", offer)
            return offer

        # Case 2: Has explicit encoding but not offered
        if desired_name:
            # Special case: Latin-1 is a weak default, accept first viable instead
            is_latin1 = desired_name in ("latin-1", "latin1", "iso8859-1", "iso-8859-1")
            if is_latin1 and first_viable:
                offer, canon = first_viable
                self._extra["charset"] = canon
                self._extra["lang"] = self.DEFAULT_LOCALE + "." + canon
                self.log.debug("encoding negotiated: %s", offer)
                return offer

            # Otherwise reject - keep client's explicit encoding
            self.log.debug("Declining offered charsets %s; prefer %s", offered, desired_name)
            return ""

        # Case 3: No explicit preference, use first viable
        if first_viable:
            offer, canon = first_viable
            self._extra["charset"] = canon
            self._extra["lang"] = self.DEFAULT_LOCALE + "." + canon
            self.log.debug("encoding negotiated: %s", offer)
            return offer

        # Case 4: No viable encodings found
        self.log.warning("No suitable encoding offered by server: %s", offered)
        return ""

    def send_naws(self) -> Tuple[int, int]:
        """
        Callback for responding to NAWS requests.

        :returns: Client window size as (rows, columns).
        """
        return (self._extra["rows"], self._extra["cols"])

    def encoding(self, outgoing: Optional[bool] = None, incoming: Optional[bool] = None) -> str:
        """
        Return encoding for the given stream direction.

        :param outgoing: Whether the return value is suitable for
            encoding bytes for transmission to server.
        :param incoming: Whether the return value is suitable for
            decoding bytes received by the client.
        :raises TypeError: When a direction argument, either ``outgoing``
            or ``incoming``, was not set ``True``.
        :returns: ``'US-ASCII'`` for the directions indicated, unless
            ``BINARY`` :rfc:`856` has been negotiated for the direction
            indicated or ``force_binary`` is set ``True``.
        """
        if not (outgoing or incoming):
            raise TypeError(
                "encoding arguments 'outgoing' and 'incoming' are required: toggle at least one."
            )

        # may we encode in the direction indicated?
        outgoing_only = outgoing and not incoming
        incoming_only = not outgoing and incoming
        bidirectional = outgoing and incoming
        may_encode = (
            (outgoing_only and self.writer.outbinary)
            or (incoming_only and self.writer.inbinary)
            or (bidirectional and self.writer.outbinary and self.writer.inbinary)
        )

        if self.force_binary or may_encode:
            # The 'charset' value, initialized using keyword argument
            # default_encoding, may be re-negotiated later.  Only the CHARSET
            # negotiation method allows the server to select an encoding, so
            # this value is reflected here by a single return statement.
            result: str = self._extra["charset"]
            return result
        return "US-ASCII"


class TelnetTerminalClient(TelnetClient):
    """Telnet client for sessions with a network virtual terminal (NVT)."""

    def send_naws(self) -> Tuple[int, int]:
        """
        Callback replies to request for window size, NAWS :rfc:`1073`.

        :returns: Window dimensions by lines and columns.
        """
        return self._winsize()

    def send_env(self, keys: Sequence[str]) -> Dict[str, Any]:
        """
        Callback replies to request for env values, NEW_ENVIRON :rfc:`1572`.

        :returns: Super class value updated with window LINES and COLUMNS.
        """
        env = super().send_env(keys)
        env["LINES"], env["COLUMNS"] = self._winsize()
        return env

    @staticmethod
    def _winsize() -> Tuple[int, int]:
        try:
            import fcntl
            import termios

            fmt = "hhhh"
            buf = b"\x00" * struct.calcsize(fmt)
            val = fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, buf)
            rows, cols, _, _ = struct.unpack(fmt, val)
            return rows, cols
        except (ImportError, IOError):
            return (int(os.environ.get("LINES", 25)), int(os.environ.get("COLUMNS", 80)))


async def open_connection(
    host: Optional[str] = None,
    port: int = 23,
    *,
    client_factory: Optional[Callable[..., client_base.BaseClient]] = None,
    family: int = 0,
    flags: int = 0,
    local_addr: Optional[Tuple[str, int]] = None,
    encoding: Union[str, bool] = "utf8",
    encoding_errors: str = "replace",
    force_binary: bool = False,
    term: str = "unknown",
    cols: int = 80,
    rows: int = 25,
    tspeed: Tuple[int, int] = (38400, 38400),
    xdisploc: str = "",
    shell: Optional[ShellCallback] = None,
    connect_minwait: float = 0,
    connect_maxwait: float = 3.0,
    connect_timeout: Optional[float] = None,
    waiter_closed: Optional[asyncio.Future[None]] = None,
    _waiter_connected: Optional[asyncio.Future[None]] = None,
    limit: Optional[int] = None,
    send_environ: Optional[Sequence[str]] = None,
    ssl: Union[bool, ssl_module.SSLContext, None] = None,
    server_hostname: Optional[str] = None,
) -> Tuple[Union[TelnetReader, TelnetReaderUnicode], Union[TelnetWriter, TelnetWriterUnicode]]:
    """
    Connect to a TCP Telnet server as a Telnet client.

    :param host: Remote Internet TCP Server host.
    :param port: Remote Internet host TCP port.
    :param client_factory: Client connection class factory.  When ``None``,
        :class:`TelnetTerminalClient` is used when *stdin* is attached to a
        terminal, :class:`TelnetClient` otherwise.
    :param family: Same meaning as
        :meth:`asyncio.loop.create_connection`.
    :param flags: Same meaning as
        :meth:`asyncio.loop.create_connection`.
    :param local_addr: Same meaning as
        :meth:`asyncio.loop.create_connection`.
    :param encoding: The default assumed encoding, or ``False`` to disable
        unicode support.  This value is used for decoding bytes received by and
        encoding bytes transmitted to the Server.  These values are preferred
        in response to NEW_ENVIRON :rfc:`1572` as environment value ``LANG``,
        and by CHARSET :rfc:`2066` negotiation.

        The server's attached ``reader, writer`` streams accept and return
        unicode, unless this value is explicitly set ``False``.  In that case,
        the attached streams interfaces are bytes-only.
    :param encoding_errors: Same meaning as :meth:`codecs.Codec.encode`.

    :param term: Terminal type sent for requests of TTYPE, :rfc:`930` or as
        Environment value TERM by NEW_ENVIRON negotiation, :rfc:`1672`.
    :param cols: Client window dimension sent as Environment value COLUMNS
        by NEW_ENVIRON negotiation, :rfc:`1672` or NAWS :rfc:`1073`.
    :param rows: Client window dimension sent as Environment value LINES by
        NEW_ENVIRON negotiation, :rfc:`1672` or NAWS :rfc:`1073`.
    :param tspeed: Client BPS line speed in form ``(rx, tx)`` for receive and
        transmit, respectively.  Sent when requested by TSPEED, :rfc:`1079`.
    :param xdisploc: String transmitted in response for request of
        XDISPLOC, :rfc:`1086` by server (X11).
    :param connect_minwait: The client allows any additional telnet
        negotiations to be demanded by the server within this period of time
        before launching the shell.  Servers should assert desired negotiation
        on-connect and in response to 1 or 2 round trips.

        A server that does not make any telnet demands, such as a TCP server
        that is not a telnet server, will delay the execution of ``shell`` for
        exactly this amount of time.
    :param connect_maxwait: If the remote end is not compliant, or
        otherwise confused by our demands, the shell continues anyway after the
        greater of this value has elapsed.  A client that is not answering
        option negotiation will delay the start of the shell by this amount.
    :param connect_timeout: Timeout in seconds for the TCP connection to be
        established.  When ``None`` (default), no timeout is applied and the
        connection attempt may block indefinitely.  When specified, a
        :exc:`ConnectionError` is raised if the connection is not established
        within the given time.

    :param force_binary: When ``True``, the encoding is used regardless
        of BINARY mode negotiation.
    :param waiter_closed: Future that completes when the connection is closed.
    :param shell: An async function that is called after negotiation completes,
        receiving arguments ``(reader, writer)``.
    :param limit: The buffer limit for reader stream.
    :param ssl: TLS configuration.  ``True`` creates a default
        :func:`ssl.create_default_context` that verifies CA certificates.
        An :class:`ssl.SSLContext` gives full control.  ``None`` (default)
        uses plain TCP.
    :param server_hostname: Hostname for TLS certificate verification.  When
        ``ssl`` is truthy and *server_hostname* is ``None``, defaults to *host*.
    :return: The reader is a :class:`~.TelnetReader` instance, the writer is a
        :class:`~.TelnetWriter` instance.
    """
    if client_factory is None:
        client_factory = TelnetClient
        if sys.platform != "win32" and sys.stdin.isatty():
            client_factory = TelnetTerminalClient

    def connection_factory() -> client_base.BaseClient:
        return client_factory(
            encoding=encoding,
            encoding_errors=encoding_errors,
            force_binary=force_binary,
            term=term,
            cols=cols,
            rows=rows,
            tspeed=tspeed,
            xdisploc=xdisploc,
            shell=shell,
            connect_minwait=connect_minwait,
            connect_maxwait=connect_maxwait,
            waiter_closed=waiter_closed,
            _waiter_connected=_waiter_connected,
            limit=limit,
            send_environ=send_environ,
        )

    # Resolve TLS context
    ssl_context: Union[ssl_module.SSLContext, None] = None
    if ssl is True:
        ssl_context = ssl_module.create_default_context()
    elif isinstance(ssl, ssl_module.SSLContext):
        ssl_context = ssl

    conn_kwargs: Dict[str, Any] = {"family": family, "flags": flags, "local_addr": local_addr}
    if ssl_context is not None:
        conn_kwargs["ssl"] = ssl_context
        conn_kwargs["server_hostname"] = server_hostname or host or "localhost"

    try:
        _, protocol = await asyncio.wait_for(
            asyncio.get_event_loop().create_connection(
                connection_factory, host or "localhost", port, **conn_kwargs
            ),
            timeout=connect_timeout,
        )
    except asyncio.TimeoutError as exc:
        raise ConnectionError(
            f"TCP connection to {host or 'localhost'}:{port}" f" timed out after {connect_timeout}s"
        ) from exc

    await protocol._waiter_connected

    assert protocol.reader is not None and protocol.writer is not None
    return protocol.reader, protocol.writer


async def run_client() -> None:
    """Command-line 'telnetlib3-client' entry point, via setuptools."""
    args = _transform_args(_get_argument_parser().parse_args())
    config_msg = f"Client configuration: {accessories.repr_mapping(args)}"

    log = accessories.make_logger(
        name=__name__, loglevel=args["loglevel"], logfile=args["logfile"], logfmt=args["logfmt"]
    )
    log.debug(config_msg)

    always_will: set[bytes] = args["always_will"]
    always_do: set[bytes] = args["always_do"]

    # Wrap client factory to inject always_will/always_do and encoding
    # flags before negotiation starts.
    encoding_explicit = args["encoding"] not in ("utf8", "utf-8", False)
    gmcp_modules: Optional[List[str]] = args.get("gmcp_modules")
    gmcp_log: bool = args.get("gmcp_log", False)

    def _client_factory(**kwargs: Any) -> client_base.BaseClient:
        client: TelnetClient
        kwargs["gmcp_modules"] = gmcp_modules
        kwargs["gmcp_log"] = gmcp_log
        if sys.platform != "win32" and sys.stdin.isatty():
            client = TelnetTerminalClient(**kwargs)
        else:
            client = TelnetClient(**kwargs)
        orig_connection_made = client.connection_made

        def _patched_connection_made(transport: asyncio.BaseTransport) -> None:
            orig_connection_made(transport)
            if always_will:
                client.writer.always_will = always_will
            client.writer.always_do = always_do
            from .telopt import GMCP as _GMCP

            client.writer.passive_do = {_GMCP}
            client.writer._encoding_explicit = encoding_explicit

        client.connection_made = _patched_connection_made  # type: ignore[method-assign]
        return client

    client_factory: Optional[Callable[..., client_base.BaseClient]] = _client_factory

    # Wrap the shell callback to inject color filter when enabled
    colormatch: str = args["colormatch"]
    shell_callback = args["shell"]
    if colormatch.lower() != "none":
        from .color_filter import (
            PALETTES,
            ColorConfig,
            ColorFilter,
            PetsciiColorFilter,
            AtasciiControlFilter,
        )

        # Auto-select encoding-specific filters
        encoding_name: str = args.get("encoding", "") or ""
        is_petscii = encoding_name.lower() in ("petscii", "cbm", "commodore", "c64", "c128")
        is_atascii = encoding_name.lower() in ("atascii", "atari8bit", "atari_8bit")
        if colormatch == "petscii":
            colormatch = "c64"
        if is_petscii and colormatch != "c64":
            colormatch = "c64"

        if colormatch not in PALETTES:
            print(
                f"Unknown palette {colormatch!r}," f" available: {', '.join(sorted(PALETTES))}",
                file=sys.stderr,
            )
            sys.exit(1)
        color_config = ColorConfig(
            palette_name=colormatch,
            brightness=args["color_brightness"],
            contrast=args["color_contrast"],
            background_color=args["background_color"],
            ice_colors=args["ice_colors"],
        )
        if is_petscii or colormatch == "c64":
            color_filter_obj: object = PetsciiColorFilter(color_config)
        elif is_atascii:
            color_filter_obj = AtasciiControlFilter()
        else:
            color_filter_obj = ColorFilter(color_config)
        original_shell = shell_callback

        async def _color_shell(
            reader: Union[TelnetReader, TelnetReaderUnicode],
            writer_arg: Union[TelnetWriter, TelnetWriterUnicode],
        ) -> None:
            writer_arg.ctx.color_filter = color_filter_obj
            await original_shell(reader, writer_arg)

        shell_callback = _color_shell

    # Wrap shell to inject raw_mode flag and input translation for retro encodings
    raw_mode_val: Optional[bool] = args.get("raw_mode", False)
    if raw_mode_val is not False:
        from .client_shell import _INPUT_XLAT, _INPUT_SEQ_XLAT, InputFilter

        enc_key = (args.get("encoding", "") or "").lower()
        byte_xlat = dict(_INPUT_XLAT.get(enc_key, {}))
        if args.get("ascii_eol"):
            byte_xlat.pop(0x0D, None)
            byte_xlat.pop(0x0A, None)
        seq_xlat = {} if args.get("ansi_keys") else _INPUT_SEQ_XLAT.get(enc_key, {})
        input_filter: Optional[InputFilter] = (
            InputFilter(seq_xlat, byte_xlat) if (seq_xlat or byte_xlat) else None
        )
        ascii_eol: bool = args.get("ascii_eol", False)
        _inner_shell = shell_callback

        async def _raw_shell(
            reader: Union[TelnetReader, TelnetReaderUnicode],
            writer_arg: Union[TelnetWriter, TelnetWriterUnicode],
        ) -> None:
            ctx = writer_arg.ctx
            ctx.raw_mode = raw_mode_val
            if ascii_eol:
                ctx.ascii_eol = True
            if input_filter is not None:
                ctx.input_filter = input_filter
            await _inner_shell(reader, writer_arg)

        shell_callback = _raw_shell

    # Wrap shell to inject typescript recording file handle
    typescript_path: Optional[str] = args.get("typescript")
    if typescript_path:
        _ts_inner = shell_callback

        async def _typescript_shell(
            reader: Union[TelnetReader, TelnetReaderUnicode],
            writer_arg: Union[TelnetWriter, TelnetWriterUnicode],
        ) -> None:
            ctx = writer_arg.ctx
            assert typescript_path is not None
            ts_file = open(typescript_path, "a", encoding="utf-8")  # noqa: SIM115
            ctx.typescript_file = ts_file
            try:
                await _ts_inner(reader, writer_arg)
            finally:
                ts_file.close()

        shell_callback = _typescript_shell

    # Build connection kwargs explicitly to avoid pylint false positive
    connection_kwargs: Dict[str, Any] = {
        "encoding": args["encoding"],
        "tspeed": args["tspeed"],
        "shell": shell_callback,
        "term": args["term"],
        "force_binary": args["force_binary"],
        "encoding_errors": args["encoding_errors"],
        "connect_minwait": args["connect_minwait"],
        "connect_timeout": args["connect_timeout"],
        "send_environ": args["send_environ"],
    }
    if args.get("ssl"):
        connection_kwargs["ssl"] = args["ssl"]
    if client_factory is not None:
        connection_kwargs["client_factory"] = client_factory

    # connect
    _, writer = await open_connection(args["host"], args["port"], **connection_kwargs)

    # repl loop
    await writer.protocol.waiter_closed


def _get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telnetlib3-client",
        description="Telnet protocol client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("host", action="store", help="hostname")
    parser.add_argument("port", nargs="?", default=23, type=int, help="port number")
    parser.add_argument("--term", default=os.environ.get("TERM", "unknown"), help="terminal type")
    parser.add_argument("--loglevel", default="warn", help="log level")
    parser.add_argument("--logfmt", default=accessories._DEFAULT_LOGFMT, help="log format")
    parser.add_argument("--logfile", help="filepath")
    parser.add_argument(
        "--shell", default="telnetlib3.telnet_client_shell", help="module.function_name"
    )
    parser.add_argument("--encoding", default="utf8", help="encoding name")
    parser.add_argument("--speed", default=38400, type=int, help="connection speed")
    parser.add_argument(
        "--encoding-errors",
        default="replace",
        help="handler for encoding errors",
        choices=("replace", "ignore", "strict"),
    )

    parser.add_argument("--force-binary", action="store_true", help="force encoding", default=True)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--raw-mode",
        action="store_true",
        default=False,
        help="force raw terminal mode (no line buffering, no local echo). "
        "Correct for BBS and retro systems. Default: auto-detect from "
        "server negotiation.",
    )
    mode_group.add_argument(
        "--line-mode",
        action="store_true",
        default=False,
        help="force line-buffered input with local echo. Appropriate for "
        "simple command-line services.",
    )
    parser.add_argument(
        "--connect-minwait", default=0, type=float, help="shell delay for negotiation"
    )
    parser.add_argument(
        "--connect-maxwait", default=4.0, type=float, help="timeout for pending negotiation"
    )
    parser.add_argument(
        "--connect-timeout",
        default=10,
        type=float,
        help="timeout for TCP connection in seconds (default: 10)",
    )
    parser.add_argument(
        "--send-environ",
        default="TERM,LANG,COLUMNS,LINES,COLORTERM",
        help="comma-separated environment variables to send (NEW_ENVIRON)",
    )
    parser.add_argument(
        "--always-will",
        action="append",
        default=[],
        metavar="OPT",
        help="always send WILL for this option (name like MXP or number, repeatable)",
    )
    parser.add_argument(
        "--always-do",
        action="append",
        default=[],
        metavar="OPT",
        help="always send DO for this option (name like GMCP or number, repeatable)",
    )
    parser.add_argument(
        "--colormatch",
        default="vga",
        metavar="PALETTE",
        help=(
            "translate basic 16-color ANSI codes to exact 24-bit RGB values"
            " from a named hardware palette, bypassing the terminal's custom"
            " palette to preserve intended MUD/BBS artwork colors"
            " (vga, xterm, none)"
        ),
    )
    parser.add_argument(
        "--color-brightness",
        default=1.0,
        type=float,
        metavar="FLOAT",
        help="color brightness scale [0.0..1.0], where 1.0 is original",
    )
    parser.add_argument(
        "--color-contrast",
        default=1.0,
        type=float,
        metavar="FLOAT",
        help="color contrast scale [0.0..1.0], where 1.0 is original",
    )
    parser.add_argument(
        "--background-color",
        default="#000000",
        metavar="#RRGGBB",
        help="forced background color as hex RGB (near-black by default)",
    )
    parser.add_argument(
        "--ice-colors",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="treat SGR 5 (blink) as bright background (iCE colors)"
        " for BBS/ANSI art (default: enabled)",
    )
    parser.add_argument(
        "--ascii-eol",
        action="store_true",
        default=False,
        help="use ASCII CR/LF for line endings instead of encoding-native "
        "EOL (e.g. ATASCII 0x9B).  Use for BBSes that display retro "
        "graphics but use standard CR/LF for line breaks.",
    )
    parser.add_argument(
        "--ansi-keys",
        action="store_true",
        default=False,
        help="transmit raw ANSI escape sequences for arrow and function "
        "keys instead of encoding-specific control codes.  Use for "
        "BBSes that expect ANSI cursor sequences.",
    )
    parser.add_argument(
        "--ssl", action="store_true", default=False, help="connect using TLS (TELNETS)"
    )
    parser.add_argument(
        "--ssl-cafile",
        default=None,
        metavar="PATH",
        help="path to CA or self-signed certificate PEM file for TLS verification",
    )
    parser.add_argument(
        "--ssl-no-verify",
        action="store_true",
        default=False,
        help="skip certificate verification for TLS (implies --ssl). "
        "WARNING: this is insecure -- connections are encrypted but "
        "the server identity is not verified, allowing "
        "man-in-the-middle attacks",
    )
    parser.add_argument(
        "--gmcp-modules",
        default=None,
        metavar="MODULES",
        help="comma-separated GMCP module specs to request "
        '(e.g. "Char 1,Room 1,IRE.Rift 1"). '
        "When provided, replaces the built-in defaults.",
    )
    parser.add_argument(
        "--gmcp-log",
        action="store_true",
        default=False,
        help="log all incoming GMCP messages at INFO level " "(default: DEBUG only)",
    )
    parser.add_argument(
        "--typescript",
        default=None,
        metavar="FILE",
        help="record session to FILE (like Unix script(1))",
    )
    return parser


def _parse_option_arg(value: str) -> bytes:
    """
    Resolve a telnet option name or integer to option bytes.

    :param value: Option name (e.g. ``"MXP"``) or decimal byte value (e.g. ``"91"``).
    :returns: Single-byte option value.
    :raises ValueError: When *value* is not a known name or valid integer.
    """
    from .telopt import option_from_name

    try:
        return option_from_name(value)
    except KeyError:
        return bytes([int(value)])


def _parse_background_color(value: str) -> Tuple[int, int, int]:
    """
    Parse hex color string to RGB tuple.

    :param value: Color string like ``"#RRGGBB"`` or ``"RRGGBB"``.
    :returns: (R, G, B) tuple with values 0-255.
    :raises ValueError: When *value* is not a valid hex color.
    """
    h = value.lstrip("#")
    if len(h) != 6:
        raise ValueError(f"invalid hex color: {value!r}")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _transform_args(args: argparse.Namespace) -> Dict[str, Any]:
    # Auto-enable force_binary for any non-ASCII encoding that uses high-bit bytes.
    from .encodings import FORCE_BINARY_ENCODINGS

    force_binary = args.force_binary
    # Three-state: True (forced raw), False (forced line), None (auto-detect)
    if args.raw_mode:
        raw_mode: Optional[bool] = True
    elif args.line_mode:
        raw_mode = False
    else:
        raw_mode = None
    enc_key = args.encoding.lower().replace("-", "_")
    if enc_key not in ("us_ascii", "ascii"):
        force_binary = True
    if enc_key in FORCE_BINARY_ENCODINGS:
        raw_mode = True

    # Build TLS context from --ssl / --ssl-cafile / --ssl-no-verify
    ssl_ctx: Union[ssl_module.SSLContext, None] = None
    if args.ssl or args.ssl_no_verify:
        if args.ssl_no_verify:
            ssl_ctx = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl_module.CERT_NONE
        elif args.ssl_cafile:
            ssl_ctx = ssl_module.create_default_context(cafile=args.ssl_cafile)
        else:
            ssl_ctx = ssl_module.create_default_context()

    return {
        "host": args.host,
        "port": args.port,
        "loglevel": args.loglevel,
        "logfile": args.logfile,
        "logfmt": args.logfmt,
        "encoding": args.encoding,
        "tspeed": (args.speed, args.speed),
        "shell": accessories.function_lookup(args.shell),
        "term": args.term,
        "force_binary": force_binary,
        "encoding_errors": args.encoding_errors,
        "connect_minwait": args.connect_minwait,
        "connect_timeout": args.connect_timeout or None,
        "send_environ": tuple(v.strip() for v in args.send_environ.split(",") if v.strip()),
        "always_will": {_parse_option_arg(v) for v in args.always_will},
        "always_do": {_parse_option_arg(v) for v in args.always_do},
        "colormatch": args.colormatch,
        "color_brightness": args.color_brightness,
        "color_contrast": args.color_contrast,
        "background_color": _parse_background_color(args.background_color),
        "ice_colors": args.ice_colors,
        "raw_mode": raw_mode,
        "ascii_eol": args.ascii_eol,
        "ansi_keys": args.ansi_keys,
        "ssl": ssl_ctx,
        "gmcp_modules": (
            [m.strip() for m in args.gmcp_modules.split(",") if m.strip()]
            if args.gmcp_modules
            else None
        ),
        "gmcp_log": args.gmcp_log,
        "typescript": args.typescript,
    }


def main() -> None:
    """Entry point for telnetlib3-client command."""
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        pass
    except OSError as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)


def _get_fingerprint_argument_parser() -> argparse.ArgumentParser:
    """Build argument parser for ``telnetlib3-fingerprint`` CLI."""
    parser = argparse.ArgumentParser(
        description="Fingerprint a remote telnet server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("host", help="remote hostname or IP")
    parser.add_argument("port", nargs="?", default=23, type=int, help="port number")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="directory for fingerprint data (default: $TELNETLIB3_DATA_DIR)",
    )
    parser.add_argument(
        "--save-json", default=None, metavar="PATH", help="write fingerprint JSON to this path"
    )
    parser.add_argument(
        "--connect-timeout", default=10, type=float, help="TCP connection timeout in seconds"
    )
    parser.add_argument("--loglevel", default="warn", help="log level")
    parser.add_argument("--logfmt", default=accessories._DEFAULT_LOGFMT, help="log format")
    parser.add_argument("--logfile", default=None, help="filepath")
    parser.add_argument(
        "--silent", action="store_true", help="suppress fingerprint output to stdout"
    )
    parser.add_argument(
        "--set-name",
        default=None,
        metavar="NAME",
        help="store this name for the fingerprint in fingerprint_names.json",
    )
    parser.add_argument(
        "--encoding",
        default="ascii",
        metavar="CODEC",
        dest="stream_encoding",
        help="character encoding of the remote server (e.g. cp037 for EBCDIC)",
    )
    parser.add_argument(
        "--ttype", default="VT100", help="terminal type sent in response to TTYPE requests"
    )
    parser.add_argument(
        "--scan-type",
        choices=["quick", "full"],
        default="quick",
        help="probe depth: 'quick' probes core options only, " "'full' includes legacy options",
    )
    parser.add_argument(
        "--send-env",
        action="append",
        metavar="KEY=VALUE",
        default=[],
        help="environment variable to send (repeatable)",
    )
    parser.add_argument(
        "--always-will",
        action="append",
        default=[],
        metavar="OPT",
        help="always send WILL for this option (name like MXP or number, repeatable)",
    )
    parser.add_argument(
        "--always-do",
        action="append",
        default=[],
        metavar="OPT",
        help="always send DO for this option (name like GMCP or number, repeatable)",
    )
    parser.add_argument(
        "--mssp-wait",
        default=5.0,
        type=float,
        help="max seconds since connect to wait for MSSP data",
    )
    parser.add_argument(
        "--banner-quiet-time",
        default=2.0,
        type=float,
        help="seconds of silence before considering banner complete",
    )
    parser.add_argument(
        "--banner-max-wait", default=8.0, type=float, help="max seconds to wait for banner data"
    )
    parser.add_argument(
        "--banner-max-bytes", default=65536, type=int, help="max bytes per banner read call"
    )
    parser.add_argument(
        "--ssl", action="store_true", default=False, help="connect using TLS (TELNETS)"
    )
    parser.add_argument(
        "--ssl-cafile",
        default=None,
        metavar="PATH",
        help="path to CA or self-signed certificate PEM file for TLS verification",
    )
    parser.add_argument(
        "--ssl-no-verify",
        action="store_true",
        default=False,
        help="skip certificate verification for TLS (implies --ssl). "
        "WARNING: this is insecure -- connections are encrypted but "
        "the server identity is not verified, allowing "
        "man-in-the-middle attacks",
    )
    return parser


async def run_fingerprint_client() -> None:
    """
    Connect to a remote telnet server and fingerprint it.

    Parses CLI arguments, binds them into
    :func:`~telnetlib3.server_fingerprinting.fingerprinting_client_shell`
    via :func:`functools.partial`, and runs the connection.
    """
    from . import fingerprinting, server_fingerprinting

    args = _get_fingerprint_argument_parser().parse_args()

    if args.data_dir is not None:
        fingerprinting.DATA_DIR = args.data_dir

    log = accessories.make_logger(
        name=__name__, loglevel=args.loglevel, logfile=args.logfile, logfmt=args.logfmt
    )
    log.debug("Fingerprint client: host=%s port=%d", args.host, args.port)

    shell = functools.partial(
        server_fingerprinting.fingerprinting_client_shell,
        host=args.host,
        port=args.port,
        save_path=args.save_json,
        silent=args.silent,
        set_name=args.set_name,
        environ_encoding=args.stream_encoding,
        scan_type=args.scan_type,
        mssp_wait=args.mssp_wait,
        banner_quiet_time=args.banner_quiet_time,
        banner_max_wait=args.banner_max_wait,
        banner_max_bytes=args.banner_max_bytes,
    )

    # Parse --always-will/--always-do option names/numbers
    fp_always_will = {_parse_option_arg(v) for v in args.always_will}
    fp_always_do = {_parse_option_arg(v) for v in args.always_do}

    # Parse --send-env KEY=VALUE pairs
    extra_env: Dict[str, str] = {}
    for item in args.send_env:
        if "=" in item:
            k, v = item.split("=", 1)
            extra_env[k] = v
        else:
            extra_env[item] = ""

    # environ_encoding must be set on the writer BEFORE negotiation
    # starts, so we wrap the client factory to inject it during
    # connection_made (before begin_negotiation fires).
    environ_encoding = args.stream_encoding
    ttype = args.ttype

    def fingerprint_client_factory(**kwargs: Any) -> client_base.BaseClient:
        # Ensure extra env keys are in the send list
        if extra_env:
            send = set(kwargs.get("send_environ") or TelnetClient.DEFAULT_SEND_ENVIRON)
            send.update(extra_env.keys())
            kwargs["send_environ"] = list(send)
        client = TelnetClient(**kwargs)
        orig_connection_made = client.connection_made
        orig_send_env = client.send_env

        def patched_connection_made(transport: asyncio.BaseTransport) -> None:
            orig_connection_made(transport)
            assert client.writer is not None
            client.writer.environ_encoding = environ_encoding
            client.writer._encoding_explicit = environ_encoding != "ascii"
            mud_opts = {opt for opt, _, _ in fingerprinting.EXTENDED_OPTIONS}
            client.writer.always_will = fp_always_will | mud_opts
            client.writer.always_do = fp_always_do | mud_opts

        def patched_send_env(keys: Sequence[str]) -> Dict[str, Any]:
            result = orig_send_env(keys)
            result.update(extra_env)
            return result

        client.connection_made = patched_connection_made  # type: ignore[method-assign]
        if extra_env:
            client.send_env = patched_send_env  # type: ignore[method-assign]
        return client

    # Build TLS context for fingerprint client
    fp_ssl: Union[ssl_module.SSLContext, None] = None
    if args.ssl or args.ssl_no_verify:
        if args.ssl_no_verify:
            fp_ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            fp_ssl.check_hostname = False
            fp_ssl.verify_mode = ssl_module.CERT_NONE
        elif args.ssl_cafile:
            fp_ssl = ssl_module.create_default_context(cafile=args.ssl_cafile)
        else:
            fp_ssl = ssl_module.create_default_context()

    waiter_closed: asyncio.Future[None] = asyncio.get_event_loop().create_future()

    fp_conn_kwargs: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "client_factory": fingerprint_client_factory,
        "shell": shell,
        "encoding": False,
        "term": ttype,
        "connect_minwait": 0,
        "connect_maxwait": 4.0,
        "connect_timeout": args.connect_timeout or None,
        "waiter_closed": waiter_closed,
    }
    if fp_ssl is not None:
        fp_conn_kwargs["ssl"] = fp_ssl

    try:
        _, writer = await open_connection(**fp_conn_kwargs)
    except OSError as err:
        log.error("%s:%d: %s", args.host, args.port, err)
        raise

    await writer.protocol.waiter_closed


def fingerprint_main() -> None:
    """Entry point for ``telnetlib3-fingerprint`` command."""
    try:
        asyncio.run(run_fingerprint_client())
    except OSError as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
