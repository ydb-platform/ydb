"""Module provides :class:`TelnetWriter` and :class:`TelnetWriterUnicode`."""

from __future__ import annotations

# std imports
import struct
import asyncio
import logging
import collections
from typing import TYPE_CHECKING, Any, Dict, Callable, Optional, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from .stream_reader import TelnetReader

# local
from . import slc
from .mud import (
    zmp_decode,
    atcp_decode,
    gmcp_decode,
    gmcp_encode,
    msdp_decode,
    msdp_encode,
    mssp_decode,
    mssp_encode,
    aardwolf_decode,
)
from .telopt import (
    AO,
    DM,
    DO,
    EC,
    EL,
    GA,
    IP,
    IS,
    SB,
    SE,
    TM,
    AYT,
    BRK,
    EOF,
    EOR,
    ESC,
    IAC,
    MSP,
    MXP,
    NOP,
    SGA,
    VAR,
    ZMP,
    ATCP,
    DONT,
    ECHO,
    GMCP,
    INFO,
    MSDP,
    MSSP,
    NAWS,
    SEND,
    SUSP,
    WILL,
    WONT,
    ABORT,
    LFLOW,
    TTYPE,
    VALUE,
    BINARY,
    LOGOUT,
    SNDLOC,
    STATUS,
    TSPEED,
    CHARSET,
    CMD_EOR,
    REQUEST,
    USERVAR,
    AARDWOLF,
    ACCEPTED,
    LFLOW_ON,
    LINEMODE,
    REJECTED,
    XDISPLOC,
    LFLOW_OFF,
    TTABLE_IS,
    TTABLE_ACK,
    TTABLE_NAK,
    NEW_ENVIRON,
    COM_PORT_OPTION,
    TTABLE_REJECTED,
    LFLOW_RESTART_ANY,
    LFLOW_RESTART_XON,
    theNULL,
    name_option,
    name_command,
    name_commands,
    option_from_name,
)
from .accessories import TRACE, hexdump
from ._session_context import TelnetSessionContext

__all__ = ("TelnetWriter", "TelnetWriterUnicode")

#: MUD options that allow empty SB payloads (e.g. ``IAC SB MXP IAC SE``).
_EMPTY_SB_OK = frozenset({MXP, MSP, ZMP, AARDWOLF, ATCP})

#: MUD protocol options that a plain telnet client should decline by default.
_MUD_PROTOCOL_OPTIONS = frozenset({GMCP, MSDP, MSSP, MSP, MXP, ZMP, AARDWOLF, ATCP})


class TelnetWriter:
    """
    Telnet IAC Interpreter implementing the telnet protocol.

    A copy of :class:`asyncio.StreamWriter` with IAC interpretation.
    """

    #: Total bytes sent to :meth:`~.feed_byte`
    byte_count = 0

    #: Whether flow control is enabled.
    lflow = True

    #: Whether flow control enabled by Transmit-Off (XOFF) (Ctrl-s), should
    #: re-enable Transmit-On (XON) only on receipt of XON (Ctrl-q).  When
    #: False, any keypress from client re-enables transmission.
    xon_any = False

    #: Whether the last byte received by :meth:`~.feed_byte` is the beginning
    #: of an IAC command.
    iac_received = None

    #: Whether the last byte received by :meth:`~.feed_byte` begins an IAC
    #: command sequence.
    cmd_received: bytes | tuple[bytes, bytes] | bool | None = None

    #: Whether the last byte received by :meth:`~.feed_byte` is a matching
    #: special line character value, if negotiated.
    slc_received = None

    #: SLC function values and callbacks are fired for clients in Kludge
    #: mode not otherwise capable of negotiating LINEMODE, providing
    #: transport remote editing function callbacks for dumb clients.
    slc_simulated = True

    default_slc_tab = slc.BSD_SLC_TAB

    #: Initial line mode requested by server if client supports LINEMODE
    #: negotiation (remote line editing and literal echo of control chars)
    default_linemode = slc.Linemode(
        bytes([ord(slc.LMODE_MODE_REMOTE) | ord(slc.LMODE_MODE_LIT_ECHO)])
    )

    def __init__(
        self,
        transport: asyncio.Transport,
        protocol: Any,
        *,
        client: bool = False,
        server: bool = False,
        reader: Optional["TelnetReader"] = None,
    ) -> None:
        """
        Initialize TelnetWriter.

        Almost all negotiation actions are performed through the writer interface,
        as any action requires writing bytes to the underling stream. This class
        implements :meth:`~.feed_byte`, which acts as a Telnet *Is-A-Command*
        (IAC) interpreter.

        The significance of the last byte passed to this method is tested
        by instance attribute :attr:`~.is_oob`, following the call to
        :meth:`~.feed_byte` to determine whether the given byte is in or out
        of band.

        A minimal Telnet Protocol method,
        :meth:`asyncio.Protocol.data_received`, should forward each byte to
        :meth:`~.feed_byte`, which returns True to indicate the given byte should be
        forwarded to a Protocol reader method.

        :param client: Whether the IAC interpreter should react from
            the client point of view.
        :param server: Whether the IAC interpreter should react from
            the server point of view.
        """
        self._transport = transport
        self._protocol = protocol
        # drain() expects that the reader has an exception() method
        if reader is not None and not callable(getattr(reader, "exception")):
            raise TypeError(
                "reader must provide 'exception' method, like "
                "asyncio.StreamReader.exception, got",
                reader,
            )
        self._reader = reader
        self._closed_fut: Optional[asyncio.Future[None]] = None

        if not any((client, server)) or all((client, server)):
            raise TypeError("keyword arguments `client', and `server' are mutually exclusive.")
        self._server = server
        self.log = logging.getLogger(__name__)

        #: List of (predicate, future) tuples for wait_for functionality
        self._waiters: list[tuple[Callable[[], bool], asyncio.Future[bool]]] = []

        #: Dictionary of telnet option byte(s) that follow an
        #: IAC-DO or IAC-DONT command, and contains a value of ``True``
        #: until IAC-WILL or IAC-WONT has been received by remote end.
        self.pending_option = Option("pending_option", self.log, on_change=self._check_waiters)

        #: Dictionary of telnet option byte(s) that follow an
        #: IAC-WILL or IAC-WONT command, sent by our end,
        #: indicating state of local capabilities.
        self.local_option = Option("local_option", self.log, on_change=self._check_waiters)

        #: Dictionary of telnet option byte(s) that follow an
        #: IAC-WILL or IAC-WONT command received by remote end,
        #: indicating state of remote capabilities.
        self.remote_option = Option("remote_option", self.log, on_change=self._check_waiters)

        #: Encoding used for NEW_ENVIRON variable names and values.
        #: Default ``"ascii"`` per :rfc:`1572`; set to ``"cp037"`` for
        #: EBCDIC hosts such as IBM OS/400.
        self.environ_encoding: str = "ascii"

        #: Set of option byte(s) for which the client always sends WILL
        #: (even when not natively supported).  Overrides the default
        #: WONT rejection in :meth:`handle_do`.
        self.always_will: set[bytes] = set()

        #: Set of option byte(s) for which the client always sends DO
        #: (even when not natively supported).  Overrides the default
        #: DONT rejection in :meth:`handle_will`.
        self.always_do: set[bytes] = set()

        #: Set of option byte(s) for which the client sends DO only
        #: in response to a server WILL (passive negotiation).
        self.passive_do: set[bytes] = set()

        #: Whether the encoding was explicitly set (not just the default
        #: ``"ascii"``).  Used by fingerprinting and client connection logic
        #: to decide whether to negotiate CHARSET.
        self._encoding_explicit: bool = False

        #: Per-connection session context.  Applications may replace this
        #: with a subclass of :class:`~telnetlib3._session_context.TelnetSessionContext` to carry
        #: additional state (e.g. MUD client macros, room graphs).
        self.ctx: TelnetSessionContext = TelnetSessionContext()

        #: Set of option byte(s) for WILL received from remote end
        #: that were rejected with DONT (unhandled options).
        self.rejected_will: set[bytes] = set()

        #: Set of option byte(s) for DO received from remote end
        #: that were rejected with WONT (unsupported options).
        self.rejected_do: set[bytes] = set()

        #: Raw bytes of the last NEW_ENVIRON SEND payload, captured
        #: for fingerprinting.  ``None`` if no SEND was received.
        self.environ_send_raw: Optional[bytes] = None

        #: Decoded MSSP variables received via subnegotiation.
        #: ``None`` until a ``SB MSSP`` payload is received and decoded.
        self.mssp_data: Optional[dict[str, str | list[str]]] = None

        #: Accumulated ZMP messages (list of [command, arg, ...] lists).
        #: Empty until ``SB ZMP`` payloads are received and decoded.
        self.zmp_data: list[list[str]] = []

        #: Accumulated ATCP messages (list of (package, value) tuples).
        #: Empty until ``SB ATCP`` payloads are received and decoded.
        self.atcp_data: list[tuple[str, str]] = []

        #: Accumulated Aardwolf messages (list of decoded dicts).
        #: Empty until ``SB AARDWOLF`` payloads are received and decoded.
        self.aardwolf_data: list[dict[str, Any]] = []

        #: Accumulated MXP subnegotiation payloads (list of raw bytes).
        #: Empty until ``SB MXP`` payloads are received.  An empty payload
        #: (``b""``) signals MXP mode activation.
        self.mxp_data: list[bytes] = []

        #: COM-PORT-OPTION (RFC 2217) data received via subnegotiation.
        #: ``None`` until an ``SB COM-PORT-OPTION`` payload is received.
        self.comport_data: Optional[dict[str, Any]] = None

        #: Sub-negotiation buffer
        self._sb_buffer: collections.deque[bytes] = collections.deque()

        #: SLC buffer
        self._slc_buffer: collections.deque[bytes] = collections.deque()

        #: SLC Tab (SLC Functions and their support level, and ascii value)
        self.slctab = slc.generate_slctab(self.default_slc_tab)

        #: Represents LINEMODE MODE negotiated or requested by client.
        #: attribute ``ack`` returns True if it is in use.
        self._linemode = slc.Linemode()

        self._connection_closed = False

        # Set default callback handlers to local methods.  A base protocol
        # wishing not to wire any callbacks at all may simply allow our stream
        # to gracefully log and do nothing about in most cases.
        self._iac_callback: dict[bytes, Callable[..., Any]] = {}
        for iac_cmd, key in (
            (BRK, "brk"),
            (IP, "ip"),
            (AO, "ao"),
            (AYT, "ayt"),
            (EC, "ec"),
            (EL, "el"),
            (EOF, "eof"),
            (SUSP, "susp"),
            (ABORT, "abort"),
            (NOP, "nop"),
            (DM, "dm"),
            (GA, "ga"),
            (CMD_EOR, "eor"),
            (TM, "tm"),
        ):
            self.set_iac_callback(cmd=iac_cmd, func=getattr(self, f"handle_{key}"))

        self._slc_callback: dict[bytes, Callable[..., Any]] = {}
        for slc_cmd, key in (
            (slc.SLC_SYNCH, "dm"),
            (slc.SLC_BRK, "brk"),
            (slc.SLC_IP, "ip"),
            (slc.SLC_AO, "ao"),
            (slc.SLC_AYT, "ayt"),
            (slc.SLC_EOR, "eor"),
            (slc.SLC_ABORT, "abort"),
            (slc.SLC_EOF, "eof"),
            (slc.SLC_SUSP, "susp"),
            (slc.SLC_EC, "ec"),
            (slc.SLC_EL, "el"),
            (slc.SLC_EW, "ew"),
            (slc.SLC_RP, "rp"),
            (slc.SLC_LNEXT, "lnext"),
            (slc.SLC_XON, "xon"),
            (slc.SLC_XOFF, "xoff"),
        ):
            self.set_slc_callback(slc_byte=slc_cmd, func=getattr(self, f"handle_{key}"))

        self._ext_callback: dict[bytes, Callable[..., Any]] = {}
        for ext_cmd, key in (
            (LOGOUT, "logout"),
            (SNDLOC, "sndloc"),
            (NAWS, "naws"),
            (TSPEED, "tspeed"),
            (TTYPE, "ttype"),
            (XDISPLOC, "xdisploc"),
            (NEW_ENVIRON, "environ"),
            (CHARSET, "charset"),
            (GMCP, "gmcp"),
            (MSDP, "msdp"),
            (MSSP, "mssp"),
            (MSP, "msp"),
            (MXP, "mxp"),
            (ZMP, "zmp"),
            (AARDWOLF, "aardwolf"),
            (ATCP, "atcp"),
        ):
            self.set_ext_callback(cmd=ext_cmd, func=getattr(self, f"handle_{key}"))

        self._ext_send_callback: dict[bytes, Callable[..., Any]] = {}
        for ext_cmd, key in (
            (TTYPE, "ttype"),
            (TSPEED, "tspeed"),
            (XDISPLOC, "xdisploc"),
            (NAWS, "naws"),
            (SNDLOC, "sndloc"),
        ):
            self.set_ext_send_callback(cmd=ext_cmd, func=getattr(self, f"handle_send_{key}"))

        for ext_cmd, key in ((CHARSET, "charset"), (NEW_ENVIRON, "environ")):
            _cbname = "handle_send_server_" if self.server else "handle_send_client_"
            self.set_ext_send_callback(cmd=ext_cmd, func=getattr(self, _cbname + key))

    @property
    def connection_closed(self) -> bool:
        """Return True if connection has been closed."""
        return self._connection_closed

    # Base protocol methods

    @property
    def transport(self) -> Optional[asyncio.BaseTransport]:
        """Return the underlying transport."""
        return self._transport

    def close(self) -> None:
        """Close the connection and release resources."""
        if self.connection_closed:
            return
        # Cancel any pending waiters
        self._cancel_waiters()
        # Proactively notify the protocol so it can release references immediately.
        # Transport will also call connection_lost(), but doing it here ensures
        # cleanup happens deterministically and is idempotent due to _closing guard.
        if self._protocol is not None:
            try:
                self._protocol.connection_lost(None)
            except Exception:
                pass
        if self._transport is not None:
            self._transport.close()
        # break circular refs
        self._ext_callback.clear()
        self._ext_send_callback.clear()
        self._slc_callback.clear()
        self._iac_callback.clear()
        self._protocol = None
        self._transport = None  # type: ignore[assignment]
        self._connection_closed = True
        # Signal that the connection is closed
        if self._closed_fut is not None and not self._closed_fut.done():
            self._closed_fut.set_result(None)

    def is_closing(self) -> bool:
        """Return True if the connection is closing or already closed."""
        if self._transport is not None:
            if self._transport.is_closing():
                return True
        if self.connection_closed:
            return True
        return False

    async def wait_closed(self) -> None:
        """
        Wait until the underlying connection has completed closing.

        This method returns when the underlying connection has been closed. It can be used to wait
        for the connection to be fully closed after calling close().
        """
        if self._connection_closed:
            # Yield to event loop for pending close callbacks
            await asyncio.sleep(0)
            return
        if self._closed_fut is None:
            self._closed_fut = asyncio.get_running_loop().create_future()
        await self._closed_fut

    def _check_waiters(self) -> None:
        """Check all registered waiters and resolve those whose conditions are met."""
        for check, fut in self._waiters[:]:
            if not fut.done() and check():
                fut.set_result(True)

    def _cancel_waiters(self) -> None:
        """Cancel all pending waiters, typically called on connection close."""
        for _check, fut in self._waiters[:]:
            if not fut.done():
                fut.cancel()
        self._waiters.clear()

    async def wait_for(
        self,
        *,
        remote: Optional[Dict[str, bool]] = None,
        local: Optional[Dict[str, bool]] = None,
        pending: Optional[Dict[str, bool]] = None,
    ) -> bool:
        """
        Wait for negotiation state conditions to be met.

        :param remote: Dict of option_name -> bool for remote_option checks.
        :param local: Dict of option_name -> bool for local_option checks.
        :param pending: Dict of option_name -> bool for pending_option checks.
        :returns: True when all conditions are met.
        :raises KeyError: If an option name is not recognized.
        :raises asyncio.CancelledError: If connection closes while waiting.

        Example::

            # Wait for TTYPE and NAWS negotiation to complete
            await writer.wait_for(remote={"TTYPE": True, "NAWS": True})

            # Wait for pending options to clear
            await writer.wait_for(pending={"TTYPE": False})
        """
        conditions = []
        for spec, option_dict in [
            (remote, self.remote_option),
            (local, self.local_option),
            (pending, self.pending_option),
        ]:
            if spec:
                for name, expected in spec.items():
                    opt = option_from_name(name)
                    conditions.append((option_dict, opt, expected))

        def check() -> bool:
            for option_dict, opt, expected in conditions:
                if expected:
                    if not option_dict.enabled(opt):
                        return False
                elif option_dict.get(opt) not in (False, None):
                    return False
            return True

        if check():
            return True

        fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._waiters.append((check, fut))

        try:
            result: bool = await fut
            return result
        finally:
            self._waiters = [(c, f) for c, f in self._waiters if f is not fut]

    async def wait_for_condition(self, predicate: Callable[["TelnetWriter"], bool]) -> bool:
        """
        Wait for a custom condition to be met.

        :param predicate: Callable taking TelnetWriter, returning bool.
        :returns: True when predicate returns True.
        :raises asyncio.CancelledError: If connection closes while waiting.

        Example::

            await writer.wait_for_condition(lambda w: w.mode == "kludge")
        """
        if predicate(self):
            return True

        def check() -> bool:
            return predicate(self)

        fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._waiters.append((check, fut))

        try:
            result: bool = await fut
            return result
        finally:
            self._waiters = [(c, f) for c, f in self._waiters if f is not fut]

    def __repr__(self) -> str:
        """Description of stream encoding state."""
        info = ["TelnetWriter"]
        if self.server:
            info.append("server")
            endpoint = "client"
        else:
            info.append("client")
            endpoint = "server"

        info.append(f"mode:{self.mode}")

        # IAC options
        info.append(f"{'+' if self.lflow else '-'}lineflow")
        info.append(f"{'+' if self.xon_any else '-'}xon_any")
        info.append(f"{'+' if self.slc_simulated else '-'}slc_sim")

        # IAC negotiation status
        _failed_reply = sorted(
            [name_commands(opt) for (opt, val) in self.pending_option.items() if val]
        )
        if _failed_reply:
            info.append(f"failed-reply:{','.join(_failed_reply)}")

        _local = sorted(
            [
                name_commands(opt)
                for (opt, val) in self.local_option.items()
                if self.local_option.enabled(opt)
            ]
        )
        if _local:
            localpoint = "server" if self.server else "client"
            info.append(f"{localpoint}-will:{','.join(_local)}")

        _remote = sorted(
            [
                name_commands(opt)
                for (opt, val) in self.remote_option.items()
                if self.remote_option.enabled(opt)
            ]
        )
        if _remote:
            info.append(f"{endpoint}-will:{','.join(_remote)}")

        return f"<{' '.join(info)}>"

    def write(self, data: bytes) -> None:
        """Write a bytes object to the protocol transport."""
        if self.connection_closed:
            self.log.debug("write after close, ignored %s bytes", len(data))
            return
        self._write(data)

    def writelines(self, lines: Sequence[bytes]) -> None:
        """
        Write unicode strings to transport.

        Note that newlines are not added.  The sequence can be any iterable object producing
        strings. This is equivalent to calling write() for each string.
        """
        self.write(b"".join(lines))

    def write_eof(self) -> None:
        """Write EOF to the transport."""
        return self._transport.write_eof()

    def can_write_eof(self) -> bool:
        """Return True if the transport supports write_eof()."""
        return self._transport.can_write_eof()

    async def drain(self) -> None:
        """
        Flush the write buffer.

        The intended use is to write

        w.write(data) await w.drain()
        """
        if self._reader is not None:
            exc = self._reader.exception()
            if exc is not None:
                raise exc
        if self._transport is not None and self._transport.is_closing():
            # Wait for protocol.connection_lost() call
            # Raise connection closing error if any,
            # ConnectionResetError otherwise
            # Yield to the event loop so connection_lost() may be
            # called.  Without this, _drain_helper() would return
            # immediately, and code that calls
            #     write(...); await drain()
            # in a loop would never call connection_lost(), so it
            # would not see an error when the socket is closed.
            await asyncio.sleep(0)
        if self._protocol is not None:
            await self._protocol._drain_helper()

    # proprietary write helper

    def feed_byte(self, byte: bytes) -> bool:
        """
        Feed a single byte into Telnet option state machine.

        :param byte: an 8-bit byte value as integer (0-255), or
            a bytes array.  When a bytes array, it must be of length
            1.
        :returns: Whether the given ``byte`` is "in band", that is, should
            be duplicated to a connected terminal or device.  ``False`` is
            returned for an ``IAC`` command for each byte until its completion.
        :raises ValueError: When an illegal IAC command is received.
        """
        self.byte_count += 1
        self.slc_received = None

        # list of IAC commands needing 3+ bytes (mbs: multibyte sequence)
        iac_mbs = (DO, DONT, WILL, WONT, SB)

        # cmd received is toggled False, unless its a mbs, then it is the
        # actual command that was received in (opt, byte) form.
        self.cmd_received = self.cmd_received in iac_mbs and self.cmd_received

        if byte == IAC:
            self.iac_received = not self.iac_received
            if not self.iac_received and self.cmd_received == SB:
                # SB buffer receives escaped IAC values
                self._sb_buffer.append(IAC)

        elif self.iac_received and not self.cmd_received:
            # parse 2nd byte of IAC
            self.cmd_received = cmd = byte
            if cmd not in iac_mbs:
                # DO, DONT, WILL, WONT are 3-byte commands, expect more.
                # Any other, expect a callback.  Otherwise this protocol
                # does not comprehend the remote end's request.
                if cmd not in self._iac_callback:
                    self.iac_received = False
                    self.cmd_received = False
                    self.log.debug(
                        "IAC %s: not a legal 2-byte cmd, treating as data", name_command(cmd)
                    )
                    return True
                self._iac_callback[cmd](cmd)
            self.iac_received = False

        elif self.iac_received and self.cmd_received == SB:
            # parse 2nd byte of IAC while while already within
            # IAC SB sub-negotiation buffer, assert command is SE.
            self.cmd_received = cmd = byte
            if cmd != SE:
                sb_opt = name_command(self._sb_buffer[0]) if self._sb_buffer else "?"
                self.log.warning(
                    "sub-negotiation SB %s (%d bytes) interrupted by IAC %s",
                    sb_opt,
                    len(self._sb_buffer),
                    name_command(cmd),
                )
                self._sb_buffer.clear()
            else:
                # sub-negotiation end (SE), fire handle_subnegotiation
                self.log.debug(
                    "sub-negotiation cmd %s SE completion byte", name_command(self._sb_buffer[0])
                )
                try:
                    self.handle_subnegotiation(self._sb_buffer)
                finally:
                    self._sb_buffer.clear()
                    self.iac_received = False
            self.iac_received = False

        elif self.cmd_received == SB:
            # continue buffering of sub-negotiation command.
            if not self._sb_buffer:
                self.log.debug("begin sub-negotiation SB %s", name_command(byte))
            self._sb_buffer.append(byte)

        elif self.cmd_received:
            # parse 3rd and final byte of IAC DO, DONT, WILL, WONT.
            cmd, opt = self.cmd_received, byte  # type: ignore[assignment]
            self.log.debug("recv IAC %s %s", name_command(cmd), name_option(opt))
            try:
                if cmd == DO:
                    try:
                        self.local_option[opt] = self.handle_do(opt)
                    finally:
                        if self.pending_option.enabled(WILL + opt):
                            self.pending_option[WILL + opt] = False
                elif cmd == DONT:
                    try:
                        self.handle_dont(opt)
                    finally:
                        self.pending_option[WILL + opt] = False
                        self.local_option[opt] = False
                elif cmd == WILL:
                    if not self.pending_option.enabled(DO + opt) and opt not in (TM, CHARSET):
                        self.log.debug("WILL %s unsolicited", name_command(opt))
                    elif opt == CHARSET and not self.pending_option.enabled(DO + opt):
                        self.log.debug(
                            "WILL %s (bi-directional capability exchange)", name_command(opt)
                        )
                    try:
                        self.handle_will(opt)
                    finally:
                        if self.pending_option.enabled(DO + opt):
                            self.pending_option[DO + opt] = False
                        # informed client, 'DONT', client responded with
                        # illegal 'WILL' response, cancel any pending option.
                        # Very unlikely state!
                        if self.pending_option.enabled(DONT + opt):
                            self.pending_option[DONT + opt] = False
                else:
                    # cmd is 'WONT'
                    self.handle_wont(opt)
                    self.pending_option[DO + opt] = False
            finally:
                # toggle iac_received on any ValueErrors/AssertionErrors raised
                self.iac_received = False
                self.cmd_received = (opt, byte)

        elif self.mode == "remote" or self.mode == "kludge" and self.slc_simulated:
            # 'byte' is tested for SLC characters
            callback, slc_name, _ = slc.snoop(byte, self.slctab, self._slc_callback)

            # Inform caller which SLC function occurred by this attribute.
            self.slc_received = slc_name
            if callback:
                self.log.debug(
                    "slc.snoop(%r): %s, callback is %s.",
                    byte,
                    slc.name_slc_command(slc_name),  # type: ignore[arg-type]
                    callback.__name__,
                )
                callback(slc_name)

        # whether this data should be forwarded (to the reader)
        return not self.is_oob

    # Our protocol methods

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get optional server protocol information."""
        # StreamWriter uses self._transport.get_extra_info, so we mix it in
        # here, but _protocol has all of the interesting telnet effects.
        # Handle case where protocol/transport may be None (connection closed).
        _missing = object()
        if self._protocol is not None:
            result = self._protocol.get_extra_info(name, _missing)
            if result is not _missing:
                return result
        if self._transport is not None:
            return self._transport.get_extra_info(name, default)
        return default

    @property
    def protocol(self) -> Any:
        """The (Telnet) protocol attached to this stream."""
        return self._protocol

    def _force_binary_on_protocol(self) -> None:
        """
        Enable ``force_binary`` on the attached protocol.

        Called when CHARSET is negotiated or LANG is received via NEW_ENVIRON, implying that the
        peer can handle non-ASCII bytes regardless of whether BINARY mode was explicitly negotiated.
        """
        if self._protocol is not None and hasattr(self._protocol, "force_binary"):
            self._protocol.force_binary = True

    @property
    def server(self) -> bool:
        """Whether this stream is of the server's point of view."""
        return bool(self._server)

    @property
    def client(self) -> bool:
        """Whether this stream is of the client's point of view."""
        return bool(not self._server)

    @property
    def inbinary(self) -> bool:
        """Whether binary data is expected to be received on reader, :rfc:`856`."""
        return self.remote_option.enabled(BINARY)

    @property
    def outbinary(self) -> bool:
        """Whether binary data may be written to the writer, :rfc:`856`."""
        return self.local_option.enabled(BINARY)

    def echo(self, data: bytes) -> None:
        """
        Conditionally write ``data`` to transport when "remote echo" enabled.

        :param data: bytes received as input, conditionally written. The default implementation
            depends on telnet negotiation willingness for local echo, only an RFC- compliant telnet
            client will correctly set or unset echo accordingly by demand.
        """
        if self.will_echo:
            self.write(data=data)

    @property
    def will_echo(self) -> bool:
        """
        Whether Server end is expected to echo back input sent by client.

        From server perspective: the server should echo (duplicate) client
        input back over the wire, the client is awaiting this data to indicate
        their input has been received.

        From client perspective: the server will not echo our input, we should
        choose to duplicate our input to standard out ourselves.
        """
        return (self.server and self.local_option.enabled(ECHO)) or (
            self.client and self.remote_option.enabled(ECHO)
        )

    @property
    def mode(self) -> str:
        """
        String describing NVT mode.

        One of:

        ``kludge``: Client acknowledges WILL-ECHO, WILL-SGA. Character-at-
            a-time and remote line editing may be provided.

        ``local``: Default NVT half-duplex mode, client performs line
            editing and transmits only after pressing send (usually CR).

        ``remote``: Client supports advanced remote line editing, using
            mixed-mode local line buffering (optionally, echoing) until
            send, but also transmits buffer up to and including special
            line characters (SLCs).
        """
        if self.remote_option.enabled(LINEMODE):
            if self._linemode.local:
                return "local"
            return "remote"
        if self.server:
            if self.local_option.enabled(ECHO) and self.local_option.enabled(SGA):
                return "kludge"
            return "local"
        if self.remote_option.enabled(ECHO) and self.remote_option.enabled(SGA):
            return "kludge"
        return "local"

    @property
    def is_oob(self) -> bool:
        """The previous byte should not be received by the API stream."""
        return bool(self.iac_received or self.cmd_received)

    @property
    def linemode(self) -> slc.Linemode:
        """
        Linemode instance for stream.

        .. note:: value is meaningful after successful LINEMODE negotiation,
            otherwise does not represent the linemode state of the stream.

        Attributes of the stream's active linemode may be tested using boolean
        instance attributes, ``edit``, ``trapsig``, ``soft_tab``, ``lit_echo``,
        ``remote``, ``local``.
        """
        return self._linemode

    def send_iac(self, buf: bytes) -> None:
        """
        Send a command starting with IAC (base 10 byte value 255).

        No transformations of bytes are performed.  Normally, if the
        byte value 255 is sent, it is escaped as ``IAC + IAC``.  This
        method ensures it is not escaped.
        """
        if not self.is_closing():
            if self.log.isEnabledFor(TRACE):
                self.log.log(TRACE, "send IAC %d bytes\n%s", len(buf), hexdump(buf, prefix=">>  "))
            self._transport.write(buf)
            if hasattr(self._protocol, "_tx_bytes"):
                self._protocol._tx_bytes += len(buf)

    def iac(self, cmd: bytes, opt: bytes = b"") -> bool:
        """
        Send Is-A-Command 3-byte negotiation command.

        Returns True if command was sent. Not all commands are legal in the context of client,
        server, or pending negotiation state, emitting a relevant debug warning to the log handler
        if not sent.

        :raises ValueError: When cmd is not DO, DONT, WILL, or WONT.
        """
        if cmd not in (DO, DONT, WILL, WONT):
            raise ValueError(f"Expected DO, DONT, WILL, WONT, got {name_command(cmd)}.")

        if cmd == DO and opt not in (TM, LOGOUT):
            if self.remote_option.enabled(opt):
                self.log.debug(
                    "skip %s %s; remote_option = True", name_command(cmd), name_command(opt)
                )
                self.pending_option[cmd + opt] = False
                return False

        if cmd in (DO, WILL):
            if self.pending_option.enabled(cmd + opt):
                self.log.debug(
                    "skip %s %s; pending_option = True", name_command(cmd), name_command(opt)
                )
                return False
            self.pending_option[cmd + opt] = True

        if cmd == WILL and opt not in (TM,):
            if self.local_option.enabled(opt):
                self.log.debug(
                    "skip %s %s; local_option = True", name_command(cmd), name_command(opt)
                )
                self.pending_option[cmd + opt] = False
                return False

        if cmd == DONT and opt not in (LOGOUT,):
            # IAC-DONT-LOGOUT is not a rejection of the negotiation option
            if opt in self.remote_option and not self.remote_option.enabled(opt):
                self.log.debug(
                    "skip %s %s; remote_option = False", name_command(cmd), name_command(opt)
                )
                return False
            self.remote_option[opt] = False

        if cmd == WONT:
            self.local_option[opt] = False

        self.log.debug("send IAC %s %s", name_command(cmd), name_command(opt))
        self.send_iac(IAC + cmd + opt)
        return True

    # Public methods for transmission signaling
    #

    def send_ga(self) -> bool:
        """
        Transmit IAC GA (Go-Ahead).

        Returns True if sent.  If IAC-DO-SGA has been received, then False is returned and IAC-GA is
        not transmitted.
        """
        if self.local_option.enabled(SGA):
            self.log.debug("cannot send GA with receipt of DO SGA")
            return False

        self.log.debug("send IAC GA")
        self.send_iac(IAC + GA)
        return True

    def send_eor(self) -> bool:
        """
        Transmit IAC CMD_EOR (End-of-Record), :rfc:`885`.

        Returns True if sent. If IAC-DO-EOR has not been received, False is returned and IAC-CMD_EOR
        is not transmitted.
        """
        if not self.local_option.enabled(EOR):
            self.log.debug("cannot send CMD_EOR without receipt of DO EOR")
            return False

        self.log.debug("send IAC CMD_EOR")
        self.send_iac(IAC + CMD_EOR)
        return True

    def send_gmcp(self, package: str, data: Any = None) -> None:
        """
        Transmit a GMCP message via subnegotiation.

        :param package: GMCP package name (e.g., ``"Char.Vitals"``)
        :param data: Optional data to encode as JSON
        """
        if not (self.local_option.enabled(GMCP) or self.remote_option.enabled(GMCP)):
            self.log.debug("cannot send GMCP without negotiation")
            return
        payload = self._escape_iac(gmcp_encode(package, data))
        self.log.debug("send IAC SB GMCP %s IAC SE", package)
        self.send_iac(IAC + SB + GMCP + payload + IAC + SE)

    def send_msdp(self, variables: dict[str, Any]) -> None:
        """
        Transmit MSDP variables via subnegotiation.

        :param variables: Dictionary of variable names to values
        """
        if not (self.local_option.enabled(MSDP) or self.remote_option.enabled(MSDP)):
            self.log.debug("cannot send MSDP without negotiation")
            return
        payload = self._escape_iac(msdp_encode(variables))
        self.log.debug("send IAC SB MSDP IAC SE")
        self.send_iac(IAC + SB + MSDP + payload + IAC + SE)

    def send_mssp(self, variables: dict[str, str | list[str]]) -> None:
        """
        Transmit MSSP variables via subnegotiation.

        :param variables: Dictionary of variable names to values
        """
        if not (self.local_option.enabled(MSSP) or self.remote_option.enabled(MSSP)):
            self.log.debug("cannot send MSSP without negotiation")
            return
        payload = self._escape_iac(mssp_encode(variables))
        self.log.debug("send IAC SB MSSP IAC SE")
        self.send_iac(IAC + SB + MSSP + payload + IAC + SE)

    # Public methods for notifying about, or soliciting state options.
    #

    def request_status(self) -> bool:
        """
        Send ``IAC-SB-STATUS-SEND`` sub-negotiation (:rfc:`859`).

        This method may only be called after ``IAC-WILL-STATUS`` has been
        received. Returns True if status request was sent.
        """
        if not self.remote_option.enabled(STATUS):
            self.log.debug("cannot send SB STATUS SEND without receipt of WILL STATUS")
        elif not self.pending_option.enabled(SB + STATUS):
            response = [IAC, SB, STATUS, SEND, IAC, SE]
            self.log.debug("send IAC SB STATUS SEND IAC SE")
            self.send_iac(b"".join(response))
            self.pending_option[SB + STATUS] = True
            return True
        else:
            self.log.info("cannot send SB STATUS SEND, request pending.")
        return False

    def request_comport_signature(self) -> bool:
        """
        Send ``IAC SB COM-PORT-OPTION SIGNATURE IAC SE``, :rfc:`2217`.

        Requests the server's COM-PORT-OPTION signature string. Returns True if the request was
        sent.
        """
        if not self.remote_option.enabled(COM_PORT_OPTION):
            self.log.debug(
                "cannot send SB COM-PORT-OPTION SIGNATURE"
                " without receipt of WILL COM-PORT-OPTION"
            )
            return False
        # RFC 2217: sub-command 0 = SIGNATURE, empty payload = request
        response = [IAC, SB, COM_PORT_OPTION, b"\x00", IAC, SE]
        self.log.debug("send IAC SB COM-PORT-OPTION SIGNATURE IAC SE")
        self.send_iac(b"".join(response))
        return True

    def request_tspeed(self) -> bool:
        """
        Send IAC-SB-TSPEED-SEND sub-negotiation, :rfc:`1079`.

        This method may only be called after ``IAC-WILL-TSPEED`` has been
        received. Returns True if TSPEED request was sent.
        """
        if not self.remote_option.enabled(TSPEED):
            self.log.debug("cannot send SB TSPEED SEND without receipt of WILL TSPEED")
        elif not self.pending_option.enabled(SB + TSPEED):
            self.pending_option[SB + TSPEED] = True
            response = [IAC, SB, TSPEED, SEND, IAC, SE]
            self.log.debug("send IAC SB TSPEED SEND IAC SE")
            self.send_iac(b"".join(response))
            self.pending_option[SB + TSPEED] = True
            return True
        else:
            self.log.debug("cannot send SB TSPEED SEND, request pending.")
        return False

    def request_charset(self) -> bool:
        """
        Request sub-negotiation CHARSET, :rfc:`2066`.

        Returns True if request is valid for telnet state, and was sent.

        The sender requests that all text sent to and by it be encoded in
        one of character sets specified by string list ``codepages``, which
        is determined by function value returned by callback registered using
        :meth:`set_ext_send_callback` with value ``CHARSET``.
        """
        # RFC 2066 Section 5: once either side has sent WILL and received DO, it may initiate.
        # Permit initiating REQUEST if either:
        # - peer has sent WILL (remote_option True), or
        # - we have sent WILL and received DO (local_option True).
        if not (self.remote_option.enabled(CHARSET) or self.local_option.enabled(CHARSET)):
            self.log.debug("cannot send SB CHARSET REQUEST without CHARSET being active")
            return False

        if self.pending_option.enabled(SB + CHARSET):
            self.log.debug("cannot send SB CHARSET REQUEST, request pending.")
            return False

        codepages = self._ext_send_callback[CHARSET]()

        sep = " "
        response: collections.deque[bytes] = collections.deque()
        response.extend([IAC, SB, CHARSET, REQUEST])
        response.extend([bytes(sep, "ascii")])
        response.extend([bytes(sep.join(codepages), "ascii")])
        response.extend([IAC, SE])
        self.log.debug("send IAC SB CHARSET REQUEST %s IAC SE", sep.join(codepages))
        self.send_iac(b"".join(response))
        self.pending_option[SB + CHARSET] = True
        return True

    def request_environ(self) -> bool:
        """
        Request sub-negotiation NEW_ENVIRON, :rfc:`1572`.

        Returns True if request is valid for telnet state, and was sent.
        """
        if not self.remote_option.enabled(NEW_ENVIRON):
            self.log.debug("cannot send SB NEW_ENVIRON SEND IS without receipt of WILL NEW_ENVIRON")
            return False

        request_list = self._ext_send_callback[NEW_ENVIRON]()

        if not request_list:
            self.log.debug(
                "request_environ: server protocol makes no demand, no request will be made."
            )
            return False

        if self.pending_option.enabled(SB + NEW_ENVIRON):
            self.log.debug("cannot send SB NEW_ENVIRON SEND IS, request pending.")
            return False

        response: collections.deque[bytes] = collections.deque()
        response.extend([IAC, SB, NEW_ENVIRON, SEND])

        for env_key in request_list:
            if env_key in (VAR, USERVAR):
                # VAR followed by IAC,SE indicates "send all the variables",
                # whereas USERVAR indicates "send all the user variables".
                # In today's era, there is little distinction between them.
                response.append(env_key)
            else:
                response.extend([VAR])
                response.extend([_escape_environ(env_key.encode(self.environ_encoding, "replace"))])
        response.extend([IAC, SE])
        self.log.debug("request_environ: %r", b"".join(response))
        self.pending_option[SB + NEW_ENVIRON] = True
        self.send_iac(b"".join(response))
        return True

    def request_xdisploc(self) -> bool:
        """
        Send XDISPLOC, SEND sub-negotiation, :rfc:`1086`.

        Returns True if request is valid for telnet state, and was sent.
        """
        if not self.remote_option.enabled(XDISPLOC):
            self.log.debug("cannot send SB XDISPLOC SEND without receipt of WILL XDISPLOC")
            return False
        if not self.pending_option.enabled(SB + XDISPLOC):
            response = [IAC, SB, XDISPLOC, SEND, IAC, SE]
            self.log.debug("send IAC SB XDISPLOC SEND IAC SE")
            self.pending_option[SB + XDISPLOC] = True
            self.send_iac(b"".join(response))
            return True

        self.log.debug("cannot send SB XDISPLOC SEND, request pending.")
        return False

    def request_ttype(self) -> bool:
        """
        Send TTYPE SEND sub-negotiation, :rfc:`930`.

        Returns True if request is valid for telnet state, and was sent.
        """
        if not self.remote_option.enabled(TTYPE):
            self.log.debug("cannot send SB TTYPE SEND without receipt of WILL TTYPE")
            return False
        if not self.pending_option.enabled(SB + TTYPE):
            response = [IAC, SB, TTYPE, SEND, IAC, SE]
            self.log.debug("send IAC SB TTYPE SEND IAC SE")
            self.pending_option[SB + TTYPE] = True
            self.send_iac(b"".join(response))
            return True
        self.log.debug("cannot send SB TTYPE SEND, request pending.")
        return False

    def request_forwardmask(self, fmask: Optional[slc.Forwardmask] = None) -> bool:
        """
        Request the client forward their terminal control characters.

        Characters are indicated in the Forwardmask instance
        ``fmask``.  When fmask is None, a forwardmask is generated for the SLC
        characters registered by :attr:`~.slctab`.
        """
        if not self.remote_option.enabled(LINEMODE):
            self.log.debug("cannot send SB LINEMODE DO without receipt of WILL LINEMODE")
        else:
            if fmask is None:
                opt = SB + LINEMODE + slc.LMODE_FORWARDMASK
                forwardmask_enabled = (
                    self.server and self.local_option.get(opt, False)
                ) or self.remote_option.get(opt, False)
                fmask = slc.generate_forwardmask(
                    binary_mode=self.local_option.enabled(BINARY),
                    tabset=self.slctab,
                    ack=forwardmask_enabled,
                )

            self.log.debug("send IAC SB LINEMODE DO LMODE_FORWARDMASK::")
            for maskbit_descr in fmask.description_table():
                self.log.debug("  %s", maskbit_descr)
            self.log.debug("send IAC SE")

            self.send_iac(IAC + SB + LINEMODE + DO + slc.LMODE_FORWARDMASK)
            if not self.is_closing():
                self._transport.write(fmask.value)
            self.send_iac(IAC + SE)

            return True
        return False

    def send_lineflow_mode(self) -> Optional[bool]:
        """
        Send LFLOW mode sub-negotiation, :rfc:`1372`.

        Returns True if request is valid for telnet state, and was sent.
        """
        if self.client:
            self.log.error("only server may send IAC SB LINEFLOW <MODE>")
        elif not self.remote_option.enabled(LFLOW):
            self.log.error("cannot send IAC SB LFLOW without receipt of WILL LFLOW")
        else:
            if self.xon_any:
                mode, desc = (LFLOW_RESTART_ANY, "LFLOW_RESTART_ANY")
            else:
                mode, desc = (LFLOW_RESTART_XON, "LFLOW_RESTART_XON")
            self.log.debug("send IAC SB LFLOW %s IAC SE", desc)
            self.send_iac(b"".join([IAC, SB, LFLOW, mode, IAC, SE]))
            return True
        return False

    def send_linemode(self, linemode: Optional[slc.Linemode] = None) -> None:
        """
        Set and Inform other end to agree to change to linemode, ``linemode``.

        An instance of the Linemode class, or self.linemode when unset.

        :raises AssertionError: When LINEMODE not negotiated.
        """
        if not (self.local_option.enabled(LINEMODE) or self.remote_option.enabled(LINEMODE)):
            raise AssertionError(
                "Cannot send LINEMODE-MODE without first (DO, WILL) LINEMODE received."
            )

        if linemode is not None:
            self.log.debug("set Linemode %r", linemode)
            self._linemode = linemode

        self.log.debug("send IAC SB LINEMODE LINEMODE-MODE %r IAC SE", self._linemode)

        self.send_iac(IAC + SB + LINEMODE + slc.LMODE_MODE)
        if not self.is_closing():
            self._transport.write(self._linemode.mask)
        self.send_iac(IAC + SE)

    # Public is-a-command (IAC) callbacks
    #
    def set_iac_callback(self, cmd: bytes, func: Callable[..., Any]) -> None:
        """
        Register callable ``func`` as callback for IAC ``cmd``.

        BRK, IP, AO, AYT, EC, EL, CMD_EOR, EOF, SUSP, ABORT, and NOP.

        These callbacks receive a single argument, the IAC ``cmd`` which
        triggered it.
        """
        self._iac_callback[cmd] = func

    def handle_nop(self, cmd: bytes) -> None:
        """Handle IAC No-Operation (NOP)."""
        self.log.debug("IAC NOP: Null Operation (unhandled).")

    def handle_ga(self, cmd: bytes) -> None:
        """Handle IAC Go-Ahead (GA)."""
        self.log.debug("IAC GA: Go-Ahead (unhandled).")

    def handle_dm(self, cmd: bytes) -> None:
        """Handle IAC Data-Mark (DM)."""
        self.log.debug("IAC DM: Data-Mark (unhandled).")

    # Public mixed-mode SLC and IAC callbacks
    #
    def handle_el(self, _byte: bytes) -> None:
        """
        Handle IAC Erase Line (EL, SLC_EL).

        Provides a function which discards all the data ready on current line of input. The prompt
        should be re-displayed.
        """
        self.log.debug("IAC EL: Erase Line (unhandled).")

    def handle_eor(self, _byte: bytes) -> None:
        """Handle IAC End of Record (CMD_EOR, SLC_EOR)."""
        self.log.debug("IAC EOR: End of Record (unhandled).")

    def handle_abort(self, _byte: bytes) -> None:
        """
        Handle IAC Abort (ABORT, SLC_ABORT).

        Similar to Interrupt Process (IP), but means only to abort or terminate the process to which
        the NVT is connected.
        """
        self.log.debug("IAC ABORT: Abort (unhandled).")

    def handle_eof(self, _byte: bytes) -> None:
        """Handle IAC End of Record (EOF, SLC_EOF)."""
        self.log.debug("IAC EOF: End of File (unhandled).")

    def handle_susp(self, _byte: bytes) -> None:
        """
        Handle IAC Suspend Process (SUSP, SLC_SUSP).

        Suspends the execution of the current process attached to the NVT in such a way that another
        process will take over control of the NVT, and the suspended process can be resumed at a
        later time.

        If the receiving system does not support this functionality, it should be ignored.
        """
        self.log.debug("IAC SUSP: Suspend (unhandled).")

    def handle_brk(self, _byte: bytes) -> None:
        """
        Handle IAC Break (BRK, SLC_BRK).

        Sent by clients to indicate BREAK keypress. This is not the same as IP (^c), but a means to
        map system-dependent break key such as found on an IBM Systems.
        """
        self.log.debug("IAC BRK: Break (unhandled).")

    def handle_ayt(self, _byte: bytes) -> None:
        """
        Handle IAC Are You There (AYT, SLC_AYT).

        Provides the user with some visible (e.g., printable) evidence that the system is still up
        and running.
        """
        self.log.debug("IAC AYT: Are You There? (unhandled).")

    def handle_ip(self, _byte: bytes) -> None:
        """Handle IAC Interrupt Process (IP, SLC_IP)."""
        self.log.debug("IAC IP: Interrupt Process (unhandled).")

    def handle_ao(self, _byte: bytes) -> None:
        """
        Handle IAC Abort Output (AO) or SLC_AO.

        Discards any remaining output on the transport buffer.

            [...] a reasonable implementation would be to suppress the
            remainder of the text string, but transmit the prompt character
            and the preceding <CR><LF>.
        """
        self.log.debug("IAC AO: Abort Output, unhandled.")

    def handle_ec(self, _byte: bytes) -> None:
        """
        Handle IAC Erase Character (EC, SLC_EC).

        Provides a function which deletes the last preceding undeleted character from data ready on
        current line of input.
        """
        self.log.debug("IAC EC: Erase Character (unhandled).")

    def handle_tm(self, cmd: bytes) -> None:
        """
        Handle IAC (WILL, WONT, DO, DONT) Timing Mark (TM).

        TM is essentially a NOP that any IAC interpreter must answer, if at least it answers WONT to
        unknown options (required), it may still be used as a means to accurately measure the "ping"
        time.
        """
        self.log.debug("IAC TM: Received %s TM (Timing Mark).", name_command(cmd))

    # public Special Line Mode (SLC) callbacks
    #
    def set_slc_callback(self, slc_byte: bytes, func: Callable[..., Any]) -> None:
        """
        Register ``func`` as callable for receipt of ``slc_byte``.

        :param slc_byte: any of SLC_SYNCH, SLC_BRK, SLC_IP, SLC_AO,
            SLC_AYT, SLC_EOR, SLC_ABORT, SLC_EOF, SLC_SUSP, SLC_EC, SLC_EL,
            SLC_EW, SLC_RP, SLC_XON, SLC_XOFF ...
        :param func: Callback receiving a single argument: the SLC function byte
            that fired it. Some SLC and IAC functions are intermixed; which
            signaling mechanism used by client can be tested by evaluating this
            argument.
        """
        self._slc_callback[slc_byte] = func

    def handle_ew(self, _slc: bytes) -> None:
        """
        Handle SLC_EW (Erase Word).

        Provides a function which deletes the last preceding undeleted character, and any subsequent
        bytes until next whitespace character from data ready on current line of input.
        """
        self.log.debug("SLC EC: Erase Word (unhandled).")

    def handle_rp(self, _slc: bytes) -> None:
        """Handle SLC Repaint (RP)."""
        self.log.debug("SLC RP: Repaint (unhandled).")

    def handle_lnext(self, _slc: bytes) -> None:
        """Handle SLC Literal Next (LNEXT) (Next character is received raw)."""
        self.log.debug("SLC LNEXT: Literal Next (unhandled)")

    def handle_xon(self, _byte: bytes) -> None:
        """Handle SLC Transmit-On (XON)."""
        self.log.debug("SLC XON: Transmit On (unhandled).")

    def handle_xoff(self, _byte: bytes) -> None:
        """Handle SLC Transmit-Off (XOFF)."""
        self.log.debug("SLC XOFF: Transmit Off.")

    # public Telnet extension callbacks
    #
    def set_ext_send_callback(self, cmd: bytes, func: Callable[..., Any]) -> None:
        """
        Register callback for inquiries of sub-negotiation of ``cmd``.

        :param func: A callable function for the given ``cmd`` byte.
            Note that the return type must match those documented.
        :param cmd: These callbacks must return any number of arguments,
            for each registered ``cmd`` byte, respectively:

            * SNDLOC: for clients, returning one argument: the string
              describing client location, such as ``b'ROOM 641-A'``,
              :rfc:`779`.

            * NAWS: for clients, returning two integer arguments (width,
              height), such as (80, 24), :rfc:`1073`.

            * TSPEED: for clients, returning two integer arguments (rx, tx)
              such as (57600, 57600), :rfc:`1079`.

            * TTYPE: for clients, returning one string, usually the terminfo(5)
              database capability name, such as 'xterm', :rfc:`1091`.

            * XDISPLOC: for clients, returning one string, the DISPLAY host
              value, in form of <host>:<dispnum>[.<screennum>], :rfc:`1096`.

            * NEW_ENVIRON: for clients, returning a dictionary of (key, val)
              pairs of environment item values, :rfc:`1408`.

            * CHARSET: for clients, receiving iterable of strings of character
              sets requested by server, callback must return one of those
              strings given, :rfc:`2066`.
        """
        self._ext_send_callback[cmd] = func

    def set_ext_callback(self, cmd: bytes, func: Callable[..., Any]) -> None:
        """
        Register ``func`` as callback for receipt of ``cmd`` negotiation.

        :param cmd: One of the following listed bytes:

        * ``LOGOUT``: for servers and clients, receiving one argument.
          Server end may receive DO or DONT as argument ``cmd``, indicating
          client's wish to disconnect, or a response to WILL, LOGOUT,
          indicating its wish not to be automatically disconnected.  Client
          end may receive WILL or WONT, indicating server's wish to disconnect,
          or acknowledgment that the client will not be disconnected.

        * ``SNDLOC``: for servers, receiving one argument: the string
          describing the client location, such as ``'ROOM 641-A'``, :rfc:`779`.

        * ``NAWS``: for servers, receiving two integer arguments (width,
          height), such as (80, 24), :rfc:`1073`.

        * ``TSPEED``: for servers, receiving two integer arguments (rx, tx)
          such as (57600, 57600), :rfc:`1079`.

        * ``TTYPE``: for servers, receiving one string, usually the
          terminfo(5) database capability name, such as 'xterm', :rfc:`1091`.

        * ``XDISPLOC``: for servers, receiving one string, the DISPLAY
          host value, in form of ``<host>:<dispnum>[.<screennum>]``,
          :rfc:`1096`.

        * ``NEW_ENVIRON``: for servers, receiving a dictionary of
          ``(key, val)`` pairs of remote client environment item values,
          :rfc:`1408`.

        * ``CHARSET``: for servers, receiving one string, the character set
          negotiated by client. :rfc:`2066`.

        * ``GMCP``: receiving two arguments (package, data), the GMCP
          package name as string and decoded JSON data (or None).

        * ``MSDP``: receiving one argument, a dict of MSDP variable
          names to values (strings, lists, or nested dicts).

        * ``MSSP``: receiving one argument, a dict of MSSP variable
          names to string values (or list of strings for multi-valued).

        :param func: The callback function to register.
        """
        self._ext_callback[cmd] = func

    def handle_xdisploc(self, xdisploc: str) -> None:
        """Receive XDISPLAY value ``xdisploc``, :rfc:`1096`."""
        #   xdisploc string format is '<host>:<dispnum>[.<screennum>]'.
        self.log.debug("X Display is %s", xdisploc)

    def handle_send_xdisploc(self) -> str:
        """Send XDISPLAY value ``xdisploc``, :rfc:`1096`."""
        #   xdisploc string format is '<host>:<dispnum>[.<screennum>]'.
        self.log.warning("X Display requested, sending empty string.")
        return ""

    def handle_sndloc(self, location: str) -> None:
        """Receive LOCATION value ``location``, :rfc:`779`."""
        self.log.debug("Location is %s", location)

    def handle_send_sndloc(self) -> str:
        """Send LOCATION value ``location``, :rfc:`779`."""
        self.log.warning("Location requested, sending empty response.")
        return ""

    def handle_ttype(self, ttype: str) -> None:
        """
        Receive TTYPE value ``ttype``, :rfc:`1091`.

        A string value that represents client's emulation capability.

        Some example values: VT220, VT100, ANSITERM, ANSI, TTY, and 5250.
        """
        self.log.debug("Terminal type is %r", ttype)

    def handle_send_ttype(self) -> str:
        """Send TTYPE value ``ttype``, :rfc:`1091`."""
        self.log.warning("Terminal type requested, sending empty string.")
        return ""

    def handle_naws(self, width: int, height: int) -> None:
        """Receive window size ``width`` and ``height``, :rfc:`1073`."""
        self.log.debug("Terminal cols=%s, rows=%s", width, height)

    def handle_send_naws(self) -> tuple[int, int]:
        """Send window size ``width`` and ``height``, :rfc:`1073`."""
        self.log.warning("Terminal size requested, sending 80x24.")
        return 80, 24

    def handle_environ(self, env: dict[str, str]) -> None:
        """Receive environment variables as dict, :rfc:`1572`."""
        self.log.debug("Environment values are %r", env)

    def handle_send_client_environ(self, _keys: Any) -> dict[str, str]:
        """
        Send environment variables as dict, :rfc:`1572`.

        If argument ``keys`` is empty, then all available values should be
        sent. Otherwise, ``keys`` is a set of environment keys explicitly
        requested.
        """
        self.log.debug("Environment values requested, sending {{}}.")
        return {}

    def handle_send_server_environ(self) -> list[str]:
        """Server requests environment variables as list, :rfc:`1572`."""
        self.log.debug("Environment values offered, requesting [].")
        return []

    def handle_tspeed(self, rx: int, tx: int) -> None:
        """Receive terminal speed from TSPEED as int, :rfc:`1079`."""
        self.log.debug("Terminal Speed rx:%s, tx:%s", rx, tx)

    def handle_send_tspeed(self) -> tuple[int, int]:
        """Send terminal speed from TSPEED as int, :rfc:`1079`."""
        self.log.debug("Terminal Speed requested, sending 9600,9600.")
        return 9600, 9600

    def handle_charset(self, charset: str) -> None:
        """Receive character set as string, :rfc:`2066`."""
        self.log.debug("Character set: %s", charset)

    def handle_gmcp(self, package: str, data: Any) -> None:
        """
        Receive GMCP message with ``package`` name and ``data``.

        :param package: GMCP package name (e.g., ``"Char.Vitals"``).
        :param data: Decoded JSON value -- may be any JSON type
            (``str``, ``int``, ``float``, ``bool``, ``None``,
            ``list``, or ``dict``).
        """
        self.log.debug("GMCP: %s %r", package, data)

    def handle_msdp(self, variables: dict[str, Any]) -> None:
        """
        Receive MSDP variables as dict.

        :param variables: Mapping of variable names to values.  Values
            may be ``str``, ``dict[str, Any]`` (MSDP table), or
            ``list[Any]`` (MSDP array) per the MSDP wire format.
        """
        self.log.debug("MSDP: %r", variables)

    def handle_mssp(self, variables: dict[str, str | list[str]]) -> None:
        """Receive MSSP variables as dict."""
        self.log.debug("MSSP: %r", variables)
        self.mssp_data = variables

    def handle_msp(self, data: bytes) -> None:
        """Receive MUD Sound Protocol subnegotiation data."""
        self.log.debug("MSP: %r", data)

    def handle_mxp(self, data: bytes) -> None:
        """Receive MUD eXtension Protocol subnegotiation data."""
        self.log.debug("MXP: %r", data)
        self.mxp_data.append(data)

    def handle_zmp(self, parts: list[str]) -> None:
        """Receive decoded ZMP message as list of ``[command, arg, ...]``."""
        self.log.debug("ZMP: %r", parts)
        self.zmp_data.append(parts)

    def handle_aardwolf(self, data: dict[str, Any]) -> None:
        """Receive decoded Aardwolf message as dict."""
        self.log.debug("AARDWOLF: %r", data)
        self.aardwolf_data.append(data)

    def handle_atcp(self, package: str, value: str) -> None:
        """Receive decoded ATCP message as ``(package, value)``."""
        self.log.debug("ATCP: %s %r", package, value)
        self.atcp_data.append((package, value))

    def handle_send_client_charset(self, _charsets: list[str]) -> str:
        """
        Send character set selection as string, :rfc:`2066`.

        Given the available encodings presented by the server, select and return only one. Returning
        an empty string indicates that no selection is made (request is ignored).
        """
        self.log.debug("Character Set requested")
        return ""

    def handle_send_server_charset(self) -> list[str]:
        """Send character set (encodings) offered to client, :rfc:`2066`."""
        return ["UTF-8"]

    def handle_logout(self, cmd: bytes) -> None:
        """
        Handle (IAC, (DO | DONT | WILL | WONT), LOGOUT), :rfc:`727`.

        Only the server end may receive (DO, DONT). Only the client end may receive (WILL, WONT).
        """
        # Close the transport on receipt of DO, Reply DONT on receipt
        # of WILL.  Nothing is done on receipt of DONT or WONT LOGOFF.
        if cmd == DO:
            self.log.debug("client requests DO LOGOUT")
            self._transport.close()
        elif cmd == DONT:
            self.log.debug("client requests DONT LOGOUT")
        elif cmd == WILL:
            self.log.debug("recv WILL TIMEOUT (timeout warning)")
            self.log.debug("send IAC DONT LOGOUT")
            self.iac(DONT, LOGOUT)
        elif cmd == WONT:
            self.log.debug("recv IAC WONT LOGOUT (server refuses logout")

    # public derivable methods DO, DONT, WILL, and WONT negotiation
    #
    def handle_do(self, opt: bytes) -> bool:
        """
        Process byte 3 of series (IAC, DO, opt) received by remote end.

        This method can be derived to change or extend protocol capabilities,
        for most cases, simply returning True if supported, False otherwise.

        In special cases of various RFC statutes, state is stored and
        answered in willing affirmative, with the exception of:

        - DO TM is *always* answered WILL TM, even if it was already
          replied to.  No state is stored ("Timing Mark"), and the IAC
          callback registered by :meth:`set_ext_callback` for cmd TM
          is called with argument byte ``DO``.
        - DO LOGOUT executes extended callback registered by cmd LOGOUT
          with argument DO (indicating a request for voluntary logoff).
        - DO STATUS sends state of all local, remote, and pending options.

        :raises ValueError: When opt is invalid for the current endpoint role (server/client).
        """
        # For unsupported capabilities, RFC specifies a response of
        # (IAC, WONT, opt).  Similarly, set ``self.local_option[opt]``
        # to ``False``.
        #
        # This method returns True if the opt enables the willingness of the
        # remote end to accept a telnet capability, such as NAWS. It returns
        # False for unsupported option, or an option invalid in that context,
        # such as LOGOUT.
        self.log.debug("handle_do(%s)", name_command(opt))
        if opt == ECHO and self.client:
            # What do we have here? A Telnet Server attempting to
            # fingerprint us as a broken 4.4BSD Telnet Client, which
            # would respond 'WILL ECHO'.  Let us just reply WONT--some
            # servers, such as dgamelaunch (nethack.alt.org) freeze up
            # unless we answer IAC-WONT-ECHO.
            self.iac(WONT, ECHO)
        elif self.server and opt in (
            LINEMODE,
            TTYPE,
            NAWS,
            NEW_ENVIRON,
            XDISPLOC,
            LFLOW,
            TSPEED,
            SNDLOC,
        ):
            self.log.debug("recv DO %s on server end, refusing.", name_command(opt))
            self.iac(WONT, opt)
        elif self.client and opt in (LOGOUT,):
            raise ValueError(f"cannot recv DO {name_command(opt)} on client end (ignored).")
        elif opt == TM:
            # timing mark is special: simply by replying, the effect
            # is accomplished ('will' or 'wont' is non-consequential):
            # the distant end is able to "time" our response. More
            # importantly, ensure that the IAC interpreter is, in fact,
            # interpreting, and, that all IAC commands up to this point
            # have been processed.
            self.iac(WILL, TM)
            self._iac_callback[TM](DO)

        elif opt == LOGOUT:
            self._ext_callback[LOGOUT](DO)

        elif opt in (
            ECHO,
            LINEMODE,
            BINARY,
            SGA,
            LFLOW,
            EOR,
            TTYPE,
            NEW_ENVIRON,
            XDISPLOC,
            TSPEED,
            CHARSET,
            NAWS,
            STATUS,
            GMCP,
            MSDP,
            MSSP,
            MSP,
            MXP,
            ZMP,
            AARDWOLF,
            ATCP,
        ):
            # Client declines MUD protocols unless explicitly opted in.
            if self.client and opt in _MUD_PROTOCOL_OPTIONS:
                if opt in self.always_will:
                    if not self.local_option.enabled(opt):
                        self.iac(WILL, opt)
                    return True
                self.log.debug("DO %s: MUD protocol, declining on client.", name_command(opt))
                if not self.local_option.enabled(opt):
                    self.iac(WONT, opt)
                return False

            # first time we've agreed, respond accordingly.
            if not self.local_option.enabled(opt):
                self.iac(WILL, opt)

            # and respond with status for some,
            if opt == NAWS:
                self._send_naws()
            elif opt == STATUS:
                self._send_status()

            # and expect a follow-up sub-negotiation for these others.
            elif opt in (LFLOW, TTYPE, NEW_ENVIRON, XDISPLOC, TSPEED, LINEMODE, MXP):
                # Note that CHARSET is not included -- either side that has sent
                # WILL and received DO may initiate SB at any time.
                self.pending_option[SB + opt] = True

        elif opt in self.always_will:
            if not self.local_option.enabled(opt):
                self.iac(WILL, opt)
        else:
            self.log.debug("DO %s not supported.", name_command(opt))
            self.rejected_do.add(opt)
            if not self.local_option.enabled(opt):
                self.iac(WONT, opt)
            return False
        return True

    def handle_dont(self, opt: bytes) -> None:
        """
        Process byte 3 of series (IAC, DONT, opt) received by remote end.

        This only results in ``self.local_option[opt]`` set to ``False``, with
        the exception of (IAC, DONT, LOGOUT), which only signals a callback
        to ``handle_logout(DONT)``.
        """
        self.log.debug("handle_dont(%s)", name_command(opt))
        if opt == LOGOUT:
            self._ext_callback[LOGOUT](DONT)
        # many implementations (wrongly!) sent a WONT in reply to DONT. It
        # sounds reasonable, but it can and will cause telnet loops. (ruby?)
        # Correctly, a DONT can not be declined, so there is no need to
        # affirm in the negative.

    def handle_will(self, opt: bytes) -> None:
        """
        Process byte 3 of series (IAC, WILL, opt) received by remote end.

        The remote end requests we perform any number of capabilities. Most
        implementations require an answer in the affirmative with DO, unless
        DO has meaning specific for only client or server end, and
        dissenting with DONT.

        WILL ECHO may only be received *for clients*, answered with DO.
        WILL NAWS may only be received *for servers*, answered with DO.
        BINARY and SGA are answered with DO.  STATUS, NEW_ENVIRON, XDISPLOC,
        and TTYPE is answered with sub-negotiation SEND. The env variables
        requested in response to WILL NEW_ENVIRON is "SEND ANY".
        All others are replied with DONT.

        The result of a supported capability is a response of (IAC, DO, opt)
        and the setting of ``self.remote_option[opt]`` of ``True``. For
        unsupported capabilities, RFC specifies a response of (IAC, DONT, opt).
        Similarly, set ``self.remote_option[opt]`` to ``False``.

        Options received in the wrong direction (e.g. WILL NAWS on client
        end) are gracefully refused with DONT per Postel's law (RFC 1123).

        :raises ValueError: When WILL ECHO is received on server end, or
            when WILL TM is received without prior DO TM.
        """
        self.log.debug("handle_will(%s)", name_command(opt))

        if opt in (
            BINARY,
            SGA,
            ECHO,
            NAWS,
            LINEMODE,
            EOR,
            SNDLOC,
            COM_PORT_OPTION,
            GMCP,
            MSDP,
            MSSP,
            MSP,
            MXP,
            ZMP,
            AARDWOLF,
            ATCP,
        ):
            if opt == ECHO and self.server:
                raise ValueError("cannot recv WILL ECHO on server end")
            if opt in (NAWS, LINEMODE, SNDLOC) and self.client:
                self.log.debug("recv WILL %s on client end, refusing.", name_command(opt))
                self.iac(DONT, opt)
                return
            # Client declines MUD protocols unless explicitly opted in.
            if self.client and opt in _MUD_PROTOCOL_OPTIONS:
                if opt in self.always_do or opt in self.passive_do:
                    if not self.remote_option.enabled(opt):
                        self.iac(DO, opt)
                        self.remote_option[opt] = True
                    return
                self.iac(DONT, opt)
                return
            if not self.remote_option.enabled(opt):
                self.iac(DO, opt)
                self.remote_option[opt] = True
            if opt in (NAWS, LINEMODE, SNDLOC, MXP):
                # expect to receive some sort of follow-up subnegotiation
                self.pending_option[SB + opt] = True
                if opt == LINEMODE:
                    # server sets the initial mode and sends forwardmask,
                    self.send_linemode(self.default_linemode)
            if opt == COM_PORT_OPTION and self.client:
                self.request_comport_signature()

        elif opt == TM:
            if opt == TM and not self.pending_option.enabled(DO + TM):
                raise ValueError("cannot recv WILL TM, must first send DO TM.")
            self._iac_callback[TM](WILL)
            self.remote_option[opt] = True

        elif opt == LOGOUT:
            if self.client:
                raise ValueError("cannot recv WILL LOGOUT on client end")
            self._ext_callback[LOGOUT](WILL)

        elif opt == STATUS:
            # Though unnecessary, if the other end claims support for STATUS,
            # we put them to the test by requesting their status.
            self.remote_option[opt] = True
            self.request_status()

        elif opt in (XDISPLOC, TTYPE, TSPEED, NEW_ENVIRON, LFLOW, CHARSET):
            # CHARSET is bi-directional: "WILL CHARSET indicates the sender
            # REQUESTS permission to, or AGREES to, use CHARSET option
            # sub-negotiation to choose a character set."; however, the
            # selected encoding is, regarding SB CHARSET REQUEST, "The sender
            # requests that all text sent to and by it be encoded in one of the
            # specified character sets. "
            #
            # Though Others -- XDISPLOC, TTYPE, TSPEED, are 1-directional.
            if not self.server and opt not in (CHARSET,):
                self.log.debug("recv WILL %s on client end, refusing.", name_command(opt))
                self.iac(DONT, opt)
                return

            # First, we need to acknowledge WILL with DO for all options
            # This was missing for CHARSET when received by client
            if opt == CHARSET and self.client:
                self.iac(DO, CHARSET)

            self.remote_option[opt] = True

            # Special handling for CHARSET: server should declare its own capability
            # by sending WILL CHARSET after receiving WILL CHARSET from client
            if opt == CHARSET and self.server:
                if not self.local_option.enabled(CHARSET):
                    # Special case: reciprocate WILL CHARSET with our own WILL CHARSET
                    # but don't set pending_option since we're not expecting a response
                    self.log.debug("send IAC WILL CHARSET (reciprocating client's WILL)")
                    self.local_option[CHARSET] = True
                    self.send_iac(IAC + WILL + CHARSET)

            # call one of the following callbacks.
            # For CHARSET, only server should automatically initiate REQUEST
            if opt == CHARSET and self.client:
                # Client received WILL CHARSET from server, but doesn't auto-request
                pass
            else:
                {
                    XDISPLOC: self.request_xdisploc,
                    TTYPE: self.request_ttype,
                    TSPEED: self.request_tspeed,
                    CHARSET: self.request_charset,
                    NEW_ENVIRON: self.request_environ,
                    LFLOW: self.send_lineflow_mode,
                }[opt]()

        elif opt in self.always_do:
            if not self.remote_option.enabled(opt):
                self.iac(DO, opt)
                self.remote_option[opt] = True
        else:
            self.iac(DONT, opt)
            self.rejected_will.add(opt)
            self.log.debug("Unhandled: WILL %s.", name_command(opt))
            if self.pending_option.enabled(DO + opt):
                self.pending_option[DO + opt] = False

    def handle_wont(self, opt: bytes) -> None:
        """
        Process byte 3 of series (IAC, WONT, opt) received by remote end.

        (IAC, WONT, opt) is a negative acknowledgment of (IAC, DO, opt) sent.

        The remote end requests we do not perform a telnet capability.

        It is not possible to decline a WONT. ``T.remote_option[opt]`` is set
        False to indicate the remote end's refusal to perform ``opt``.

        :raises ValueError: When WONT TM is received without prior DO TM.
        """
        self.log.debug("handle_wont(%s)", name_command(opt))
        if opt == TM and not self.pending_option.enabled(DO + TM):
            raise ValueError("WONT TM received but DO TM was not sent")
        if opt == TM:
            self.log.debug("WONT TIMING-MARK")
            self.remote_option[opt] = False
        elif opt == LOGOUT:
            if not self.pending_option.enabled(DO + LOGOUT):
                self.log.warning("Server sent WONT LOGOUT unsolicited")
            self._ext_callback[LOGOUT](WONT)
        else:
            self.remote_option[opt] = False

    # public derivable Sub-Negotation parsing
    #
    def handle_subnegotiation(self, buf: collections.deque[bytes]) -> None:
        """
        Callback for end of sub-negotiation buffer.

        SB options handled here are TTYPE, XDISPLOC, NEW_ENVIRON,
        NAWS, and STATUS, and are delegated to their ``handle_``
        equivalent methods. Implementers of additional SB options
        should extend this method.

        :raises ValueError: When the sub-negotiation buffer is empty, starts
            with NUL, is too short, or contains an unhandled command.
        """
        if not buf:
            raise ValueError("SE: buffer empty")
        if buf[0] == theNULL:
            raise ValueError("SE: buffer is NUL")
        # MUD protocols may send empty SB payloads (e.g. IAC SB MXP IAC SE).
        if len(buf) == 1 and buf[0] not in _EMPTY_SB_OK:
            raise ValueError(f"SE: buffer too short: {buf!r}")

        cmd = buf[0]
        if self.pending_option.enabled(SB + cmd):
            self.pending_option[SB + cmd] = False
        else:
            self.log.debug("[SB + %s] unsolicited", name_command(cmd))

        fn_call = {
            LINEMODE: self._handle_sb_linemode,
            LFLOW: self._handle_sb_lflow,
            NAWS: self._handle_sb_naws,
            SNDLOC: self._handle_sb_sndloc,
            NEW_ENVIRON: self._handle_sb_environ,
            CHARSET: self._handle_sb_charset,
            TTYPE: self._handle_sb_ttype,
            TSPEED: self._handle_sb_tspeed,
            XDISPLOC: self._handle_sb_xdisploc,
            STATUS: self._handle_sb_status,
            COM_PORT_OPTION: self._handle_sb_comport,
            GMCP: self._handle_sb_gmcp,
            MSDP: self._handle_sb_msdp,
            MSSP: self._handle_sb_mssp,
            MSP: self._handle_sb_msp,
            MXP: self._handle_sb_mxp,
            ZMP: self._handle_sb_zmp,
            AARDWOLF: self._handle_sb_aardwolf,
            ATCP: self._handle_sb_atcp,
        }.get(cmd)
        if fn_call is None:
            raise ValueError(f"SB unhandled: cmd={name_command(cmd)}, buf={buf!r}")

        fn_call(buf)

    # Our Private API methods

    @staticmethod
    def _escape_iac(buf: bytes) -> bytes:
        r"""Replace bytes in buf ``IAC`` (``b'\xff'``) by ``IAC IAC``."""
        return buf.replace(IAC, IAC + IAC)

    def _write(self, buf: bytes, escape_iac: bool = True) -> None:
        """
        Write bytes to transport, conditionally escaping IAC.

        :param buf: bytes to write to transport.
        :param escape_iac: whether bytes in buffer ``buf`` should be
            escaped of byte ``IAC``.  This should be set ``False`` for direct
            writes of ``IAC`` commands.
        """
        if not isinstance(buf, (bytes, bytearray)):
            raise TypeError(f"buf expected bytes, got {type(buf)}")
        if not self.is_closing():
            if escape_iac:
                # when escape_iac is True, we may safely assume downstream
                # application has provided an encoded string. Prior to 2.0.1, `buf`
                # was inspected to raise TypeError for any bytes of ordinal value
                # greater than 127, but it was removed for performance.
                buf = self._escape_iac(buf)

            if self.log.isEnabledFor(TRACE):
                self.log.log(TRACE, "send %d bytes\n%s", len(buf), hexdump(buf, prefix=">>  "))
            self._transport.write(buf)
            if hasattr(self._protocol, "_tx_bytes"):
                self._protocol._tx_bytes += len(buf)

    # Private sub-negotiation (SB) routines

    def _handle_sb_charset(self, buf: collections.deque[bytes]) -> None:
        buf.popleft()
        opt = buf.popleft()
        if opt == REQUEST:
            # "<Sep>  is a separator octet, the value of which is chosen by the
            # sender.  Examples include a space or a semicolon."
            sep = buf.popleft()
            # decode any offered character sets (b'CHAR-SET')
            # to a python-normalized unicode string ('charset').
            offers = [charset.decode("ascii") for charset in b"".join(buf).split(sep)]
            selected = self._ext_send_callback[CHARSET](offers)
            if selected is None:
                self.log.debug("send IAC SB CHARSET REJECTED IAC SE")
                self.send_iac(IAC + SB + CHARSET + REJECTED + IAC + SE)
            else:
                response: collections.deque[bytes] = collections.deque()
                response.extend([IAC, SB, CHARSET, ACCEPTED])
                response.extend([bytes(selected, "ascii")])
                response.extend([IAC, SE])
                self.log.debug("send IAC SB CHARSET ACCEPTED %s IAC SE", selected)
                self.send_iac(b"".join(response))
                self.environ_encoding = selected
                self._force_binary_on_protocol()
        elif opt == ACCEPTED:
            charset = b"".join(buf).decode("ascii")
            self.log.debug("recv IAC SB CHARSET ACCEPTED %s IAC SE", charset)
            self.environ_encoding = charset
            self._force_binary_on_protocol()
            self._ext_callback[CHARSET](charset)
        elif opt == REJECTED:
            self.log.warning("recv IAC SB CHARSET REJECTED IAC SE")
        elif opt in (TTABLE_IS, TTABLE_ACK, TTABLE_NAK, TTABLE_REJECTED):
            raise NotImplementedError(
                f"Translation table command received but not supported: {opt!r}"
            )
        else:
            raise ValueError(f"Illegal option follows IAC SB CHARSET: {opt!r}.")

    def _handle_sb_tspeed(self, buf: collections.deque[bytes]) -> None:
        """Callback handles IAC-SB-TSPEED-<buf>-SE."""
        cmd = buf.popleft()
        opt = buf.popleft()
        opt_kind = {IS: "IS", SEND: "SEND"}.get(opt)
        self.log.debug("recv %s %s: %r", name_command(cmd), opt_kind, b"".join(buf))

        if opt == IS:
            rx_str, tx_str = str(), str()
            while len(buf):
                value = buf.popleft()
                if value == b",":
                    break
                rx_str += value.decode("ascii")
            while len(buf):
                value = buf.popleft()
                if value == b",":
                    break
                tx_str += value.decode("ascii")
            self.log.debug("sb_tspeed: %s, %s", rx_str, tx_str)
            try:
                rx_int, tx_int = int(rx_str), int(tx_str)
            except ValueError as err:
                self.log.error(
                    "illegal TSPEED values received (rx=%r, tx=%r): %s", rx_str, tx_str, err
                )
                return
            self._ext_callback[TSPEED](rx_int, tx_int)
        elif opt == SEND:
            rx, tx = self._ext_send_callback[TSPEED]()
            brx = f"{rx}".encode("ascii")
            btx = f"{tx}".encode("ascii")
            response = [IAC, SB, TSPEED, IS, brx, b",", btx, IAC, SE]
            self.log.debug("send: IAC SB TSPEED IS %r,%r IAC SE", brx, btx)
            self.send_iac(b"".join(response))
            if self.pending_option.enabled(WILL + TSPEED):
                self.pending_option[WILL + TSPEED] = False

    def _handle_sb_xdisploc(self, buf: collections.deque[bytes]) -> None:
        """Callback handles IAC-SB-XDISPLOC-<buf>-SE."""
        cmd = buf.popleft()
        opt = buf.popleft()

        opt_kind = {IS: "IS", SEND: "SEND"}.get(opt)
        self.log.debug("recv %s %s: %r", name_command(cmd), opt_kind, b"".join(buf))

        if opt == IS:
            xdisploc_str = b"".join(buf).decode("ascii")
            self.log.debug("recv IAC SB XDISPLOC IS %r IAC SE", xdisploc_str)
            self._ext_callback[XDISPLOC](xdisploc_str)
        elif opt == SEND:
            xdisploc_str = self._ext_send_callback[XDISPLOC]().encode("ascii")
            response = [IAC, SB, XDISPLOC, IS, xdisploc_str, IAC, SE]
            self.log.debug("send IAC SB XDISPLOC IS %r IAC SE", xdisploc_str)
            self.send_iac(b"".join(response))
            if self.pending_option.enabled(WILL + XDISPLOC):
                self.pending_option[WILL + XDISPLOC] = False

    def _handle_sb_ttype(self, buf: collections.deque[bytes]) -> None:
        """Callback handles IAC-SB-TTYPE-<buf>-SE."""
        cmd = buf.popleft()
        opt = buf.popleft()

        opt_kind = {IS: "IS", SEND: "SEND"}.get(opt)
        self.log.debug("recv %s %s: %r", name_command(cmd), opt_kind, b"".join(buf))

        if opt == IS:
            if not self.server:
                self.log.warning("ignoring TTYPE IS from server: %r", b"".join(buf))
                return
            ttype_str = b"".join(buf).decode("ascii")
            self.log.debug("recv IAC SB TTYPE IS %r", ttype_str)
            self._ext_callback[TTYPE](ttype_str)
        elif opt == SEND:
            ttype_str = self._ext_send_callback[TTYPE]().encode("ascii")
            response = [IAC, SB, TTYPE, IS, ttype_str, IAC, SE]
            self.log.debug("send IAC SB TTYPE IS %r IAC SE", ttype_str)
            self.send_iac(b"".join(response))
            if self.pending_option.enabled(WILL + TTYPE):
                self.pending_option[WILL + TTYPE] = False

    def _handle_sb_environ(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles (IAC, SB, NEW_ENVIRON, <buf>, SE), :rfc:`1572`.

        For requests beginning with IS, or subsequent requests beginning
        with INFO, any callback registered by :meth:`set_ext_callback` of
        cmd NEW_ENVIRON is passed a dictionary of (key, value) replied-to
        by client.

        For requests beginning with SEND, the callback registered by
        ``set_ext_send_callback`` is provided with a list of keys
        requested from the server; or None if only VAR and/or USERVAR
        is requested, indicating to "send them all".
        """
        cmd = buf.popleft()
        opt = buf.popleft()

        opt_kind = {IS: "IS", INFO: "INFO", SEND: "SEND"}.get(opt)
        raw = b"".join(buf)

        if opt == SEND:
            self.environ_send_raw = raw

        env = _decode_env_buf(raw, encoding=self.environ_encoding)
        env_keys = [k for k in env if k]
        if env_keys:
            self.log.debug("recv %s %s: %s", name_command(cmd), opt_kind, ", ".join(env_keys))
        else:
            self.log.debug("recv %s %s (all)", name_command(cmd), opt_kind)

        if opt in (IS, INFO):
            if opt == IS:
                if not self.pending_option.enabled(SB + cmd):
                    self.log.debug("%s %s unsolicited", name_command(cmd), opt_kind)
                self.pending_option[SB + cmd] = False
            elif self.pending_option.get(SB + cmd, None) is False:
                # a pending option of value of 'False' means it was previously
                # completed, subsequent environment values *should* have been
                # sent as command INFO ...
                self.log.warning("%s IS already recv; expected INFO.", name_command(cmd))
            if env:
                self._ext_callback[cmd](env)
        elif opt == SEND:
            # client-side, we do _not_ honor the 'send all VAR' or 'send all
            # USERVAR' requests -- it is a small bit of a security issue.
            reply_env = self._ext_send_callback[NEW_ENVIRON](list(env.keys()))
            send_env = _encode_env_buf(reply_env, encoding=self.environ_encoding)
            response = [IAC, SB, NEW_ENVIRON, IS, send_env, IAC, SE]
            if reply_env:
                self.log.debug(
                    "env send: %s", ", ".join(f"{k}={v!r}" for k, v in reply_env.items())
                )
            else:
                self.log.debug("env send: (empty)")
            self.send_iac(b"".join(response))
            if self.pending_option.enabled(WILL + TTYPE):
                self.pending_option[WILL + TTYPE] = False

    def _handle_sb_sndloc(self, buf: collections.deque[bytes]) -> None:
        """Fire callback for IAC-SB-SNDLOC-<buf>-SE (:rfc:`779`)."""
        buf.popleft()
        location_str = b"".join(buf).decode("ascii")
        self._ext_callback[SNDLOC](location_str)

    def _send_naws(self) -> None:
        """Fire callback for IAC-DO-NAWS from server."""
        # Similar to the callback method order fired by _handle_sb_naws(),
        # we expect our parameters in order of (rows, cols), matching the
        # termios.TIOCGWINSZ and terminfo(5) cup capability order.
        rows, cols = self._ext_send_callback[NAWS]()

        # NAWS limits columns and rows to a size of 0-65535 (unsigned short).
        #
        # >>> struct.unpack('!HH', b'\xff\xff\xff\xff')
        # (65535, 65535).
        rows, cols = max(min(65535, rows), 0), max(min(65535, cols), 0)

        # NAWS is sent in (col, row) order:
        #
        #    IAC SB NAWS WIDTH[1] WIDTH[0] HEIGHT[1] HEIGHT[0] IAC SE
        #
        value = self._escape_iac(struct.pack("!HH", cols, rows))
        response = [IAC, SB, NAWS, value, IAC, SE]
        self.log.debug("send IAC SB NAWS (rows=%s, cols=%s) IAC SE", rows, cols)
        self.send_iac(b"".join(response))

    def _handle_sb_naws(self, buf: collections.deque[bytes]) -> None:
        """Fire callback for IAC-SB-NAWS-<cols_rows[4]>-SE (:rfc:`1073`)."""
        buf.popleft()
        if not self.remote_option.enabled(NAWS):
            self.log.info(
                "received IAC SB NAWS without receipt of IAC WILL NAWS -- assuming NAWS-enabled"
            )
            self.remote_option[NAWS] = True
        # note a similar formula:
        #
        #    cols, rows = ((256 * buf[0]) + buf[1],
        #                  (256 * buf[2]) + buf[3])
        cols, rows = struct.unpack("!HH", b"".join(buf))
        self.log.debug("recv IAC SB NAWS (cols=%s, rows=%s) IAC SE", cols, rows)

        # Flip the bytestream order (cols, rows) -> (rows, cols).
        #
        # This is for good reason: it matches the termios.TIOCGWINSZ
        # structure, which also matches the terminfo(5) capability, 'cup'.
        self._ext_callback[NAWS](rows, cols)

    def _handle_sb_lflow(self, buf: collections.deque[bytes]) -> None:
        """Callback responds to IAC SB LFLOW, :rfc:`1372`."""
        buf.popleft()  # LFLOW
        if not self.local_option.enabled(LFLOW):
            raise ValueError("received IAC SB LFLOW without first receiving IAC DO LFLOW.")
        opt = buf.popleft()
        if opt in (LFLOW_OFF, LFLOW_ON):
            self.lflow = opt is LFLOW_ON
            self.log.debug("LFLOW (toggle-flow-control) %s", "ON" if self.lflow else "OFF")

        elif opt in (LFLOW_RESTART_ANY, LFLOW_RESTART_XON):
            self.xon_any = opt is LFLOW_RESTART_XON
            self.log.debug(
                "LFLOW (toggle-flow-control) %s", "RESTART_ANY" if self.xon_any else "RESTART_XON"
            )

        else:
            raise ValueError(f"Unknown IAC SB LFLOW option received: {buf!r}")

    def _handle_sb_status(self, buf: collections.deque[bytes]) -> None:
        """
        Callback responds to IAC SB STATUS, :rfc:`859`.

        This method simply delegates to either of :meth:`_receive_status`
        or :meth:`_send_status`.
        """
        buf.popleft()
        opt = buf.popleft()
        if opt == SEND:
            self._send_status()
        elif opt == IS:
            self._receive_status(buf)
        else:
            raise ValueError(f"Illegal byte following IAC SB STATUS: {opt!r}, expected SEND or IS.")

    def _receive_status(self, buf: collections.deque[bytes]) -> None:
        """
        Callback responds to IAC SB STATUS IS, :rfc:`859`.

        :param buf: sub-negotiation byte buffer containing status data.
            Parses ``WILL/WONT/DO/DONT <opt>`` pairs and ``SB <opt> <data> SE``
            blocks.  Compares the remote peer's reported option state against
            our own and logs a summary of agreed, disagreed, and subnegotiation
            parameters.
        """
        buf_list = list(buf)
        agreed = []
        disagreed = []
        sb_info = []

        i = 0
        while i < len(buf_list):
            if i + 1 >= len(buf_list):
                self.log.debug("STATUS: trailing byte: %s", buf_list[i])
                break

            cmd = buf_list[i]

            # SB <opt> <data...> SE block
            if cmd == SB:
                opt = buf_list[i + 1]
                # find matching SE
                se_idx = None
                for j in range(i + 2, len(buf_list)):
                    if buf_list[j] == SE:
                        se_idx = j
                        break
                if se_idx is None:
                    sb_data = b"".join(buf_list[i + 2 :])
                    sb_info.append(f"{name_command(opt)} {sb_data.hex()}")
                    break
                sb_data = b"".join(buf_list[i + 2 : se_idx])
                sb_info.append(_format_sb_status(opt, sb_data))
                i = se_idx + 1
                continue

            if cmd not in (DO, DONT, WILL, WONT):
                self.log.debug("STATUS: unknown byte at pos %d: %s", i, cmd)
                i += 1
                continue

            opt = buf_list[i + 1]
            opt_name = name_command(opt)
            if cmd in (DO, DONT):
                enabled = self.local_option.enabled(opt)
                matching = (cmd == DO and enabled) or (cmd == DONT and not enabled)
            else:
                enabled = self.remote_option.enabled(opt)
                matching = (cmd == WILL and enabled) or (cmd == WONT and not enabled)

            if matching:
                agreed.append(opt_name)
            else:
                disagreed.append(opt_name)

            i += 2

        if agreed:
            self.log.debug("STATUS agreed: %s", ", ".join(agreed))
        if disagreed:
            self.log.debug("STATUS disagreed: %s", ", ".join(disagreed))
        if sb_info:
            self.log.debug("STATUS subneg: %s", "; ".join(sb_info))

    def _send_status(self) -> None:
        """Callback responds to IAC SB STATUS SEND, :rfc:`859`."""
        if not (self.pending_option.enabled(WILL + STATUS) or self.local_option.enabled(STATUS)):
            raise ValueError("Only sender of IAC WILL STATUS may reply by IAC SB STATUS IS.")

        response: collections.deque[bytes] = collections.deque()
        response.extend([IAC, SB, STATUS, IS])
        for opt, status in self.local_option.items():
            # status is 'WILL' for local option states that are True,
            # and 'WONT' for options that are False.
            if opt == STATUS:
                continue
            response.extend([WILL if status else WONT, opt])
        for opt, status in self.remote_option.items():
            # status is 'DO' for remote option states that are True,
            # or for any DO option requests pending reply. status is
            # 'DONT' for any remote option states that are False,
            # or for any DONT option requests pending reply.
            if opt == STATUS:
                continue
            if status or DO + opt in self.pending_option:
                response.extend([DO, opt])
            elif not status or DONT + opt in self.pending_option:
                response.extend([DONT, opt])
        response.extend([IAC, SE])
        self.log.debug(
            "send IAC SB STATUS IS %s IAC SE",
            " ".join([name_command(byte) for byte in list(response)[4:-2]]),
        )
        self.send_iac(b"".join(response))
        if self.pending_option.enabled(WILL + STATUS):
            self.pending_option[WILL + STATUS] = False

    # Special Line Character and other LINEMODE functions.
    #
    def _handle_sb_linemode(self, buf: collections.deque[bytes]) -> None:
        """Callback responds to bytes following IAC SB LINEMODE."""
        buf.popleft()
        opt = buf.popleft()
        if opt == slc.LMODE_MODE:
            self._handle_sb_linemode_mode(buf)
        elif opt == slc.LMODE_SLC:
            self._handle_sb_linemode_slc(buf)
        elif opt in (DO, DONT, WILL, WONT):
            sb_opt = buf.popleft()
            if sb_opt != slc.LMODE_FORWARDMASK:
                raise ValueError(
                    f"Illegal byte follows IAC SB LINEMODE {name_command(opt)}: {sb_opt!r}, "
                    "expected LMODE_FORWARDMASK."
                )
            self.log.debug("recv IAC SB LINEMODE %s LMODE_FORWARDMASK,", name_command(opt))
            self._handle_sb_forwardmask(opt, buf)
        else:
            raise ValueError(f"Illegal IAC SB LINEMODE option {opt!r}")

    def _handle_sb_linemode_mode(self, mode: collections.deque[bytes]) -> None:
        """
        Callback handles mode following IAC SB LINEMODE LINEMODE_MODE.

        :param mode: a single byte

        Result of agreement to enter ``mode`` given applied by setting the
        value of ``self.linemode``, and sending acknowledgment if necessary.
        """
        if not mode:
            raise ValueError("IAC SB LINEMODE LINEMODE-MODE: missing mode byte")
        suggest_mode = slc.Linemode(mode[0])

        self.log.debug("recv IAC SB LINEMODE LINEMODE-MODE %r IAC SE", suggest_mode.mask)

        if not suggest_mode.ack:
            # RFC 1184: if the proposed mode is the same as our current
            # mode (ignoring the ACK bit), suppress the redundant ACK to
            # prevent an infinite echo loop with misbehaving servers that
            # re-send the same MODE without ACK repeatedly.
            if self._linemode == suggest_mode:
                self.log.debug(
                    "suppressing redundant ACK for unchanged LINEMODE-MODE %r", suggest_mode.mask
                )
                return
            # Guard: LINEMODE must be negotiated before we can send a reply
            if not (self.local_option.enabled(LINEMODE) or self.remote_option.enabled(LINEMODE)):
                self.log.warning(
                    "ignoring LINEMODE-MODE %r: LINEMODE not negotiated", suggest_mode.mask
                )
                return
            # This implementation acknowledges and sets local linemode
            # to *any* setting the remote end suggests, requiring a
            # reply.  See notes later under server receipt of acknowledged
            # linemode.
            self.send_linemode(
                linemode=slc.Linemode(
                    mask=bytes([ord(suggest_mode.mask) | ord(slc.LMODE_MODE_ACK)])
                )
            )
            return

        # " In all cases, a response is never generated to a MODE
        #   command that has the MODE_ACK bit set."
        #
        # simply: cannot call self.send_linemode() here forward.

        if self.client:
            if self._linemode != suggest_mode:
                # " When a MODE command is received with the MODE_ACK bit set,
                #   and the mode is different that what the current mode is,
                #   the client will ignore the new mode"
                #
                self.log.warning(
                    "server mode differs from local mode, "
                    "though ACK bit is set. Local mode will "
                    "remain."
                )
                self.log.warning("!remote: %r", suggest_mode)
                self.log.warning("  local: %r", self._linemode)
                return

            self.log.debug("Linemode matches, acknowledged by server.")
            self._linemode = suggest_mode
            return

        # as a server, we simply honor whatever is given.  This is also
        # problematic in some designers may wish to implement shells
        # that specifically do not honor some parts of the bitmask, we
        # must provide them an any/force-on/force-off mode-table interface.
        if self._linemode != suggest_mode:
            self.log.debug("We suggested, - %r", self._linemode)
            self.log.debug("Client choses + %r", suggest_mode)
        else:
            self.log.debug("Linemode agreed by client: %r", self._linemode)

        self._linemode = suggest_mode

    def _handle_sb_linemode_slc(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles IAC-SB-LINEMODE-SLC-<buf>.

        Processes SLC command function triplets found in ``buf`` and replies
        accordingly.
        """
        if not len(buf) - 2 % 3:
            raise ValueError(f"SLC buffer wrong size: expect multiple of 3: {len(buf) - 2}")
        self._slc_start()
        while len(buf):
            func = buf.popleft()
            flag = buf.popleft()
            value = buf.popleft()
            slc_def = slc.SLC(flag, value)
            self._slc_process(func, slc_def)
        self._slc_end()
        if self.server:
            self.request_forwardmask()

    def _slc_end(self) -> None:
        """Transmit SLC commands buffered by :meth:`_slc_send`."""
        if len(self._slc_buffer):
            self.log.debug("send (slc_end): %r", b"".join(self._slc_buffer))
            buf = b"".join(self._slc_buffer)
            if not self.is_closing():
                self._transport.write(self._escape_iac(buf))
            self._slc_buffer.clear()

        self.log.debug("slc_end: [..] IAC SE")
        self.send_iac(IAC + SE)

    def _slc_start(self) -> None:
        """Send IAC SB LINEMODE SLC header."""
        self.log.debug("slc_start: IAC SB LINEMODE SLC [..]")
        self.send_iac(IAC + SB + LINEMODE + slc.LMODE_SLC)

    def _slc_send(self, slctab: Optional[dict[bytes, slc.SLC]] = None) -> None:
        """
        Send supported SLC characters of current tabset, or specified tabset.

        :param slctab: SLC byte tabset as dictionary, such as slc.BSD_SLC_TAB.
        """
        send_count = 0
        slctab = slctab or self.slctab
        for func in range(slc.NSLC + 1):
            if func == 0 and self.client:
                # only the server may send an octet with the first
                # byte (func) set as 0 (SLC_NOSUPPORT).
                continue

            _default = slc.SLC_nosupport()
            if self.slctab.get(bytes([func]), _default).nosupport:
                continue

            self._slc_add(bytes([func]))
            send_count += 1
        self.log.debug("slc_send: %s functions queued.", send_count)

    def _slc_add(self, func: bytes, slc_def: Optional[slc.SLC] = None) -> None:
        """
        Prepare slc triplet response (function, flag, value) for transmission.

        For the given SLC_func byte and slc_def instance providing
        byte attributes ``flag`` and ``val``. If no slc_def is provided,
        the slc definition of ``slctab`` is used by key ``func``.
        """
        if slc_def is None:
            slc_def = self.slctab[func]
        self.log.debug("_slc_add (%-10s %s)", slc.name_slc_command(func) + ",", slc_def)
        if len(self._slc_buffer) >= slc.NSLC * 6:
            raise ValueError("SLC: buffer full!")
        self._slc_buffer.extend([func, slc_def.mask, slc_def.val])

    def _slc_process(self, func: bytes, slc_def: slc.SLC) -> None:
        """
        Process an SLC definition provided by remote end.

        Ensure the function definition is in-bounds and an SLC option
        we support. Store SLC_VARIABLE changes to self.slctab, keyed
        by SLC byte function ``func``.

        The special definition (0, SLC_DEFAULT|SLC_VARIABLE, 0) has the
        side-effect of replying with a full slc tabset, resetting to
        the default tabset, if indicated.
        """
        # out of bounds checking
        if ord(func) > slc.NSLC:
            self.log.warning("SLC not supported (out of range): (%r)", func)
            self._slc_add(func, slc.SLC_nosupport())
            return

        # process special request
        if func == theNULL:
            if slc_def.level == slc.SLC_DEFAULT:
                # client requests we send our default tab,
                self.log.debug("_slc_process: client request SLC_DEFAULT")
                self._slc_send(self.default_slc_tab)
            elif slc_def.level == slc.SLC_VARIABLE:
                # client requests we send our current tab,
                self.log.debug("_slc_process: client request SLC_VARIABLE")
                self._slc_send()
            else:
                self.log.warning("func(0) flag expected, got %s.", slc_def)
            return

        self.log.debug(
            "_slc_process %-9s mine=%s, his=%s",
            slc.name_slc_command(func),
            self.slctab[func],
            slc_def,
        )

        # evaluate slc
        mylevel, myvalue = (self.slctab[func].level, self.slctab[func].val)
        if slc_def.level == mylevel and myvalue == slc_def.val:
            return
        if slc_def.level == mylevel and slc_def.ack:
            return
        if slc_def.ack:
            self.log.debug("slc value mismatch with ack bit set: (%r,%r)", myvalue, slc_def.val)
            return
        self._slc_change(func, slc_def)

    def _slc_change(self, func: bytes, slc_def: slc.SLC) -> None:
        """
        Update SLC tabset with SLC definition provided by remote end.

        Modify private attribute ``slctab`` appropriately for the level
        and value indicated, except for slc tab functions of value
        SLC_NOSUPPORT and reply as appropriate through :meth:`_slc_add`.
        """
        hislevel = slc_def.level
        mylevel = self.slctab[func].level
        if hislevel == slc.SLC_NOSUPPORT:
            # client end reports SLC_NOSUPPORT; use a
            # nosupport definition with ack bit set
            self.slctab[func] = slc.SLC_nosupport()
            self.slctab[func].set_flag(slc.SLC_ACK)
            self._slc_add(func)
            return

        if hislevel == slc.SLC_DEFAULT:
            # client end requests we use our default level
            if mylevel == slc.SLC_DEFAULT:
                # client end telling us to use SLC_DEFAULT on an SLC we do not
                # support (such as SYNCH). Set flag to SLC_NOSUPPORT instead
                # of the SLC_DEFAULT value that it begins with
                self.slctab[func].set_mask(slc.SLC_NOSUPPORT)
            else:
                # set current flag to the flag indicated in default tab
                default_slc = self.default_slc_tab.get(func)
                self.slctab[func].set_mask(default_slc.mask)
            # set current value to value indicated in default tab
            self.default_slc_tab.get(func, slc.SLC_nosupport())
            self.slctab[func].set_value(slc_def.val)
            self._slc_add(func)
            return

        # client wants to change to a new value, or,
        # refuses to change to our value, accept their value.
        if self.slctab[func].val != theNULL:
            self.slctab[func].set_value(slc_def.val)
            self.slctab[func].set_mask(slc_def.mask)
            slc_def.set_flag(slc.SLC_ACK)
            self._slc_add(func, slc_def)
            return

        # if our byte value is b'\x00', it is not possible for us to support
        # this request. If our level is default, just ack whatever was sent.
        # it is a value we cannot change.
        if mylevel == slc.SLC_DEFAULT:
            # If our level is default, store & ack whatever was sent
            self.slctab[func].set_mask(slc_def.mask)
            self.slctab[func].set_value(slc_def.val)
            slc_def.set_flag(slc.SLC_ACK)
            self._slc_add(func, slc_def)
        elif slc_def.level == slc.SLC_CANTCHANGE and mylevel == slc.SLC_CANTCHANGE:
            # "degenerate to SLC_NOSUPPORT"
            self.slctab[func].set_mask(slc.SLC_NOSUPPORT)
            self._slc_add(func)
        else:
            # mask current level to levelbits (clears ack),
            self.slctab[func].set_mask(self.slctab[func].level)
            if mylevel == slc.SLC_CANTCHANGE:
                slc_def = self.default_slc_tab.get(func, slc.SLC_nosupport())
                self.slctab[func].val = slc_def.val
            self._slc_add(func)

    def _handle_sb_forwardmask(self, cmd: bytes, buf: collections.deque[bytes]) -> None:
        """
        Callback handles request for LINEMODE <cmd> LMODE_FORWARDMASK.

        :param cmd: one of DO, DONT, WILL, WONT.
        :param buf: bytes following IAC SB LINEMODE DO FORWARDMASK.
        """
        # set and report about pending options by 2-byte opt,
        # not well tested, no known implementations exist !
        if self.server:
            if not self.remote_option.enabled(LINEMODE):
                self.log.info(
                    "receive and accept LMODE_FORWARDMASK %s without LINEMODE enabled",
                    name_command(cmd),
                )
            if cmd in (DO, DONT):
                self.log.warning(
                    "cannot recv %s LMODE_FORWARDMASK on server end", name_command(cmd)
                )
                return
        if self.client:
            if not self.local_option.enabled(LINEMODE):
                self.log.info(
                    "receive and accept LMODE_FORWARDMASK %s without LINEMODE enabled",
                    name_command(cmd),
                )
            if cmd in (WILL, WONT):
                self.log.warning(
                    "cannot recv %s LMODE_FORWARDMASK on client end", name_command(cmd)
                )
                return
            if cmd == DONT and len(buf) > 0:
                self.log.warning("Illegal bytes follow DONT LMODE_FORWARDMASK: %r", buf)
                return
            if cmd == DO and len(buf) == 0:
                self.log.warning("bytes must follow DO LMODE_FORWARDMASK")
                return

        opt = SB + LINEMODE + slc.LMODE_FORWARDMASK
        if cmd in (WILL, WONT):
            self.remote_option[opt] = bool(cmd is WILL)
        elif cmd in (DO, DONT):
            self.local_option[opt] = bool(cmd is DO)
            if cmd == DO:
                self._handle_do_forwardmask(buf)

    # RFC 2217 sub-command names (server-to-client response codes)
    _COMPORT_SUBCMDS: dict[int, str] = {
        0: "SIGNATURE",
        100: "SIGNATURE",
        101: "SET-BAUDRATE",
        102: "SET-DATASIZE",
        103: "SET-PARITY",
        104: "SET-STOPSIZE",
        105: "SET-CONTROL",
        106: "NOTIFY-LINESTATE",
        107: "NOTIFY-MODEMSTATE",
        108: "FLOWCONTROL-SUSPEND",
        109: "FLOWCONTROL-RESUME",
        110: "SET-LINESTATE-MASK",
        111: "SET-MODEMSTATE-MASK",
        112: "PURGE-DATA",
    }

    _COMPORT_PARITY: dict[int, str] = {
        0: "REQUEST",
        1: "NONE",
        2: "ODD",
        3: "EVEN",
        4: "MARK",
        5: "SPACE",
    }

    _COMPORT_STOPSIZE: dict[int, str] = {0: "REQUEST", 1: "1", 2: "2", 3: "1.5"}

    def _handle_sb_comport(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles ``IAC SB COM-PORT-OPTION`` per :rfc:`2217`.

        Parses the sub-command byte and payload, storing results in
        :attr:`comport_data` for fingerprinting.

        :param buf: bytes following ``IAC SB COM-PORT-OPTION``.
        """
        buf.popleft()  # COM_PORT_OPTION byte
        if not buf:
            self.log.debug("SB COM-PORT-OPTION: empty payload")
            return

        subcmd = ord(buf.popleft())
        payload = b"".join(buf)
        subcmd_name = self._COMPORT_SUBCMDS.get(subcmd, f"UNKNOWN-{subcmd}")

        if self.comport_data is None:
            self.comport_data = {}

        if subcmd in (0, 100) and payload:
            # SIGNATURE response
            sig = payload.decode("ascii", errors="replace")
            self.comport_data["signature"] = sig
            self.log.debug("COM-PORT-OPTION SIGNATURE: %r", sig)
        elif subcmd in (1, 101) and len(payload) == 4:
            # SET-BAUDRATE response: 4-byte big-endian uint32
            baudrate = int.from_bytes(payload, "big")
            self.comport_data["baudrate"] = baudrate
            self.log.debug("COM-PORT-OPTION BAUDRATE: %d", baudrate)
        elif subcmd in (2, 102) and len(payload) == 1:
            # SET-DATASIZE response: 1 byte (5-8 bits, 0=request)
            datasize = payload[0]
            self.comport_data["datasize"] = datasize
            self.log.debug("COM-PORT-OPTION DATASIZE: %d", datasize)
        elif subcmd in (3, 103) and len(payload) == 1:
            # SET-PARITY response
            parity = payload[0]
            self.comport_data["parity"] = self._COMPORT_PARITY.get(parity, f"unknown-{parity}")
            self.log.debug("COM-PORT-OPTION PARITY: %s", self.comport_data["parity"])
        elif subcmd in (4, 104) and len(payload) == 1:
            # SET-STOPSIZE response
            stopsize = payload[0]
            self.comport_data["stopsize"] = self._COMPORT_STOPSIZE.get(
                stopsize, f"unknown-{stopsize}"
            )
            self.log.debug("COM-PORT-OPTION STOPSIZE: %s", self.comport_data["stopsize"])
        elif subcmd in (5, 105) and len(payload) == 1:
            # SET-CONTROL response
            self.comport_data["control"] = payload[0]
            self.log.debug("COM-PORT-OPTION CONTROL: %d", payload[0])
        elif subcmd in (6, 106) and len(payload) == 1:
            # NOTIFY-LINESTATE
            self.comport_data["linestate"] = payload[0]
            self.log.debug("COM-PORT-OPTION LINESTATE: 0x%02x", payload[0])
        elif subcmd in (7, 107) and len(payload) == 1:
            # NOTIFY-MODEMSTATE
            self.comport_data["modemstate"] = payload[0]
            self.log.debug("COM-PORT-OPTION MODEMSTATE: 0x%02x", payload[0])
        else:
            self.log.debug("COM-PORT-OPTION %s (subcmd=%d): %r", subcmd_name, subcmd, payload)

    def _handle_sb_gmcp(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles Generic MUD Communication Protocol (GMCP) subnegotiation.

        :param buf: bytes following IAC SB GMCP.
        """
        buf.popleft()
        payload = b"".join(buf)
        encoding = self.environ_encoding or "utf-8"
        package, data = gmcp_decode(payload, encoding=encoding)
        self._ext_callback[GMCP](package, data)

    def _handle_sb_msdp(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles MUD Server Data Protocol (MSDP) subnegotiation.

        :param buf: bytes following IAC SB MSDP.
        """
        buf.popleft()
        payload = b"".join(buf)
        encoding = self.environ_encoding or "utf-8"
        variables = msdp_decode(payload, encoding=encoding)
        self._ext_callback[MSDP](variables)

    def _handle_sb_mssp(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles MUD Server Status Protocol (MSSP) subnegotiation.

        :param buf: bytes following IAC SB MSSP.
        """
        buf.popleft()
        payload = b"".join(buf)
        encoding = self.environ_encoding or "utf-8"
        variables = mssp_decode(payload, encoding=encoding)
        self._ext_callback[MSSP](variables)

    def _handle_sb_msp(self, buf: collections.deque[bytes]) -> None:
        """
        Handle MUD Sound Protocol (MSP) subnegotiation.

        :param buf: bytes following IAC SB MSP.
        """
        buf.popleft()
        payload = b"".join(buf)
        self._ext_callback[MSP](payload)

    def _handle_sb_mxp(self, buf: collections.deque[bytes]) -> None:
        """
        Handle MUD eXtension Protocol (MXP) subnegotiation.

        :param buf: bytes following IAC SB MXP.
        """
        buf.popleft()
        payload = b"".join(buf)
        self._ext_callback[MXP](payload)

    def _handle_sb_zmp(self, buf: collections.deque[bytes]) -> None:
        """
        Handle Zenith MUD Protocol (ZMP) subnegotiation.

        :param buf: bytes following IAC SB ZMP.
        """
        buf.popleft()
        payload = b"".join(buf)
        encoding = self.environ_encoding or "utf-8"
        parts = zmp_decode(payload, encoding=encoding)
        self._ext_callback[ZMP](parts)

    def _handle_sb_aardwolf(self, buf: collections.deque[bytes]) -> None:
        """
        Handle Aardwolf protocol subnegotiation.

        :param buf: bytes following IAC SB AARDWOLF.
        """
        buf.popleft()
        payload = b"".join(buf)
        data = aardwolf_decode(payload)
        self._ext_callback[AARDWOLF](data)

    def _handle_sb_atcp(self, buf: collections.deque[bytes]) -> None:
        """
        Handle Achaea Telnet Client Protocol (ATCP) subnegotiation.

        :param buf: bytes following IAC SB ATCP.
        """
        buf.popleft()
        payload = b"".join(buf)
        encoding = self.environ_encoding or "utf-8"
        package, value = atcp_decode(payload, encoding=encoding)
        self._ext_callback[ATCP](package, value)

    def _handle_do_forwardmask(self, buf: collections.deque[bytes]) -> None:
        """
        Callback handles request for LINEMODE DO FORWARDMASK.

        :param buf: bytes following IAC SB LINEMODE DO FORWARDMASK.
        """
        mask = b"".join(buf)
        self.log.debug("FORWARDMASK received (%d bytes), not applied", len(mask))


class TelnetWriterUnicode(TelnetWriter):
    """
    A Unicode StreamWriter interface for Telnet protocol.

    See ancestor class, :class:`TelnetWriter` for details.

    Requires the ``fn_encoding`` callback, receiving mutually boolean keyword
    argument ``outgoing=True`` to determine what encoding should be used to
    decode the value in the direction specified.

    The encoding may be conditionally negotiated by CHARSET, :rfc:`2066`, or
    discovered by ``LANG`` environment variables by NEW_ENVIRON, :rfc:`1572`.
    """

    def __init__(
        self,
        transport: asyncio.Transport,
        protocol: Any,
        fn_encoding: Callable[..., str],
        *,
        encoding_errors: str = "strict",
        client: bool = False,
        server: bool = False,
        reader: Optional["TelnetReader"] = None,
    ) -> None:
        """Initialize TelnetWriterUnicode with encoding callback."""
        self.fn_encoding = fn_encoding
        self.encoding_errors = encoding_errors
        super().__init__(transport, protocol, client=client, server=server, reader=reader)

    def encode(self, string: str, errors: Optional[str] = None) -> bytes:
        """
        Encode ``string`` using protocol-preferred encoding.

        :param string: unicode string to encode.
        :param errors: same as meaning in :meth:`codecs.Codec.encode`, when
            ``None`` (default), value of class initializer keyword argument,
            ``encoding_errors``.

        .. note::

            Though a unicode interface, when ``outbinary`` mode has not
            been protocol negotiated, ``fn_encoding`` strictly enforces 7-bit
            ASCII range (ordinal byte values less than 128), as a strict
            compliance of the telnet RFC.
        """
        encoding = self.fn_encoding(outgoing=True)
        return bytes(string, encoding, errors or self.encoding_errors)

    def write(self, string: str, errors: Optional[str] = None) -> None:  # type: ignore[override]
        """
        Write unicode string to transport, using protocol-preferred encoding.

        If the connection is closed, nothing is done.

        :param string: unicode string text to write to endpoint using the
            protocol's preferred encoding.  When the protocol ``encoding``
            keyword is explicitly set to ``False``, the given string should be
            only raw ``b'bytes'``.
        :param errors: same as meaning in :meth:`codecs.Codec.encode`, when
            ``None`` (default), value of class initializer keyword argument,
            ``encoding_errors``.
        """
        if self.connection_closed:
            return
        errors = errors or self.encoding_errors
        self._write(self.encode(string, errors))

    def writelines(  # type: ignore[override]
        self, lines: Sequence[str], errors: Optional[str] = None
    ) -> None:
        """
        Write unicode strings to transport.

        Note that newlines are not added.  The sequence can be any iterable object producing
        strings. This is equivalent to calling write() for each string.
        """
        self.write(string="".join(lines), errors=errors)

    def echo(self, string: str, errors: Optional[str] = None) -> None:  # type: ignore[override]
        """
        Conditionally write ``string`` to transport when "remote echo" enabled.

        :param string: string received as input, conditionally written.
        :param errors: same as meaning in :meth:`codecs.Codec.encode`.

        This method may only be called from the server perspective.  The
        default implementation depends on telnet negotiation willingness for
        local echo: only an RFC-compliant telnet client will correctly set or
        unset echo accordingly by demand.
        """
        if self.will_echo:
            self.write(string=string, errors=errors)


class Option(dict[bytes, bool]):
    """
    Telnet option state negotiation helper class.

    This class simply acts as a logging decorator for state changes of a dictionary describing
    telnet option negotiation.
    """

    def __init__(
        self, name: str, log: logging.Logger, on_change: Optional[Callable[[], None]] = None
    ) -> None:
        """
        Class initializer.

        :param name: decorated name representing option class, such as 'local', 'remote', or
            'pending'.
        :param on_change: optional callback invoked when option state changes.
        """
        self.name, self.log = name, log
        self._on_change = on_change
        dict.__init__(self)

    def enabled(self, key: bytes) -> bool:
        """
        Return True if option is enabled.

        :param key: telnet option byte(s).
        """
        return bool(self.get(key, None) is True)

    def __setitem__(self, key: bytes, value: bool) -> None:
        # the real purpose of this class, tracking state negotiation.
        if value != dict.get(self, key, None):
            descr = " + ".join(
                [name_command(bytes([byte])) for byte in key[:2]] + [repr(byte) for byte in key[2:]]
            )
            self.log.debug("%s[%s] = %s", self.name, descr, value)
        dict.__setitem__(self, key, value)
        if self._on_change is not None:
            self._on_change()


def _escape_environ(buf: bytes) -> bytes:
    """
    Return new buffer with VAR and USERVAR escaped, if present in ``buf``.

    :param buf: given bytes buffer.
    :returns: buffer with escape characters inserted.
    """
    return buf.replace(VAR, ESC + VAR).replace(USERVAR, ESC + USERVAR)


def _unescape_environ(buf: bytes) -> bytes:
    """
    Return new buffer with escape characters removed for VAR and USERVAR.

    :param buf: given bytes buffer.
    :returns: buffer with escape characters removed.
    """
    return buf.replace(ESC + VAR, VAR).replace(ESC + USERVAR, USERVAR)


def _encode_env_buf(env: dict[str, str], encoding: str = "ascii") -> bytes:
    """
    Encode dictionary for transmission as environment variables, :rfc:`1572`.

    :param env: dictionary of environment values.
    :param encoding: Character encoding for names and values.
    :returns: buffer meant to follow sequence IAC SB NEW_ENVIRON IS.
        It is not terminated by IAC SE.

    Returns bytes array ``buf`` for use in sequence (IAC, SB,
    NEW_ENVIRON, IS, <buf>, IAC, SE) as set forth in :rfc:`1572`.
    """
    buf: collections.deque[bytes] = collections.deque()
    for key, value in env.items():
        buf.append(VAR)
        buf.extend([_escape_environ(key.encode(encoding, "replace"))])
        buf.append(VALUE)
        buf.extend([_escape_environ(f"{value}".encode(encoding, "replace"))])
    return b"".join(buf)


def _format_sb_status(opt: bytes, data: bytes) -> str:
    """
    Format a STATUS IS subnegotiation block as a human-readable string.

    :param opt: Option byte (e.g. NAWS, TTYPE).
    :param data: Subnegotiation payload bytes.
    :returns: Descriptive string like ``"NAWS 80x25"`` or ``"TTYPE IS VT100"``.
    """
    opt_name = name_command(opt)
    if opt == NAWS and len(data) == 4:
        w = (data[0] << 8) | data[1]
        h = (data[2] << 8) | data[3]
        return f"{opt_name} {w}x{h}"
    if opt in (TTYPE, XDISPLOC, SNDLOC) and len(data) >= 2:
        kind = {IS: "IS", SEND: "SEND"}.get(data[0:1], data[0:1].hex())
        text = data[1:].decode("ascii", errors="replace")
        return f"{opt_name} {kind} {text}"
    if data:
        return f"{opt_name} {data.hex()}"
    return opt_name


def _decode_env_buf(buf: bytes, encoding: str = "ascii") -> dict[str, str]:
    """
    Decode environment values to dictionary, :rfc:`1572`.

    :param buf: bytes array following sequence IAC SB NEW_ENVIRON
        SEND or IS up to IAC SE.
    :returns: dictionary representing the environment values decoded from buf.

    This implementation does not distinguish between ``USERVAR`` and ``VAR``.
    """
    env = {}

    # build table of (non-escaped) delimiters by index of buf[].
    breaks = [
        idx
        for (idx, byte) in enumerate(buf)
        if (bytes([byte]) in (VAR, USERVAR) and (idx == 0 or bytes([buf[idx - 1]]) != ESC))
    ]

    for idx, ptr in enumerate(breaks):
        # find buf[] starting, ending positions, begin after
        # buf[0], which is currently valued VAR or USERVAR
        start = ptr + 1
        if idx == len(breaks) - 1:
            end = len(buf)
        else:
            end = breaks[idx + 1]

        pair = buf[start:end].split(VALUE, 1)
        key = _unescape_environ(pair[0]).decode(encoding, "replace")
        if len(pair) == 1:
            value = ""
        else:
            value = _unescape_environ(pair[1]).decode(encoding, "replace")
        env[key] = value

    return env
