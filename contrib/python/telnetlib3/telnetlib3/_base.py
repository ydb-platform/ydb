"""Shared base utilities for server and client telnet protocol implementations."""

from __future__ import annotations

# std imports
import sys
import types
import logging
import datetime
import traceback
from typing import Any, Type, Callable, Optional

# Pre-allocated single-byte cache to avoid per-byte bytes() allocations
_ONE_BYTE = [bytes([i]) for i in range(256)]


def _log_exception(
    log_fn: Callable[..., Any],
    e_type: Optional[Type[BaseException]],
    e_value: Optional[BaseException],
    e_tb: Optional[types.TracebackType],
) -> None:
    """Log an exception's traceback and message via *log_fn*."""
    rows_tbk = [line for line in "\n".join(traceback.format_tb(e_tb)).split("\n") if line]
    rows_exc = [line.rstrip() for line in traceback.format_exception_only(e_type, e_value)]

    for line in rows_tbk + rows_exc:
        log_fn(line)


def _process_data_chunk(
    data: bytes,
    writer: Any,
    reader: Any,
    slc_special: frozenset[int] | None,
    log_fn: Callable[..., Any],
) -> bool:
    """
    Scan *data* for IAC and SLC bytes, feed regular bytes to *reader*.

    :param data: Raw bytes received from the transport.
    :param writer: TelnetWriter instance for IAC interpretation.
    :param reader: TelnetReader instance for in-band data.
    :param slc_special: Frozenset of special byte values (IAC + SLC triggers),
        or ``None`` when only IAC (255) is special.
    :param log_fn: Callable for logging exceptions (e.g. ``logger.warning``).
    :returns: ``True`` if any IAC/SB command was observed.
    """
    cmd_received = False
    n = len(data)
    i = 0
    out_start = 0
    feeding_oob = bool(writer.is_oob)

    while i < n:
        if not feeding_oob:
            if slc_special is None:
                next_iac = data.find(255, i)
                if next_iac == -1:
                    if n > out_start:
                        reader.feed_data(data[out_start:])
                    return cmd_received
                i = next_iac
            else:
                while i < n and data[i] not in slc_special:
                    i += 1
            if i > out_start:
                reader.feed_data(data[out_start:i])
            if i >= n:
                break

        try:
            recv_inband = writer.feed_byte(_ONE_BYTE[data[i]])
        except ValueError as exc:
            logging.getLogger(__name__).debug("Invalid telnet byte: %s", exc)
        except BaseException:
            _log_exception(log_fn, *sys.exc_info())
        else:
            if recv_inband:
                reader.feed_data(data[i : i + 1])
            else:
                cmd_received = True
        i += 1
        out_start = i
        feeding_oob = bool(writer.is_oob)

    return cmd_received


class TelnetProtocolBase:
    """Mixin providing properties and helpers shared by server and client protocols."""

    _when_connected: Optional[datetime.datetime] = None
    _last_received: Optional[datetime.datetime] = None
    _transport: Any = None
    _extra: dict[str, Any]

    @property
    def duration(self) -> float:
        """Time elapsed since client connected, in seconds as float."""
        assert self._when_connected is not None
        return (datetime.datetime.now() - self._when_connected).total_seconds()

    @property
    def idle(self) -> float:
        """Time elapsed since data last received, in seconds as float."""
        assert self._last_received is not None
        return (datetime.datetime.now() - self._last_received).total_seconds()

    def __repr__(self) -> str:
        hostport = self.get_extra_info("peername", ["-", "closing"])[:2]
        return f"<Peer {hostport[0]} {hostport[1]}>"

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get optional protocol or transport information."""
        if self._transport:
            default = self._transport.get_extra_info(name, default)
        return self._extra.get(name, default)
