"""
MUD telnet protocol encoding and decoding utilities.

Provides encode/decode functions for:
- GMCP (Generic MUD Communication Protocol, option 201)
- MSDP (MUD Server Data Protocol, option 69)
- MSSP (MUD Server Status Protocol, option 70)
- ZMP (Zenith MUD Protocol, option 93)
- ATCP (Achaea Telnet Client Protocol, option 200)
- AARDWOLF (Aardwolf protocol, option 102)

All encode functions return the payload bytes only (the content between
``IAC SB <option>`` and ``IAC SE``). The caller is responsible for
framing with subnegotiation markers.
"""

from __future__ import annotations

# std imports
import json
from typing import Any

# local
from .telopt import (
    MSDP_VAL,
    MSDP_VAR,
    MSSP_VAL,
    MSSP_VAR,
    MSDP_ARRAY_OPEN,
    MSDP_TABLE_OPEN,
    MSDP_ARRAY_CLOSE,
    MSDP_TABLE_CLOSE,
)

__all__ = (
    "gmcp_encode",
    "gmcp_decode",
    "msdp_encode",
    "msdp_decode",
    "mssp_encode",
    "mssp_decode",
    "MsdpParser",
    "zmp_decode",
    "atcp_decode",
    "aardwolf_decode",
)


def _decode_best_effort(buf: bytes, encoding: str = "utf-8") -> str:
    """
    Decode bytes trying *encoding* first, falling back to latin-1.

    :param buf: Raw bytes to decode.
    :param encoding: Primary encoding to attempt.
    :returns: Decoded string.
    """
    try:
        return buf.decode(encoding)
    except (UnicodeDecodeError, LookupError):
        return buf.decode("latin-1")


def gmcp_encode(package: str, data: Any = None) -> bytes:
    """
    Encode a GMCP message.

    :param package: GMCP package name (e.g., "Char.Vitals")
    :param data: Optional data to encode as JSON
    :returns: Encoded GMCP payload bytes
    """
    if data is None:
        return package.encode("utf-8")
    return package.encode("utf-8") + b" " + json.dumps(data, separators=(",", ":")).encode("utf-8")


def gmcp_decode(buf: bytes, encoding: str = "utf-8") -> tuple[str, Any]:
    """
    Decode a GMCP payload.

    :param buf: GMCP payload bytes
    :param encoding: Character encoding to try first, falls back to latin-1.
    :returns: Tuple of (package, data), where data is None if no JSON present
    :raises ValueError: If JSON is malformed
    """
    parts = buf.split(b" ", 1)
    if len(parts) == 1:
        return (_decode_best_effort(buf, encoding), None)

    package = _decode_best_effort(parts[0], encoding)
    text = _decode_best_effort(parts[1], encoding).strip()
    if not text:
        return (package, None)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in GMCP payload: {exc}") from exc
    return (package, data)


def msdp_encode(variables: dict[str, Any]) -> bytes:
    """
    Encode variables to MSDP wire format.

    :param variables: Dictionary of variable names to values
    :returns: Encoded MSDP payload bytes
    """

    def encode_value(value: Any) -> bytes:
        """Encode a single MSDP value."""
        if isinstance(value, dict):
            result = MSDP_TABLE_OPEN
            for key, val in value.items():
                result += MSDP_VAR + key.encode("utf-8") + MSDP_VAL + encode_value(val)
            result += MSDP_TABLE_CLOSE
            return result
        if isinstance(value, list):
            result = MSDP_ARRAY_OPEN
            for item in value:
                result += MSDP_VAL + encode_value(item)
            result += MSDP_ARRAY_CLOSE
            return result
        return str(value).encode("utf-8")

    result = b""
    for key, value in variables.items():
        result += MSDP_VAR + key.encode("utf-8") + MSDP_VAL + encode_value(value)
    return result


class MsdpParser:
    """State machine for parsing MSDP wire bytes."""

    _DELIMITERS = (MSDP_VAR, MSDP_VAL, MSDP_TABLE_CLOSE, MSDP_ARRAY_CLOSE)

    def __init__(self, buf: bytes, encoding: str = "utf-8") -> None:
        """Initialize parser with raw MSDP buffer."""
        self.buf = buf
        self.idx = 0
        self.encoding = encoding

    def _read_string(self) -> str:
        start = self.idx
        while (
            self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] not in self._DELIMITERS
        ):
            self.idx += 1
        return _decode_best_effort(self.buf[start : self.idx], self.encoding)

    def _read_key(self) -> str:
        start = self.idx
        while self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] not in (
            MSDP_VAL,
            MSDP_VAR,
        ):
            self.idx += 1
        return _decode_best_effort(self.buf[start : self.idx], self.encoding)

    def _parse_table(self) -> dict[str, Any]:
        table: dict[str, Any] = {}
        while self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] != MSDP_TABLE_CLOSE:
            if self.buf[self.idx : self.idx + 1] == MSDP_VAR:
                self.idx += 1
                key = self._read_key()
                if self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] == MSDP_VAL:
                    self.idx += 1
                table[key] = self.parse_value()
        if self.idx < len(self.buf):
            self.idx += 1
        return table

    def _parse_array(self) -> list[Any]:
        array: list[Any] = []
        while self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] != MSDP_ARRAY_CLOSE:
            if self.buf[self.idx : self.idx + 1] == MSDP_VAL:
                self.idx += 1
            array.append(self.parse_value())
        if self.idx < len(self.buf):
            self.idx += 1
        return array

    def parse_value(self) -> Any:
        """Parse a single MSDP value at current position."""
        if self.idx >= len(self.buf):
            return ""
        marker = self.buf[self.idx : self.idx + 1]
        self.idx += 1
        if marker == MSDP_TABLE_OPEN:
            return self._parse_table()
        if marker == MSDP_ARRAY_OPEN:
            return self._parse_array()
        self.idx -= 1
        return self._read_string()

    def parse(self) -> dict[str, Any]:
        """Parse the full MSDP buffer into a dict."""
        result: dict[str, Any] = {}
        while self.idx < len(self.buf):
            if self.buf[self.idx : self.idx + 1] == MSDP_VAR:
                self.idx += 1
                key = self._read_key()
                if self.idx < len(self.buf) and self.buf[self.idx : self.idx + 1] == MSDP_VAL:
                    self.idx += 1
                    result[key] = self.parse_value()
            else:
                self.idx += 1
        return result


def msdp_decode(buf: bytes, encoding: str = "utf-8") -> dict[str, Any]:
    """
    Decode MSDP wire bytes to dictionary.

    :param buf: MSDP payload bytes
    :param encoding: Character encoding to try first, falls back to latin-1.
    :returns: Dictionary of variable names to values
    """
    return MsdpParser(buf, encoding=encoding).parse()


def mssp_encode(variables: dict[str, str | list[str]]) -> bytes:
    """
    Encode variables to MSSP wire format.

    :param variables: Dictionary of variable names to string values or lists
    :returns: Encoded MSSP payload bytes
    """
    result = b""
    for key, value in variables.items():
        result += MSSP_VAR + key.encode("utf-8")
        if isinstance(value, list):
            for item in value:
                result += MSSP_VAL + item.encode("utf-8")
        else:
            result += MSSP_VAL + value.encode("utf-8")
    return result


def mssp_decode(buf: bytes, encoding: str = "utf-8") -> dict[str, str | list[str]]:
    """
    Decode MSSP wire bytes to dictionary.

    :param buf: MSSP payload bytes
    :param encoding: Character encoding to try first, falls back to latin-1.
    :returns: Dictionary with str values for single entries, list[str] for multiple
    """
    result: dict[str, str | list[str]] = {}
    idx = 0
    current_var: str | None = None

    while idx < len(buf):
        if buf[idx : idx + 1] == MSSP_VAR:
            idx += 1
            var_start = idx
            while idx < len(buf) and buf[idx : idx + 1] not in (MSSP_VAL, MSSP_VAR):
                idx += 1
            current_var = _decode_best_effort(buf[var_start:idx], encoding)
        elif buf[idx : idx + 1] == MSSP_VAL:
            idx += 1
            val_start = idx
            while idx < len(buf) and buf[idx : idx + 1] not in (MSSP_VAL, MSSP_VAR):
                idx += 1
            value = _decode_best_effort(buf[val_start:idx], encoding)

            if current_var is not None:
                if current_var in result:
                    existing = result[current_var]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        result[current_var] = [existing, value]
                else:
                    result[current_var] = value
        else:
            idx += 1

    return result


def zmp_decode(buf: bytes, encoding: str = "utf-8") -> list[str]:
    """
    Decode ZMP payload to list of NUL-delimited strings.

    The first element is the command name, the rest are arguments.

    :param buf: ZMP payload bytes (NUL-delimited).
    :param encoding: Character encoding to try first, falls back to latin-1.
    :returns: List of strings ``[command, arg1, arg2, ...]``.
    """
    if not buf:
        return []
    # Split on NUL bytes and strip trailing empty string from final NUL.
    parts = buf.split(b"\x00")
    if parts and parts[-1] == b"":
        parts = parts[:-1]
    return [_decode_best_effort(p, encoding) for p in parts]


def atcp_decode(buf: bytes, encoding: str = "utf-8") -> tuple[str, str]:
    """
    Decode ATCP payload to ``(package, value)`` tuple.

    Format is ``package.name value`` separated by the first space.
    If no space is present, *value* is an empty string.

    :param buf: ATCP payload bytes.
    :param encoding: Character encoding to try first, falls back to latin-1.
    :returns: Tuple of ``(package, value)``.
    """
    parts = buf.split(b" ", 1)
    package = _decode_best_effort(parts[0], encoding)
    value = _decode_best_effort(parts[1], encoding) if len(parts) > 1 else ""
    return (package, value)


# Aardwolf channel byte meanings (server -> client).
_AARDWOLF_CHANNELS: dict[int, str] = {
    100: "status",
    101: "tick",
    102: "affect",
    103: "group",
    104: "skill",
    105: "quest",
    106: "spell",
    107: "stat",
    108: "message",
}


def aardwolf_decode(buf: bytes) -> dict[str, Any]:
    """
    Decode Aardwolf protocol payload.

    :param buf: Aardwolf payload bytes (typically 1-2 bytes).
    :returns: Dict with ``channel``, ``channel_byte``, ``data_byte``,
        and ``data_bytes`` (for longer payloads).
    """
    if not buf:
        return {"channel": "unknown", "channel_byte": 0, "data_bytes": b""}
    channel_byte = buf[0]
    channel_name = _AARDWOLF_CHANNELS.get(channel_byte, f"0x{channel_byte:02x}")
    result: dict[str, Any] = {"channel": channel_name, "channel_byte": channel_byte}
    if len(buf) == 2:
        result["data_byte"] = buf[1]
    if len(buf) > 1:
        result["data_bytes"] = buf[1:]
    return result
