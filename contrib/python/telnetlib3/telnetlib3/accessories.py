"""Accessory functions."""

from __future__ import annotations

# std imports
import shlex
import asyncio
import logging
import importlib
from typing import TYPE_CHECKING, Any, Dict, Union, Mapping, Callable, Optional

#: Custom TRACE log level, below DEBUG (10).
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

if TYPE_CHECKING:  # pragma: no cover
    from .stream_reader import TelnetReader, TelnetReaderUnicode

__all__ = (
    "TRACE",
    "encoding_from_lang",
    "name_unicode",
    "eightbits",
    "hexdump",
    "make_logger",
    "repr_mapping",
    "function_lookup",
    "make_reader_task",
)

PATIENCE_MESSAGES = [
    "Contemplate the virtue of patience",
    "Endure delays with fortitude",
    "To wait calmly requires discipline",
    "Suspend expectations of imminence",
    "The tide hastens for no man",
    "Cultivate a stoic calmness",
    "The tranquil mind eschews impatience",
    "Deliberation is preferable to haste",
]


def get_version() -> str:
    """Return the current version of telnetlib3."""
    return "3.0.0"  # keep in sync with pyproject.toml and docs/conf.py !!


def encoding_from_lang(lang: str) -> Optional[str]:
    """
    Parse encoding from LANG environment value.

    Returns the encoding portion if present, or None if the LANG value
    does not contain an encoding suffix (no '.' separator).

    :param lang: LANG environment value (e.g., 'en_US.UTF-8@misc')
    :returns: Encoding string (e.g., 'UTF-8') or None if no encoding found.

    Example::

        >>> encoding_from_lang('en_US.UTF-8@misc')
        'UTF-8'
        >>> encoding_from_lang('en_IL')
        None
    """
    if "." not in lang:
        return None
    _, encoding = lang.split(".", 1)
    if "@" in encoding:
        encoding, _ = encoding.split("@", 1)
    return encoding


def name_unicode(ucs: str) -> str:
    """Return 7-bit ascii printable of any string."""
    # more or less the same as curses.ascii.unctrl -- but curses
    # module is conditionally excluded from many python distributions!
    bits = ord(ucs)
    if 32 <= bits <= 126:
        # ascii printable as one cell, as-is
        rep = chr(bits)
    elif bits == 127:
        rep = "^?"
    elif bits < 32:
        rep = "^" + chr(((bits & 0x7F) | 0x20) + 0x20)
    else:
        rep = rf"\x{bits:02x}"
    return rep


def eightbits(number: int) -> str:
    """
    Binary representation of ``number`` padded to 8 bits.

    Example::

        >>> eightbits(ord('a'))
        '0b01100001'
    """
    # useful only so far in context of a forwardmask or any bitmask.
    _, value = bin(number).split("b")
    return f"0b{int(value):08d}"


def hexdump(data: bytes, prefix: str = "") -> str:
    """
    Format *data* as ``hexdump -C`` style output.

    Each 16-byte row shows the offset, hex bytes grouped 8+8,
    and printable ASCII on the right::

        00000000  48 65 6c 6c 6f 20 57 6f  72 6c 64 0d 0a         |Hello World..|

    :param data: Raw bytes to format.
    :param prefix: String prepended to every line (e.g. ``">>  "``).
    :rtype: str
    """
    lines: list[str] = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        hex_left = " ".join(f"{b:02x}" for b in chunk[:8])
        hex_right = " ".join(f"{b:02x}" for b in chunk[8:])
        ascii_part = "".join(chr(b) if 0x20 <= b < 0x7F else "." for b in chunk)
        lines.append(f"{prefix}{offset:08x}  {hex_left:<23s}  {hex_right:<23s}  |{ascii_part}|")
    return "\n".join(lines)


_DEFAULT_LOGFMT = " ".join(
    ("%(asctime)s", "%(levelname)s", "%(filename)s:%(lineno)d", "%(message)s")
)


def make_logger(
    name: str, loglevel: str = "info", logfile: Optional[str] = None, logfmt: str = _DEFAULT_LOGFMT
) -> logging.Logger:
    """Create and return simple logger for given arguments."""
    lvl = getattr(logging, loglevel.upper(), None)
    if lvl is None:
        lvl = logging.getLevelName(loglevel.upper())

    _cfg: Dict[str, Any] = {"format": logfmt}
    if logfile:
        _cfg["filename"] = logfile
    logging.basicConfig(**_cfg)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            handler.terminator = "\r\n"
    logging.getLogger().setLevel(lvl)
    logging.getLogger(name).setLevel(lvl)
    return logging.getLogger(name)


def repr_mapping(mapping: Mapping[str, Any]) -> str:
    """Return printable string, 'key=value [key=value ...]' for mapping."""
    return " ".join(f"{key}={shlex.quote(str(value))}" for key, value in mapping.items())


def function_lookup(pymod_path: str) -> Callable[..., Any]:
    """Return callable function target from standard module.function path."""
    module_name, func_name = pymod_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    shell_function: Callable[..., Any] = getattr(module, func_name)
    return shell_function


def make_reader_task(
    reader: "Union[TelnetReader, TelnetReaderUnicode, asyncio.StreamReader]", size: int = 2**12
) -> "asyncio.Task[Any]":
    """Return asyncio task wrapping coroutine of reader.read(size)."""
    return asyncio.ensure_future(reader.read(size))
