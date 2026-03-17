"""Shared type aliases for telnetlib3 public API."""

from __future__ import annotations

# std imports
from typing import Any, Dict, Tuple, Union, Literal, Callable, Coroutine

# local
from .stream_reader import TelnetReader, TelnetReaderUnicode
from .stream_writer import TelnetWriter, TelnetWriterUnicode

# Encoding parameter: str for Unicode mode, False for binary mode.
Encoding = Union[str, Literal[False]]

# Shell callback: async def shell(reader, writer) -> None.
ShellCallback = Callable[
    [Union[TelnetReader, TelnetReaderUnicode], Union[TelnetWriter, TelnetWriterUnicode]],
    Coroutine[Any, Any, None],
]

# Reader/writer union types.
ReaderType = Union[TelnetReader, TelnetReaderUnicode]
WriterType = Union[TelnetWriter, TelnetWriterUnicode]

# Environment mapping.
EnvironMapping = Dict[str, str]

# NAWS window dimensions (cols, rows).
WindowSize = Tuple[int, int]

# Terminal speed (rx, tx).
TerminalSpeed = Tuple[int, int]
