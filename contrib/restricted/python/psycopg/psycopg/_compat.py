"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import sys
from typing import Any, Iterator

if sys.version_info >= (3, 11):
    from typing import LiteralString, Self
else:
    from typing_extensions import LiteralString, Self

if sys.version_info >= (3, 12):
    _asyncio_run_snippet = (
        "running 'asyncio.run(...,"
        " loop_factory=asyncio.SelectorEventLoop(selectors.SelectSelector()))'"
    )

else:
    _asyncio_run_snippet = (
        "setting 'asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())'"
    )

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

if sys.version_info >= (3, 14):
    from string.templatelib import Interpolation, Template
else:
    from dataclasses import dataclass

    class Template:
        strings: tuple[str]
        interpolations: tuple[Interpolation]

        def __new__(cls, *args: str | Interpolation) -> Self:
            return cls()

        def __iter__(self) -> Iterator[str | Interpolation]:
            return
            yield

    @dataclass
    class Interpolation:
        value: Any
        expression: str
        conversion: str | None
        format_spec: str


__all__ = [
    "Interpolation",
    "LiteralString",
    "Self",
    "Template",
    "TypeVar",
]
