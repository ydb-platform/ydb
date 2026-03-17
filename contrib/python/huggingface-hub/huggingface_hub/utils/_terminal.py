# Copyright 2025 The HuggingFace Team. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contains utilities to print stuff to the terminal (styling, helpers)."""

import os
import shutil
import sys
from typing import Optional, Union


class StatusLine:
    """Minimal TTY status line for sync progress (stderr, single-line overwrite)."""

    def __init__(self, enabled: bool = True):
        self._active = enabled and sys.stderr.isatty()

    def update(self, msg: str) -> None:
        if not self._active:
            return
        width = shutil.get_terminal_size().columns
        if len(msg) > width - 1:
            msg = msg[: width - 4] + "..."
        sys.stderr.write(f"\r\033[K\033[90m{msg}\033[0m")
        sys.stderr.flush()

    def done(self, msg: str) -> None:
        if not self._active:
            return
        width = shutil.get_terminal_size().columns
        if len(msg) > width - 1:
            msg = msg[: width - 4] + "..."
        sys.stderr.write(f"\r\033[K\033[90m{msg}\033[0m\n")
        sys.stderr.flush()


class ANSI:
    """
    Helper for en.wikipedia.org/wiki/ANSI_escape_code
    """

    _blue = "\u001b[34m"
    _bold = "\u001b[1m"
    _gray = "\u001b[90m"
    _green = "\u001b[32m"
    _red = "\u001b[31m"
    _reset = "\u001b[0m"
    _yellow = "\u001b[33m"

    @classmethod
    def blue(cls, s: str) -> str:
        return cls._format(s, cls._blue)

    @classmethod
    def bold(cls, s: str) -> str:
        return cls._format(s, cls._bold)

    @classmethod
    def gray(cls, s: str) -> str:
        return cls._format(s, cls._gray)

    @classmethod
    def green(cls, s: str) -> str:
        return cls._format(s, cls._green)

    @classmethod
    def red(cls, s: str) -> str:
        return cls._format(s, cls._bold + cls._red)

    @classmethod
    def yellow(cls, s: str) -> str:
        return cls._format(s, cls._yellow)

    @classmethod
    def _format(cls, s: str, code: str) -> str:
        if os.environ.get("NO_COLOR"):
            # See https://no-color.org/
            return s
        return f"{code}{s}{cls._reset}"


def tabulate(
    rows: list[list[Union[str, int]]],
    headers: list[str],
    alignments: Optional[dict[str, str]] = None,
) -> str:
    """
    Inspired by:

    - stackoverflow.com/a/8356620/593036
    - stackoverflow.com/questions/9535954/printing-lists-as-tabular-data
    """
    _ALIGN_MAP = {"left": "<", "right": ">"}
    for row in rows:
        if len(row) < len(headers):
            raise IndexError(f"Row has {len(row)} values but expected {len(headers)} (headers: {headers})")
    col_widths = [max(len(str(x)) for x in col) for col in zip(*rows, headers)]
    col_aligns = [_ALIGN_MAP.get((alignments or {}).get(h, "left"), "<") for h in headers]
    row_format = " ".join(f"{{:{a}{w}}}" for a, w in zip(col_aligns, col_widths))
    lines = []
    lines.append(row_format.format(*headers))
    lines.append(row_format.format(*["-" * w for w in col_widths]))
    for row in rows:
        lines.append(row_format.format(*row))
    return "\n".join(lines)
