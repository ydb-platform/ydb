from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    from .monitor import Monitor

current_monitor: ContextVar[Monitor] = ContextVar("current_monitor")
current_stdout: ContextVar[TextIO] = ContextVar("current_stdout")
command_done: ContextVar[asyncio.Event] = ContextVar("command_done")
