"""
Task for Scheduler and AsyncScheduler
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

from typing import Any
from dataclasses import dataclass, field
from collections.abc import Callable


@dataclass(order=True)
class Task:
    time: float
    action: Callable[[], Any] | None = field(compare=False)
