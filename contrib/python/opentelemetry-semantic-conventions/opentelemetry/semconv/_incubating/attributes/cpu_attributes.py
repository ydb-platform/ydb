# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

CPU_LOGICAL_NUMBER: Final = "cpu.logical_number"
"""
The logical CPU number [0..n-1].
"""

CPU_MODE: Final = "cpu.mode"
"""
The mode of the CPU.
"""


class CpuModeValues(Enum):
    USER = "user"
    """User."""
    SYSTEM = "system"
    """System."""
    NICE = "nice"
    """Nice."""
    IDLE = "idle"
    """Idle."""
    IOWAIT = "iowait"
    """IO Wait."""
    INTERRUPT = "interrupt"
    """Interrupt."""
    STEAL = "steal"
    """Steal."""
    KERNEL = "kernel"
    """Deprecated: Replaced by `system`."""
