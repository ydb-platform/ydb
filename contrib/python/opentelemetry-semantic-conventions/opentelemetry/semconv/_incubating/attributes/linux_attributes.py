# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

LINUX_MEMORY_SLAB_STATE: Final = "linux.memory.slab.state"
"""
Deprecated: Replaced by `system.memory.linux.slab.state`.
"""


@deprecated(
    "The attribute linux.memory.slab.state is deprecated - Replaced by `system.memory.linux.slab.state`"
)
class LinuxMemorySlabStateValues(Enum):
    RECLAIMABLE = "reclaimable"
    """reclaimable."""
    UNRECLAIMABLE = "unreclaimable"
    """unreclaimable."""
