# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

DISK_IO_DIRECTION: Final = "disk.io.direction"
"""
The disk IO operation direction.
"""


class DiskIoDirectionValues(Enum):
    READ = "read"
    """read."""
    WRITE = "write"
    """write."""
