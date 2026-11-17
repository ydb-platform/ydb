# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

MESSAGE_COMPRESSED_SIZE: Final = "message.compressed_size"
"""
Deprecated: Deprecated, no replacement at this time.
"""

MESSAGE_ID: Final = "message.id"
"""
Deprecated: Deprecated, no replacement at this time.
"""

MESSAGE_TYPE: Final = "message.type"
"""
Deprecated: Deprecated, no replacement at this time.
"""

MESSAGE_UNCOMPRESSED_SIZE: Final = "message.uncompressed_size"
"""
Deprecated: Deprecated, no replacement at this time.
"""


@deprecated(
    "The attribute message.type is deprecated - Deprecated, no replacement at this time"
)
class MessageTypeValues(Enum):
    SENT = "SENT"
    """sent."""
    RECEIVED = "RECEIVED"
    """received."""
