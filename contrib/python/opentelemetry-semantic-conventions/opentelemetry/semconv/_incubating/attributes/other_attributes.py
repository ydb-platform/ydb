# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

STATE: Final = "state"
"""
Deprecated: Replaced by `db.client.connection.state`.
"""


@deprecated(
    "The attribute state is deprecated - Replaced by `db.client.connection.state`"
)
class StateValues(Enum):
    IDLE = "idle"
    """idle."""
    USED = "used"
    """used."""
