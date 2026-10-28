# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

SESSION_ID: Final = "session.id"
"""
A unique id to identify a session.
"""

SESSION_PREVIOUS_ID: Final = "session.previous_id"
"""
The previous `session.id` for this user, when known.
"""
