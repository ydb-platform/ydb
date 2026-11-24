# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

USER_EMAIL: Final = "user.email"
"""
User email address.
"""

USER_FULL_NAME: Final = "user.full_name"
"""
User's full name.
"""

USER_HASH: Final = "user.hash"
"""
Unique user hash to correlate information for a user in anonymized form.
Note: Useful if `user.id` or `user.name` contain confidential information and cannot be used.
"""

USER_ID: Final = "user.id"
"""
Unique identifier of the user.
"""

USER_NAME: Final = "user.name"
"""
Short name or login/username of the user.
"""

USER_ROLES: Final = "user.roles"
"""
Array of user roles at the time of the event.
"""
