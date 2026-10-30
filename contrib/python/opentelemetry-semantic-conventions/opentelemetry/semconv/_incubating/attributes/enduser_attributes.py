# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

ENDUSER_ID: Final = "enduser.id"
"""
Unique identifier of an end user in the system. It maybe a username, email address, or other identifier.
Note: Unique identifier of an end user in the system.

> [!Warning]
> This field contains sensitive (PII) information.
"""

ENDUSER_PSEUDO_ID: Final = "enduser.pseudo.id"
"""
Pseudonymous identifier of an end user. This identifier should be a random value that is not directly linked or associated with the end user's actual identity.
Note: Pseudonymous identifier of an end user.

> [!Warning]
> This field contains sensitive (linkable PII) information.
"""

ENDUSER_ROLE: Final = "enduser.role"
"""
Deprecated: Use `user.roles` instead.
"""

ENDUSER_SCOPE: Final = "enduser.scope"
"""
Deprecated: Removed, no replacement at this time.
"""
