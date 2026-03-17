#!/usr/bin/env python
"""Constants related to the APPROLE auth method."""

DEFAULT_MOUNT_POINT = "approle"
ALLOWED_TOKEN_TYPES = [
    "service",
    "batch",
    "default",
    "default-service",
    "default-batch",
]
