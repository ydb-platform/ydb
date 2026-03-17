#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

# This file houses functions and constants shared by both integration and unit tests
import os

CLOUD_PROVIDERS = {"aws", "azure", "gcp"}
EXTERNAL_SKIP_TAGS = {"internal"}
INTERNAL_SKIP_TAGS = {"external"}
RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return RUNNING_ON_GH
