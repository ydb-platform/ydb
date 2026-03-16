#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import pytest

from snowflake.connector.telemetry_oob import TelemetryService


@pytest.fixture(autouse=True, scope="session")
def disable_oob_telemetry():
    oob_telemetry_service = TelemetryService.get_instance()
    original_state = oob_telemetry_service.enabled
    oob_telemetry_service.disable()
    yield None
    if original_state:
        oob_telemetry_service.enable()
