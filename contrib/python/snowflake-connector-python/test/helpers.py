#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import Pattern, Sequence
from unittest.mock import Mock

from snowflake.connector.compat import OK


def create_mock_response(status_code: int) -> Mock:
    """Create a Mock "Response" with a given status code. See `test_result_batch.py` for examples.
    Args:
        status_code: the status code of the response.
    Returns:
        A Mock object that can be used as a Mock Response in tests.
    """
    mock_resp = Mock()
    mock_resp.status_code = status_code
    mock_resp.raw = "success" if status_code == OK else "fail"
    return mock_resp


def verify_log_tuple(
    module: str,
    level: int,
    message: str | Pattern,
    log_tuples: Sequence[tuple[str, int, str]],
):
    """Convenience function to be able to search for regex patterns in log messages.

    Designed to search caplog.record_tuples.

    Notes:
        - module could be extended to take a pattern too
    """
    for _module, _level, _message in log_tuples:
        if _module == module and _level == level:
            if _message == message or (
                isinstance(message, Pattern) and message.search(_message)
            ):
                return True
    return False
