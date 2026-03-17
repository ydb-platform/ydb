#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from unittest.mock import MagicMock


def test_bind_upload_agent_uploading_multiple_files():
    from snowflake.connector.bind_upload_agent import BindUploadAgent

    csr = MagicMock(auto_spec=True)
    rows = [bytes(10)] * 10
    agent = BindUploadAgent(csr, rows, stream_buffer_size=10)
    agent.upload()
    assert csr.execute.call_count == 11  # 1 for stage creation + 10 files


def test_bind_upload_agent_row_size_exceed_buffer_size():
    from snowflake.connector.bind_upload_agent import BindUploadAgent

    csr = MagicMock(auto_spec=True)
    rows = [bytes(15)] * 10
    agent = BindUploadAgent(csr, rows, stream_buffer_size=10)
    agent.upload()
    assert csr.execute.call_count == 11  # 1 for stage creation + 10 files
