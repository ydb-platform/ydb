#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import uuid
from io import BytesIO
from logging import getLogger
from typing import TYPE_CHECKING

from .errors import BindUploadError, Error

if TYPE_CHECKING:  # pragma: no cover
    from .cursor import SnowflakeCursor

logger = getLogger(__name__)


class BindUploadAgent:
    _STAGE_NAME = "SYSTEMBIND"
    _CREATE_STAGE_STMT = (
        f"create or replace temporary stage {_STAGE_NAME} "
        "file_format=(type=csv field_optionally_enclosed_by='\"')"
    )

    def __init__(
        self,
        cursor: SnowflakeCursor,
        rows: list[bytes],
        stream_buffer_size: int = 1024 * 1024 * 10,
    ):
        """Construct an agent that uploads binding parameters as CSV files to a temporary stage.

        Args:
            cursor: The cursor object.
            rows: Rows of binding parameters in CSV format.
            stream_buffer_size: Size of each file, default to 10MB.
        """
        self.cursor = cursor
        self.rows = rows
        self._stream_buffer_size = stream_buffer_size
        self.stage_path = f"@{self._STAGE_NAME}/{uuid.uuid4().hex}"

    def _create_stage(self):
        self.cursor.execute(self._CREATE_STAGE_STMT)

    def upload(self):
        try:
            self._create_stage()
        except Error as err:
            self.cursor.connection._session_parameters[
                "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"
            ] = 0
            logger.debug("Failed to create stage for binding.")
            raise BindUploadError from err

        row_idx = 0
        while row_idx < len(self.rows):
            f = BytesIO()
            size = 0
            while True:
                f.write(self.rows[row_idx])
                size += len(self.rows[row_idx])
                row_idx += 1
                if row_idx >= len(self.rows) or size >= self._stream_buffer_size:
                    break
            try:
                self.cursor.execute(
                    f"PUT file://{row_idx}.csv {self.stage_path}", file_stream=f
                )
            except Error as err:
                logger.debug("Failed to upload the bindings file to stage.")
                raise BindUploadError from err
            f.close()
