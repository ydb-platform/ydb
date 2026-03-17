#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from snowflake.connector.cursor import SnowflakeCursor


def put(
    csr: SnowflakeCursor,
    file_path: str,
    stage_path: str,
    from_path: bool,
    sql_options: str | None = "",
    **kwargs,
) -> SnowflakeCursor:
    """Execute PUT <file> <stage> <options> query with given cursor.

    Args:
        csr: Snowflake cursor object.
        file_path: Path to the target file in local system; Or <filename>.<extension> when from_path is False.
        stage_path: Destination path of file on the stage.
        from_path: Whether the target file is fetched with given path, specify file_stream=<IO> if False.
        sql_options: Optional arguments to the PUT command.
        **kwargs: Optional arguments passed to SnowflakeCursor.execute()

    Returns:
        A result class with the results in it. This can either be json, or an arrow result class.
    """
    sql = "put 'file://{file}' @{stage} {sql_options}"
    if from_path:
        kwargs.pop("file_stream", None)
    else:
        # PUT from stream
        file_path = os.path.basename(file_path)
    if kwargs.pop("commented", False):
        sql = "--- test comments\n" + sql
    sql = sql.format(
        file=file_path.replace("\\", "\\\\"), stage=stage_path, sql_options=sql_options
    )
    return csr.execute(sql, **kwargs)
