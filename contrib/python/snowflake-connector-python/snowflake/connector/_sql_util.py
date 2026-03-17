#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import re

from .constants import FileTransferType

COMMENT_START_SQL_RE = re.compile(
    r"""
                                  ^\s*(?:
                                      /\*[\w\W]*?\*/
                                  )""",
    re.VERBOSE,
)

PUT_SQL_RE = re.compile(r"^\s*put", flags=re.IGNORECASE)
GET_SQL_RE = re.compile(r"^\s*get", flags=re.IGNORECASE)


def remove_starting_comments(sql: str) -> str:
    """Remove all comments from the start of a SQL statement."""
    commentless_sql = sql
    while True:
        start_comment = COMMENT_START_SQL_RE.match(commentless_sql)
        if start_comment is None:
            break
        commentless_sql = commentless_sql[start_comment.end() :]
    return commentless_sql


def get_file_transfer_type(sql: str) -> FileTransferType | None:
    """Decide whether a SQL is a file transfer and return its type.

    None is returned if the SQL isn't a file transfer so that this function can be
    used in an if-statement.
    """
    commentless_sql = remove_starting_comments(sql)
    if PUT_SQL_RE.match(commentless_sql):
        return FileTransferType.PUT
    elif GET_SQL_RE.match(commentless_sql):
        return FileTransferType.GET


def is_put_statement(sql: str) -> bool:
    return get_file_transfer_type(sql) == FileTransferType.PUT


def is_get_statement(sql: str) -> bool:
    return get_file_transfer_type(sql) == FileTransferType.GET
