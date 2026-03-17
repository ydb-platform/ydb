#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
import random
import re
import string
from io import StringIO
from typing import Sequence

COMMENT_PATTERN_RE = re.compile(r"^\s*\-\-")
EMPTY_LINE_RE = re.compile(r"^\s*$")

_logger = logging.getLogger(__name__)


class SQLDelimiter:
    """Class that wraps a SQL delimiter string.

    Since split_statements is a generator this mutable object will allow it change while executing.
    """

    def __str__(self):
        return self.sql_delimiter

    def __init__(self, sql_delimiter: str = ";"):
        """Initializes SQLDelimiter with a string."""
        self.sql_delimiter = sql_delimiter


def split_statements(
    buf: StringIO,
    remove_comments: bool = False,
    delimiter: SQLDelimiter | None = None,
):
    """Splits a stream into SQL statements (ends with a semicolon) or commands (!...).

    Args:
        buf: Unicode data stream.
        remove_comments: Whether or not to remove all comments (Default value = False).
        delimiter: The delimiter string that separates SQL commands from each other.

    Yields:
        A SQL statement or a command.
    """
    if delimiter is None:
        delimiter = SQLDelimiter()  # Use default delimiter if none was given.
    in_quote = False
    ch_quote = None
    in_comment = False
    in_double_dollars = False
    previous_delimiter = None

    line = buf.readline()
    if isinstance(line, bytes):
        raise TypeError("Input data must not be binary type.")

    statement = []
    while line != "":
        col = 0
        col0 = 0
        len_line = len(line)
        sql_delimiter = delimiter.sql_delimiter
        if not previous_delimiter or sql_delimiter != previous_delimiter:
            # Only (re)compile new Regexes if they should be
            escaped_delim = re.escape(sql_delimiter)
            # Special characters possible in the sql delimiter are '_', '/' and ';'. If a delimiter does not end, or
            # start with a special character then look for word separation with \b regex.
            if re.match(r"\w", sql_delimiter[0]):
                RE_START = re.compile(rf"^[^\w$]?{escaped_delim}")
            else:
                RE_START = re.compile(rf"^.?{escaped_delim}")
            if re.match(r"\w", sql_delimiter[-1]):
                RE_END = re.compile(rf"{escaped_delim}[^\w$]?$")
            else:
                RE_END = re.compile(rf"{escaped_delim}.?$")
            previous_delimiter = sql_delimiter
        while True:
            if col >= len_line:
                if col0 < col:
                    if not in_comment and not in_quote and not in_double_dollars:
                        statement.append((line[col0:col], True))
                        if len(statement) == 1 and statement[0][0] == "":
                            statement = []
                        break
                    elif not in_comment and (in_quote or in_double_dollars):
                        statement.append((line[col0:col], True))
                    elif not remove_comments:
                        statement.append((line[col0:col], False))
                break
            elif in_comment:
                if line[col:].startswith("*/"):
                    in_comment = False
                    if not remove_comments:
                        statement.append((line[col0 : col + 2], False))
                    col += 2
                    col0 = col
                else:
                    col += 1
            elif in_double_dollars:
                if line[col:].startswith("$$"):
                    in_double_dollars = False
                    statement.append((line[col0 : col + 2], False))
                    col += 2
                    col0 = col
                else:
                    col += 1
            elif in_quote:
                if (
                    line[col] == "\\"
                    and col < len_line - 1
                    and line[col + 1] in (ch_quote, "\\")
                ):
                    col += 2
                elif line[col] == ch_quote:
                    if (
                        col < len_line - 1
                        and line[col + 1] != ch_quote
                        or col == len_line - 1
                    ):
                        # exits quote
                        in_quote = False
                        statement.append((line[col0 : col + 1], True))
                        col += 1
                        col0 = col
                    else:
                        # escaped quote and still in quote
                        col += 2
                else:
                    col += 1
            else:
                if line[col] in ("'", '"'):
                    in_quote = True
                    ch_quote = line[col]
                    col += 1
                elif line[col] in (" ", "\t"):
                    statement.append((line[col0 : col + 1], True))
                    col += 1
                    col0 = col
                elif line[col:].startswith("--"):
                    statement.append((line[col0:col], True))
                    if not remove_comments:
                        # keep the comment
                        statement.append((line[col:], False))
                    col = len_line + 1
                    col0 = col
                elif line[col:].startswith("/*") and not line[col0:].startswith(
                    "file://"
                ):
                    if not remove_comments:
                        statement.append((line[col0 : col + 2], False))
                    else:
                        statement.append((line[col0:col], False))
                    col += 2
                    col0 = col
                    in_comment = True
                elif line[col:].startswith("$$"):
                    statement.append((line[col0 : col + 2], True))
                    col += 2
                    col0 = col
                    in_double_dollars = True
                elif (
                    RE_START.match(line[col - 1 : col + len(sql_delimiter)])
                    if col > 0
                    else (RE_START.match(line[col : col + len(sql_delimiter)]))
                ) and (RE_END.match(line[col : col + len(sql_delimiter) + 1])):
                    statement.append((line[col0:col] + ";", True))
                    col += len(sql_delimiter)
                    try:
                        if line[col] == ">":
                            col += 1
                            statement[-1] = (statement[-1][0] + ">", statement[-1][1])
                    except IndexError:
                        pass
                    if COMMENT_PATTERN_RE.match(line[col:]) or EMPTY_LINE_RE.match(
                        line[col:]
                    ):
                        if not remove_comments:
                            # keep the comment
                            statement.append((line[col:], False))
                        col = len_line
                    while col < len_line and line[col] in (" ", "\t"):
                        col += 1
                    yield _concatenate_statements(statement)
                    col0 = col
                    statement = []
                elif col == 0 and line[col] == "!":  # command
                    if len(statement) > 0:
                        yield _concatenate_statements(statement)
                        statement = []
                    yield (
                        line.strip()[: -len(sql_delimiter)]
                        if line.strip().endswith(sql_delimiter)
                        else line.strip()
                    ).strip(), False
                    break
                else:
                    col += 1
        line = buf.readline()

    if len(statement) > 0:
        yield _concatenate_statements(statement)


def _concatenate_statements(statement_list):
    """Concatenate statements.

    Each statement should be a tuple of statement and is_put_or_get.

    The is_put_or_get is set to True if the statement is PUT or GET otherwise False for valid statement.
    None is set if the statement is empty or comment only.

    Args:
        statement_list: List of statement parts.

    Returns:
        Tuple of statements and whether they are PUT or GET.
    """
    valid_statement_list = []
    is_put_or_get = None
    for text, is_statement in statement_list:
        valid_statement_list.append(text)
        if is_put_or_get is None and is_statement and len(text.strip()) >= 3:
            is_put_or_get = text[:3].upper() in ("PUT", "GET")
    return "".join(valid_statement_list).strip(), is_put_or_get


def construct_hostname(region, account):
    """Constructs hostname from region and account."""
    if region == "us-west-2":
        region = ""
    if region:
        if account.find(".") > 0:
            account = account[0 : account.find(".")]
        host = f"{account}.{region}.snowflakecomputing.com"
    else:
        host = f"{account}.snowflakecomputing.com"
    return host


def parse_account(account):
    url_parts = account.split(".")
    # if this condition is true, then we have some extra
    # stuff in the account field.
    if len(url_parts) > 1:
        if url_parts[1] == "global":
            # remove external ID from account
            parsed_account = url_parts[0][0 : url_parts[0].rfind("-")]
        else:
            # remove region subdomain
            parsed_account = url_parts[0]
    else:
        parsed_account = account

    return parsed_account


def random_string(
    length: int = 10,
    prefix: str = "",
    suffix: str = "",
    choices: Sequence[str] = string.ascii_lowercase,
) -> str:
    """Our convenience function to generate random string for object names.

    Args:
        length: How many random characters to choose from choices.
        prefix: Prefix to add to random string generated.
        suffix: Suffix to add to random string generated.
        choices: A generator of things to choose from.
    """
    random_part = "".join([random.choice(choices) for _ in range(length)])
    return "".join([prefix, random_part, suffix])
