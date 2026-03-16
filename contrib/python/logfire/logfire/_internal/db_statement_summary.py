from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

MAX_QUERY_MESSAGE_LENGTH = 80


def message_from_db_statement(attributes: Mapping[str, Any], message: str | None, span_name: str) -> str | None:
    """Try to construct a useful span message from OTel db statement.

    Returns: A new string to use as span message or None to keep the original message.
    """
    db_statement = attributes.get('db.statement')
    if not isinstance(db_statement, str):
        # covers `None` and anything any other unexpected type
        return None

    if message is not None and message != span_name:
        # if the message attribute is set and is different from the span name
        # return None and thereby use the message attribute
        return None

    db_statement = db_statement.strip()
    if isinstance(message, str):
        db_name = attributes.get('db.name')
        if db_name and isinstance(db_name, str) and message.endswith(db_name):
            operation = message[: -len(db_name) - 1]
        else:
            operation = message
        if operation not in db_statement:
            # operation is not in the db_statement, message is custom so avoid custom behavior
            return None
        if not re.fullmatch(r'\S+', operation):
            # operation is not a simple word, avoid custom behavior
            return None

    return summarize_query(db_statement)


# we don't use \S here since that includes "(" which is not valid in a table name
TABLE_RE = '[\\w."\'`-]+'
SELECT_RE = re.compile(rf'SELECT (.+?) FROM ({TABLE_RE})', flags=re.I | re.DOTALL)
SELECT_CTE_RE = re.compile(rf'WITH (.+?) SELECT (.+?) FROM ({TABLE_RE})', flags=re.I | re.DOTALL)
SELECT_SUBQUERY_RE = re.compile(rf'SELECT (.+?) FROM (\(.+?\)) AS ({TABLE_RE})', flags=re.I | re.DOTALL)
INSERT_RE = re.compile(rf'INSERT INTO ({TABLE_RE}) (\(.+?\)) VALUES (\(.+?\))', flags=re.I)


def summarize_query(db_statement: str) -> str | None:
    """Summarize a database statement, specifically SQL queries.

    Args:
        db_statement: The database statement to summarize.

    Returns: A new string to use as span message or None to keep the original message.

    """
    db_statement = db_statement.strip()
    if len(db_statement) <= MAX_QUERY_MESSAGE_LENGTH:
        return db_statement

    # remove comments
    db_statement = re.sub(r'^[ \t]*--.*', '', db_statement, flags=re.MULTILINE)

    # collapse multiple white spaces to a single space, warning - this can break/change string literals
    # but I think that's okay in this scenario
    db_statement = re.sub(r'\s{2,}', ' ', db_statement).strip()
    if len(db_statement) <= MAX_QUERY_MESSAGE_LENGTH:
        return db_statement

    if select_match := SELECT_SUBQUERY_RE.match(db_statement):
        expr, subquery, table = select_match.groups()
        return select(expr, table, match_end=select_match.end(), db_statement=db_statement, sub_query=subquery)
    elif select_match := SELECT_RE.match(db_statement):
        return select(*select_match.groups(), match_end=select_match.end(), db_statement=db_statement)
    elif select_match := SELECT_CTE_RE.match(db_statement):
        ctes, expr, table = select_match.groups()
        return select(expr, table, match_end=select_match.end(), db_statement=db_statement, ctes=ctes)
    elif insert_match := INSERT_RE.match(db_statement):
        table, columns, values = insert_match.groups()
        return f'INSERT INTO {table} {truncate(columns, 25)} VALUES {truncate(values, 25)}'
    else:
        return fallback(db_statement)


def select(
    expr: str, table: str, *, match_end: int, db_statement: str, ctes: str | None = None, sub_query: str | None = None
) -> str:
    expr = truncate(expr, 20)

    if sub_query:
        summary = f'SELECT {expr} FROM {truncate(sub_query, 25)} AS {truncate(table, 15)}'
    else:
        # 25 for table because this is the best identifier of the query
        summary = f'SELECT {expr} FROM {truncate(table, 25)}'

    if ctes:
        cte_as = re.findall(rf'({TABLE_RE}) AS', ctes, flags=re.I)
        if len(cte_as) == 1:
            summary = f'WITH {truncate(cte_as[0], 10)} AS (…) {summary}'
        else:
            summary = f'WITH …[{len(cte_as)} CTEs] {summary}'

    joins = re.findall(rf'JOIN ({TABLE_RE})', db_statement[match_end:], flags=re.I)
    if len(joins) == 1:
        summary = f'{summary} JOIN {truncate(joins[0], 10)} ON …'
    elif joins:
        summary = f'{summary} …[{len(joins)} JOINs]'

    if re.search('WHERE', db_statement[match_end:], flags=re.I):
        summary = f'{summary} WHERE …'

    if limit := re.search(r'LIMIT (\d+)', db_statement[match_end:], flags=re.I):
        summary = f'{summary} LIMIT {limit.group(1)}'

    return truncate(summary, MAX_QUERY_MESSAGE_LENGTH)


def truncate(s: str, length: int) -> str:
    if len(s) <= length:
        return s
    else:
        half_length = length // 2
        return f'{s[:half_length]}…{s[-half_length:]}'


FALLBACK_HALF = MAX_QUERY_MESSAGE_LENGTH // 2 - 2


def fallback(db_statement: str):
    return f'{db_statement[:FALLBACK_HALF]} … {db_statement[-FALLBACK_HALF:]}'
