from typing import Optional

from sqlalchemy import Table
from sqlalchemy.sql.selectable import FromClause, Select

from clickhouse_connect.driver.binding import quote_identifier


def full_table(table_name: str, schema: Optional[str] = None) -> str:
    if table_name.startswith('(') or '.' in table_name or not schema:
        return quote_identifier(table_name)
    return f'{quote_identifier(schema)}.{quote_identifier(table_name)}'


def format_table(table: Table):
    return full_table(table.name, table.schema)


def final(select_stmt: Select, table: Optional[FromClause] = None) -> Select:
    """
    Apply the ClickHouse FINAL modifier to a select statement.

    Args:
        select_stmt: The SQLAlchemy Select statement to modify.
        table: Optional explicit table/alias to apply FINAL to. When omitted the
            method will use the single FROM element present on the select. A
            ValueError is raised if the statement has no FROMs or more than one
            FROM element and table is not provided.

    Returns:
        A new Select that renders the FINAL modifier for the target table.
    """
    if not isinstance(select_stmt, Select):
        raise TypeError("final() expects a SQLAlchemy Select instance")

    target = table
    if target is None:
        froms = select_stmt.get_final_froms()
        if not froms:
            raise ValueError("final() requires a table to apply the FINAL modifier.")
        if len(froms) > 1:
            raise ValueError("final() is ambiguous for statements with multiple FROM clauses. Specify the table explicitly.")
        target = froms[0]

    if not isinstance(target, FromClause):
        raise TypeError("table must be a SQLAlchemy FromClause when provided")

    return select_stmt.with_hint(target, "FINAL")


def _select_final(self: Select, table: Optional[FromClause] = None) -> Select:
    """
    Select.final() convenience wrapper around the module-level final() helper.
    """
    return final(self, table=table)


# Monkey-patch the Select class to add the .final() convenience method
Select.final = _select_final
