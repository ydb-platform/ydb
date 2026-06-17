from sqlalchemy import Table, and_
from sqlalchemy.sql.selectable import FromClause, Select

from clickhouse_connect.cc_sqlalchemy.sql.clauses import ArrayJoin, LimitByClause, PreWhereClause
from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join as _array_join_fromclause
from clickhouse_connect.driver.binding import quote_identifier

# Non-rendering statement-hint dialect tag. Used only to force distinct
# compiled-statement cache keys when FINAL/SAMPLE/PREWHERE/LIMIT BY are applied.
_CH_MODIFIER_DIALECT = "_ch_modifier"


def full_table(table_name: str, schema: str | None = None) -> str:
    if table_name.startswith("(") or "." in table_name or not schema:
        return quote_identifier(table_name)
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


def format_table(table: Table):
    return full_table(table.name, table.schema)


def _resolve_target(select_stmt: Select, table: FromClause | None, method_name: str) -> FromClause:
    if not isinstance(select_stmt, Select):
        raise TypeError(f"{method_name}() expects a SQLAlchemy Select instance")

    target = table
    if target is None:
        froms = select_stmt.get_final_froms()
        if not froms:
            raise ValueError(f"{method_name}() requires a table to apply the {method_name.upper()} modifier.")
        if len(froms) > 1:
            raise ValueError(f"{method_name}() is ambiguous for statements with multiple FROM clauses. Specify the table explicitly.")
        target = froms[0]

    # FINAL/SAMPLE apply to the underlying table, not to an ArrayJoin wrapper.
    while isinstance(target, ArrayJoin):
        target = target.left

    if not isinstance(target, FromClause):
        raise TypeError("table must be a SQLAlchemy FromClause when provided")

    return target


def _target_cache_key(target: FromClause) -> str:
    if hasattr(target, "fullname"):
        return target.fullname
    return target.name


def final(select_stmt: Select, table: FromClause | None = None) -> Select:
    """Apply the ClickHouse FINAL modifier. For ReplacingMergeTree-family engines."""
    target = _resolve_target(select_stmt, table, "final")
    ch_final = getattr(select_stmt, "_ch_final", set())

    if target in ch_final:
        return select_stmt

    hint_key = _target_cache_key(target)
    new_stmt = select_stmt.with_statement_hint(f"FINAL:{hint_key}", dialect_name=_CH_MODIFIER_DIALECT)
    new_stmt._ch_final = ch_final | {target}
    return new_stmt


def _select_final(self: Select, table: FromClause | None = None) -> Select:
    return final(self, table=table)


def sample(select_stmt: Select, sample_value: str | int | float, table: FromClause | None = None) -> Select:
    """Apply the ClickHouse SAMPLE modifier. sample_value may be a float (fraction), int (row count), or string expression like '1/10 OFFSET 1/2'."""
    target = _resolve_target(select_stmt, table, "sample")

    hint_key = _target_cache_key(target)
    new_stmt = select_stmt.with_statement_hint(f"SAMPLE:{hint_key}:{sample_value}", dialect_name=_CH_MODIFIER_DIALECT)
    ch_sample = dict(getattr(select_stmt, "_ch_sample", {}))
    ch_sample[target] = sample_value
    new_stmt._ch_sample = ch_sample
    return new_stmt


def _select_sample(self: Select, sample_value: str | int | float, table: FromClause | None = None) -> Select:
    return sample(self, sample_value=sample_value, table=table)


def _apply_array_join(select_stmt: Select, cols, alias, is_left: bool) -> Select:
    if not isinstance(select_stmt, Select):
        raise TypeError("array_join() expects a SQLAlchemy Select instance")

    if not cols:
        raise ValueError("array_join() requires at least one array column")

    froms = select_stmt.get_final_froms()
    if not froms:
        raise ValueError("array_join() requires the Select to have a FROM clause to wrap.")
    if len(froms) > 1:
        raise ValueError(
            "array_join() is ambiguous for statements with multiple FROM clauses. "
            "Use the module-level array_join(left, array_column, ...) with select_from() instead."
        )
    target = froms[0]

    columns = list(cols)
    if len(columns) == 1:
        array_column = columns[0]
        alias_arg = alias
    else:
        array_column = columns
        if alias is None:
            alias_arg = None
        elif isinstance(alias, (list, tuple)):
            alias_arg = list(alias)
        else:
            raise ValueError("alias must be a list/tuple matching the number of columns when multiple columns are provided")

    aj = _array_join_fromclause(target, array_column, alias=alias_arg, is_left=is_left)
    return select_stmt.select_from(aj)


def _select_array_join(self: Select, *cols, alias=None) -> Select:
    return _apply_array_join(self, cols, alias, is_left=False)


def _select_left_array_join(self: Select, *cols, alias=None) -> Select:
    return _apply_array_join(self, cols, alias, is_left=True)


def prewhere(select_stmt, whereclause):
    """Apply ClickHouse PREWHERE. Multiple calls compose with AND."""
    if not isinstance(select_stmt, Select):
        raise TypeError("prewhere() expects a SQLAlchemy Select instance")

    existing = getattr(select_stmt, "_ch_prewhere", None)
    combined = and_(existing.whereclause, whereclause) if existing is not None else whereclause

    # Hint key is str(combined) (structural, with bind placeholders) rather
    # than id() so equivalent statements share a compiled-statement cache entry.
    new_stmt = select_stmt.with_statement_hint(f"PREWHERE:{str(combined)}", dialect_name=_CH_MODIFIER_DIALECT)
    new_stmt._ch_prewhere = PreWhereClause(combined)
    return new_stmt


def limit_by(select_stmt, by_clauses, limit, offset=None):
    """Apply ClickHouse LIMIT BY (top-N per group). Renders `LIMIT [offset,] limit BY by_clauses`."""
    if not isinstance(select_stmt, Select):
        raise TypeError("limit_by() expects a SQLAlchemy Select instance")

    by_tuple = tuple(by_clauses)
    if not by_tuple:
        raise ValueError("limit_by() requires at least one by_clause")

    by_key = ",".join(str(c) for c in by_tuple)
    new_stmt = select_stmt.with_statement_hint(f"LIMIT_BY:{limit}:{offset}:{by_key}", dialect_name=_CH_MODIFIER_DIALECT)
    new_stmt._ch_limit_by = LimitByClause(by_tuple, limit, offset)
    return new_stmt


def _select_prewhere(self, whereclause):
    return prewhere(self, whereclause)


def _select_limit_by(self, by_clauses, limit, offset=None):
    return limit_by(self, by_clauses, limit, offset)


Select.sample = _select_sample
Select.final = _select_final
Select.array_join = _select_array_join
Select.left_array_join = _select_left_array_join
Select.prewhere = _select_prewhere
Select.limit_by = _select_limit_by
