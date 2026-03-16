from typing import List, Optional, Union

from sqlglot import exp

from collate_sqllineage.core.models import DataFunction, Path, SubQuery, Table
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotFunction,
    SqlGlotSubQuery,
    SqlGlotSubQueryLineageHolder,
    SqlGlotTable,
)
from collate_sqllineage.utils.helpers import escape_identifier_name


def extract_nested_functions_from_args(func_exp: exp.Func) -> List[DataFunction]:
    """
    Extract nested function calls from a function's arguments.
    :param func_exp: sqlglot Function expression
    :return: List of DataFunction objects found in arguments
    """
    nested_functions: List[DataFunction] = []

    if not hasattr(func_exp, "expressions") or not func_exp.expressions:
        return nested_functions

    for arg in func_exp.expressions:
        if isinstance(arg, exp.Dot):
            if isinstance(arg.expression, (exp.Anonymous, exp.Func)):
                nested_func = arg.expression

                if isinstance(arg.this, exp.Identifier):
                    schema_name = arg.this.name
                    nested_functions.append(
                        SqlGlotFunction.of(nested_func, schema_name)
                    )
                else:
                    nested_functions.append(SqlGlotFunction.of(nested_func))

        elif isinstance(arg, (exp.Anonymous, exp.Func)):
            nested_functions.append(SqlGlotFunction.of(arg))

    return nested_functions


def get_dataset_from_table(
    table_exp: exp.Table,
    holder: SqlGlotSubQueryLineageHolder,
) -> Union[Path, SubQuery, Table, DataFunction]:
    """
    Build a SubQuery, Table, or DataFunction object from a sqlglot Table expression.
    :param table_exp: sqlglot Table expression
    :param holder: SqlGlotSubQueryLineageHolder to check for CTEs
    :return: Path, SubQuery, Table, or DataFunction object
    """
    # Handle T-SQL table hints like (NOLOCK), (UPDLOCK), etc.
    # These appear as Anonymous functions wrapping the table name
    TSQL_TABLE_HINTS = {
        "NOLOCK",
        "UPDLOCK",
        "READUNCOMMITTED",
        "READCOMMITTED",
        "REPEATABLEREAD",
        "SERIALIZABLE",
        "ROWLOCK",
        "PAGLOCK",
        "TABLOCK",
        "TABLOCKX",
        "HOLDLOCK",
    }

    # Initialize is_table_hint flag
    is_table_hint = False
    func_exp = None

    if isinstance(table_exp.this, (exp.Anonymous, exp.Func)):
        func_exp = table_exp.this

        # Check for T-SQL table hints (e.g., NOLOCK, UPDLOCK)
        # These should be treated as regular tables, not functions
        is_table_hint = bool(
            isinstance(func_exp, exp.Anonymous)
            and func_exp.this
            and isinstance(func_exp.this, (exp.Identifier, str))
            and func_exp.expressions
            and all(
                isinstance(e, exp.Column) and e.this.this.upper() in TSQL_TABLE_HINTS
                for e in func_exp.expressions
            )
        )

        # If it's a T-SQL table hint, unwrap the Anonymous node to get the actual table identifier
        if is_table_hint:
            table_exp.set("this", func_exp.this)

        # Only process as function if NOT a table hint
        if not is_table_hint:
            if (
                isinstance(func_exp, exp.Anonymous)
                and func_exp.name
                and func_exp.name.upper() == "TABLE"
                and func_exp.expressions
            ):
                inner_exp = func_exp.expressions[0]

                if isinstance(inner_exp, exp.Dot):
                    current: Optional[exp.Dot] = inner_exp
                    while isinstance(current, exp.Dot):
                        if isinstance(current.expression, (exp.Anonymous, exp.Func)):
                            func_exp = current.expression

                            schema_parts: List[str] = []
                            temp = inner_exp
                            while isinstance(temp, exp.Dot):
                                if isinstance(temp.this, exp.Dot):
                                    temp = temp.this
                                elif isinstance(temp.this, exp.Identifier):
                                    schema_parts.insert(0, temp.this.name)
                                    break

                            if isinstance(inner_exp.this, exp.Dot) and isinstance(
                                inner_exp.this.expression, exp.Identifier
                            ):
                                schema_parts.append(inner_exp.this.expression.name)

                            schema_name = (
                                ".".join(schema_parts) if schema_parts else None
                            )
                            return SqlGlotFunction.of(func_exp, schema_name)

                        if hasattr(current, "expression"):
                            current = current.expression
                        else:
                            current = None
                        if current is None or not isinstance(current, exp.Dot):
                            break
                elif isinstance(inner_exp, (exp.Anonymous, exp.Func)):
                    func_exp = inner_exp

            schema_parts = []
            if table_exp.catalog:
                schema_parts.append(table_exp.catalog)
            if table_exp.db:
                schema_parts.append(table_exp.db)
            schema_name = ".".join(schema_parts) if schema_parts else None

            nested_functions = extract_nested_functions_from_args(func_exp)
            for nested_func in nested_functions:
                holder.add_read(nested_func)

            return SqlGlotFunction.of(func_exp, schema_name)

    # After unwrapping T-SQL hints above, extract the table name normally
    table_name = table_exp.name if hasattr(table_exp, "name") else str(table_exp.this)

    # Check for file-format based tables (e.g., parquet.`path`)
    FILE_FORMATS = {"parquet", "json", "csv", "orc", "avro", "text", "delta"}
    if table_exp.db and table_exp.db.lower() in FILE_FORMATS and table_name:
        # The "table name" is actually a file path
        return Path(escape_identifier_name(table_name))

    # Only match CTE if table is unqualified (db.wtab1 should not match CTE wtab1)
    if not table_exp.db and not table_exp.catalog:
        cte_dict = {s.alias: s for s in holder.cte}
        cte = cte_dict.get(table_name)
        if cte is not None:
            alias = table_exp.alias or table_name
            return SqlGlotSubQuery.of(cte.query, alias)

    if isinstance(table_exp.this, exp.Literal):
        return Path(escape_identifier_name(str(table_exp.this)))

    return SqlGlotTable.of(table_exp)
