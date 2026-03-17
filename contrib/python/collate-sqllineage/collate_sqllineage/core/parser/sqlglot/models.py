from typing import List, Optional, Union

from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import (
    Column,
    DataFunction,
    Location,
    Path,
    Schema,
    SubQuery,
    Table,
)
from collate_sqllineage.core.parser.sqlglot.utils import is_subquery
from collate_sqllineage.utils.entities import ColumnQualifierTuple
from collate_sqllineage.utils.helpers import escape_identifier_name

IDENTIFIER_KEYWORD = "IDENTIFIER"


class SqlGlotTable(Table):
    @staticmethod
    def _get_table_name(table_exp: exp.Table) -> str:
        """
        Extract table name from Table expression, handling special cases like IDENTIFIER().
        :param table_exp: sqlglot Table expression
        :return: table name string
        """
        # Handle Snowflake IDENTIFIER() function for dynamic table names
        # e.g., CREATE TABLE IDENTIFIER('table_name') AS SELECT ...
        if (
            isinstance(table_exp.this, exp.Anonymous)
            and table_exp.this.name
            and table_exp.this.name.upper() == IDENTIFIER_KEYWORD
            and table_exp.this.expressions
            and len(table_exp.this.expressions) == 1
            and isinstance(table_exp.this.expressions[0], exp.Literal)
        ):
            return str(table_exp.this.expressions[0].this)

        return getattr(table_exp, "name", str(table_exp.this))

    @staticmethod
    def of(table_exp: exp.Table) -> Table:
        """
        Create a Table object from a sqlglot Table expression
        :param table_exp: sqlglot Table expression
        :return: Table object
        """
        table_name = SqlGlotTable._get_table_name(table_exp)
        db = table_exp.db or None
        catalog = table_exp.catalog or None

        parent_parts = []
        if catalog:
            parent_parts.append(escape_identifier_name(catalog))
        if db:
            parent_parts.append(escape_identifier_name(db))

        parent_name = ".".join(parent_parts) if parent_parts else None
        schema = Schema(parent_name) if parent_name is not None else Schema()

        alias = table_exp.alias or None
        kwargs = {"alias": alias} if alias else {}

        return Table(table_name, schema, **kwargs)


class SqlGlotFunction(DataFunction):
    @staticmethod
    def of(function_exp: exp.Func, schema_name: Optional[str] = None) -> DataFunction:
        """
        Create a DataFunction object from a sqlglot Function expression
        :param function_exp: sqlglot Function expression
        :param schema_name: optional schema name
        :return: DataFunction object
        """
        func_name = getattr(function_exp, "name", None) or type(function_exp).__name__

        parent_name = schema_name
        schema = Schema(parent_name) if parent_name is not None else Schema()

        alias = function_exp.alias or None
        kwargs = {"alias": alias} if alias else {}

        return DataFunction(func_name, schema, **kwargs)


class SqlGlotSubQuery(SubQuery):
    @staticmethod
    def of(subquery_exp: Expression, alias: Optional[str]) -> SubQuery:
        """
        Create a SubQuery object from a sqlglot expression
        :param subquery_exp: sqlglot Subquery or Select expression
        :param alias: optional alias name
        :return: SubQuery object
        """
        # Get the SQL representation of the subquery
        sql_text = (
            subquery_exp.sql() if hasattr(subquery_exp, "sql") else str(subquery_exp)
        )
        return SubQuery(subquery_exp, sql_text, alias)


class SqlGlotColumn(Column):
    @staticmethod
    def of(column_exp: Expression, **kwargs) -> Column:  # type: ignore[override]
        """
        Create a Column object from a sqlglot expression
        :param column_exp: sqlglot Column or other expression
        :return: Column object
        """
        if isinstance(column_exp, exp.Column):
            # Get column name and alias
            col_name = (
                column_exp.name if hasattr(column_exp, "name") else str(column_exp.this)
            )
            alias = (
                column_exp.alias
                if hasattr(column_exp, "alias") and column_exp.alias
                else None
            )

            # Get table qualifier
            table_qualifier = None
            if column_exp.table:
                table_qualifier = getattr(
                    column_exp.table, "name", str(column_exp.table)
                )

            if alias:
                # Column has an alias
                source_columns = [ColumnQualifierTuple(col_name, table_qualifier)]
                return Column(alias, source_columns=tuple(source_columns))
            else:
                # Column without alias
                source_columns = [ColumnQualifierTuple(col_name, table_qualifier)]
                return Column(col_name, source_columns=tuple(source_columns))

        elif isinstance(column_exp, exp.Alias):
            # Handle aliased expressions
            alias = column_exp.alias
            source_expression = column_exp.this
            source_columns = SqlGlotColumn._extract_source_columns(source_expression)
            return Column(alias, source_columns=tuple(source_columns))

        elif isinstance(column_exp, exp.Star):
            table_qualifier = None
            if hasattr(column_exp, "table") and column_exp.table:
                table_qualifier = getattr(
                    column_exp.table, "name", str(column_exp.table)
                )
            return Column("*", source_columns=(("*", table_qualifier),))

        elif isinstance(column_exp, exp.Cast):
            # Handle CAST expressions
            # Distinguish between postgres-style :: and ANSI CAST() syntax:
            # - col::type (postgres) has only 2 args: 'this' and 'to'
            # - CAST(col AS type) has 6 args: 'this', 'to', 'format', 'safe', 'action', 'default'
            alias = column_exp.alias if hasattr(column_exp, "alias") else None
            source_columns = SqlGlotColumn._extract_source_columns(column_exp)

            if alias:
                # Has explicit alias, use it
                col_name = alias
            elif len(column_exp.args) == 2 and isinstance(column_exp.this, exp.Column):
                # Postgres-style cast (col::type) - use inner column name
                col_name = column_exp.this.name
            else:
                # ANSI CAST or complex expression - use full SQL representation
                col_name = str(column_exp)

            return Column(col_name, source_columns=tuple(source_columns))

        else:
            source_columns = SqlGlotColumn._extract_source_columns(column_exp)
            col_name = column_exp.alias or str(column_exp)
            return Column(col_name, source_columns=tuple(source_columns))

    @staticmethod
    def _extract_source_columns(expression: Expression) -> List[ColumnQualifierTuple]:
        """
        Extract source columns from an expression
        :param expression: sqlglot expression
        :return: list of ColumnQualifierTuple
        """
        source_columns: List[ColumnQualifierTuple] = []

        if isinstance(expression, exp.Window):
            # Window specs (PARTITION BY, ORDER BY) don't create direct column lineage
            if hasattr(expression, "this") and expression.this:
                source_columns.extend(
                    SqlGlotColumn._extract_source_columns(expression.this)
                )

        elif isinstance(expression, exp.Func):
            for arg in expression.args.values():
                if isinstance(arg, list):
                    for item in arg:
                        if isinstance(item, Expression):
                            source_columns.extend(
                                SqlGlotColumn._extract_source_columns(item)
                            )
                elif isinstance(arg, Expression):
                    source_columns.extend(SqlGlotColumn._extract_source_columns(arg))

        elif isinstance(expression, (exp.Subquery, exp.Select)):
            if is_subquery(expression):
                from collate_sqllineage.runner import LineageRunner

                sql_text = getattr(expression, "sql", lambda: str(expression))()
                try:
                    src_cols = [
                        lineage[0]
                        for lineage in LineageRunner(sql_text).get_column_lineage(
                            exclude_subquery=False
                        )
                    ]
                    source_columns = [
                        ColumnQualifierTuple(
                            src_col.raw_name,
                            src_col.parent.raw_name if src_col.parent else None,
                        )
                        for src_col in src_cols
                    ]
                except Exception:  # noqa: B902
                    pass

        elif isinstance(expression, (exp.Binary, exp.Unary)):
            for child in expression.iter_expressions():
                source_columns.extend(SqlGlotColumn._extract_source_columns(child))

        elif isinstance(expression, exp.Case):
            for child in expression.iter_expressions():
                source_columns.extend(SqlGlotColumn._extract_source_columns(child))

        elif isinstance(expression, exp.Column):
            col_name = getattr(expression, "name", str(expression.this))
            table_qualifier = None
            if expression.table:
                table_qualifier = getattr(
                    expression.table, "name", str(expression.table)
                )
            source_columns.append(ColumnQualifierTuple(col_name, table_qualifier))

        elif isinstance(expression, exp.Star):
            table_qualifier = None
            if hasattr(expression, "table") and expression.table:
                table_qualifier = getattr(
                    expression.table, "name", str(expression.table)
                )
            source_columns.append(ColumnQualifierTuple("*", table_qualifier))

        elif isinstance(expression, exp.Literal):
            pass

        else:
            for col in expression.find_all(exp.Column):
                col_name = getattr(col, "name", str(col.this))
                table_qualifier = None
                if col.table:
                    table_qualifier = getattr(col.table, "name", str(col.table))
                source_columns.append(ColumnQualifierTuple(col_name, table_qualifier))

        return source_columns


class SqlGlotSubQueryLineageHolder(SubQueryLineageHolder):
    """
    SqlGlot-specific SubQuery Lineage Holder that tracks FROM/JOIN tables
    for accurate alias resolution in parent extractors.

    This is needed because SqlGlot's extractor needs to track which tables
    came from FROM/JOIN clauses vs scalar subqueries to prevent alias pollution.
    """

    def __init__(self) -> None:
        super().__init__()
        self._from_join_tables: List[
            Union[DataFunction, Location, Path, SubQuery, Table]
        ] = []


class SqlGlotStatementLineageHolder(SqlGlotSubQueryLineageHolder):
    """
    SqlGlot-specific Statement Lineage Holder that includes FROM/JOIN tracking
    and statement-level features (drop, rename).

    Extends SqlGlotSubQueryLineageHolder with statement-level operations.
    """

    def add_drop(self, value) -> None:
        """Add a table to be dropped"""
        from collate_sqllineage.utils.constant import NodeTag

        self.graph.add_node(value, **{NodeTag.DROP: True})

    def add_rename(self, src: Table, tgt: Table) -> None:
        """Add a table rename operation"""
        from collate_sqllineage.utils.constant import EdgeType

        self.graph.add_edge(src, tgt, type=EdgeType.RENAME)

    @staticmethod
    def of(holder: SubQueryLineageHolder) -> "SqlGlotStatementLineageHolder":
        """Create SqlGlotStatementLineageHolder from a SubQueryLineageHolder"""
        stmt_holder = SqlGlotStatementLineageHolder()
        stmt_holder.graph = holder.graph
        return stmt_holder
