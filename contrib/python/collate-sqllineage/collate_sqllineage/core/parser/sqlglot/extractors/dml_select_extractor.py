from typing import List, Tuple, Union

from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.core.models import (
    AnalyzerContext,
    Column,
    DataFunction,
    Location,
    Path,
    SubQuery,
    Table,
)
from collate_sqllineage.core.parser import SourceHandlerMixin
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.holder_utils import get_dataset_from_table
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotColumn,
    SqlGlotFunction,
    SqlGlotSubQuery,
    SqlGlotSubQueryLineageHolder,
    SqlGlotTable,
)
from collate_sqllineage.exceptions import SQLLineageException
from collate_sqllineage.utils.constant import EdgeType


class DmlSelectExtractor(LineageHolderExtractor, SourceHandlerMixin):
    """
    DML Select queries lineage extractor for sqlglot
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression"]

    def __init__(self, dialect: str):
        super().__init__(dialect)
        self.columns: List[Column] = []
        self.tables: List[Union[DataFunction, Location, Path, SubQuery, Table]] = []
        self.union_barriers: List[Tuple[int, int]] = []
        self.should_link_columns = False

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
        link_columns: bool = True,
        preserve_state: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for a given SELECT statement.
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return 'SqlGlotSubQueryLineageHolder' object
        """
        saved_columns = self.columns
        saved_tables = self.tables
        saved_union_barriers = self.union_barriers
        saved_should_link = self.should_link_columns

        # Preserve shared column/table state when traversing set expressions (UNION/INTERSECT/EXCEPT)
        # so column lineage can be linked once at the top-level.
        if not preserve_state:
            self.columns = []
            self.tables = []
            self.union_barriers = []
            self.should_link_columns = False

        try:
            holder = self._init_holder(context)
            subqueries = [SqlGlotSubQuery.of(statement, None)] if is_sub_query else []

            # Extract target table from SELECT INTO clause (Postgres, Redshift, T-SQL)
            if statement.args.get("into"):
                into_exp = statement.args["into"]
                if into_exp and into_exp.this:
                    target_table = SqlGlotTable.of(into_exp.this)
                    holder.add_write(target_table)

            if isinstance(statement, exp.Table):
                self._extract_tables_from_expression(statement, holder, subqueries)
                if statement.args.get("joins"):
                    for join in statement.args["joins"]:
                        join_this = join.this if join else None
                        if join_this is not None:
                            self._extract_tables_from_expression(
                                join_this, holder, subqueries
                            )
                return holder

            if isinstance(statement, exp.Subquery):
                if statement.this:
                    self._extract_tables_from_expression(
                        statement.this, holder, subqueries
                    )

            if hasattr(statement, "ctes") and statement.ctes:
                for cte in statement.ctes:
                    cte_alias = cte.alias or None
                    cte_query = cte.this if hasattr(cte, "this") else cte
                    if cte_alias and cte_query:
                        cte_subquery = SqlGlotSubQuery.of(cte_query, cte_alias)
                        holder.add_cte(cte_subquery)
                        subqueries.append(cte_subquery)

            if statement.args.get("expressions"):
                for select_exp in statement.args["expressions"]:
                    self._extract_column(select_exp)
                    self._extract_subqueries_from_expression(select_exp, subqueries)
                    # Handle special functions that perform DML operations
                    self._extract_dml_functions(select_exp, holder)

            if holder.write:
                self.should_link_columns = True

            if statement.args.get("from"):
                from_exp = statement.args["from"]
                from_this = from_exp.this if from_exp else None
                if from_this is not None:
                    self._extract_tables_from_expression(from_this, holder, subqueries)

            if statement.args.get("joins"):
                for join in statement.args["joins"]:
                    join_this = join.this if join else None
                    if join_this is not None:
                        self._extract_tables_from_expression(
                            join_this, holder, subqueries
                        )

            if statement.args.get("where"):
                where_exp = statement.args["where"]
                self._extract_subqueries_from_expression(where_exp, subqueries)

            elif isinstance(statement, (exp.Union, exp.Intersect, exp.Except)):
                # Extract each branch without linking columns yet, then link once using barriers.
                if statement.left:
                    holder |= self.extract(
                        statement.left,
                        AnalyzerContext(prev_cte=holder.cte),
                        link_columns=False,
                        preserve_state=True,
                    )
                    self.union_barriers.append((len(self.columns), len(self.tables)))

                if statement.right:
                    holder |= self.extract(
                        statement.right,
                        AnalyzerContext(prev_cte=holder.cte),
                        link_columns=False,
                        preserve_state=True,
                    )

            seen_subqueries = set()
            unique_subqueries = []
            for sq in subqueries:
                sq_key = id(sq.query)
                if sq_key not in seen_subqueries:
                    seen_subqueries.add(sq_key)
                    unique_subqueries.append(sq)

            for sq in unique_subqueries:
                query_to_extract = sq.query
                if isinstance(query_to_extract, exp.Subquery) and hasattr(
                    query_to_extract, "this"
                ):
                    query_to_extract = query_to_extract.this
                sq_holder = self.extract(
                    query_to_extract, AnalyzerContext(sq, holder.cte)
                )
                # Accumulate FROM/JOIN tables only from CTEs and FROM clause subqueries,
                # excluding scalar subqueries to prevent alias resolution pollution
                is_cte_or_from_join = sq in holder.cte or sq in self.tables
                if is_cte_or_from_join:
                    holder._from_join_tables.extend(sq_holder._from_join_tables)
                holder |= sq_holder

            for tbl in self.tables:
                holder.add_read(tbl)

            # Track FROM/JOIN clause tables for accurate alias resolution in parent extractors
            holder._from_join_tables.extend(self.tables)

            # Link column lineage only when this extractor owns the write target.
            if link_columns and self.should_link_columns and self.columns:
                self._create_column_edges(holder)
                self._link_column_lineage(holder)

            return holder
        finally:
            if not preserve_state:
                self.columns = saved_columns
                self.tables = saved_tables
                self.union_barriers = saved_union_barriers
                self.should_link_columns = saved_should_link

    def _extract_column(self, expression: Expression) -> None:
        """
        Extract column from a SELECT expression
        :param expression: expression to process
        """
        try:
            column = SqlGlotColumn.of(expression)
            self.columns.append(column)
        except Exception:  # noqa: S110
            pass

    def _extract_subqueries_from_expression(
        self, expression: Expression, subqueries: List[SubQuery]
    ) -> None:
        """
        Extract subqueries from an expression.
        :param expression: expression to search for subqueries
        :param subqueries: list to append found subqueries to
        """
        if expression is None:
            return

        for subquery_exp in expression.find_all(exp.Subquery):
            sq = SqlGlotSubQuery.of(subquery_exp, None)
            subqueries.append(sq)

    def _extract_dml_functions(
        self, expression: Expression, holder: SqlGlotSubQueryLineageHolder
    ) -> None:
        """
        Extract table lineage from special DML functions in SELECT expressions.
        Handles functions like Vertica's swap_partitions_between_tables.
        :param expression: expression to search for DML functions
        :param holder: SqlGlotSubQueryLineageHolder to add table lineage to
        """
        if expression is None:
            return

        # Handle special DML functions
        for func in expression.find_all(exp.Anonymous):
            # Vertica's swap_partitions_between_tables(source, min, max, target)
            if (
                func.this
                and func.this.upper() == "SWAP_PARTITIONS_BETWEEN_TABLES"
                and len(func.expressions) >= 4
            ):
                source_arg = func.expressions[0]
                target_arg = func.expressions[3]

                if isinstance(source_arg, exp.Literal) and source_arg.is_string:
                    source_table = Table(source_arg.this)
                    holder.add_read(source_table)

                if isinstance(target_arg, exp.Literal) and target_arg.is_string:
                    target_table = Table(target_arg.this)
                    holder.add_write(target_table)

    def _create_column_edges(self, holder: SqlGlotSubQueryLineageHolder) -> None:
        """
        Create HAS_COLUMN edges between tables/subqueries and their columns.
        """
        alias_mapping = self.get_alias_mapping_from_table_group(self.tables, holder)

        for column in self.columns:
            source_columns = column.to_source_columns(alias_mapping)

            for src_col in source_columns:
                if src_col.parent is not None:
                    holder.graph.add_edge(
                        src_col.parent, src_col, type=EdgeType.HAS_COLUMN
                    )

    def _link_column_lineage(self, holder: SqlGlotSubQueryLineageHolder) -> None:
        """
        Link columns to tables for column lineage.
        """
        self.union_barriers.append((len(self.columns), len(self.tables)))

        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]

            if not holder.write:
                continue

            writes = {write for write in holder.write if isinstance(write, Table)}
            if len(writes) > 1:
                raise SQLLineageException

            # Prefer Table over SubQuery to avoid self-referential edges when CREATE VIEW uses CTEs
            tgt_tbl = list(writes)[0] if writes else list(holder.write)[0]

            alias_mapping = self.get_alias_mapping_from_table_group(tbl_grp, holder)
            use_target_columns = len(holder.target_columns) == len(col_grp)

            for idx, tgt_col in enumerate(col_grp):
                tgt_col.parent = tgt_tbl

                final_tgt_col = (
                    holder.target_columns[idx] if use_target_columns else tgt_col
                )

                if final_tgt_col.parent is None:
                    final_tgt_col.parent = tgt_tbl

                for src_col in tgt_col.to_source_columns(alias_mapping):
                    holder.add_column_lineage(src_col, final_tgt_col)

    def _extract_tables_from_expression(
        self,
        expression: Expression,
        holder: SqlGlotSubQueryLineageHolder,
        subqueries: List[SubQuery],
    ) -> None:
        """
        Extract tables and subqueries from an expression.
        :param expression: expression to process
        :param holder: lineage holder
        :param subqueries: list to append subqueries to
        """
        if expression is None:
            return

        if isinstance(expression, exp.Table):
            dataset = get_dataset_from_table(expression, holder)
            self.tables.append(dataset)
            holder.add_read(dataset)

            if expression.args.get("joins"):
                for join in expression.args["joins"]:
                    join_this = join.this if join else None
                    if join_this is not None:
                        self._extract_tables_from_expression(
                            join_this, holder, subqueries
                        )

        elif isinstance(expression, (exp.Subquery, exp.Select)):
            # Check if this is a "fake" subquery from nested parentheses like ((tab1 JOIN tab2))
            # In this case, the subquery contains a Table or another Subquery, not a SELECT
            if isinstance(expression, exp.Subquery) and isinstance(
                expression.this, (exp.Table, exp.Subquery)
            ):
                # This is a parenthesized table expression, not a real subquery
                # Extract tables from the inner expression
                self._extract_tables_from_expression(
                    expression.this, holder, subqueries
                )

                # Also process any joins at this level (outer parentheses might have joins)
                if expression.args.get("joins"):
                    for join in expression.args["joins"]:
                        join_this = join.this if join else None
                        if join_this is not None:
                            self._extract_tables_from_expression(
                                join_this, holder, subqueries
                            )
                return

            # Process any joins first
            if expression.args.get("joins"):
                for join in expression.args["joins"]:
                    join_this = join.this if join else None
                    if join_this is not None:
                        self._extract_tables_from_expression(
                            join_this, holder, subqueries
                        )

            # For real SELECT subqueries, add them to the subqueries list
            alias = expression.alias or None
            sq = SqlGlotSubQuery.of(expression, alias)
            subqueries.append(sq)
            self.tables.append(sq)

        elif isinstance(expression, exp.Unnest):
            func = SqlGlotFunction.of(expression)
            self.tables.append(func)
            holder.add_read(func)

        elif isinstance(expression, exp.TableFromRows):
            # Extract table functions from TABLE(function()) syntax (e.g., Snowflake GENERATOR)
            if expression.this and isinstance(
                expression.this, (exp.Anonymous, exp.Func)
            ):
                func = SqlGlotFunction.of(expression.this)
                self.tables.append(func)
                holder.add_read(func)

        elif isinstance(expression, exp.Alias):
            self._extract_tables_from_expression(expression.this, holder, subqueries)

        elif isinstance(expression, exp.Lateral):
            self._extract_tables_from_expression(expression.this, holder, subqueries)

        elif expression and hasattr(expression, "this"):
            self._extract_tables_from_expression(expression.this, holder, subqueries)
