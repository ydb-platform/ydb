import networkx as nx
from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.core.models import AnalyzerContext, Column
from collate_sqllineage.core.parser.sqlglot.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotSubQueryLineageHolder,
    SqlGlotTable,
)
from collate_sqllineage.utils.constant import NodeTag


class DdlCreateExtractor(LineageHolderExtractor):
    """
    DDL CREATE queries lineage extractor for sqlglot
    """

    SUPPORTED_STMT_TYPES = ["create_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for CREATE statements (TABLE, VIEW, etc.)
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return 'SqlGlotSubQueryLineageHolder' object
        """
        holder = self._init_holder(context)

        if isinstance(statement, exp.Create):
            target = statement.this
            target_table = None
            if target:
                if isinstance(target, exp.Table):
                    target_table = SqlGlotTable.of(target)
                    holder.add_write(target_table)
                elif isinstance(target, exp.Schema) and target.this:
                    table = target.this
                    if isinstance(table, exp.Table):
                        target_table = SqlGlotTable.of(table)
                        holder.add_write(target_table)

            if statement.expression:
                select_exp = statement.expression

                # Unwrap Subquery if the expression is parenthesized
                if isinstance(select_exp, exp.Subquery):
                    select_exp = select_exp.this

                if isinstance(
                    select_exp, (exp.Select, exp.Union, exp.Intersect, exp.Except)
                ):
                    select_context = AnalyzerContext(
                        prev_write={target_table} if target_table else None,
                        prev_cte=context.prev_cte,
                    )

                    select_extractor = DmlSelectExtractor(self.dialect)
                    select_holder = select_extractor.extract(
                        select_exp, select_context, False
                    )

                    for table in select_holder.read:
                        holder.add_read(table)
                    for cte in select_holder.cte:
                        holder.add_cte(cte)

                    holder.graph = nx.compose(holder.graph, select_holder.graph)

                    # Map custom column names: CREATE VIEW my_view (col1, col2) AS SELECT ...
                    if (
                        target_table
                        and isinstance(target, exp.Schema)
                        and target.expressions
                    ):
                        custom_col_names = [
                            str(col.this) if hasattr(col, "this") else str(col)
                            for col in target.expressions
                        ]

                        select_col_names = []
                        if (
                            isinstance(select_exp, exp.Select)
                            and select_exp.expressions
                        ):
                            for col_exp in select_exp.expressions:
                                if isinstance(col_exp, exp.Column):
                                    col_name = (
                                        str(col_exp.this)
                                        if hasattr(col_exp, "this")
                                        else str(col_exp)
                                    )
                                    select_col_names.append(col_name)
                                elif isinstance(col_exp, exp.Alias):
                                    select_col_names.append(str(col_exp.alias))
                                else:
                                    select_col_names.append(str(col_exp))

                        col_name_mapping = {
                            src: custom
                            for src, custom in zip(select_col_names, custom_col_names)
                            if src != custom
                        }

                        if col_name_mapping:
                            columns_to_update = [
                                (node, Column(col_name_mapping[node.raw_name]))
                                for node in list(holder.graph.nodes)
                                if isinstance(node, Column)
                                and node.parent == target_table
                                and node.raw_name in col_name_mapping
                            ]

                            for old_col, new_col in columns_to_update:
                                new_col.parent = target_table
                                predecessors = list(holder.graph.predecessors(old_col))
                                successors = list(holder.graph.successors(old_col))
                                holder.graph.add_node(
                                    new_col, **holder.graph.nodes[old_col]
                                )
                                for predecessor in predecessors:
                                    holder.graph.add_edge(predecessor, new_col)
                                for successor in successors:
                                    holder.graph.add_edge(new_col, successor)
                                holder.graph.remove_node(old_col)

                    # Remove subquery write nodes, keeping only the target table
                    if target_table:
                        holder_writes = set(holder.write)
                        for write_node in holder_writes:
                            if write_node != target_table:
                                holder.graph.nodes[write_node].pop(NodeTag.WRITE, None)

            properties = statement.args.get("properties")
            if properties and properties.expressions:
                for prop in properties.expressions:
                    if isinstance(prop, exp.LikeProperty) and prop.this:
                        source_table = prop.this
                        if isinstance(source_table, exp.Table):
                            holder.add_read(SqlGlotTable.of(source_table))

            # Handle CLONE (Snowflake/BigQuery)
            clone = statement.args.get("clone")
            if clone and isinstance(clone, exp.Clone):
                if clone.this and isinstance(clone.this, exp.Table):
                    holder.add_read(SqlGlotTable.of(clone.this))

        return holder
