from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext


class UpdateExtractor(BaseExtractor):
    """
    Update statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["update_statement"]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        tgt_flag = False
        columns = []
        subqueries = []
        # track potential alias reference in UPDATE clause
        potential_table_alias = None

        for segment in list_child_segments(statement):
            if segment.type == "from_expression":
                # UPDATE with JOIN, mysql only syntax
                # from_expression is wrapped directly under update_statement and there's no from_clause in this case
                # we need to pass update_statement to the function
                if from_join_tables := self._list_table_from_from_clause_or_join_clause(
                    statement, holder
                ):
                    holder.add_write(from_join_tables[0])
                    for join_table in from_join_tables[1:]:
                        holder.add_read(join_table)

            if segment.type == "keyword" and segment.raw_upper == "UPDATE":
                tgt_flag = True
                continue

            if tgt_flag:
                if write_table := self.find_table(segment):
                    if "." in segment.raw:
                        # Qualified table name, definitely not an alias
                        holder.add_write(write_table)
                    else:
                        # tsql allows table alias here, don't add as write table yet, will resolve in FROM clause
                        potential_table_alias = segment.raw
                tgt_flag = False

            if segment.type == "set_clause_list":
                for set_clause in segment.get_children("set_clause"):
                    column_references = set_clause.get_children("column_reference")
                    if len(column_references) == 2:
                        tgt_cqt = extract_column_qualifier(column_references[0])
                        src_cqt = extract_column_qualifier(column_references[1])
                        if tgt_cqt is not None and src_cqt is not None:
                            columns.append(
                                Column(tgt_cqt.column, source_columns=[src_cqt])
                            )

            if segment.type == "from_clause":
                # UPDATE FROM, ansi syntax
                # there can be multiple from items, each may be a table or a subquery
                for sq in self.list_subquery(segment):
                    subqueries.append(sq)

                from_join_tables = self._list_table_from_from_clause_or_join_clause(
                    segment, holder
                )
                target_table = None
                if potential_table_alias is not None and from_join_tables:
                    for table in from_join_tables:
                        if (
                            hasattr(table, "alias")
                            and table.alias == potential_table_alias
                        ):
                            target_table = table
                            holder.add_write(table)
                            # set to None to indicate resolved
                            potential_table_alias = None

                for read_table in from_join_tables:
                    if read_table != target_table:
                        holder.add_read(read_table)

        if potential_table_alias is not None:
            # still unresolved, add as is
            holder.add_write(Table(potential_table_alias))

        for tgt_col in columns:
            tgt_col.parent = list(holder.write)[0]
            for src_col in tgt_col.to_source_columns(
                holder.get_alias_mapping_from_table_group(list(holder.read))
            ):
                holder.add_column_lineage(src_col, tgt_col)

        self.extract_subquery(subqueries, holder)

        return holder
