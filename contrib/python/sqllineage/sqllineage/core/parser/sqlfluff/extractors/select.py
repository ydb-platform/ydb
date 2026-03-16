from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffTable,
)
from sqllineage.core.parser.sqlfluff.utils import (
    find_table_identifier,
    is_set_expression,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class SelectExtractor(BaseExtractor, SourceHandlerMixin):
    """
    Select statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression", "bracketed"]

    def __init__(self, dialect: str, metadata_provider: MetaDataProvider):
        super().__init__(dialect, metadata_provider)
        self.columns = []
        self.tables = []
        self.union_barriers = []

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        subqueries = []
        segments = (
            [statement]
            if statement.type == "set_expression"
            else list_child_segments(statement)
        )
        for segment in segments:
            for sq in self.list_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            if is_set_expression(segment):
                subqueries.extend(self._collect_subqueries_in_set_expression(segment))

        self.extract_subquery(subqueries, holder)

        for segment in segments:
            self._handle_select_statement_child_segments(segment, holder)

            if is_set_expression(segment):
                self._handle_set_expression(segment, holder)

        self.end_of_query_cleanup(holder)

        holder.expand_wildcard(self.metadata_provider)

        return holder

    def _handle_select_statement_child_segments(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ):
        self._handle_swap_partition(segment, holder)
        self._handle_select_into(segment, holder)
        self.tables.extend(
            self._list_table_from_from_clause_or_join_clause(segment, holder)
        )
        self._handle_column(segment)

    def _handle_swap_partition(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ):
        """
        A handler for swap_partitions_between_tables function supported by vertica
        """
        if self.dialect == "vertica" and segment.type == "select_clause":
            if select_clause_element := segment.get_child("select_clause_element"):
                if function := select_clause_element.get_child("function"):
                    if function_name := function.get_child("function_name"):
                        if function_name.raw_upper == "SWAP_PARTITIONS_BETWEEN_TABLES":
                            if function_contents := function.get_child(
                                "function_contents"
                            ):
                                if bracketed := function_contents.get_child(
                                    "bracketed"
                                ):
                                    expressions = bracketed.get_children("expression")
                                    holder.add_read(
                                        SqlFluffTable(
                                            escape_identifier_name(expressions[0].raw)
                                        )
                                    )
                                    holder.add_write(
                                        SqlFluffTable(
                                            escape_identifier_name(expressions[3].raw)
                                        )
                                    )

    def _handle_select_into(self, segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for SELECT INTO statement supported by some dialects
        """
        if segment.type in ["into_table_clause", "into_clause"]:
            if identifier := find_table_identifier(segment):
                if table := self.find_table(identifier):
                    holder.add_write(table)

    def _handle_set_expression(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        # Recursively handle set_expression and nested bracketed set_expressions
        for idx, child in enumerate(
            segment.get_children("select_statement", "bracketed")
        ):
            if idx != 0:
                self.union_barriers.append((len(self.columns), len(self.tables)))
            match child.type:
                case "select_statement":
                    for seg in list_child_segments(child):
                        self._handle_select_statement_child_segments(seg, holder)
                case "bracketed":
                    # If the bracketed child contains another set_expression, recurse; otherwise handle its contents
                    inner_children = list_child_segments(child)
                    if any(c.type == "set_expression" for c in inner_children):
                        for c in inner_children:
                            if c.type == "set_expression":
                                self._handle_set_expression(c, holder)
                    else:
                        for seg in inner_children:
                            self._handle_select_statement_child_segments(seg, holder)

    def _collect_subqueries_in_set_expression(self, segment: BaseSegment):
        subqueries = []
        for child in segment.get_children("select_statement", "bracketed"):
            match child.type:
                case "select_statement":
                    for seg in list_child_segments(child):
                        subqueries.extend(self.list_subquery(seg))
                case "bracketed":
                    inner_children = list_child_segments(child)
                    for c in inner_children:
                        if c.type == "set_expression":
                            subqueries.extend(
                                self._collect_subqueries_in_set_expression(c)
                            )
                        else:
                            subqueries.extend(self.list_subquery(c))
        return subqueries

    def _handle_column(self, segment: BaseSegment) -> None:
        """
        Column handler method
        """
        if segment.type == "select_clause":
            for sub_segment in segment.get_children("select_clause_element"):
                self.columns.append(SqlFluffColumn.of(sub_segment))
