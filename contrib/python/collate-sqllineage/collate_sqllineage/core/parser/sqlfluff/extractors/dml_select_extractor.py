from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from collate_sqllineage.core.parser.sqlfluff.utils import retrieve_segments


class DmlSelectExtractor(LineageHolderExtractor):
    """
    DML Select queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression", "if_then_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SubQueryLineageHolder' object
        """
        handlers, conditional_handlers = self._init_handlers()
        holder = self._init_holder(context)
        subqueries = [SqlFluffSubQuery.of(statement, None)] if is_sub_query else []
        segments = (
            [statement]
            if statement.type == "set_expression"
            else retrieve_segments(statement)
        )
        for segment in segments:

            # Handle common_table_expression (CTEs) specially
            if segment.type == "common_table_expression":
                # Extract the CTE name (identifier) and query (bracketed)
                cte_name = None
                cte_query = None
                for cte_child in (
                    segment.segments if hasattr(segment, "segments") else []
                ):
                    if cte_child.type == "identifier":
                        cte_name = cte_child.raw
                    elif cte_child.type == "bracketed":
                        cte_query = cte_child

                if cte_name and cte_query:
                    # Create a SubQuery with the correct CTE name as the alias
                    cte_sq = SqlFluffSubQuery.of(cte_query, cte_name)
                    # Add to CTE set (this will create the SubQuery -> 'name' edge)
                    holder.add_cte(cte_sq)
                    subqueries.append(cte_sq)
                continue  # Don't process CTE with normal handlers

            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # call end of query hook here as loop is over
        for conditional_handler in conditional_handlers:
            conditional_handler.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= self.extract(sq.query, AnalyzerContext(sq, holder.cte))

        for sq in holder.extra_subqueries:
            holder |= self.extract(sq.query, AnalyzerContext(sq, holder.cte))

        return holder
