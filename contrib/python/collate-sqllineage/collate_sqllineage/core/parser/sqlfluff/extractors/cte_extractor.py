from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.dml_insert_extractor import (
    DmlInsertExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from collate_sqllineage.core.parser.sqlfluff.utils import retrieve_segments


class DmlCteExtractor(LineageHolderExtractor):
    """
    DML CTE queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["with_compound_statement"]

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
        handlers, _ = self._init_handlers()

        holder = self._init_holder(context)

        subqueries = []
        segments = retrieve_segments(statement)

        for segment in segments:
            for current_handler in handlers:
                current_handler.handle(segment, holder)

            if segment.type in {"select_statement", "set_expression"}:
                holder |= DmlSelectExtractor(self.dialect).extract(
                    segment,
                    AnalyzerContext(
                        prev_cte=holder.cte,
                        prev_write=holder.write,
                        target_columns=holder.target_columns,
                    ),
                )

            if segment.type == "insert_statement":
                holder |= DmlInsertExtractor(self.dialect).extract(
                    segment,
                    AnalyzerContext(prev_cte=holder.cte),
                )

            identifier = None
            bracketed_node = None
            if segment.type == "common_table_expression":
                sub_segments = retrieve_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "identifier":
                        identifier = sub_segment.raw
                    if sub_segment.type == "bracketed":
                        bracketed_node = sub_segment
                        for sq in self.parse_subquery(sub_segment):
                            if identifier:
                                sq.alias = identifier
                            subqueries.append(sq)

                # Add CTE once, using the bracketed node regardless of whether AS is present
                if identifier and bracketed_node:
                    holder.add_cte(SqlFluffSubQuery.of(bracketed_node, identifier))

        # By recursively extracting each extractor of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= DmlSelectExtractor(self.dialect).extract(
                sq.query,
                AnalyzerContext(sq, prev_cte=holder.cte),
            )

        return holder
