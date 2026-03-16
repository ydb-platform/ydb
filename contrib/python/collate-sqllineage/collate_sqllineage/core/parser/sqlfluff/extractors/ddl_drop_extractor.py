from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffTable


class DdlDropExtractor(LineageHolderExtractor):
    """
    DDL Drop queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["drop_table_statement"]

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
        holder = StatementLineageHolder()
        for table in {
            SqlFluffTable.of(t)
            for t in statement.segments
            if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder
