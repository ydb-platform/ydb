from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)


class NoopExtractor(LineageHolderExtractor):
    """
    Extractor for queries which do not provide any lineage
    """

    SUPPORTED_STMT_TYPES = [
        "delete_statement",
        "truncate_table",
        "refresh_statement",
        "cache_table",
        "uncache_table",
        "show_statement",
        "use_statement",
    ]

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
        return SubQueryLineageHolder()
