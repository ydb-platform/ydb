from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.utils.entities import AnalyzerContext


class NoopExtractor(BaseExtractor):
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
        "describe_statement",
        "use_statement",
        "declare_segment",
        "analyze_statement",
        "add_jar_statement",
        "create_function_statement",
        "drop_function_statement",
        "set_statement",
    ]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        return StatementLineageHolder()
