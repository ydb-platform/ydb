from sqlglot.expressions import Expression

from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.models import SqlGlotSubQueryLineageHolder


class NoopExtractor(LineageHolderExtractor):
    """
    No-op extractor for statement types that don't need lineage extraction
    """

    SUPPORTED_STMT_TYPES = [
        "command",
        "use_statement",
        "uncache",  # UNCACHE TABLE statements
        "cache",  # CACHE TABLE statements
        "copy",  # COPY statements
        "refresh",  # REFRESH statements
        "truncatetable",  # TRUNCATE TABLE statements
    ]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Return an empty lineage holder
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return: Empty 'SqlGlotSubQueryLineageHolder' object
        """
        return self._init_holder(context)
