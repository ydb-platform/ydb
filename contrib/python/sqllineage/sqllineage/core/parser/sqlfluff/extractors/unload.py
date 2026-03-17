from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.models import Path
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.utils.entities import AnalyzerContext


class UnloadExtractor(BaseExtractor):
    """
    Unload statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "unload_statement",
    ]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        if bracketed := statement.get_child("bracketed"):
            if quoted_literal := bracketed.get_child("quoted_literal"):
                raw_sql = quoted_literal.raw.strip("'").strip('"')
                holder |= self._analyze_inner_sql(raw_sql)
        if quoted_literal := statement.get_child("quoted_literal"):
            raw_path = quoted_literal.raw.strip("'").strip('"')
            holder.add_write(Path(raw_path))
        return holder

    def _analyze_inner_sql(self, sql: str) -> StatementLineageHolder:
        from sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer

        analyzer = SqlFluffLineageAnalyzer("", self.dialect)
        return analyzer.analyze(sql, self.metadata_provider)
