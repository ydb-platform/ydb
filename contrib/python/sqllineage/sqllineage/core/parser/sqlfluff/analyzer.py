import warnings

from sqlfluff.core import (
    FluffConfig,
    Linter,
    SQLLexError,
    SQLParseError,
    dialect_readout,
)
from sqlfluff.core.parser import BaseSegment

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.exceptions import (
    InvalidSyntaxException,
    UnsupportedStatementException,
)
from sqllineage.utils.entities import AnalyzerContext


class SqlFluffLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    PARSER_NAME = "sqlfluff"
    SUPPORTED_DIALECTS = list(dialect.label for dialect in dialect_readout())

    def __init__(self, file_path: str, dialect: str, silent_mode: bool = False):
        self._sqlfluff_config = FluffConfig.from_path(
            path=file_path, overrides={"dialect": dialect}
        )
        self._silent_mode = silent_mode
        self.tsql_split_cache: dict[str, BaseSegment] = {}

    def split_tsql(self, sql: str) -> list[str]:
        """
        use sqlfluff parse to split tsql statements. This is in particular for semicolon not present cases.
        The result is cached so that later analyze method doesn't have to parse regarding single statement sql.
        """
        sqls = []
        for segment in self._list_specific_statement_segment(sql):
            self.tsql_split_cache[segment.raw] = segment
            sqls.append(segment.raw)
        return sqls

    def analyze(
        self, sql: str, metadata_provider: MetaDataProvider
    ) -> StatementLineageHolder:
        if sql in self.tsql_split_cache:
            statement_segments = [self.tsql_split_cache[sql]]
        else:
            statement_segments = self._list_specific_statement_segment(sql)
        if len(statement_segments) == 0:
            raise UnsupportedStatementException(
                f"SQLLineage cannot parse SQL:{sql}"
            )  # pragma: no cover
        else:
            statement_segment = statement_segments[0]
            for extractor in [
                extractor_cls(self._sqlfluff_config.get("dialect"), metadata_provider)
                for extractor_cls in BaseExtractor.__subclasses__()
            ]:
                if extractor.can_extract(statement_segment.type):
                    lineage_holder = extractor.extract(
                        statement_segment, AnalyzerContext()
                    )
                    return StatementLineageHolder.of(lineage_holder)
            else:
                if self._silent_mode:
                    warnings.warn(
                        f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:{sql}"
                    )
                    return StatementLineageHolder()
                else:
                    raise UnsupportedStatementException(
                        f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:{sql}"
                    )

    def _list_specific_statement_segment(self, sql: str):
        parsed = Linter(config=self._sqlfluff_config).parse_string(sql)
        violations = [
            str(e)
            for e in parsed.violations
            if isinstance(e, (SQLLexError, SQLParseError))
        ]
        if violations:
            violation_msg = "\n".join(violations)
            raise InvalidSyntaxException(
                f"This SQL statement is unparsable, please check potential syntax error for SQL:\n"
                f"{sql}\n"
                f"{violation_msg}"
            )
        segments = []
        for top_segment in getattr(parsed.tree, "segments", []):
            match top_segment.type:
                case "statement":
                    segments.append(top_segment.segments[0])
                case "batch":
                    statements = top_segment.get_children("statement")
                    if len(statements) > 1:
                        warnings.warn(
                            "SQL statements is not split by semicolon. "
                            "SQLLineage is not guaranteed to generate correct result under this circumstances.",
                            SyntaxWarning,
                            stacklevel=2,
                        )
                    for statement in statements:
                        segments.append(statement.segments[0])
        return segments
