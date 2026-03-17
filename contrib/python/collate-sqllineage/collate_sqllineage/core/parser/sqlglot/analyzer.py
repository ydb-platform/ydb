import sqlglot
from sqlglot.errors import ParseError

from collate_sqllineage.core.analyzer import LineageAnalyzer
from collate_sqllineage.core.holders import StatementLineageHolder
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.models import SqlGlotSubQueryLineageHolder
from collate_sqllineage.core.parser.sqlglot.utils import (
    clean_parentheses,
    get_statement_type,
    is_subquery_statement,
    remove_statement_parentheses,
)
from collate_sqllineage.exceptions import (
    InvalidSyntaxException,
    UnsupportedStatementException,
)

# Mapping from sqlfluff/common dialect names to sqlglot dialect names
DIALECT_MAP = {
    "ansi": "",
    "sparksql": "spark",
}


class SqlGlotLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlglot`"""

    def __init__(self, dialect: str):
        self._dialect = DIALECT_MAP.get(dialect, dialect)

    def analyze(self, sql: str) -> StatementLineageHolder:
        # remove nested parentheses that sqlglot might have issues with
        sql = clean_parentheses(sql)
        is_sub_query = is_subquery_statement(sql)
        if is_sub_query:
            sql = remove_statement_parentheses(sql)

        try:
            # Parse SQL using sqlglot
            parsed_expressions = sqlglot.parse(sql, dialect=self._dialect)

            if not parsed_expressions:
                raise InvalidSyntaxException(
                    f"This SQL statement is unparsable, please check potential syntax error for SQL: {sql}"
                )

            # Get the first parsed expression
            statement = parsed_expressions[0]
            if statement is None:
                raise InvalidSyntaxException(
                    f"This SQL statement is unparsable, please check potential syntax error for SQL: {sql}"
                )

            self.parsed_result = statement

            # Check for parse errors
            if statement.error_messages():
                raise InvalidSyntaxException(
                    f"This SQL statement has syntax errors: {statement.error_messages()} for SQL: {sql}"
                )

            statement_type = get_statement_type(statement)

            extractors = [
                extractor_cls(self._dialect)
                for extractor_cls in LineageHolderExtractor.__subclasses__()
            ]

            if any(extractor.can_extract(statement_type) for extractor in extractors):
                lineage_holder = SqlGlotSubQueryLineageHolder()
                for extractor in extractors:
                    if extractor.can_extract(statement_type):
                        lineage_holder = extractor.extract(
                            statement, AnalyzerContext(), is_sub_query
                        )
                        break
                return StatementLineageHolder.of(lineage_holder)
            else:
                raise UnsupportedStatementException(
                    f"SQLLineage doesn't support analyzing statement type [{statement_type}] for SQL: {sql}"
                )
        except ParseError as e:
            raise InvalidSyntaxException(
                f"Failed to parse SQL statement: {str(e)} for SQL: {sql}"
            )
