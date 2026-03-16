from sqlfluff.core import Linter

from collate_sqllineage.core.analyzer import LineageAnalyzer
from collate_sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.utils import (
    clean_parentheses,
    get_statement_segment,
    is_subquery_statement,
    remove_statement_parentheses,
)
from collate_sqllineage.exceptions import (
    InvalidSyntaxException,
    UnsupportedStatementException,
)


class SqlFluffLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    def __init__(self, dialect: str):
        self._dialect = dialect

    def analyze(self, sql: str) -> StatementLineageHolder:
        # remove nested parentheses that sqlfluff cannot parse
        sql = clean_parentheses(sql)
        is_sub_query = is_subquery_statement(sql)
        if is_sub_query:
            sql = remove_statement_parentheses(sql)
        linter = Linter(dialect=self._dialect)
        parsed_string = linter.parse_string(sql)
        self.parsed_result = parsed_string
        statement_segment = get_statement_segment(parsed_string)
        extractors = [
            extractor_cls(self._dialect)
            for extractor_cls in LineageHolderExtractor.__subclasses__()
        ]
        if statement_segment is None or statement_segment.type == "unparsable":
            raise InvalidSyntaxException(
                f"This SQL statement is unparsable, please check potential syntax error for SQL:"
                f"{sql}"
            )
        else:
            if any(
                extractor.can_extract(statement_segment.type)
                for extractor in extractors
            ):
                if "unparsable" in statement_segment.descendant_type_set:
                    raise InvalidSyntaxException(
                        f"{statement_segment.type} is partially unparsable, "
                        f"please check potential syntax error for SQL:"
                        f"{sql}"
                    )
                else:
                    lineage_holder = SubQueryLineageHolder()
                    for extractor in extractors:
                        if extractor.can_extract(statement_segment.type):
                            lineage_holder = extractor.extract(
                                statement_segment, AnalyzerContext(), is_sub_query
                            )
                            break
                    return StatementLineageHolder.of(lineage_holder)
            else:
                raise UnsupportedStatementException(
                    f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:"
                    f"{sql}"
                )
