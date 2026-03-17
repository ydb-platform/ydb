import re

from sqlparse.sql import (
    Case,
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Token,
)
from sqlparse.tokens import Literal, Wildcard

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, SubQuery, Table
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlparse.handlers.base import NextTokenBaseHandler
from sqllineage.core.parser.sqlparse.models import (
    SqlParseColumn,
    SqlParseSubQuery,
    SqlParseTable,
)
from sqllineage.core.parser.sqlparse.utils import (
    get_subquery_parentheses,
    is_subquery,
    is_values_clause,
)
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.helpers import escape_identifier_name


class SourceHandler(SourceHandlerMixin, NextTokenBaseHandler):
    """Source Table & Column Handler."""

    SOURCE_TABLE_TOKENS = (
        r"FROM",
        # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
        r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
    )

    def __init__(self):
        self.column_flag = False
        self.columns = []
        self.tables = []
        self.union_barriers = []
        super().__init__()

    def _indicate(self, token: Token) -> bool:
        if token.normalized in ("UNION", "UNION ALL"):
            self.union_barriers.append((len(self.columns), len(self.tables)))

        if self.column_flag is True and bool(token.normalized == "DISTINCT"):
            # special handling for SELECT DISTINCT
            return True

        if any(re.match(regex, token.normalized) for regex in self.SOURCE_TABLE_TOKENS):
            self.column_flag = False
            return True
        elif bool(token.normalized == "SELECT"):
            self.column_flag = True
            return True
        else:
            return False

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if self.column_flag:
            self._handle_column(token)
        else:
            self._handle_table(token, holder)

    def _handle_table(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if isinstance(token, Identifier):
            self._add_dataset_from_identifier(token, holder)
        elif isinstance(token, IdentifierList):
            # This is to support join in ANSI-89 syntax
            for identifier in token.get_sublists():
                self._add_dataset_from_identifier(identifier, holder)
        elif isinstance(token, Parenthesis):
            if is_subquery(token):
                # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as Parenthesis
                # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
                self.tables.append(SqlParseSubQuery.of(token, None))
            elif is_values_clause(token):
                # SELECT * FROM (VALUES ...), no operation needed
                pass
            else:
                # SELECT * FROM (tab2) or SELECT * FROM (tab2 JOIN tab1), which is valid syntax
                using_flag = False
                for t in token.tokens:
                    if t.is_keyword and t.normalized == "USING":
                        using_flag = True
                    if isinstance(t, Identifier) or (
                        isinstance(t, Parenthesis) and using_flag is False
                    ):
                        self._handle(t, holder)
        elif token.ttype == Literal.String.Single:
            self.tables.append(Path(token.value))
        elif isinstance(token, Function):
            # functions like unnest or generator can output a sequence of values as source, ignore it for now
            pass
        else:
            raise SQLLineageException(
                f"An Identifier is expected, got {type(token).__name__}[value: {token}] instead."
            )

    def _handle_column(self, token: Token) -> None:
        column_token_types = (Identifier, Function, Operation, Case, Parenthesis)
        if isinstance(token, column_token_types) or token.ttype is Wildcard:
            column_tokens = [token]
        elif isinstance(token, IdentifierList):
            column_tokens = [
                sub_token
                for sub_token in token.tokens
                if (
                    isinstance(sub_token, column_token_types)
                    and not sub_token.value.startswith(
                        "@"
                    )  # ignore tsql variable column starts with @
                )
                or sub_token.ttype is Wildcard  # For case like: select a.col, b.*
            ]
        else:
            # SELECT constant value will end up here
            column_tokens = []
        for token in column_tokens:
            self.columns.append(SqlParseColumn.of(token))

    def _add_dataset_from_identifier(
        self, identifier: Identifier, holder: SubQueryLineageHolder
    ) -> None:
        first_token = identifier.token_first(skip_cm=True)
        if isinstance(first_token, Function):
            # function() as alias, no dataset involved
            return None
        elif isinstance(first_token, Parenthesis) and is_values_clause(first_token):
            # (VALUES ...) AS alias, no dataset involved
            return None
        dataset: Table | SubQuery | Path
        path_match = re.match(r"(parquet|csv|json)\.`(.*)`", identifier.value)
        if path_match is not None:
            dataset = Path(path_match.groups()[1])
        else:
            read: Table | SubQuery | None = None
            subqueries = get_subquery_parentheses(identifier)
            if len(subqueries) > 0:
                # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                parenthesis, alias = subqueries[0]
                read = SqlParseSubQuery.of(parenthesis, alias)
            else:
                cte_dict = {s.alias: s for s in holder.cte}
                if "." not in identifier.value:
                    cte = cte_dict.get(
                        escape_identifier_name(identifier.get_real_name())
                    )
                    if cte is not None:
                        # could reference CTE with or without alias
                        read = SqlParseSubQuery.of(
                            cte.query,
                            identifier.get_alias() or identifier.get_real_name(),
                        )
                if read is None:
                    read = SqlParseTable.of(identifier)
            dataset = read
        if dataset:
            self.tables.append(dataset)
