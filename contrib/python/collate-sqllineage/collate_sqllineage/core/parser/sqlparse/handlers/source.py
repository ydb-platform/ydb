import logging
import re
import traceback

from typing import List, Optional

from sqlparse.sql import (
    Case,
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Token,
)
from sqlparse.tokens import Literal, Name as TokenName, Wildcard

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import DataFunction, Location, Path, Schema
from collate_sqllineage.core.parser import SourceHandlerMixin
from collate_sqllineage.core.parser.sqlparse.handlers.base import NextTokenBaseHandler
from collate_sqllineage.core.parser.sqlparse.holder_utils import (
    get_dataset_from_identifier,
)
from collate_sqllineage.core.parser.sqlparse.models import (
    SqlParseColumn,
    SqlParseFunction,
    SqlParseSubQuery,
)
from collate_sqllineage.core.parser.sqlparse.utils import is_subquery, is_values_clause
from collate_sqllineage.exceptions import SQLLineageException

logger = logging.getLogger(__name__)

RESERVED_FUNCTION_TOKEN = ("table",)


class SourceHandler(SourceHandlerMixin, NextTokenBaseHandler):
    """Source Table & Column Handler."""

    SOURCE_TABLE_TOKENS = (
        r"FROM",
        # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
        r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
    )

    def __init__(self):
        self.column_flag = False
        self.columns: List = []
        self.tables: List = []
        self.union_barriers: List = []
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

    def _data_function_handler(
        self, function: Token, schema_name: Optional[str] = None
    ):
        """
        Handle function as table
        :param function: function segment
        """
        if isinstance(function, Function):
            if (
                function.tokens[0].ttype == TokenName
                and function.tokens[0].normalized.lower() not in RESERVED_FUNCTION_TOKEN
            ):
                self.tables.append(
                    DataFunction(
                        function.tokens[0].value,
                        Schema(schema_name) if schema_name else Schema(),
                    )
                )
                parenthesis = list(function.get_sublists())[0]
            else:

                identifier, parenthesis = list(function.get_sublists())[:2]
                if (
                    isinstance(identifier, Identifier)
                    and identifier.normalized.lower() not in RESERVED_FUNCTION_TOKEN
                ):
                    self.tables.append(SqlParseFunction.of(identifier, schema_name))

            expressions = list(parenthesis.get_sublists())
            if expressions:
                if isinstance(expressions[0], IdentifierList):
                    for expression in list(expressions[0].get_sublists()) or []:
                        self._data_function_handler(expression)
                if isinstance(expressions[0], Identifier) or isinstance(
                    expressions[0], Function
                ):
                    self._data_function_handler(expressions[0])
        elif isinstance(function, Identifier):
            schema_name_build = ""
            for expression in function.tokens or []:
                if isinstance(expression, Function):
                    self._data_function_handler(
                        expression, schema_name_build[:-1] or None
                    )
                elif expression.ttype == TokenName:
                    schema_name_build += expression.normalized + "."

    def _handle_table(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if isinstance(token, Identifier):
            if token.normalized.lower() in RESERVED_FUNCTION_TOKEN:
                self.indicator = True
                return
            if Function.__name__ in {type(t).__name__ for t in token.get_sublists()}:
                self._data_function_handler(token)
                return
            if token.value.strip().startswith("@"):
                # Named location reference, e.g. @STAGE_01, @db.schema.stage
                self.tables.append(Location(token.value.strip()))
                return
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
                # SELECT * FROM (tab2), which is valid syntax
                self._handle(token.tokens[1], holder)
        elif token.ttype == Literal.String.Single:
            self.tables.append(Path(token.value))
        elif isinstance(token, Function):
            # functions like unnest or generator can output a sequence of values as source
            self._data_function_handler(token)
        else:
            raise SQLLineageException(
                "An Identifier is expected, got %s[value: %s] instead."
                % (type(token).__name__, token)
            )

    def _handle_column(self, token: Token) -> None:
        column_token_types = (Identifier, Function, Operation, Case, Parenthesis)
        if isinstance(token, column_token_types) or token.ttype is Wildcard:
            column_tokens = [token]
        elif isinstance(token, IdentifierList):
            column_tokens = [
                sub_token
                for sub_token in token.tokens
                if isinstance(sub_token, column_token_types)
            ]
        else:
            # SELECT constant value will end up here
            column_tokens = []
        for token in column_tokens:
            try:
                self.columns.append(SqlParseColumn.of(token))
            except Exception as err:
                logger.warning(f"Failed to parse column {str(token)} due to {err}")
                logger.debug(traceback.format_exc())

    def _add_dataset_from_identifier(
        self, identifier: Identifier, holder: SubQueryLineageHolder
    ) -> None:
        dataset = get_dataset_from_identifier(identifier, holder)
        if dataset:
            self.tables.append(dataset)
