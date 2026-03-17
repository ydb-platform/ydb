from sqlparse.sql import Function, Identifier, IdentifierList, Token

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlparse.handlers.base import NextTokenBaseHandler
from sqllineage.core.parser.sqlparse.models import SqlParseSubQuery


class CTEHandler(NextTokenBaseHandler):
    """Common Table Expression (With Queries) Handler."""

    CTE_TOKENS = ("WITH",)

    def _indicate(self, token: Token) -> bool:
        return token.normalized in self.CTE_TOKENS

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        # when CTE used without AS, it will be parsed as Function. This syntax is valid in SparkSQL
        cte_token_types = (Identifier, Function)
        if isinstance(token, cte_token_types):
            cte = [token]
        elif isinstance(token, IdentifierList):
            cte = [
                token for token in token.tokens if isinstance(token, cte_token_types)
            ]
        else:
            # CREATE TABLE tbl1 (col1 VARCHAR) WITH (bucketed_by = ARRAY['col1'], bucket_count = 256).
            # This syntax is valid for bucketing in Trino and not the CTE, token will be Parenthesis here
            cte = []
        for token in cte:
            sublist = list(token.get_sublists())
            if sublist and not (isinstance(sublist[0], Function)):
                # CTE: tbl AS (SELECT 1), tbl is alias and (SELECT 1) is subquery Parenthesis
                holder.add_cte(SqlParseSubQuery.of(sublist[0], token.get_real_name()))
