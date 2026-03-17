from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, Token

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.parser.sqlparse.handlers.base import NextTokenBaseHandler
from collate_sqllineage.core.parser.sqlparse.models import SqlParseSubQuery


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
            if sublist:
                # CTE: tbl AS (SELECT 1), tbl is alias and (SELECT 1) is subquery Parenthesis
                # When CTE used without AS (SparkSQL), token is Function with sublists [Identifier, Parenthesis]
                # We need the Parenthesis (the actual query), not the Identifier (the CTE name)
                query_token = next(
                    (s for s in sublist if isinstance(s, Parenthesis)), sublist[0]
                )
                holder.add_cte(SqlParseSubQuery.of(query_token, token.get_real_name()))
