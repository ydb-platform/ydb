from typing import List, Optional

from sqlparse import tokens as T
from sqlparse.engine import grouping
from sqlparse.lexer import Lexer
from sqlparse.sql import (
    Case,
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Token,
    TokenList,
)
from sqlparse.utils import imt

from collate_sqllineage.core.models import Column, DataFunction, Schema, SubQuery, Table
from collate_sqllineage.core.parser.sqlparse.utils import get_parameters, is_subquery
from collate_sqllineage.utils.entities import ColumnQualifierTuple
from collate_sqllineage.utils.helpers import escape_identifier_name


class SqlParseTable(Table):
    @staticmethod
    def of(table: Identifier) -> Table:
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = table._token_matching(
            lambda token: imt(token, m=(T.Punctuation, ".")),
            start=len(table.tokens),
            reverse=True,
        )
        real_name = table._get_first_name(dot_idx, real_name=True)
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(token.value)
                    for token in table.tokens[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        alias = table.get_alias()
        kwargs = {"alias": alias} if alias else {}
        return Table(real_name, schema, **kwargs)


class SqlParseFunction(DataFunction):
    @staticmethod
    def of(function: Identifier, schema_name: Optional[str] = None) -> DataFunction:
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = function._token_matching(
            lambda token: imt(token, m=(T.Punctuation, ".")),
            start=len(function.tokens),
            reverse=True,
        )
        real_name = function._get_first_name(dot_idx, real_name=True)
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(token.value)
                    for token in function.tokens[:dot_idx]
                ]
            )
            if dot_idx
            else None
        ) or schema_name
        schema = Schema(parent_name) if parent_name is not None else Schema()
        alias = function.get_alias()
        kwargs = {"alias": alias} if alias else {}
        return DataFunction(real_name, schema, **kwargs)


class SqlParseSubQuery(SubQuery):
    @staticmethod
    def of(subquery: Parenthesis, alias: Optional[str]) -> SubQuery:
        return SubQuery(subquery, subquery.value, alias)


class SqlParseColumn(Column):
    @staticmethod
    def of(column: Token, **kwargs) -> Column:
        if isinstance(column, Identifier):
            alias = column.get_alias()
            if alias:
                # handle column alias, including alias for column name or Case, Function
                kw_idx, kw = column.token_next_by(m=(T.Keyword, "AS"))
                if kw_idx is None:
                    # alias without AS
                    kw_idx, _ = column.token_next_by(i=Identifier)
                if kw_idx is None:
                    # invalid syntax: col AS, without alias
                    return Column(alias)
                else:
                    idx, _ = column.token_prev(kw_idx, skip_cm=True)
                    expr = grouping.group(TokenList(column.tokens[: idx + 1]))[0]
                    source_columns = SqlParseColumn._extract_source_columns(expr)
                    return Column(
                        alias,
                        source_columns=source_columns,
                    )
            else:
                # select column name directly without alias
                return Column(
                    column.get_real_name(),
                    source_columns=(
                        (column.get_real_name(), column.get_parent_name()),
                    ),
                )
        else:
            # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
            source_columns = SqlParseColumn._extract_source_columns(column)
            return Column(
                column.value,
                source_columns=source_columns,
            )

    @staticmethod
    def _extract_source_columns(token: Token) -> List[ColumnQualifierTuple]:
        if isinstance(token, Function):
            # max(col1) AS col2
            source_columns = [
                cqt
                for tk in get_parameters(token)
                for cqt in SqlParseColumn._extract_source_columns(tk)
            ]
        elif isinstance(token, Parenthesis):
            if is_subquery(token):
                # This is to avoid circular import
                from collate_sqllineage.runner import LineageRunner

                # (SELECT avg(col1) AS col1 FROM tab3), used after WHEN or THEN in CASE clause
                src_cols = [
                    lineage[0]
                    for lineage in LineageRunner(token.value).get_column_lineage(
                        exclude_subquery=False
                    )
                ]
                source_columns = [
                    ColumnQualifierTuple(
                        src_col.raw_name,
                        src_col.parent.raw_name if src_col.parent else None,
                    )
                    for src_col in src_cols
                ]
            else:
                # (col1 + col2) AS col3
                source_columns = [
                    cqt
                    for tk in token.tokens[1:-1]
                    for cqt in SqlParseColumn._extract_source_columns(tk)
                ]
        elif isinstance(token, Operation):
            # col1 + col2 AS col3
            source_columns = [
                cqt
                for tk in token.get_sublists()
                for cqt in SqlParseColumn._extract_source_columns(tk)
            ]
        elif isinstance(token, Case):
            # CASE WHEN col1 = 2 THEN "V1" WHEN col1 = "2" THEN "V2" END AS col2
            source_columns = [
                cqt
                for tk in token.get_sublists()
                for cqt in SqlParseColumn._extract_source_columns(tk)
            ]
        elif isinstance(token, Comparison):
            source_columns = SqlParseColumn._extract_source_columns(
                token.left
            ) + SqlParseColumn._extract_source_columns(token.right)
        elif isinstance(token, IdentifierList):
            source_columns = [
                cqt
                for tk in token.get_sublists()
                for cqt in SqlParseColumn._extract_source_columns(tk)
            ]
        elif isinstance(token, Identifier):
            real_name = token.get_real_name()
            # ignore function dtypes that don't need to check for extract column
            FUNC_DTYPE = ["decimal", "numeric"]
            has_function = any(
                isinstance(t, Function) and t.get_real_name() not in FUNC_DTYPE
                for t in token.tokens
            )
            is_kw = (
                Lexer.get_default_instance().is_keyword(real_name)
                if real_name is not None
                else False
            )
            if (
                # real name is None: col1=1 AS int
                real_name is None
                # real_name is decimal: case when col1 > 0 then col2 else col3 end as decimal(18, 0)
                or (real_name in FUNC_DTYPE and isinstance(token.tokens[-1], Function))
                or (is_kw and has_function)
            ):
                source_columns = [
                    cqt
                    for tk in token.get_sublists()
                    for cqt in SqlParseColumn._extract_source_columns(tk)
                ]
            else:
                # col1 AS col2
                source_columns = [
                    ColumnQualifierTuple(token.get_real_name(), token.get_parent_name())
                ]
        else:
            if token.ttype == T.Wildcard:
                # select *
                source_columns = [ColumnQualifierTuple(token.value, None)]
            else:
                # typically, T.Literal here
                source_columns = []
        return source_columns
