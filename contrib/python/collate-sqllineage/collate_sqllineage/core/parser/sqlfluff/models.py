from typing import List, Optional, Tuple, no_type_check

from sqlfluff.core.parser import BaseSegment

from collate_sqllineage import SQLPARSE_DIALECT
from collate_sqllineage.core.models import Column, DataFunction, Schema, SubQuery, Table
from collate_sqllineage.core.parser.sqlfluff.utils import (
    get_identifier,
    get_innermost_bracketed,
    is_subquery,
    is_wildcard,
    retrieve_segments,
)
from collate_sqllineage.utils.entities import ColumnQualifierTuple
from collate_sqllineage.utils.helpers import escape_identifier_name

NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE = [
    "function",
    "over_clause",
    "expression",
    "case_expression",
    "when_clause",
    "else_clause",
    "select_clause_element",
    "cast_expression",
    "function_contents",
]

SOURCE_COLUMN_SEGMENT_TYPE = NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE + [
    "identifier",
    "column_reference",
]

IDENTIFIER_KEYWORD = "IDENTIFIER"


class SqlFluffTable(Table):
    """
    Data Class for SqlFluffTable
    """

    @no_type_check
    @staticmethod
    def _get_table_identifier(table: BaseSegment) -> str:
        if table.type == "identifier":
            return table.raw

        if (
            table.segments[0].type == "keyword"
            and table.segments[0].raw_upper == IDENTIFIER_KEYWORD
        ):
            bracketed_identifier = get_innermost_bracketed(table)
            if bracketed_identifier:
                return str(bracketed_identifier.get_child("identifier").raw)

        return table.segments[0].raw

    @staticmethod
    def of(table: BaseSegment, alias: Optional[str] = None) -> Table:
        """
        Build an object of type 'Table'
        :param table: table segment to be processed
        :param alias: alias of the table segment
        :return: 'Table' object
        """
        dot_idx = None
        for idx in range(len(table.segments) - 2, -1, -1):
            token = table.segments[idx]
            if bool(token.type == "symbol"):
                dot_idx, _ = idx, token
                break
        real_name = (
            table.segments[dot_idx + 1].raw
            if dot_idx
            else SqlFluffTable._get_table_identifier(table)
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in table.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return Table(real_name, schema, **kwargs)


class SqlFluffFunction(DataFunction):
    """
    Data Class for SqlFluffFunction
    """

    @no_type_check
    @staticmethod
    def _get_function_identifier(function: BaseSegment) -> str:
        if function.type == "identifier":
            return function.raw

        if (
            function.segments[0].type == "keyword"
            and function.segments[0].raw_upper == IDENTIFIER_KEYWORD
        ):
            bracketed_identifier = get_innermost_bracketed(function)
            if bracketed_identifier:
                return str(bracketed_identifier.get_child("identifier").raw)

        return function.segments[0].raw

    @staticmethod
    def of(function: BaseSegment, alias: Optional[str] = None) -> DataFunction:
        """
        Build an object of type 'Function'
        :param function: function segment to be processed
        :param alias: alias of the function segment
        :return: 'Function' object
        """
        dot_idx = None
        for idx in range(len(function.segments) - 2, -1, -1):
            token = function.segments[idx]
            if bool(token.type == "symbol"):
                dot_idx, _ = idx, token
                break
        real_name = (
            function.segments[dot_idx + 1].raw
            if dot_idx
            else SqlFluffFunction._get_function_identifier(function)
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in function.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return DataFunction(real_name, schema, **kwargs)


class SqlFluffSubQuery(SubQuery):
    """
    Data Class for SqlFluffSubQuery
    """

    @staticmethod
    def of(subquery: BaseSegment, alias: Optional[str]) -> SubQuery:
        """
        Build a 'SubQuery' object
        :param subquery: subquery segment
        :param alias: subquery alias
        :return: 'SubQuery' object
        """
        return SubQuery(subquery, subquery.raw, alias)


class SqlFluffColumn(Column):
    """
    Data Class for SqlFluffColumn
    """

    @staticmethod
    def of(column: BaseSegment, **kwargs) -> Column:
        """
        Build a 'SqlFluffSubQuery' object
        :param column: column segment
        :return:
        """
        if column.type == "select_clause_element":
            source_columns, alias = SqlFluffColumn._get_column_and_alias(column)
            if alias:
                return Column(
                    alias,
                    source_columns=source_columns,
                )
            if source_columns:
                sub_segments = retrieve_segments(column)
                column_name = None
                for sub_segment in sub_segments:
                    if sub_segment.type == "column_reference":
                        column_name = get_identifier(sub_segment)
                    elif sub_segment.type == "wildcard_expression":
                        column_name = "*"
                    elif sub_segment.type == "expression":
                        # special handling for postgres style type cast, col as target column name instead of col::type
                        sub2_segments = retrieve_segments(sub_segment)
                        if len(sub2_segments) == 1:
                            if (sub2_segments[0]).type == "cast_expression":
                                sub3_segments = retrieve_segments(sub2_segments[0])
                                if len(retrieve_segments(sub2_segments[0])) == 2:
                                    if (sub3_segments[0]).type == "column_reference":
                                        column_name = get_identifier(sub3_segments[0])
                return Column(
                    column.raw if column_name is None else column_name,
                    source_columns=source_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = SqlFluffColumn._extract_source_columns(column)
        return Column(
            column.raw,
            source_columns=source_columns,
        )

    @staticmethod
    def _extract_source_columns(segment: BaseSegment) -> List[ColumnQualifierTuple]:
        """
        :param segment: segment to be processed
        :return: list of extracted source columns
        """
        if segment.type == "identifier":
            return [ColumnQualifierTuple(segment.raw, None)]
        if is_wildcard(segment):
            if segment.segments:
                wildcard_segment = segment.segments[0]
                parent, column = SqlFluffColumn._get_column_and_parent(wildcard_segment)
            else:
                parent, column = None, segment.raw
            return [ColumnQualifierTuple(column, parent)]
        if segment.type == "column_reference":
            parent, column = SqlFluffColumn._get_column_and_parent(segment)
            return [ColumnQualifierTuple(column, parent)]
        if segment.type in NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE:
            sub_segments = retrieve_segments(segment)
            col_list = []
            for sub_segment in sub_segments:
                if sub_segment.type == "bracketed":
                    if is_subquery(sub_segment):
                        col_list += SqlFluffColumn._get_column_from_subquery(
                            sub_segment
                        )
                    else:
                        col_list += SqlFluffColumn._get_column_from_parenthesis(
                            sub_segment
                        )
                elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                    sub_segment
                ):
                    res = SqlFluffColumn._extract_source_columns(sub_segment)
                    col_list.extend(res)
            return col_list
        return []

    @staticmethod
    def _get_column_from_subquery(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        """
        :param sub_segment: segment to be processed
        :return: A list of source columns from a segment
        """
        # This is to avoid circular import
        from collate_sqllineage.runner import LineageRunner

        src_cols = [
            lineage[0]
            for lineage in LineageRunner(
                sub_segment.raw, dialect=SQLPARSE_DIALECT
            ).get_column_lineage(exclude_subquery=False)
        ]
        source_columns = [
            ColumnQualifierTuple(
                src_col.raw_name,
                (
                    src_col.parent.raw_name
                    if (src_col.parent and hasattr(src_col.parent, "raw_name"))
                    else None
                ),
            )
            for src_col in src_cols
        ]
        return source_columns

    @staticmethod
    def _get_column_from_parenthesis(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        """
        :param sub_segment: segment to be processed
        :return: list of columns and alias from the segment
        """
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment)
        if col:
            return col
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, False)
        return col if col else []

    @staticmethod
    def _get_column_and_alias(
        segment: BaseSegment, check_bracketed: bool = True
    ) -> Tuple[List[ColumnQualifierTuple], Optional[str]]:
        alias = None
        columns = []
        sub_segments = retrieve_segments(segment, check_bracketed)
        for sub_segment in sub_segments:
            if sub_segment.type == "alias_expression":
                alias = get_identifier(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = SqlFluffColumn._extract_source_columns(sub_segment)
                columns += res if res else []

        return columns, alias

    @staticmethod
    def _get_column_and_parent(col_segment: BaseSegment) -> Tuple[Optional[str], str]:
        identifiers = retrieve_segments(col_segment)
        if len(identifiers) > 1:
            return identifiers[-2].raw, identifiers[-1].raw
        return None, identifiers[-1].raw
