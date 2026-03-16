from functools import reduce
from operator import add
from typing import List, Tuple

from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import AnalyzerContext, SubQuery
from collate_sqllineage.core.parser.sqlfluff.handlers.base import (
    ConditionalSegmentBaseHandler,
    SegmentBaseHandler,
)
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from collate_sqllineage.core.parser.sqlfluff.utils import (
    get_multiple_identifiers,
    get_subqueries,
    is_subquery,
)
from collate_sqllineage.utils.entities import SubQueryTuple


class LineageHolderExtractor:
    """
    Abstract class implementation for extract 'SubQueryLineageHolder' from different statement types
    """

    SUPPORTED_STMT_TYPES: List[str] = []

    def __init__(self, dialect: str):
        self.dialect = dialect

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.SUPPORTED_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SubQueryLineageHolder' object
        """
        raise NotImplementedError

    @classmethod
    def parse_subquery(cls, segment: BaseSegment) -> List[SubQuery]:
        """
        The parse_subquery function takes a segment as an argument.
        :param segment: segment to determine if it is a subquery
        :return: A list of `SqlFluffSubQuery` objects, otherwise, if the segment is not matching any of the expected
        types it returns an empty list.
        """
        result: List[SubQuery] = []
        identifiers = get_multiple_identifiers(segment)
        if identifiers and len(identifiers) > 1:
            # for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            return reduce(
                add,
                [
                    cls._parse_subquery(get_subqueries(identifier))
                    for identifier in identifiers
                ],
                [],
            )
        if segment.type in ["select_clause", "from_clause", "where_clause"]:
            result = cls._parse_subquery(get_subqueries(segment))
        elif is_subquery(segment):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SqlFluffSubQuery.of(segment, None)]
        return result

    @classmethod
    def _parse_subquery(cls, subqueries: List[SubQueryTuple]) -> List[SubQuery]:
        """
        Convert a list of 'SqlFluffSubQueryTuple' to 'SqlFluffSubQuery'
        :param subqueries:  a list of 'SqlFluffSubQueryTuple'
        :return:  a list of 'SqlFluffSubQuery'
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in subqueries
        ]

    def _init_handlers(
        self,
    ) -> Tuple[List[SegmentBaseHandler], List[ConditionalSegmentBaseHandler]]:
        """
        Initialize handlers used during the extraction of lineage
        :return: A tuple with a list of SegmentBaseHandler and ConditionalSegmentBaseHandler
        """
        handlers: List[SegmentBaseHandler] = [
            handler_cls(self.dialect)
            for handler_cls in SegmentBaseHandler.__subclasses__()
        ]
        conditional_handlers = [
            handler_cls(self.dialect)
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        return handlers, conditional_handlers

    @staticmethod
    def _init_holder(context: AnalyzerContext) -> SubQueryLineageHolder:
        """
        Initialize lineage holder for a given 'AnalyzerContext'
        :param context: a previous context that the lineage extractor must consider
        :return: an initialized SubQueryLineageHolder
        """
        holder = SubQueryLineageHolder()

        if context.prev_cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.prev_cte:
                holder.add_cte(cte)

        if context.prev_write is not None:
            # If within subquery, then manually add subquery as target table
            for write in context.prev_write:
                holder.add_write(write)

        if context.prev_write is None and context.subquery is not None:
            # If within subquery, then manually add subquery as target table
            holder.add_write(context.subquery)

        if context.target_columns:
            # target columns can be referred while creating column level lineage
            holder.add_target_column(*context.target_columns)

        return holder
