from functools import reduce
from operator import add

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Path, SubQuery, Table
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    find_from_expression_element,
    find_table_identifier,
    is_subquery,
    list_child_segments,
    list_join_clause,
    list_subqueries,
)
from sqllineage.utils.constant import NodeTag
from sqllineage.utils.entities import AnalyzerContext, SubQueryTuple
from sqllineage.utils.helpers import escape_identifier_name


class BaseExtractor:
    """
    Abstract class implementation for extract 'SubQueryLineageHolder' from different statement types
    """

    SUPPORTED_STMT_TYPES: list[str] = []

    def __init__(self, dialect: str, metadata_provider: MetaDataProvider):
        self.dialect = dialect
        self.metadata_provider = metadata_provider

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
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :return 'SubQueryLineageHolder' object
        """
        raise NotImplementedError

    @classmethod
    def find_table(cls, segment: BaseSegment) -> Table | None:
        table = None
        if segment.type in ["table_reference", "object_reference"]:
            table = SqlFluffTable.of(segment)
        return table

    @classmethod
    def list_subquery(cls, segment: BaseSegment) -> list[SubQuery]:
        """
        The parse_subquery function takes a segment as an argument.
        :param segment: segment to determine if it is a subquery
        :return: A list of `SqlFluffSubQuery` objects, otherwise, if the segment is not matching any of the expected
        types it returns an empty list.
        """
        result: list[SubQuery] = []
        identifiers = segment.get_children("from_expression")
        if identifiers and len(identifiers) > 1:
            # for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            return reduce(
                add,
                [
                    cls._parse_subquery(list_subqueries(identifier))
                    for identifier in identifiers
                ],
                [],
            )
        if segment.type in ["select_clause", "from_clause", "where_clause"]:
            result = cls._parse_subquery(list_subqueries(segment))
        elif is_subquery(segment):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SqlFluffSubQuery.of(segment, None)]
        return result

    def _list_table_from_from_clause_or_join_clause(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> list[Table | SubQuery | Path]:
        """
        Extract table from from_clause or join_clause, join_clause is a child node of from_clause.
        """
        tables = []
        if segment.type in ["from_clause", "join_clause", "update_statement"]:
            if len(from_expressions := segment.get_children("from_expression")) > 1:
                # SQL89 style of join
                for from_expression in from_expressions:
                    if from_expression_element := find_from_expression_element(
                        from_expression
                    ):
                        tables += self._add_dataset_from_expression_element(
                            from_expression_element, holder
                        )
            else:
                if from_expression_element := find_from_expression_element(segment):
                    tables += self._add_dataset_from_expression_element(
                        from_expression_element, holder
                    )
                for join_clause in list_join_clause(segment):
                    tables += self._list_table_from_from_clause_or_join_clause(
                        join_clause, holder
                    )

        return tables

    @staticmethod
    def _add_dataset_from_expression_element(
        segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> list[Table | SubQuery | Path]:
        """
        Append tables and subqueries identified in the 'from_expression_element' type segment to the table and
        holder extra subqueries sets
        """
        tables: list[Table | SubQuery | Path] = []
        all_segments = [
            seg for seg in list_child_segments(segment) if seg.type != "keyword"
        ]
        if table_expression := segment.get_child("table_expression"):
            if table_expression.get_child("function"):
                # for UNNEST or generator function, no dataset involved
                return tables
        first_segment = all_segments[0]
        if first_segment.type == "bracketed":
            if table_expression := first_segment.get_child("table_expression"):
                if table_expression.get_child("values_clause"):
                    # (VALUES ...) AS alias, no dataset involved
                    return tables
        subqueries = list_subqueries(segment)
        if subqueries:
            for sq in subqueries:
                bracketed, alias = sq
                read_sq = SqlFluffSubQuery.of(bracketed, alias)
                tables.append(read_sq)
        else:
            table_identifier = find_table_identifier(segment)
            if table_identifier:
                subquery_flag = False
                alias_expression = None
                if len(all_segments) > 1 and all_segments[1].type == "alias_expression":
                    alias_expression = all_segments[1]
                elif len(all_segments) == 1 and all_segments[0].type == "bracketed":
                    # the alias_expression may be deeply nested in the bracketed segment
                    alias_expression = next(
                        all_segments[0].recursive_crawl("alias_expression"), None
                    )
                alias = None
                if alias_expression is not None:
                    all_segments = list_child_segments(alias_expression)
                    alias = str(
                        all_segments[1].raw
                        if len(all_segments) > 1
                        else all_segments[0].raw
                    )
                if "." not in table_identifier.raw:
                    cte_dict = {s.alias: s for s in holder.cte}
                    cte = cte_dict.get(escape_identifier_name(table_identifier.raw))
                    if cte is not None:
                        # could reference CTE with or without alias
                        tables.append(
                            SqlFluffSubQuery.of(
                                cte.query,
                                alias or table_identifier.raw,
                            )
                        )
                        subquery_flag = True
                if subquery_flag is False:
                    if table_identifier.type == "file_reference":
                        tables.append(
                            Path(
                                escape_identifier_name(
                                    table_identifier.segments[-1].raw
                                )
                            )
                        )
                    else:
                        tables.append(SqlFluffTable.of(table_identifier, alias=alias))

        return tables

    @classmethod
    def _parse_subquery(cls, subqueries: list[SubQueryTuple]) -> list[SubQuery]:
        """
        Convert a list of 'SqlFluffSubQueryTuple' to 'SqlFluffSubQuery'
        :param subqueries:  a list of 'SqlFluffSubQueryTuple'
        :return:  a list of 'SqlFluffSubQuery'
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in subqueries
        ]

    def delegate_to(
        self,
        extractor_cls: "type[BaseExtractor]",
        segment: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        """
        delegate to another type of extractor to extract
        """
        return extractor_cls(self.dialect, self.metadata_provider).extract(
            segment, context
        )

    def extract_subquery(
        self, subqueries: list[SubQuery], holder: SubQueryLineageHolder
    ):
        """
        extract subqueries collected from statement-level segment
        """
        from .cte import CteExtractor
        from .select import SelectExtractor

        for sq in subqueries:
            extractor_cls = (
                CteExtractor
                if sq.query.get_child("with_compound_statement")
                else SelectExtractor
            )
            subquery_holder = extractor_cls(
                self.dialect, self.metadata_provider
            ).extract(sq.query, AnalyzerContext(cte=holder.cte, write={sq}))
            # remove WRITE tag from subquery so that the combined holder won't have multiple WRITE dataset
            subquery_holder.go.update_vertices(sq, **{NodeTag.WRITE: False})
            holder |= subquery_holder

    @staticmethod
    def _init_holder(context: AnalyzerContext) -> SubQueryLineageHolder:
        """
        Initialize lineage holder for a given 'AnalyzerContext'
        :param context: a previous context that the lineage extractor must consider
        :return: an initialized SubQueryLineageHolder
        """
        holder = SubQueryLineageHolder()

        if context.cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.cte:
                holder.add_cte(cte)

        if context.write is not None:
            # If within subquery, then manually add subquery as target table
            for write in context.write:
                holder.add_write(write)

        if context.write_columns:
            # target columns can be referred while creating column level lineage
            holder.add_write_column(*context.write_columns)

        return holder
