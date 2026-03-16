from typing import List

from sqlglot.expressions import Expression

from collate_sqllineage.core.models import AnalyzerContext, SubQuery
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotSubQuery,
    SqlGlotSubQueryLineageHolder,
)
from collate_sqllineage.core.parser.sqlglot.utils import get_subqueries, is_subquery


class LineageHolderExtractor:
    """
    Abstract class implementation for extract 'SqlGlotSubQueryLineageHolder' from different statement types
    """

    SUPPORTED_STMT_TYPES: List[str] = []

    def __init__(self, dialect: str):
        self.dialect = dialect

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a statement type string
        """
        return statement_type in self.SUPPORTED_STMT_TYPES

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return 'SqlGlotSubQueryLineageHolder' object
        """
        raise NotImplementedError

    @classmethod
    def parse_subquery(cls, expression: Expression) -> List[SubQuery]:
        """
        Parse subqueries from an expression
        :param expression: expression to check for subqueries
        :return: A list of SubQuery objects
        """
        result: List[SubQuery] = []

        if is_subquery(expression):
            result = [SqlGlotSubQuery.of(expression, None)]
        else:
            result = cls._parse_subquery(get_subqueries(expression))

        return result

    @classmethod
    def _parse_subquery(cls, subqueries: List) -> List[SubQuery]:  # type: ignore[type-arg]
        """
        Convert subquery tuples to SqlGlotSubQuery objects
        :param subqueries: list of SubQueryTuple (named tuples with parenthesis and alias fields)
        :return: list of SqlGlotSubQuery objects
        """
        return [SqlGlotSubQuery.of(sq.parenthesis, sq.alias) for sq in subqueries]

    @staticmethod
    def _init_holder(context: AnalyzerContext) -> SqlGlotSubQueryLineageHolder:
        """
        Initialize lineage holder for a given 'AnalyzerContext'
        :param context: a previous context that the lineage extractor must consider
        :return: an initialized SqlGlotSubQueryLineageHolder
        """
        holder = SqlGlotSubQueryLineageHolder()

        if context.prev_cte is not None:
            for cte in context.prev_cte:
                holder.add_cte(cte)

        if context.prev_write is not None:
            for write in context.prev_write:
                holder.add_write(write)

        if context.prev_write is None and context.subquery is not None:
            holder.add_write(context.subquery)

        if context.target_columns:
            holder.add_target_column(*context.target_columns)

        return holder
