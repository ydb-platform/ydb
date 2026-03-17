from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotStatementLineageHolder,
    SqlGlotSubQueryLineageHolder,
    SqlGlotTable,
)


class DdlDropExtractor(LineageHolderExtractor):
    """
    DDL Drop queries lineage extractor for SqlGlot
    """

    SUPPORTED_STMT_TYPES = ["drop_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for DROP statements.

        :param statement: SqlGlot DROP expression
        :param context: AnalyzerContext
        :param is_sub_query: Not applicable for DROP statements
        :return: SqlGlotSubQueryLineageHolder with dropped tables
        """
        holder = SqlGlotStatementLineageHolder.of(self._init_holder(context))

        # Handle DROP TABLE/VIEW statements
        if isinstance(statement, exp.Drop):
            # DROP can have multiple tables in some dialects
            # statement.this contains the table(s) being dropped
            if statement.this:
                # Handle both single table and multiple tables
                tables = (
                    statement.this
                    if isinstance(statement.this, list)
                    else [statement.this]
                )
                for table_exp in tables:
                    if isinstance(table_exp, (exp.Table)):
                        table = SqlGlotTable.of(table_exp)
                        holder.add_drop(table)

        return holder
