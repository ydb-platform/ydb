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


class DdlAlterExtractor(LineageHolderExtractor):
    """
    DDL ALTER queries lineage extractor for sqlglot
    """

    SUPPORTED_STMT_TYPES = ["alter_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for ALTER statements
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return 'SqlGlotSubQueryLineageHolder' object
        """
        holder = SqlGlotStatementLineageHolder.of(self._init_holder(context))

        if isinstance(statement, exp.Alter):
            # Check for actions
            actions = statement.args.get("actions", [])

            # Check for RENAME action
            has_rename = any(isinstance(action, exp.AlterRename) for action in actions)

            if has_rename:
                # For RENAME, track the rename edge
                old_table = (
                    SqlGlotTable.of(statement.this)
                    if isinstance(statement.this, exp.Table)
                    else None
                )
                for action in actions:
                    if isinstance(action, exp.AlterRename) and action.this:
                        new_table = (
                            SqlGlotTable.of(action.this)
                            if isinstance(action.this, exp.Table)
                            else None
                        )
                        if old_table and new_table:
                            holder.add_rename(old_table, new_table)
            else:
                # For other ALTER operations, the table being altered is the target
                if statement.this and isinstance(statement.this, exp.Table):
                    holder.add_write(SqlGlotTable.of(statement.this))

                # Check for SWAP WITH action
                for action in actions:
                    if isinstance(action, exp.SwapTable):
                        # The table we're swapping with is the source
                        if action.this and isinstance(action.this, exp.Table):
                            holder.add_read(SqlGlotTable.of(action.this))

        return holder
