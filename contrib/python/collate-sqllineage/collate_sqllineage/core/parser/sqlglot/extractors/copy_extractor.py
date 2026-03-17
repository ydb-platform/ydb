from sqlglot import exp
from sqlglot.expressions import Expression

from collate_sqllineage.core.models import AnalyzerContext, Location, Path
from collate_sqllineage.core.parser.sqlglot.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlglot.models import (
    SqlGlotSubQueryLineageHolder,
    SqlGlotTable,
)


class CopyExtractor(LineageHolderExtractor):
    """
    COPY statement lineage extractor for sqlglot (Postgres, Redshift, Snowflake).
    Handles:
    - COPY table FROM 'path'
    - COPY INTO table FROM 'path'
    - COPY INTO @location FROM table (Snowflake unload)
    - COPY INTO table FROM @location (Snowflake load)
    """

    SUPPORTED_STMT_TYPES = ["copy_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    @staticmethod
    def _is_location(table_expr: exp.Table) -> bool:
        """
        Check if a Table expression represents a named location (e.g. Snowflake stage).
        These are parsed as Table(this=Var(this=@NAME)).
        """
        return isinstance(table_expr.this, exp.Var) and table_expr.this.name.startswith(
            "@"
        )

    @staticmethod
    def _to_location(table_expr: exp.Table) -> Location:
        """Convert a location Table expression to a Location object."""
        return Location(table_expr.this.name)

    def _resolve_file_expr(
        self, file_expr: Expression, holder: SqlGlotSubQueryLineageHolder
    ) -> None:
        """
        Resolve a file expression from the COPY files list and add it as a read source.
        """
        if isinstance(file_expr, exp.Literal):
            if file_expr.this:
                holder.add_read(Path(file_expr.this))
        elif isinstance(file_expr, exp.Table):
            if self._is_location(file_expr):
                holder.add_read(self._to_location(file_expr))
            elif isinstance(file_expr.this, exp.Literal):
                if file_expr.this.this:
                    holder.add_read(Path(file_expr.this.this))
            else:
                holder.add_read(SqlGlotTable.of(file_expr))
        elif isinstance(file_expr, exp.Subquery):
            # COPY INTO @location FROM (SELECT ... FROM table)
            for table in file_expr.find_all(exp.Table):
                if self._is_location(table):
                    holder.add_read(self._to_location(table))
                else:
                    holder.add_read(SqlGlotTable.of(table))

    def extract(
        self,
        statement: Expression,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlGlotSubQueryLineageHolder:
        """
        Extract lineage for COPY statements.
        :param statement: a sqlglot Expression
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is a subquery
        :return 'SqlGlotSubQueryLineageHolder' object
        """
        holder = self._init_holder(context)

        if isinstance(statement, exp.Copy):
            target = statement.this
            files = (
                statement.args.get("files", []) if hasattr(statement, "args") else []
            )

            if isinstance(target, exp.Table) and self._is_location(target):
                # Unload: COPY INTO @location FROM table
                holder.add_write(self._to_location(target))
            elif isinstance(target, exp.Table):
                # Load: COPY INTO table FROM @location/path
                holder.add_write(SqlGlotTable.of(target))

            for file_expr in files or []:
                self._resolve_file_expr(file_expr, holder)

        return holder
