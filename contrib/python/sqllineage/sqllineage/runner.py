import logging
import warnings
from collections import OrderedDict
from typing import Any

from sqllineage import DEFAULT_DIALECT, SQLPARSE_DIALECT
from sqllineage.config import SQLLineageConfig
from sqllineage.core.holders import SQLLineageHolder
from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Table
from sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from sqllineage.core.parser.sqlparse.analyzer import SqlParseLineageAnalyzer
from sqllineage.drawing import draw_lineage_graph
from sqllineage.io import to_cytoscape
from sqllineage.utils.constant import LineageLevel
from sqllineage.utils.helpers import split, trim_comment

logger = logging.getLogger(__name__)


def lazy_method(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._evaluated:
            self._eval()
        return func(*args, **kwargs)

    return wrapper


def lazy_property(func):
    return property(lazy_method(func))


class LineageRunner:
    def __init__(
        self,
        sql: str,
        dialect: str = DEFAULT_DIALECT,
        metadata_provider: MetaDataProvider = DummyMetaDataProvider(),
        verbose: bool = False,
        silent_mode: bool = False,
        draw_options: dict[str, Any] | None = None,
        file_path: str = ".",
    ):
        """
        The entry point of SQLLineage after command line options are parsed.

        :param sql: a string representation of SQL statements.
        :param dialect: sql dialect
        :param metadata_provider: metadata service object providing table schema
        :param verbose: verbose flag indicating whether statement-wise lineage result will be shown
        :param silent_mode: boolean flag indicating whether to skip lineage analysis for unknown statement types
        :param file_path: path of the SQL file.
        """
        if dialect == SQLPARSE_DIALECT:
            warnings.warn(
                f"dialect `{SQLPARSE_DIALECT}` is deprecated, use `ansi` or dialect of your SQL instead. "
                f"`{SQLPARSE_DIALECT}` will be completely removed in v1.6.x",
                DeprecationWarning,
                stacklevel=2,
            )
        self._sql = sql
        self._file_path = file_path
        self._verbose = verbose
        self._draw_options = draw_options if draw_options else {}
        self._evaluated = False
        self._stmt: list[str] = []
        self._dialect = dialect
        self._metadata_provider = metadata_provider
        self._silent_mode = silent_mode

    @lazy_method
    def __str__(self):
        """
        print out the Lineage Summary.
        """
        statements = self.statements()
        source_tables = "\n    ".join(str(t) for t in self.source_tables)
        target_tables = "\n    ".join(str(t) for t in self.target_tables)
        combined = f"""Statements(#): {len(statements)}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
"""
        if self.intermediate_tables:
            intermediate_tables = "\n    ".join(
                str(t) for t in self.intermediate_tables
            )
            combined += f"""Intermediate Tables:
    {intermediate_tables}"""
        if self._verbose:
            result = ""
            for i, holder in enumerate(self._stmt_holders):
                stmt_short = statements[i].replace("\n", "")
                if len(stmt_short) > 50:
                    stmt_short = stmt_short[:50] + "..."
                content = str(holder).replace("\n", "\n    ")
                result += f"""Statement #{i + 1}: {stmt_short}
    {content}
"""
            combined = result + "==========\nSummary:\n" + combined
        return combined

    @lazy_method
    def to_cytoscape(self, level=LineageLevel.TABLE) -> list[dict[str, dict[str, str]]]:
        """
        to turn the DAG into cytoscape format.
        """
        if level == LineageLevel.COLUMN:
            return to_cytoscape(self._sql_holder.column_lineage_graph, compound=True)
        else:
            return to_cytoscape(self._sql_holder.table_lineage_graph)

    def draw(self) -> None:
        """
        to draw the lineage directed graph
        """
        draw_options = self._draw_options
        if draw_options.get("f") is None:
            draw_options.pop("f", None)
            draw_options["e"] = self._sql
            draw_options["dialect"] = self._dialect
            draw_options["metadata_provider"] = self._metadata_provider
        return draw_lineage_graph(**draw_options)

    @lazy_method
    def statements(self) -> list[str]:
        """
        a list of SQL statements.
        """
        return [trim_comment(s) for s in self._stmt]

    @lazy_property
    def source_tables(self) -> list[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.source_tables, key=lambda x: str(x))

    @lazy_property
    def target_tables(self) -> list[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.target_tables, key=lambda x: str(x))

    @lazy_property
    def intermediate_tables(self) -> list[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.intermediate_tables, key=lambda x: str(x))

    @lazy_method
    def get_column_lineage(
        self, exclude_path_ending_in_subquery=True, exclude_subquery_columns=False
    ) -> list[tuple[Column, Column]]:
        """
        a list of column tuple :class:`sqllineage.models.Column`
        """
        # sort by target column, and then source column
        return sorted(
            self._sql_holder.get_column_lineage(
                exclude_path_ending_in_subquery, exclude_subquery_columns
            ),
            key=lambda x: (str(x[-1]), str(x[0])),
        )

    def print_column_lineage(self) -> None:
        """
        print column level lineage to stdout
        """
        for path in self.get_column_lineage():
            print(" <- ".join(str(col) for col in reversed(path)))

    def print_table_lineage(self) -> None:
        """
        print table level lineage to stdout
        """
        print(str(self))

    def _eval(self):
        analyzer = (
            SqlParseLineageAnalyzer()
            if self._dialect == SQLPARSE_DIALECT
            else SqlFluffLineageAnalyzer(
                self._file_path, self._dialect, self._silent_mode
            )
        )
        if SQLLineageConfig.TSQL_NO_SEMICOLON and self._dialect == "tsql":
            self._stmt = analyzer.split_tsql(self._sql.strip())
        else:
            if SQLLineageConfig.TSQL_NO_SEMICOLON and self._dialect != "tsql":
                warnings.warn(
                    f"Dialect={self._dialect}, TSQL_NO_SEMICOLON will be ignored unless dialect is tsql"
                )
            self._stmt = split(self._sql.strip())

        with self._metadata_provider.session() as session:
            stmt_holders = []
            for stmt in self._stmt:
                stmt_holder = analyzer.analyze(stmt, session.metadata_provider)
                if write := stmt_holder.write:
                    tgt_table = next(iter(write))
                    if isinstance(tgt_table, Table) and (
                        tgt_columns := stmt_holder.get_table_columns(tgt_table)
                    ):
                        session.register_session_metadata(tgt_table, tgt_columns)
                stmt_holders.append(stmt_holder)
            self._stmt_holders = stmt_holders
            self._sql_holder = SQLLineageHolder.of(
                session.metadata_provider, *self._stmt_holders
            )
        self._evaluated = True

    @staticmethod
    def supported_dialects() -> dict[str, list[str]]:
        """
        an ordered dict (so we can make sure the default parser implementation comes first)
        with key, value as parser_name, dialect list respectively
        """
        dialects = OrderedDict(
            [
                (
                    SqlFluffLineageAnalyzer.PARSER_NAME,
                    SqlFluffLineageAnalyzer.SUPPORTED_DIALECTS,
                ),
                (
                    SqlParseLineageAnalyzer.PARSER_NAME,
                    SqlParseLineageAnalyzer.SUPPORTED_DIALECTS,
                ),
            ]
        )
        return dialects
