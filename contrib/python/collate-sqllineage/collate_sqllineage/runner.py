import logging
import warnings
from typing import Dict, List, Optional, Tuple, Type, Union

from collate_sqllineage import DEFAULT_DIALECT, SQLPARSE_DIALECT
from collate_sqllineage.core.holders import SQLLineageHolder
from collate_sqllineage.core.models import Column, Table
from collate_sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from collate_sqllineage.core.parser.sqlglot.analyzer import SqlGlotLineageAnalyzer
from collate_sqllineage.core.parser.sqlparse.analyzer import SqlParseLineageAnalyzer
from collate_sqllineage.drawing import draw_lineage_graph
from collate_sqllineage.io import to_cytoscape
from collate_sqllineage.utils.constant import LineageLevel
from collate_sqllineage.utils.helpers import (
    sanitize_template_params,
    split,
    trim_comment,
)

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


class LineageRunner(object):
    def __init__(
        self,
        sql: str,
        dialect: str = DEFAULT_DIALECT,
        analyzer: Optional[
            Union[
                Type[SqlGlotLineageAnalyzer],
                Type[SqlFluffLineageAnalyzer],
                Type[SqlParseLineageAnalyzer],
            ]
        ] = None,
        encoding: Optional[str] = None,
        verbose: bool = False,
        draw_options: Optional[Dict[str, Optional[str]]] = None,
    ):
        """
        The entry point of SQLLineage after command line options are parsed.

        :param sql: a string representation of SQL statements.
        :param encoding: the encoding for sql string
        :param verbose: verbose flag indicate whether statement-wise lineage result will be shown
        """
        if dialect == SQLPARSE_DIALECT:
            warnings.warn(
                "dialect `non-validating` is deprecated, use `ansi` or dialect of your SQL instead. "
                "`non-validating` will stop being the default dialect in v1.5.x release "
                "and be completely removed in v1.6.x",
                DeprecationWarning,
                stacklevel=2,
            )
        self._encoding = encoding
        self._sql = sql
        self._verbose = verbose
        self._draw_options = draw_options if draw_options else {}
        self._evaluated = False
        self._stmt: List[str] = []
        self._dialect = dialect

        self._analyzer: Union[
            SqlGlotLineageAnalyzer, SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer
        ]
        if self._dialect == SQLPARSE_DIALECT or (
            analyzer is not None and issubclass(analyzer, SqlParseLineageAnalyzer)
        ):
            self._analyzer = SqlParseLineageAnalyzer()
        elif analyzer is not None:
            self._analyzer = analyzer(self._dialect)
        else:
            self._analyzer = SqlGlotLineageAnalyzer(self._dialect)

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
    def to_cytoscape(self, level=LineageLevel.TABLE) -> List[Dict[str, Dict[str, str]]]:
        """
        to turn the DAG into cytoscape format.
        """
        if level == LineageLevel.COLUMN:
            self._sql_holder.get_column_lineage()
            return to_cytoscape(self._sql_holder.column_lineage_graph, compound=True)
        else:
            return to_cytoscape(self._sql_holder.table_lineage_graph)

    def draw(self, dialect: str) -> None:
        """
        to draw the lineage directed graph
        """
        draw_options = self._draw_options
        if draw_options.get("f") is None:
            draw_options.pop("f", None)
            draw_options["e"] = self._sql
            draw_options["dialect"] = dialect
        return draw_lineage_graph(**draw_options)

    @lazy_method
    def statements(self) -> List[str]:
        """
        a list of SQL statements.
        """
        return [trim_comment(s) for s in self._stmt]

    @lazy_property
    def source_tables(self) -> List[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.source_tables, key=lambda x: str(x))

    @lazy_property
    def target_tables(self) -> List[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.target_tables, key=lambda x: str(x))

    @lazy_property
    def intermediate_tables(self) -> List[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.intermediate_tables, key=lambda x: str(x))

    @lazy_method
    def get_column_lineage(self, exclude_subquery=True) -> List[Tuple[Column, Column]]:
        """
        a list of column tuple :class:`sqllineage.models.Column`
        """
        # sort by target column, and then source column
        return sorted(
            self._sql_holder.get_column_lineage(exclude_subquery),
            key=lambda x: (str(x[-1]), str(x[0])),
        )

    def print_column_lineage(self) -> None:
        """
        print column level lineage to stdout
        """
        print("Column Lineage:")
        for path in self.get_column_lineage():
            print("    " + " <- ".join(str(col) for col in reversed(path)))

    def print_table_lineage(self) -> None:
        """
        print table level lineage to stdout
        """
        print(str(self))

    def _eval(self):
        self._stmt = split(sanitize_template_params(self._sql.strip()))
        self._stmt_holders = [self._analyzer.analyze(stmt) for stmt in self._stmt]
        if hasattr(self._analyzer, "parsed_result"):
            self._parsed_result = self._analyzer.parsed_result
        else:
            self._parsed_result = None
        self._sql_holder = SQLLineageHolder.of(*self._stmt_holders)
        self._evaluated = True
