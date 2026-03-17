import itertools

from sqllineage.core.graph import get_graph_operator_class
from sqllineage.core.graph_operator import GraphOperator
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Path, Schema, SubQuery, Table
from sqllineage.utils.constant import EdgeDirection, EdgeTag, EdgeType, NodeTag

DATASET_CLASSES = (Path, Table)


class ColumnLineageMixin:
    def get_column_lineage(
        self, exclude_path_ending_in_subquery=True, exclude_subquery_columns=False
    ) -> set[tuple[Column, ...]]:
        """
        :param exclude_path_ending_in_subquery:  exclude_subquery rename to exclude_path_ending_in_subquery
               exclude column from SubQuery in the ending path
        :param exclude_subquery_columns: exclude column from SubQuery in the path.

        return a list of column tuple :class:`sqllineage.models.Column`
        """
        self.go: GraphOperator  # For mypy attribute checking
        # filter all the column node in the graph
        column_graph = self.go.get_sub_graph(
            *[v for v in self.go.retrieve_vertices_by_props() if isinstance(v, Column)]
        )
        source_columns = column_graph.retrieve_source_vertices()
        target_columns = column_graph.retrieve_target_vertices()
        # handle column-level self-loop case like table-level
        selfloop_columns = column_graph.retrieve_selfloop_vertices()
        for column_group in [source_columns, target_columns]:
            for column in selfloop_columns:
                if column not in column_group:
                    column_group.append(column)
        # if a column lineage path ends at SubQuery, then it should be pruned
        if exclude_path_ending_in_subquery:
            target_columns = {
                node for node in target_columns if isinstance(node.parent, Table)
            }
        columns = set()
        for source, target in itertools.product(source_columns, target_columns):
            simple_paths = self.go.list_lineage_paths(source, target)
            for path in simple_paths:
                if exclude_subquery_columns:
                    path = [
                        node for node in path if not isinstance(node.parent, SubQuery)
                    ]
                    if len(path) > 1:
                        columns.add(tuple(path))
                else:
                    columns.add(tuple(path))
        return columns


class SubQueryLineageHolder(ColumnLineageMixin):
    """
    SubQuery/Query Level Lineage Result.

    SubQueryLineageHolder will hold attributes like read, write, cte.

    Each of them is a set[:class:`sqllineage.core.models.Table`].

    This is the most atomic representation of lineage result.
    """

    def __init__(self) -> None:
        self.go = get_graph_operator_class()()

    def __or__(self, other):
        self.go.merge(other.go)
        return self

    def _property_getter(self, prop) -> set[SubQuery | Table]:
        vertices: list[SubQuery | Table] = self.go.retrieve_vertices_by_props(
            **{prop: True}
        )
        return {t for t in vertices}

    def _property_setter(self, value, prop) -> None:
        self.go.add_vertex_if_not_exist(value, **{prop: True})

    @property
    def read(self) -> set[SubQuery | Table]:
        return self._property_getter(NodeTag.READ)

    def add_read(self, value) -> None:
        self._property_setter(value, NodeTag.READ)
        # the same table can be added (in SQL: joined) multiple times with different alias
        if hasattr(value, "alias"):
            self.go.add_edge_if_not_exist(value, value.alias, EdgeType.HAS_ALIAS)

    @property
    def write(self) -> set[SubQuery | Table]:
        # SubQueryLineageHolder.write can return a single SubQuery or Table, or both when __or__ together.
        # This is different from StatementLineageHolder.write, where Table is the only possibility.
        return self._property_getter(NodeTag.WRITE)

    def add_write(self, value) -> None:
        self._property_setter(value, NodeTag.WRITE)

    @property
    def cte(self) -> set[SubQuery]:
        return self._property_getter(NodeTag.CTE)  # type: ignore

    def add_cte(self, value) -> None:
        self._property_setter(value, NodeTag.CTE)

    @property
    def write_columns(self) -> list[Column]:
        """
        return a list of columns that write table contains.
        It's either manually added via `add_write_column` if specified in DML
        or automatic added via `add_column_lineage` after parsing from SELECT
        """
        tgt_cols = []
        if tgt_tbl := self._get_target_table():
            tbl_col_edges = self.go.retrieve_edges_by_vertex(
                tgt_tbl, EdgeDirection.OUT, EdgeType.HAS_COLUMN
            )
            tgt_col_with_idx: list[tuple[Column, int]] = sorted(
                [(e.target, e.attributes.get(EdgeTag.INDEX, 0)) for e in tbl_col_edges],
                key=lambda x: x[1],
            )
            tgt_cols = [x[0] for x in tgt_col_with_idx]
        return tgt_cols

    def add_write_column(self, *tgt_cols: Column) -> None:
        """
        in case of DML with column specified, like:

        .. code-block:: sql

            INSERT INTO tab1 (col1, col2)
            SELECT col3, col4

        this method is called to make sure tab1 has column col1 and col2 instead of col3 and col4
        """
        if self.write:
            tgt_tbl = list(self.write)[0]
            for idx, tgt_col in enumerate(tgt_cols):
                tgt_col.parent = tgt_tbl
                self.go.add_edge_if_not_exist(
                    tgt_tbl, tgt_col, EdgeType.HAS_COLUMN, **{EdgeTag.INDEX: idx}
                )

    def add_column_lineage(self, src: Column, tgt: Column) -> None:
        """
        link source column to target.
        """
        self.go.add_edge_if_not_exist(src, tgt, EdgeType.LINEAGE)
        self.go.add_edge_if_not_exist(tgt.parent, tgt, EdgeType.HAS_COLUMN)
        self.go.add_edge_if_not_exist(src.parent, src, EdgeType.HAS_COLUMN)

    def get_table_columns(self, table: Table | SubQuery) -> list[Column]:
        return [
            edge.target
            for edge in self.go.retrieve_edges_by_vertex(
                table, EdgeDirection.OUT, EdgeType.HAS_COLUMN
            )
            if isinstance(edge.target, Column) and edge.target.raw_name != "*"
        ]

    def expand_wildcard(self, metadata_provider: MetaDataProvider) -> None:
        if tgt_table := self._get_target_table():
            for column in self.write_columns:
                if column.raw_name == "*":
                    tgt_wildcard = column
                    src_wildcards = self.get_source_columns(tgt_wildcard)
                    # Enable positional mapping only for UNION-of-* into a real table; avoid join/subquery cases
                    wildcard_in_union = (
                        isinstance(tgt_table, Table)
                        and len(self.write_columns) == 1
                        and len(src_wildcards) > 1
                    )
                    for src_wildcard in src_wildcards:
                        if source_table := src_wildcard.parent:
                            src_table_columns = []
                            if isinstance(source_table, SubQuery):
                                # the columns of SubQuery can be inferred from graph
                                src_table_columns = self.get_table_columns(source_table)
                            elif isinstance(source_table, Table) and metadata_provider:
                                # search by metadata service
                                src_table_columns = metadata_provider.get_table_columns(
                                    source_table
                                )
                            if src_table_columns:
                                self._replace_wildcard(
                                    tgt_table,
                                    src_table_columns,
                                    tgt_wildcard,
                                    src_wildcard,
                                    wildcard_in_union=wildcard_in_union,
                                )

    def get_alias_mapping_from_table_group(
        self, table_group: list[Path | Table | SubQuery]
    ) -> dict[str, Path | Table | SubQuery]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        """
        alias_map = {
            edge.target: edge.source
            for edge in self.go.retrieve_edges_by_label(label=EdgeType.HAS_ALIAS)
            if edge.source in table_group
        }
        unqualified_map = {
            table.raw_name: table for table in table_group if isinstance(table, Table)
        }
        qualified_map = {
            str(table): table for table in table_group if isinstance(table, Table)
        }
        return alias_map | unqualified_map | qualified_map

    def _get_target_table(self) -> SubQuery | Table | None:
        table = None
        if write_only := self.write.difference(self.read):
            table = next(iter(write_only))
        return table

    def get_source_columns(self, node: Column) -> list[Column]:
        return [
            e.source
            for e in self.go.retrieve_edges_by_vertex(
                node, EdgeDirection.IN, EdgeType.LINEAGE
            )
            if isinstance(e.source, Column)
        ]

    def _replace_wildcard(
        self,
        tgt_table: Table | SubQuery,
        src_table_columns: list[Column],
        tgt_wildcard: Column,
        src_wildcard: Column,
        wildcard_in_union: bool = False,
    ) -> None:
        target_columns = self.get_table_columns(tgt_table)
        for idx, src_col in enumerate(src_table_columns):
            # Prefer positional mapping only when enabled (e.g., subsequent UNION arms)
            if wildcard_in_union and idx < len(target_columns):
                target_col = target_columns[idx]
            else:
                # otherwise, if target column with same name exists (union scenario), reuse it; or create a new one
                existing_col = next(
                    (c for c in target_columns if c.raw_name == src_col.raw_name),
                    None,
                )
                if existing_col is None:
                    new_column = Column(src_col.raw_name)
                    new_column.parent = tgt_table
                    self.go.add_edge_if_not_exist(
                        tgt_table, new_column, EdgeType.HAS_COLUMN
                    )
                    target_col = new_column
                    # keep local target_columns in sync to preserve order for the same call
                    target_columns.append(target_col)
                else:
                    target_col = existing_col
            # ensure source column node exists and link lineage
            if src_col.parent is not None:
                self.go.add_edge_if_not_exist(
                    src_col.parent, src_col, EdgeType.HAS_COLUMN
                )
            self.go.add_edge_if_not_exist(src_col, target_col, EdgeType.LINEAGE)
        # preserve SubQuery wildcards in the lineage graph to maintain the wildcard chain in case of partial expansion
        # otherwise, remove wildcard for Table
        if not isinstance(tgt_table, SubQuery):
            self.go.drop_vertices(tgt_wildcard)
            if src_wildcard.parent is not None and not isinstance(
                src_wildcard.parent, SubQuery
            ):
                self.go.drop_vertices(src_wildcard)


class StatementLineageHolder(SubQueryLineageHolder, ColumnLineageMixin):
    """
    Statement Level Lineage Result.

    Based on SubQueryLineageHolder, StatementLineageHolder holds extra attributes like drop and rename

    For drop, it is a set[:class:`sqllineage.core.models.Table`].

    For rename, it a set[tuple[:class:`sqllineage.core.models.Table`, :class:`sqllineage.core.models.Table`]],
    with the first table being original table before renaming and the latter after renaming.
    """

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in ["read", "write", "cte", "drop", "rename"]
        )

    def __repr__(self):
        return str(self)

    @property
    def read(self) -> set[Table]:  # type: ignore
        return {t for t in super().read if isinstance(t, DATASET_CLASSES)}

    @property
    def write(self) -> set[Table]:  # type: ignore
        return {t for t in super().write if isinstance(t, DATASET_CLASSES)}

    @property
    def drop(self) -> set[Table]:
        return self._property_getter(NodeTag.DROP)  # type: ignore

    def add_drop(self, value) -> None:
        self._property_setter(value, NodeTag.DROP)

    @property
    def rename(self) -> set[tuple[Table, Table]]:
        return {
            (e.source, e.target)
            for e in self.go.retrieve_edges_by_label(EdgeType.RENAME)
        }

    def add_rename(self, src: Table, tgt: Table) -> None:
        self.go.add_edge_if_not_exist(src, tgt, EdgeType.RENAME)

    @staticmethod
    def of(holder: SubQueryLineageHolder) -> "StatementLineageHolder":
        stmt_holder = StatementLineageHolder()
        stmt_holder.go = holder.go
        return stmt_holder


class SQLLineageHolder(ColumnLineageMixin):
    def __init__(self, go: GraphOperator):
        """
        The combined lineage result in representation of Directed Acyclic Graph.

        :param go: the Graph Operator holding all the combined lineage result.
        """
        self.go = go
        self._selfloop_tables = self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        self._sourceonly_tables = self.__retrieve_tag_tables(NodeTag.SOURCE_ONLY)
        self._targetonly_tables = self.__retrieve_tag_tables(NodeTag.TARGET_ONLY)

    @property
    def table_lineage_graph(self) -> GraphOperator:
        """
        The table level GraphOperator held by SQLLineageHolder
        """
        table_nodes = [
            v
            for v in self.go.retrieve_vertices_by_props()
            if isinstance(v, DATASET_CLASSES)
        ]
        return self.go.get_sub_graph(*table_nodes)

    @property
    def column_lineage_graph(self) -> GraphOperator:
        """
        The column level GraphOperator held by SQLLineageHolder
        """
        column_nodes = [
            v for v in self.go.retrieve_vertices_by_props() if isinstance(v, Column)
        ]
        return self.go.get_sub_graph(*column_nodes)

    @property
    def source_tables(self) -> set[Table | Path]:
        """
        a list of source :class:`sqllineage.core.models.Table`
        """
        source_tables = set(self.table_lineage_graph.retrieve_source_vertices())
        source_tables |= self._selfloop_tables
        source_tables |= self._sourceonly_tables
        return source_tables

    @property
    def target_tables(self) -> set[Table | Path]:
        """
        a list of target :class:`sqllineage.core.models.Table`
        """
        target_tables = set(self.table_lineage_graph.retrieve_target_vertices())
        target_tables |= self._selfloop_tables
        target_tables |= self._targetonly_tables
        return target_tables

    @property
    def intermediate_tables(self) -> set[Table | Path]:
        """
        a list of intermediate :class:`sqllineage.core.models.Table`
        """
        all_tables: list[Table | Path] = (
            self.table_lineage_graph.retrieve_vertices_by_props()
        )
        intermediate_tables = {
            table
            for table in all_tables
            if len(
                self.table_lineage_graph.retrieve_edges_by_vertex(
                    table, EdgeDirection.IN
                )
            )
            > 0
            and len(
                self.table_lineage_graph.retrieve_edges_by_vertex(
                    table, EdgeDirection.OUT
                )
            )
            > 0
        }
        intermediate_tables -= self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        return intermediate_tables

    def __retrieve_tag_tables(self, tag) -> set[Path | Table]:
        return {
            vertex
            for vertex in self.go.retrieve_vertices_by_props(**{tag: True})
            if isinstance(vertex, DATASET_CLASSES)
        }

    @staticmethod
    def of(metadata_provider, *args: StatementLineageHolder) -> "SQLLineageHolder":
        """
        To assemble multiple :class:`sqllineage.core.holders.StatementLineageHolder` into
        :class:`sqllineage.core.holders.SQLLineageHolder`
        """
        ngo = get_graph_operator_class()()
        for holder in args:
            ngo.merge(holder.go)
            if holder.drop:
                for table in holder.drop:
                    if (
                        len(ngo.retrieve_edges_by_vertex(table, EdgeDirection.IN)) == 0
                        and len(ngo.retrieve_edges_by_vertex(table, EdgeDirection.OUT))
                        == 0
                    ):
                        ngo.drop_vertices(table)
            elif holder.rename:
                for table_old, table_new in holder.rename:
                    for edge in ngo.retrieve_edges_by_vertex(
                        table_old, EdgeDirection.IN
                    ):
                        ngo.add_edge_if_not_exist(
                            edge.source, table_new, edge.label, **edge.attributes
                        )
                    for edge in ngo.retrieve_edges_by_vertex(
                        table_old, EdgeDirection.OUT
                    ):
                        ngo.add_edge_if_not_exist(
                            table_new, edge.target, edge.label, **edge.attributes
                        )
                    ngo.drop_vertices(table_old)
                    # remove possible self-loop edge created by rename
                    ngo.drop_edge(table_new, table_new)
                    if (
                        len(ngo.retrieve_edges_by_vertex(table_new, EdgeDirection.IN))
                        == 0
                        and len(
                            ngo.retrieve_edges_by_vertex(table_new, EdgeDirection.OUT)
                        )
                        == 0
                    ):
                        ngo.drop_vertices(table_new)
            else:
                read, write = holder.read, holder.write
                if len(read) > 0 and len(write) == 0:
                    # source only table comes from SELECT statement
                    ngo.update_vertices(*read, **{NodeTag.SOURCE_ONLY: True})
                elif len(read) == 0 and len(write) > 0:
                    # target only table comes from case like: 1) INSERT/UPDATE constant values; 2) CREATE TABLE
                    ngo.update_vertices(*write, **{NodeTag.TARGET_ONLY: True})
                else:
                    for source, target in itertools.product(read, write):
                        ngo.add_edge_if_not_exist(source, target, EdgeType.LINEAGE)
        # selfloop table comes from cases like: INSERT INTO tbl (part='xx') SELECT * FROM tbl WHERE part = ''
        ngo.update_vertices(
            *ngo.retrieve_selfloop_vertices(), **{NodeTag.SELFLOOP: True}
        )
        # find all the columns that we can't assign accurately to a parent table (with multiple parent candidates)
        unresolved_column_lineages = [
            (e.source, e.target)
            for e in ngo.retrieve_edges_by_label(label=EdgeType.LINEAGE)
            if isinstance(e.source, Column) and len(e.source.parent_candidates) > 1
        ]
        for unresolved_col, tgt_col in unresolved_column_lineages:
            # check if there's only one parent candidate contains the column with same name
            src_cols = []
            # check if source column exists in graph (either from subquery or from table created in prev statement)
            for parent in unresolved_col.parent_candidates:
                src_col_candidate = Column(unresolved_col.raw_name)
                src_col_candidate.parent = parent
                parent_columns = [
                    e.target
                    for e in ngo.retrieve_edges_by_vertex(
                        parent, EdgeDirection.OUT, EdgeType.HAS_COLUMN
                    )
                ]
                if src_col_candidate in parent_columns:
                    src_cols.append(src_col_candidate)
            # if not in graph, check if defined in table schema by metadata service
            if len(src_cols) == 0 and bool(metadata_provider):
                for parent in unresolved_col.parent_candidates:
                    if (
                        isinstance(parent, Table)
                        and str(parent.schema) != Schema.unknown
                    ):
                        for parent_col in metadata_provider.get_table_columns(parent):
                            if unresolved_col.raw_name == parent_col.raw_name:
                                src_cols.append(parent_col)

            # Multiple sources is a correct case for JOIN with USING
            # It incorrect for JOIN with ON, but sql without specifying an alias in this case will be invalid
            for src_col in src_cols:
                ngo.add_edge_if_not_exist(src_col, tgt_col, EdgeType.LINEAGE)
            if len(src_cols) > 0:
                # only delete unresolved column when it's resolved
                ngo.drop_edge(unresolved_col, tgt_col)

        # when unresolved column got resolved, it will be orphan node, and we can remove it
        for unresolved_col, _ in unresolved_column_lineages:
            if (
                len(ngo.retrieve_edges_by_vertex(unresolved_col, EdgeDirection.OUT))
                == 0
                and len(ngo.retrieve_edges_by_vertex(unresolved_col, EdgeDirection.IN))
                == 0
            ):
                ngo.drop_vertices(unresolved_col)
        return SQLLineageHolder(ngo)
