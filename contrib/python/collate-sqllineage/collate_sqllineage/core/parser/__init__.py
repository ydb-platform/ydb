from typing import Dict, List, Tuple, Union

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.models import (
    Column,
    DataFunction,
    Location,
    Path,
    SubQuery,
    Table,
)
from collate_sqllineage.exceptions import SQLLineageException
from collate_sqllineage.utils.constant import EdgeType


class SourceHandlerMixin:
    tables: List[Union[DataFunction, Location, Path, SubQuery, Table]]
    columns: List[Column]
    union_barriers: List[Tuple[int, int]]

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        # Add all tables to holder as read tables
        for i, tbl in enumerate(self.tables):
            holder.add_read(tbl)

        # Add the current barriers
        self.union_barriers.append((len(self.columns), len(self.tables)))

        # Process each group of columns and tables
        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]

            # Skip if no write operations
            if not holder.write:
                continue

            # Validate that we have only one write target
            writes = {write for write in holder.write if isinstance(write, Table)}
            if len(writes) > 1:
                raise SQLLineageException

            tgt_tbl = list(holder.write)[0]

            # Compute the alias mapping once for all columns in this group
            alias_mapping = self.get_alias_mapping_from_table_group(tbl_grp, holder)

            # Check if we need to use target_columns
            use_target_columns = len(holder.target_columns) == len(col_grp)

            # Process each column in the group
            for idx, tgt_col in enumerate(col_grp):
                tgt_col.parent = tgt_tbl

                # Get the correct target column if available
                final_tgt_col = (
                    holder.target_columns[idx] if use_target_columns else tgt_col
                )

                # Process source columns for this target column
                for src_col in tgt_col.to_source_columns(alias_mapping):
                    holder.add_column_lineage(src_col, final_tgt_col)

    @classmethod
    def get_alias_mapping_from_table_group(
        cls,
        table_group: List[Union[DataFunction, Location, Path, Table, SubQuery]],
        holder: SubQueryLineageHolder,
    ) -> Dict[str, Union[DataFunction, Location, Path, Table, SubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        """
        # Create a single dictionary and populate it with all mappings
        result = {}

        # Get all relevant edges for HAS_ALIAS relationship
        has_alias_edges = [
            (src, tgt)
            for src, tgt, attr in holder.graph.edges(data=True)
            if attr.get("type") == EdgeType.HAS_ALIAS and src in table_group
        ]

        # Add normal alias mappings
        for src, tgt in has_alias_edges:
            result[tgt] = src

        # Add lowercase alias mappings
        for src, tgt in has_alias_edges:
            result[tgt.lower()] = src

        # Process tables and data functions
        for table in table_group:
            if isinstance(table, Table) or isinstance(table, DataFunction):
                # Add raw name mappings
                result[table.raw_name] = table
                result[str(table)] = table

                # Add lowercase mappings
                result[table.raw_name.lower()] = table
                result[str(table).lower()] = table

        return result
