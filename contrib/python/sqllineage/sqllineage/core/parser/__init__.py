from sqllineage.config import SQLLineageConfig
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.exceptions import SQLLineageException


class SourceHandlerMixin:
    tables: list[Path | SubQuery | Table]
    columns: list[Column]
    union_barriers: list[tuple[int, int]]

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        for i, tbl in enumerate(self.tables):
            holder.add_read(tbl)
        self.union_barriers.append((len(self.columns), len(self.tables)))
        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]
            if holder.write:
                if len(holder.write) > 1:
                    raise SQLLineageException
                tgt_tbl = next(iter(holder.write))
                lateral_column_aliases: dict[str, list[Column]] = {}
                for idx, tgt_col_from_query in enumerate(col_grp):
                    tgt_col_from_query.parent = tgt_tbl
                    tgt_col_resolved = tgt_col_from_query
                    src_cols_resolved = []
                    for src_col in tgt_col_from_query.to_source_columns(
                        holder.get_alias_mapping_from_table_group(tbl_grp),
                    ):
                        if len(write_columns := holder.write_columns) == len(col_grp):
                            # example query: create view test (col3) select col1 as col2 from tab
                            # without write_columns = [col3] information, by default src_col = col1 and tgt_col = col2
                            # when write_columns exist and length matches, we want tgt_col = col3 instead of col2
                            # for invalid query: create view test (col3, col4) select col1 as col2 from tab,
                            # when the length doesn't match, we fall back to default behavior
                            tgt_col_resolved = write_columns[idx]
                        # lateral column alias handling
                        lca_flag = False
                        if SQLLineageConfig.LATERAL_COLUMN_ALIAS_REFERENCE:
                            if metadata_provider := getattr(
                                self, "metadata_provider", None
                            ):
                                from_dataset = False
                                for parent_candidate in src_col.parent_candidates:
                                    if isinstance(
                                        parent_candidate, Table
                                    ) and src_col in metadata_provider.get_table_columns(
                                        parent_candidate
                                    ):
                                        from_dataset = True
                                    elif isinstance(
                                        parent_candidate, SubQuery
                                    ) and src_col in holder.get_table_columns(
                                        parent_candidate
                                    ):
                                        from_dataset = True
                                if not from_dataset and (
                                    lca_cols_resolved := lateral_column_aliases.get(
                                        src_col.raw_name, []
                                    )
                                ):
                                    src_cols_resolved.extend(lca_cols_resolved)
                                    lca_flag = True
                        if not lca_flag:
                            src_cols_resolved.append(src_col)

                    for src_col_resolved in src_cols_resolved:
                        holder.add_column_lineage(src_col_resolved, tgt_col_resolved)
                        if (
                            SQLLineageConfig.LATERAL_COLUMN_ALIAS_REFERENCE
                            and tgt_col_from_query.from_alias
                        ):
                            lateral_column_aliases[tgt_col_from_query.raw_name] = (
                                src_cols_resolved
                            )
