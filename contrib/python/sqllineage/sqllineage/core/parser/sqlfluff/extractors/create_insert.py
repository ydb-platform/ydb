from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffColumn, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class CreateInsertExtractor(BaseExtractor):
    """
    Create statement and Insert statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "create_table_statement",
        "create_table_as_statement",
        "create_view_statement",
        "insert_statement",
        "insert_overwrite_directory_hive_fmt_statement",
    ]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        src_flag = tgt_flag = part_flag = False
        partition_columns = []
        # collect partition column names first so that we can filter them out from write_columns later
        if any(
            segment.type == "keyword" and segment.raw_upper == "PARTITION"
            for segment in statement.get_children("keyword")
        ):
            if bracketed := statement.get_child("bracketed"):
                partition_columns = [
                    c.raw for c in bracketed.recursive_crawl("column_reference")
                ]

        for segment in list_child_segments(statement):
            if segment.type == "with_compound_statement":
                holder |= self.delegate_to_cte(segment, holder)
            elif segment.type == "bracketed" and any(
                s.type == "with_compound_statement" for s in segment.segments
            ):
                for sgmt in segment.segments:
                    if sgmt.type == "with_compound_statement":
                        holder |= self.delegate_to_cte(segment, holder)
            elif segment.type in ("select_statement", "set_expression"):
                holder |= self.delegate_to_select(segment, holder)
            elif segment.type == "values_clause":
                for bracketed in segment.get_children("bracketed"):
                    for expression in bracketed.get_children("expression"):
                        if sub_bracketed := expression.get_child("bracketed"):
                            if sub_expression := sub_bracketed.get_child("expression"):
                                if select_statement := sub_expression.get_child(
                                    "select_statement"
                                ):
                                    holder |= self.delegate_to_select(
                                        select_statement, holder
                                    )
            elif segment.type == "bracketed" and (
                subquery_segments := list(
                    segment.recursive_crawl(
                        "select_statement", "set_expression", recurse_into=False
                    )
                )
            ):
                # note regular subquery within SELECT statement is handled by SelectExtractor, this is only to handle
                # top-level subquery in DML like: 1) create table foo as (subquery); 2) insert into foo (subquery)
                # subquery here isn't added as read source, and it inherits DML-level write_columns if parsed
                for subquery_segment in subquery_segments:
                    holder |= self.delegate_to_select(subquery_segment, holder)

            elif segment.type == "bracketed":
                if part_flag:
                    part_flag = False
                    # partition columns is handled ahead of the for loop, so just continue
                    continue
                else:
                    # In case of bracketed column reference, add these target columns to holder
                    # so that when we compute the column level lineage
                    # we keep these columns into consideration
                    sub_segments = list_child_segments(segment)
                    if all(
                        sub_segment.type in ["column_reference", "column_definition"]
                        for sub_segment in sub_segments
                    ):
                        # target columns only apply to bracketed column_reference and column_definition
                        columns = []
                        for sub_segment in sub_segments:
                            if sub_segment.type == "column_definition":
                                if identifier := sub_segment.get_child("identifier"):
                                    sub_segment = identifier
                            columns.append(SqlFluffColumn.of(sub_segment))
                        holder.add_write_column(*columns)

            elif segment.type == "keyword":
                if segment.raw_upper in [
                    "INSERT",  # bigquery supports INTO as optional keyword so INSERT is the indicator
                    "INTO",
                    "OVERWRITE",  # sparksql supports INSERT OVERWRITE with TABLE as optional keyword
                    "TABLE",
                    "VIEW",
                    "DIRECTORY",
                ] or (
                    tgt_flag is True and segment.raw_upper in ["IF", "NOT", "EXISTS"]
                ):
                    tgt_flag = True
                elif segment.raw_upper in ["LIKE", "CLONE"]:
                    src_flag = True
                elif segment.raw_upper == "PARTITION":
                    part_flag = True
                continue

            if tgt_flag:
                if segment.type in ["table_reference", "object_reference"]:
                    write_obj = SqlFluffTable.of(segment)
                    holder.add_write(write_obj)
                    # get target table columns from metadata if available
                    if (
                        isinstance(write_obj, Table)
                        and self.metadata_provider
                        and statement.type == "insert_statement"
                    ):
                        all_columns = self.metadata_provider.get_table_columns(
                            table=write_obj
                        )
                        # Filter out partition columns from metadata as write columns is supposed to match the
                        # size of select columns
                        non_partition_columns = [
                            col
                            for col in all_columns
                            if col.raw_name not in partition_columns
                        ]
                        holder.add_write_column(*non_partition_columns)
                elif segment.type == "literal":
                    if segment.raw.isnumeric():
                        # Special Handling for Spark Bucket Table DDL
                        pass
                    else:
                        holder.add_write(Path(escape_identifier_name(segment.raw)))
                tgt_flag = False
            if src_flag:
                if segment.type in ["table_reference", "object_reference"]:
                    holder.add_read(SqlFluffTable.of(segment))
                src_flag = False
        return holder

    def delegate_to_cte(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> SubQueryLineageHolder:
        from .cte import CteExtractor

        return self.delegate_to(
            CteExtractor,
            segment,
            AnalyzerContext(
                cte=holder.cte, write=holder.write, write_columns=holder.write_columns
            ),
        )

    def delegate_to_select(
        self,
        segment: BaseSegment,
        holder: SubQueryLineageHolder,
    ) -> SubQueryLineageHolder:
        return self.delegate_to(
            SelectExtractor,
            segment,
            AnalyzerContext(
                cte=holder.cte,
                write=holder.write,
                write_columns=holder.write_columns,
            ),
        )
