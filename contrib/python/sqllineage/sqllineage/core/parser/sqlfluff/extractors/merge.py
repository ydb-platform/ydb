from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.models import Column, SubQuery, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.cte import CteExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    extract_identifier,
    extract_innermost_bracketed,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext


class MergeExtractor(BaseExtractor):
    """
    Merge statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["merge_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        src_flag = tgt_flag = False
        direct_source: Table | SubQuery | None = None
        segments = list_child_segments(statement)
        for i, segment in enumerate(segments):
            if segment.type == "merge_match":
                merge_match = segment
                for merge_when_matched_clause in merge_match.get_children(
                    "merge_when_matched_clause"
                ):
                    if merge_update_clause := merge_when_matched_clause.get_child(
                        "merge_update_clause"
                    ):
                        if set_clause_list := merge_update_clause.get_child(
                            "set_clause_list"
                        ):
                            for set_clause in set_clause_list.get_children(
                                "set_clause"
                            ):
                                columns = set_clause.get_children("column_reference")
                                if len(columns) == 2:
                                    src_col = tgt_col = None
                                    if src_cqt := extract_column_qualifier(columns[1]):
                                        src_col = Column(src_cqt.column)
                                        if direct_source is not None:
                                            src_col.parent = direct_source
                                    if tgt_cqt := extract_column_qualifier(columns[0]):
                                        tgt_col = Column(tgt_cqt.column)
                                        tgt_col.parent = list(holder.write)[0]
                                    if src_col is not None and tgt_col is not None:
                                        holder.add_column_lineage(src_col, tgt_col)
                for merge_when_not_matched_clause in merge_match.get_children(
                    "merge_when_not_matched_clause"
                ):
                    if merge_insert := merge_when_not_matched_clause.get_child(
                        "merge_insert_clause"
                    ):
                        insert_columns = []
                        if bracketed := merge_insert.get_child("bracketed"):
                            for column_reference in bracketed.get_children(
                                "column_reference"
                            ):
                                if cqt := extract_column_qualifier(column_reference):
                                    tgt_col = Column(cqt.column)
                                    tgt_col.parent = list(holder.write)[0]
                                    insert_columns.append(tgt_col)
                            values_bracketed = None
                            if values_clause := merge_insert.get_child("values_clause"):
                                # ANSI dialect and most others inheriting from it, VALUES is wrapped in values_clause
                                values_bracketed = values_clause.get_child("bracketed")
                            else:
                                # tsql dialect: VALUES keyword is a direct child, followed by bracketed
                                values_flag = False
                                for seg in list_child_segments(merge_insert):
                                    if values_flag and seg.type == "bracketed":
                                        values_bracketed = seg
                                        break
                                    if (
                                        seg.type == "keyword"
                                        and seg.raw_upper == "VALUES"
                                    ):
                                        values_flag = True

                            if values_bracketed:
                                for j, e in enumerate(
                                    values_bracketed.get_children(
                                        "literal", "expression"
                                    )
                                ):
                                    if column_reference_optional := e.get_child(
                                        "column_reference"
                                    ):
                                        if cqt := extract_column_qualifier(
                                            column_reference_optional
                                        ):
                                            src_col = Column(cqt.column)
                                            if direct_source is not None:
                                                src_col.parent = direct_source
                                            holder.add_column_lineage(
                                                src_col, insert_columns[j]
                                            )
            elif segment.type == "keyword":
                if segment.raw_upper in ["MERGE", "INTO"]:
                    tgt_flag = True
                elif segment.raw_upper == "USING":
                    src_flag = True
                continue

            if tgt_flag:
                if table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False
            if src_flag:
                if table := self.find_table(segment):
                    holder.add_read(table)
                    direct_source = table
                elif segment.type == "bracketed":
                    next_segment = segments[i + 1]
                    direct_source = SqlFluffSubQuery.of(
                        extract_innermost_bracketed(segment),
                        (
                            extract_identifier(next_segment)
                            if next_segment.type == "alias_expression"
                            else None
                        ),
                    )
                    holder.add_read(direct_source)

                    extractor_cls = (
                        CteExtractor
                        if direct_source.query.get_child(
                            "with_compound_statement"
                        )  # in case the subquery is a CTE query
                        else SelectExtractor  # in case the subquery is a select query
                    )
                    holder |= self.delegate_to(
                        extractor_cls,
                        direct_source.query,
                        AnalyzerContext(cte=holder.cte, write={direct_source}),
                    )
                src_flag = False
        return holder
