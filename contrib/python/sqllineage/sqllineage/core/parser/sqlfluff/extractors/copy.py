from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.models import Path
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.utils import (
    find_from_expression_element,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class CopyExtractor(BaseExtractor):
    """
    Copy statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "copy_statement",
        "copy_into_table_statement",
    ]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        src_flag = tgt_flag = False
        for segment in list_child_segments(statement):
            if segment.type == "from_clause":
                if from_expression_element := find_from_expression_element(segment):
                    for table_expression in from_expression_element.get_children(
                        "table_expression"
                    ):
                        if storage_location := table_expression.get_child(
                            "storage_location"
                        ):
                            holder.add_read(Path(storage_location.raw))
            elif segment.type == "keyword":
                if segment.raw_upper in ["COPY", "INTO"]:
                    tgt_flag = True
                elif segment.raw_upper == "FROM":
                    src_flag = True
                continue

            if tgt_flag:
                if table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False
            if src_flag:
                if segment.type in ["literal", "storage_location"]:
                    path = Path(escape_identifier_name(segment.raw))
                    holder.add_read(path)
                src_flag = False

        return holder
