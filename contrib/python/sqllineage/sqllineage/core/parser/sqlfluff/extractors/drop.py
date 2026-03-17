from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.utils import list_child_segments
from sqllineage.utils.entities import AnalyzerContext


class DropExtractor(BaseExtractor):
    """
    Drop statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["drop_table_statement", "drop_view_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        drop_flag = False
        for segment in list_child_segments(statement):
            if (
                segment.type == "keyword"
                and segment.raw_upper in ["TABLE", "VIEW"]
                or (drop_flag is True and segment.raw_upper in ["IF", "EXISTS"])
            ):
                drop_flag = True
                continue
            if drop_flag:
                if table := self.find_table(segment):
                    holder.add_drop(table)
                drop_flag = False
        return holder
