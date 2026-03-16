from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.utils.entities import AnalyzerContext


class RenameExtractor(BaseExtractor):
    """
    Rename statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "alter_table_statement",
        "rename_statement",
        "rename_table_statement",
    ]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        tables = []
        for t in statement.segments:
            if table := self.find_table(t):
                tables.append(table)
        keywords = [t for t in statement.segments if t.type == "keyword"]
        if any(k.raw_upper == "RENAME" for k in keywords) and len(tables) % 2 == 0:
            for i in range(0, len(tables), 2):
                holder.add_rename(tables[i], tables[i + 1])
        elif (
            any(k.raw_upper in ["EXCHANGE", "SWAP"] for k in keywords)
            and len(tables) == 2
        ):
            # ALTER TABLE EXCHANGE PARTITION/SWAP
            holder.add_write(tables[0])
            holder.add_read(tables[1])
        return holder
