from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from collate_sqllineage.core.models import AnalyzerContext
from collate_sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffTable


class DdlAlterExtractor(LineageHolderExtractor):
    """
    DDL Alter queries lineage extractor
    """

    SOURCE_KEYWORDS = {"EXCHANGE", "SWAP"}

    SUPPORTED_STMT_TYPES = [
        "alter_table_statement",
        "rename_statement",
        "rename_table_statement",
    ]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SubQueryLineageHolder' object
        """
        holder = StatementLineageHolder()
        tables = []
        for t in statement.segments:
            if t.type == "table_reference":
                tables.append(SqlFluffTable.of(t))
        keywords = [t for t in statement.segments if t.type == "keyword"]
        if any(k.raw_upper == "RENAME" for k in keywords):
            if statement.type == "alter_table_statement" and len(tables) == 2:
                holder.add_rename(tables[0], tables[1])
        if (
            any(k.raw_upper in self.SOURCE_KEYWORDS for k in keywords)
            and len(tables) == 2
        ):
            holder.add_write(tables[0])
            holder.add_read(tables[1])
        return holder
