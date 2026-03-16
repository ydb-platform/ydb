from abc import abstractmethod

from collate_sqllineage.core.holders import StatementLineageHolder


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer
    Parser specific implementation should inherit this class and implement analyze method
    """

    @abstractmethod
    def analyze(self, sql: str) -> StatementLineageHolder:
        """
        to analyze single statement sql and store the result into
        :class:`sqllineage.core.holders.StatementLineageHolder`.
        """
