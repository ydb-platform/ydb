from abc import ABC, abstractmethod

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider


class LineageAnalyzer(ABC):
    """SQL Statement Level Lineage Analyzer
    Parser specific implementation should inherit this class and implement analyze method
    """

    PARSER_NAME: str = ""
    SUPPORTED_DIALECTS: list[str] = []

    @abstractmethod
    def analyze(
        self, sql: str, metadata_provider: MetaDataProvider
    ) -> StatementLineageHolder:
        """
        to analyze single statement sql and store the result into StatementLineageHolder.

        :param sql: single-statement SQL string to be processed
        :param metadata_provider: :class:`sqllineage.core.metadata_provider.MetaDataProvider` provides metadata on
                                  tables to help lineage analyzing
        :return: :class:`sqllineage.core.holders.StatementLineageHolder`
        """
        raise NotImplementedError
