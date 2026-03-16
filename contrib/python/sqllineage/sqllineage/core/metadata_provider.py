from abc import ABC, abstractmethod

from sqllineage.core.models import Column, Table


class MetaDataProvider(ABC):
    """
    Base class used to provide metadata like table schema.

    When parse below sql:

    .. code-block:: sql

        INSERT INTO db1.table1
        SELECT c1
        FROM db2.table2 t2
        JOIN db3.table3 t3 ON t2.id = t3.id

    Only by literal analysis, we don't know which table is selected column c1 from.
    A subclass of MetaDataProvider implementing _get_table_columns passing to :class:`sqllineage.runner.LineageRunner`.
    can help parse column lineage correctly.
    """

    def __init__(self) -> None:
        self._session_metadata: dict[str, list[str]] = {}

    def get_table_columns(self, table: Table, **kwargs) -> list[Column]:
        """
        return columns of given table.
        """
        if (key := str(table)) in self._session_metadata:
            cols = self._session_metadata[key]
        else:
            cols = self._get_table_columns(str(table.schema), table.raw_name, **kwargs)
        columns = []
        for col in cols:
            column = Column(col)
            column.parent = table
            columns.append(column)
        return columns

    @abstractmethod
    def _get_table_columns(self, schema: str, table: str, **kwargs) -> list[str]:
        raise NotImplementedError

    def register_session_metadata(self, table: Table, columns: list[Column]) -> None:
        """Register session-level metadata, like temporary table or view created."""
        self._session_metadata[str(table)] = [c.raw_name for c in columns]

    def deregister_session_metadata(self) -> None:
        """Deregister session-level metadata."""
        self._session_metadata.clear()

    def session(self):
        return MetaDataSession(self)

    def __bool__(self):
        """
        bool value tells whether this provider is ready to provide metadata
        """
        return True


class MetaDataSession:
    """
    Create an analyzer session which can register session-level metadata as a supplement to global metadata.
    This way, table or views created during the session can be queried.
    All session-level metadata will be deregistered once session closed.
    """

    def __init__(self, metadata_provider: MetaDataProvider):
        self.metadata_provider = metadata_provider

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.metadata_provider.deregister_session_metadata()

    def register_session_metadata(self, table: Table, columns: list[Column]) -> None:
        self.metadata_provider.register_session_metadata(table, columns)
