import logging
from typing import Any

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import (
    make_url,
)  # note: from sqlalchemy import make_url is only possible for v2.x
from sqlalchemy.exc import NoSuchModuleError, NoSuchTableError, OperationalError

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.exceptions import MetaDataProviderException

logger = logging.getLogger(__name__)


class SQLAlchemyMetaDataProvider(MetaDataProvider):
    """
    SQLAlchemyMetaDataProvider queries metadata from database using SQLAlchemy
    """

    def __init__(self, url: str, engine_kwargs: dict[str, Any] | None = None):
        """
        :param url: sqlalchemy url
        :param engine_kwargs: a dictionary of keyword arguments that will be passed to sqlalchemy create_engine
        """
        super().__init__()
        self.metadata_obj = MetaData()
        try:
            if engine_kwargs is None:
                engine_kwargs = {}
            self.engine = create_engine(url, **engine_kwargs)
        except NoSuchModuleError as e:
            u = make_url(url)
            raise MetaDataProviderException(
                f"SQLAlchemy dialect driver {u.drivername} is not installed correctly"
            ) from e
        try:
            self.engine.connect()
        except OperationalError as e:
            raise MetaDataProviderException(f"Could not connect to {url}") from e

    def __del__(self):
        # dispose the engine to close all connections
        if hasattr(self, "engine") and self.engine is not None:
            self.engine.dispose()

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> list[str]:
        columns = []
        try:
            sqlalchemy_table = Table(
                table, self.metadata_obj, schema=schema, autoload_with=self.engine
            )
            columns = [c.name for c in sqlalchemy_table.columns]
        except (NoSuchTableError, OperationalError):
            logger.warning(
                "error listing columns for table %s.%s in %s, return empty list instead",
                schema,
                table,
                self.engine.url,
            )
        return columns
