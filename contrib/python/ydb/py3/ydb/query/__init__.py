__all__ = [
    "BaseQueryTxMode",
    "QueryExplainResultFormat",
    "QueryOnlineReadOnly",
    "QuerySerializableReadWrite",
    "QuerySnapshotReadOnly",
    "QuerySnapshotReadWrite",
    "QueryStaleReadOnly",
    "QuerySessionPool",
    "QueryClientSettings",
    "QuerySession",
    "QueryStatsMode",
    "QueryTxContext",
    "QuerySchemaInclusionMode",
    "QueryResultSetFormat",
    "ArrowCompressionCodecType",
    "ArrowCompressionCodec",
    "ArrowFormatSettings",
    "ArrowFormatMeta",
]

import logging
from typing import Optional, TYPE_CHECKING

from .base import (
    QueryClientSettings,
    QueryExplainResultFormat,
    QueryStatsMode,
    QuerySchemaInclusionMode,
    QueryResultSetFormat,
)

from .session import QuerySession
from .transaction import QueryTxContext

from .._grpc.grpcwrapper.ydb_query_public_types import (
    BaseQueryTxMode,
    QueryOnlineReadOnly,
    QuerySerializableReadWrite,
    QuerySnapshotReadOnly,
    QuerySnapshotReadWrite,
    QueryStaleReadOnly,
    ArrowCompressionCodecType,
    ArrowCompressionCodec,
    ArrowFormatSettings,
    ArrowFormatMeta,
)

from .pool import QuerySessionPool

if TYPE_CHECKING:
    from ..driver import Driver as SyncDriver

logger = logging.getLogger(__name__)


class QueryClientSync:
    _driver: "SyncDriver"

    def __init__(self, driver: "SyncDriver", query_client_settings: Optional[QueryClientSettings] = None):
        self._driver = driver
        self._settings = query_client_settings

    def session(self) -> QuerySession:
        return QuerySession(self._driver, self._settings)
