__all__ = [
    "BaseQueryTxMode",
    "QueryOnlineReadOnly",
    "QuerySerializableReadWrite",
    "QuerySnapshotReadOnly",
    "QueryStaleReadOnly",
    "QuerySessionPool",
    "QueryClientSettings",
    "QuerySession",
    "QueryTxContext",
]

import logging

from .base import (
    QueryClientSettings,
)

from .session import QuerySession
from .transaction import QueryTxContext

from .._grpc.grpcwrapper import common_utils
from .._grpc.grpcwrapper.ydb_query_public_types import (
    BaseQueryTxMode,
    QueryOnlineReadOnly,
    QuerySerializableReadWrite,
    QuerySnapshotReadOnly,
    QueryStaleReadOnly,
)

from .pool import QuerySessionPool

logger = logging.getLogger(__name__)


class QueryClientSync:
    def __init__(self, driver: common_utils.SupportedDriverType, query_client_settings: QueryClientSettings = None):
        logger.warning("QueryClientSync is an experimental API, which could be changed.")
        self._driver = driver
        self._settings = query_client_settings

    def session(self) -> QuerySession:
        return QuerySession(self._driver, self._settings)
