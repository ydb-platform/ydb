__all__ = [
    "QueryOnlineReadOnly",
    "QuerySerializableReadWrite",
    "QuerySnapshotReadOnly",
    "QueryStaleReadOnly",
    "QuerySessionPool",
    "QueryClientSync",
    "QuerySessionSync",
]

import logging

from .base import (
    QueryClientSettings,
)

from .session import QuerySessionSync

from .._grpc.grpcwrapper import common_utils
from .._grpc.grpcwrapper.ydb_query_public_types import (
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

    def session(self) -> QuerySessionSync:
        return QuerySessionSync(self._driver, self._settings)
