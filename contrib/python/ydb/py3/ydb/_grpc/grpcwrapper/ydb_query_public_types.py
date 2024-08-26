import abc
import typing

from .common_utils import IToProto

try:
    from ydb.public.api.protos import ydb_query_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_query_pb2


class BaseQueryTxMode(IToProto):
    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass


class QuerySnapshotReadOnly(BaseQueryTxMode):
    def __init__(self):
        self._name = "snapshot_read_only"

    @property
    def name(self) -> str:
        return self._name

    def to_proto(self) -> ydb_query_pb2.SnapshotModeSettings:
        return ydb_query_pb2.SnapshotModeSettings()


class QuerySerializableReadWrite(BaseQueryTxMode):
    def __init__(self):
        self._name = "serializable_read_write"

    @property
    def name(self) -> str:
        return self._name

    def to_proto(self) -> ydb_query_pb2.SerializableModeSettings:
        return ydb_query_pb2.SerializableModeSettings()


class QueryOnlineReadOnly(BaseQueryTxMode):
    def __init__(self, allow_inconsistent_reads: bool = False):
        self.allow_inconsistent_reads = allow_inconsistent_reads
        self._name = "online_read_only"

    @property
    def name(self):
        return self._name

    def to_proto(self) -> ydb_query_pb2.OnlineModeSettings:
        return ydb_query_pb2.OnlineModeSettings(allow_inconsistent_reads=self.allow_inconsistent_reads)


class QueryStaleReadOnly(BaseQueryTxMode):
    def __init__(self):
        self._name = "stale_read_only"

    @property
    def name(self):
        return self._name

    def to_proto(self) -> ydb_query_pb2.StaleModeSettings:
        return ydb_query_pb2.StaleModeSettings()
