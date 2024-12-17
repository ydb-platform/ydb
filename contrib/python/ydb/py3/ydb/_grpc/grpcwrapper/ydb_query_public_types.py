import abc
import typing

from .common_utils import IToProto

try:
    from ydb.public.api.protos import ydb_query_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_query_pb2


class BaseQueryTxMode(IToProto):
    """Abstract class for Query Transaction Modes."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass


class QuerySnapshotReadOnly(BaseQueryTxMode):
    """All the read operations within a transaction access the database snapshot.
    All the data reads are consistent. The snapshot is taken when the transaction begins,
    meaning the transaction sees all changes committed before it began.
    """

    def __init__(self):
        self._name = "snapshot_read_only"

    @property
    def name(self) -> str:
        return self._name

    def to_proto(self) -> ydb_query_pb2.SnapshotModeSettings:
        return ydb_query_pb2.SnapshotModeSettings()


class QuerySerializableReadWrite(BaseQueryTxMode):
    """This mode guarantees that the result of successful parallel transactions is equivalent
    to their serial execution, and there are no read anomalies for successful transactions.
    """

    def __init__(self):
        self._name = "serializable_read_write"

    @property
    def name(self) -> str:
        return self._name

    def to_proto(self) -> ydb_query_pb2.SerializableModeSettings:
        return ydb_query_pb2.SerializableModeSettings()


class QueryOnlineReadOnly(BaseQueryTxMode):
    """Each read operation in the transaction is reading the data that is most recent at execution time.
    The consistency of retrieved data depends on the allow_inconsistent_reads setting:
        * false (consistent reads): Each individual read operation returns consistent data,
          but no consistency is guaranteed between reads.
          Reading the same table range twice may return different results.
        * true (inconsistent reads): Even the data fetched by a particular
          read operation may contain inconsistent results.
    """

    def __init__(self, allow_inconsistent_reads: bool = False):
        self.allow_inconsistent_reads = allow_inconsistent_reads
        self._name = "online_read_only"

    @property
    def name(self):
        return self._name

    def with_allow_inconsistent_reads(self) -> "QueryOnlineReadOnly":
        self.allow_inconsistent_reads = True
        return self

    def to_proto(self) -> ydb_query_pb2.OnlineModeSettings:
        return ydb_query_pb2.OnlineModeSettings(allow_inconsistent_reads=self.allow_inconsistent_reads)


class QueryStaleReadOnly(BaseQueryTxMode):
    """Read operations within a transaction may return results that are slightly out-of-date
    (lagging by fractions of a second). Each individual read returns consistent data,
    but no consistency between different reads is guaranteed.
    """

    def __init__(self):
        self._name = "stale_read_only"

    @property
    def name(self):
        return self._name

    def to_proto(self) -> ydb_query_pb2.StaleModeSettings:
        return ydb_query_pb2.StaleModeSettings()
