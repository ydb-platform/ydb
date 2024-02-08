# -*- coding: utf-8 -*-
import abc
import ydb
from abc import abstractmethod
import logging
import time
import random
import enum

import six
from . import (
    issues,
    convert,
    settings as settings_impl,
    scheme,
    types,
    _utilities,
    _apis,
    _sp_impl,
    _session_impl,
    _tx_ctx_impl,
    tracing,
)
from ._errors import check_retriable_error

try:
    from . import interceptor
except ImportError:
    interceptor = None

_allow_split_transaction = True

logger = logging.getLogger(__name__)

##################################################################
# A deprecated aliases in case when direct import has been used  #
##################################################################
SessionPoolEmpty = issues.SessionPoolEmpty
DataQuery = types.DataQuery


class DescribeTableSettings(settings_impl.BaseRequestSettings):
    def __init__(self):
        super(DescribeTableSettings, self).__init__()
        self.include_shard_key_bounds = False
        self.include_table_stats = False

    def with_include_shard_key_bounds(self, value):
        self.include_shard_key_bounds = value
        return self

    def with_include_table_stats(self, value):
        self.include_table_stats = value
        return self


class ExecDataQuerySettings(settings_impl.BaseRequestSettings):
    def __init__(self):
        super(ExecDataQuerySettings, self).__init__()
        self.keep_in_cache = True

    def with_keep_in_cache(self, value):
        self.keep_in_cache = value
        return self


class KeyBound(object):
    __slots__ = ("_equal", "value", "type")

    def __init__(self, key_value, key_type=None, inclusive=False):
        """
        Represents key bound.
        :param key_value: An iterable with key values
        :param key_type: A type of key
        :param inclusive: A flag that indicates bound includes key provided in the value.
        """

        try:
            iter(key_value)
        except TypeError:
            assert False, "value must be iterable!"

        if isinstance(key_type, types.TupleType):
            key_type = key_type.proto

        self._equal = inclusive
        self.value = key_value
        self.type = key_type

    def is_inclusive(self):
        return self._equal

    def is_exclusive(self):
        return not self._equal

    def __str__(self):
        if self._equal:
            return "InclusiveKeyBound(Tuple%s)" % str(self.value)
        return "ExclusiveKeyBound(Tuple%s)" % str(self.value)

    @classmethod
    def inclusive(cls, key_value, key_type):
        return cls(key_value, key_type, True)

    @classmethod
    def exclusive(cls, key_value, key_type):
        return cls(key_value, key_type, False)


class KeyRange(object):
    __slots__ = ("from_bound", "to_bound")

    def __init__(self, from_bound, to_bound):
        self.from_bound = from_bound
        self.to_bound = to_bound

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "KeyRange(%s, %s)" % (str(self.from_bound), str(self.to_bound))


class Column(object):
    def __init__(self, name, type, family=None):
        self._name = name
        self._type = type
        self.family = family

    def __eq__(self, other):
        return self.name == other.name and self._type.item == other.type.item

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    def with_family(self, family):
        self.family = family
        return self

    @property
    def type_pb(self):
        try:
            return self._type.proto
        except Exception:
            return self._type


@enum.unique
class FeatureFlag(enum.IntEnum):
    UNSPECIFIED = 0
    ENABLED = 1
    DISABLED = 2


@enum.unique
class AutoPartitioningPolicy(enum.IntEnum):
    AUTO_PARTITIONING_POLICY_UNSPECIFIED = 0
    DISABLED = 1
    AUTO_SPLIT = 2
    AUTO_SPLIT_MERGE = 3


@enum.unique
class IndexStatus(enum.IntEnum):
    INDEX_STATUS_UNSPECIFIED = 0
    READY = 1
    BUILDING = 2


class CachingPolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.CachingPolicy()
        self.preset_name = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def to_pb(self):
        return self._pb


class ExecutionPolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.ExecutionPolicy()
        self.preset_name = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def to_pb(self):
        return self._pb


class CompactionPolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.CompactionPolicy()
        self.preset_name = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def to_pb(self):
        return self._pb


class SplitPoint(object):
    def __init__(self, *args):
        self._value = tuple(args)

    @property
    def value(self):
        return self._value


class ExplicitPartitions(object):
    def __init__(self, split_points):
        self.split_points = split_points


class PartitioningPolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.PartitioningPolicy()
        self.preset_name = None
        self.uniform_partitions = None
        self.auto_partitioning = None
        self.explicit_partitions = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def with_uniform_partitions(self, uniform_partitions):
        self._pb.uniform_partitions = uniform_partitions
        self.uniform_partitions = uniform_partitions
        return self

    def with_explicit_partitions(self, explicit_partitions):
        self.explicit_partitions = explicit_partitions
        return self

    def with_auto_partitioning(self, auto_partitioning):
        self._pb.auto_partitioning = auto_partitioning
        self.auto_partitioning = auto_partitioning
        return self

    def to_pb(self, table_description):
        if self.explicit_partitions is not None:
            column_types = {}
            pk = set(table_description.primary_key)
            for column in table_description.columns:
                if column.name in pk:
                    column_types[column.name] = column.type

            for split_point in self.explicit_partitions.split_points:
                typed_value = self._pb.explicit_partitions.split_points.add()
                split_point_type = types.TupleType()
                prefix_size = len(split_point.value)
                for pl_el_id, pk_name in enumerate(table_description.primary_key):
                    if pl_el_id >= prefix_size:
                        break

                    split_point_type.add_element(column_types[pk_name])

                typed_value.type.MergeFrom(split_point_type.proto)
                typed_value.value.MergeFrom(
                    convert.from_native_value(split_point_type.proto, split_point.value)
                )

        return self._pb


class TableIndex(object):
    def __init__(self, name):
        self._pb = _apis.ydb_table.TableIndex()
        self._pb.name = name
        self.name = name
        self.index_columns = []
        # output only.
        self.status = None

    def with_global_index(self):
        self._pb.global_index.SetInParent()
        return self

    def with_index_columns(self, *columns):
        for column in columns:
            self._pb.index_columns.append(column)
            self.index_columns.append(column)
        return self

    def to_pb(self):
        return self._pb


class ReplicationPolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.ReplicationPolicy()
        self.preset_name = None
        self.replicas_count = None
        self.allow_promotion = None
        self.create_per_availability_zone = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def with_replicas_count(self, replicas_count):
        self._pb.replicas_count = replicas_count
        self.replicas_count = replicas_count
        return self

    def with_create_per_availability_zone(self, create_per_availability_zone):
        self._pb.create_per_availability_zone = create_per_availability_zone
        self.create_per_availability_zone = create_per_availability_zone
        return self

    def with_allow_promotion(self, allow_promotion):
        self._pb.allow_promotion = allow_promotion
        self.allow_promotion = allow_promotion
        return self

    def to_pb(self):
        return self._pb


class StoragePool(object):
    def __init__(self, media):
        self.media = media

    def to_pb(self):
        return _apis.ydb_table.StoragePool(media=self.media)


class StoragePolicy(object):
    def __init__(self):
        self._pb = _apis.ydb_table.StoragePolicy()
        self.preset_name = None
        self.syslog = None
        self.log = None
        self.data = None
        self.keep_in_memory = None
        self.external = None

    def with_preset_name(self, preset_name):
        self._pb.preset_name = preset_name
        self.preset_name = preset_name
        return self

    def with_syslog_storage_settings(self, syslog_settings):
        self._pb.syslog.MergeFrom(syslog_settings.to_pb())
        self.syslog = syslog_settings
        return self

    def with_log_storage_settings(self, log_settings):
        self._pb.log.MergeFrom(log_settings.to_pb())
        self.log = log_settings
        return self

    def with_data_storage_settings(self, data_settings):
        self._pb.data.MergeFrom(data_settings.to_pb())
        self.data = data_settings
        return self

    def with_external_storage_settings(self, external_settings):
        self._pb.external.MergeFrom(external_settings.to_pb())
        self.external = external_settings
        return self

    def with_keep_in_memory(self, keep_in_memory):
        self._pb.keep_in_memory = keep_in_memory
        self.keep_in_memory = keep_in_memory
        return self

    def to_pb(self):
        return self._pb


class TableProfile(object):
    def __init__(self):
        self.preset_name = None
        self.compaction_policy = None
        self.partitioning_policy = None
        self.storage_policy = None
        self.execution_policy = None
        self.replication_policy = None
        self.caching_policy = None

    def with_preset_name(self, preset_name):
        self.preset_name = preset_name
        return self

    def with_compaction_policy(self, compaction_policy):
        self.compaction_policy = compaction_policy
        return self

    def with_partitioning_policy(self, partitioning_policy):
        self.partitioning_policy = partitioning_policy
        return self

    def with_execution_policy(self, execution_policy):
        self.execution_policy = execution_policy
        return self

    def with_caching_policy(self, caching_policy):
        self.caching_policy = caching_policy
        return self

    def with_storage_policy(self, storage_policy):
        self.storage_policy = storage_policy
        return self

    def with_replication_policy(self, replication_policy):
        self.replication_policy = replication_policy
        return self

    def to_pb(self, table_description):
        pb = _apis.ydb_table.TableProfile()

        if self.preset_name is not None:
            pb.preset_name = self.preset_name

        if self.execution_policy is not None:
            pb.execution_policy.MergeFrom(self.execution_policy.to_pb())

        if self.storage_policy is not None:
            pb.storage_policy.MergeFrom(self.storage_policy.to_pb())

        if self.replication_policy is not None:
            pb.replication_policy.MergeFrom(self.replication_policy.to_pb())

        if self.caching_policy is not None:
            pb.caching_policy.MergeFrom(self.caching_policy.to_pb())

        if self.compaction_policy is not None:
            pb.compaction_policy.MergeFrom(self.compaction_policy.to_pb())

        if self.partitioning_policy is not None:
            pb.partitioning_policy.MergeFrom(
                self.partitioning_policy.to_pb(table_description)
            )

        return pb


class DateTypeColumnModeSettings(object):
    def __init__(self, column_name, expire_after_seconds=0):
        self.column_name = column_name
        self.expire_after_seconds = expire_after_seconds

    def to_pb(self):
        pb = _apis.ydb_table.DateTypeColumnModeSettings()

        pb.column_name = self.column_name
        pb.expire_after_seconds = self.expire_after_seconds

        return pb


@enum.unique
class ColumnUnit(enum.IntEnum):
    UNIT_UNSPECIFIED = 0
    UNIT_SECONDS = 1
    UNIT_MILLISECONDS = 2
    UNIT_MICROSECONDS = 3
    UNIT_NANOSECONDS = 4


class ValueSinceUnixEpochModeSettings(object):
    def __init__(self, column_name, column_unit, expire_after_seconds=0):
        self.column_name = column_name
        self.column_unit = column_unit
        self.expire_after_seconds = expire_after_seconds

    def to_pb(self):
        pb = _apis.ydb_table.ValueSinceUnixEpochModeSettings()

        pb.column_name = self.column_name
        pb.column_unit = self.column_unit
        pb.expire_after_seconds = self.expire_after_seconds

        return pb


class TtlSettings(object):
    def __init__(self):
        self.date_type_column = None
        self.value_since_unix_epoch = None

    def with_date_type_column(self, column_name, expire_after_seconds=0):
        self.date_type_column = DateTypeColumnModeSettings(
            column_name, expire_after_seconds
        )
        return self

    def with_value_since_unix_epoch(
        self, column_name, column_unit, expire_after_seconds=0
    ):
        self.value_since_unix_epoch = ValueSinceUnixEpochModeSettings(
            column_name, column_unit, expire_after_seconds
        )
        return self

    def to_pb(self):
        pb = _apis.ydb_table.TtlSettings()

        if self.date_type_column is not None:
            pb.date_type_column.MergeFrom(self.date_type_column.to_pb())
        elif self.value_since_unix_epoch is not None:
            pb.value_since_unix_epoch.MergeFrom(self.value_since_unix_epoch.to_pb())
        else:
            raise RuntimeError("Unspecified ttl settings mode")

        return pb


class TableStats(object):
    def __init__(self):
        self.partitions = None
        self.store_size = 0

    def with_store_size(self, store_size):
        self.store_size = store_size
        return self

    def with_partitions(self, partitions):
        self.partitions = partitions
        return self


class ReadReplicasSettings(object):
    def __init__(self):
        self.per_az_read_replicas_count = 0
        self.any_az_read_replicas_count = 0

    def with_any_az_read_replicas_count(self, any_az_read_replicas_count):
        self.any_az_read_replicas_count = any_az_read_replicas_count
        return self

    def with_per_az_read_replicas_count(self, per_az_read_replicas_count):
        self.per_az_read_replicas_count = per_az_read_replicas_count
        return self

    def to_pb(self):
        pb = _apis.ydb_table.ReadReplicasSettings()
        if self.per_az_read_replicas_count > 0:
            pb.per_az_read_replicas_count = self.per_az_read_replicas_count
        elif self.any_az_read_replicas_count > 0:
            pb.any_az_read_replicas_count = self.any_az_read_replicas_count
        return pb


class PartitioningSettings(object):
    def __init__(self):
        self.partitioning_by_size = 0
        self.partition_size_mb = 0
        self.partitioning_by_load = 0
        self.min_partitions_count = 0
        self.max_partitions_count = 0

    def with_max_partitions_count(self, max_partitions_count):
        self.max_partitions_count = max_partitions_count
        return self

    def with_min_partitions_count(self, min_partitions_count):
        self.min_partitions_count = min_partitions_count
        return self

    def with_partitioning_by_load(self, partitioning_by_load):
        self.partitioning_by_load = partitioning_by_load
        return self

    def with_partition_size_mb(self, partition_size_mb):
        self.partition_size_mb = partition_size_mb
        return self

    def with_partitioning_by_size(self, partitioning_by_size):
        self.partitioning_by_size = partitioning_by_size
        return self

    def to_pb(self):
        pb = _apis.ydb_table.PartitioningSettings()
        pb.partitioning_by_size = self.partitioning_by_size
        pb.partition_size_mb = self.partition_size_mb
        pb.partitioning_by_load = self.partitioning_by_load
        pb.min_partitions_count = self.min_partitions_count
        pb.max_partitions_count = self.max_partitions_count
        return pb


class StorageSettings(object):
    def __init__(self):
        self.tablet_commit_log0 = None
        self.tablet_commit_log1 = None
        self.external = None
        self.store_external_blobs = 0

    def with_store_external_blobs(self, store_external_blobs):
        self.store_external_blobs = store_external_blobs
        return self

    def with_external(self, external):
        self.external = external
        return self

    def with_tablet_commit_log1(self, tablet_commit_log1):
        self.tablet_commit_log1 = tablet_commit_log1
        return self

    def with_tablet_commit_log0(self, tablet_commit_log0):
        self.tablet_commit_log0 = tablet_commit_log0
        return self

    def to_pb(self):
        st = _apis.ydb_table.StorageSettings()
        st.store_external_blobs = self.store_external_blobs
        if self.external:
            st.external.MergeFrom(self.external.to_pb())
        if self.tablet_commit_log0:
            st.tablet_commit_log0.MergeFrom(self.tablet_commit_log0.to_pb())
        if self.tablet_commit_log1:
            st.tablet_commit_log1.MergeFrom(self.tablet_commit_log1.to_pb())
        return st


@enum.unique
class Compression(enum.IntEnum):
    UNSPECIFIED = 0
    NONE = 1
    LZ4 = 2


class ColumnFamily(object):
    def __init__(self):
        self.compression = 0
        self.name = None
        self.data = None
        self.keep_in_memory = 0

    def with_name(self, name):
        self.name = name
        return self

    def with_compression(self, compression):
        self.compression = compression
        return self

    def with_data(self, data):
        self.data = data
        return self

    def with_keep_in_memory(self, keep_in_memory):
        self.keep_in_memory = keep_in_memory
        return self

    def to_pb(self):
        cm = _apis.ydb_table.ColumnFamily()
        cm.keep_in_memory = self.keep_in_memory
        cm.compression = self.compression
        if self.name is not None:
            cm.name = self.name
        if self.data is not None:
            cm.data.MergeFrom(self.data.to_pb())
        return cm


class TableDescription(object):
    def __init__(self):
        self.columns = []
        self.primary_key = []
        self.profile = None
        self.indexes = []
        self.column_families = []
        self.ttl_settings = None
        self.attributes = {}
        self.uniform_partitions = 0
        self.partition_at_keys = None
        self.compaction_policy = None
        self.key_bloom_filter = 0
        self.read_replicas_settings = None
        self.partitioning_settings = None
        self.storage_settings = None

    def with_storage_settings(self, storage_settings):
        self.storage_settings = storage_settings
        return self

    def with_column(self, column):
        self.columns.append(column)
        return self

    def with_columns(self, *columns):
        for column in columns:
            self.with_column(column)
        return self

    def with_primary_key(self, key):
        self.primary_key.append(key)
        return self

    def with_primary_keys(self, *keys):
        for pk in keys:
            self.with_primary_key(pk)
        return self

    def with_column_family(self, column_family):
        self.column_families.append(column_family)
        return self

    def with_column_families(self, *column_families):
        for column_family in column_families:
            self.with_column_family(column_family)
        return self

    def with_indexes(self, *indexes):
        for index in indexes:
            self.with_index(index)
        return self

    def with_index(self, index):
        self.indexes.append(index)
        return self

    def with_profile(self, profile):
        self.profile = profile
        return self

    def with_ttl(self, ttl_settings):
        self.ttl_settings = ttl_settings
        return self

    def with_attributes(self, attributes):
        self.attributes = attributes
        return self

    def with_uniform_partitions(self, uniform_partitions):
        self.uniform_partitions = uniform_partitions
        return self

    def with_partition_at_keys(self, partition_at_keys):
        self.partition_at_keys = partition_at_keys
        return self

    def with_key_bloom_filter(self, key_bloom_filter):
        self.key_bloom_filter = key_bloom_filter
        return self

    def with_partitioning_settings(self, partitioning_settings):
        self.partitioning_settings = partitioning_settings
        return self

    def with_read_replicas_settings(self, read_replicas_settings):
        self.read_replicas_settings = read_replicas_settings
        return self

    def with_compaction_policy(self, compaction_policy):
        self.compaction_policy = compaction_policy
        return self


@six.add_metaclass(abc.ABCMeta)
class AbstractTransactionModeBuilder(object):
    @property
    @abc.abstractmethod
    def name(self):
        pass

    @property
    @abc.abstractmethod
    def settings(self):
        pass


class SnapshotReadOnly(AbstractTransactionModeBuilder):
    __slots__ = ("_pb", "_name")

    def __init__(self):
        self._pb = _apis.ydb_table.SnapshotModeSettings()
        self._name = "snapshot_read_only"

    @property
    def settings(self):
        return self._pb

    @property
    def name(self):
        return self._name


class SerializableReadWrite(AbstractTransactionModeBuilder):
    __slots__ = ("_pb", "_name")

    def __init__(self):
        self._name = "serializable_read_write"
        self._pb = _apis.ydb_table.SerializableModeSettings()

    @property
    def settings(self):
        return self._pb

    @property
    def name(self):
        return self._name


class OnlineReadOnly(AbstractTransactionModeBuilder):
    __slots__ = ("_pb", "_name")

    def __init__(self):
        self._pb = _apis.ydb_table.OnlineModeSettings()
        self._pb.allow_inconsistent_reads = False
        self._name = "online_read_only"

    def with_allow_inconsistent_reads(self):
        self._pb.allow_inconsistent_reads = True
        return self

    @property
    def settings(self):
        return self._pb

    @property
    def name(self):
        return self._name


class StaleReadOnly(AbstractTransactionModeBuilder):
    __slots__ = ("_pb", "_name")

    def __init__(self):
        self._pb = _apis.ydb_table.StaleModeSettings()
        self._name = "stale_read_only"

    @property
    def settings(self):
        return self._pb

    @property
    def name(self):
        return self._name


class BackoffSettings(object):
    def __init__(self, ceiling=6, slot_duration=0.001, uncertain_ratio=0.5):
        self.ceiling = ceiling
        self.slot_duration = slot_duration
        self.uncertain_ratio = uncertain_ratio

    def calc_timeout(self, retry_number):
        slots_count = 1 << min(retry_number, self.ceiling)
        max_duration_ms = slots_count * self.slot_duration * 1000.0
        # duration_ms = random.random() * max_duration_ms * uncertain_ratio) + max_duration_ms * (1 - uncertain_ratio)
        duration_ms = max_duration_ms * (
            random.random() * self.uncertain_ratio + 1.0 - self.uncertain_ratio
        )
        return duration_ms / 1000.0


class RetrySettings(object):
    def __init__(
        self,
        max_retries=10,
        max_session_acquire_timeout=None,
        on_ydb_error_callback=None,
        backoff_ceiling=6,
        backoff_slot_duration=1,
        get_session_client_timeout=5,
        fast_backoff_settings=None,
        slow_backoff_settings=None,
        idempotent=False,
    ):
        self.max_retries = max_retries
        self.max_session_acquire_timeout = max_session_acquire_timeout
        self.on_ydb_error_callback = (
            (lambda e: None) if on_ydb_error_callback is None else on_ydb_error_callback
        )
        self.fast_backoff = (
            BackoffSettings(10, 0.005)
            if fast_backoff_settings is None
            else fast_backoff_settings
        )
        self.slow_backoff = (
            BackoffSettings(backoff_ceiling, backoff_slot_duration)
            if slow_backoff_settings is None
            else slow_backoff_settings
        )
        self.retry_not_found = True
        self.idempotent = idempotent
        self.retry_internal_error = True
        self.unknown_error_handler = lambda e: None
        self.get_session_client_timeout = get_session_client_timeout
        if max_session_acquire_timeout is not None:
            self.get_session_client_timeout = min(
                self.max_session_acquire_timeout, self.get_session_client_timeout
            )

    def with_fast_backoff(self, backoff_settings):
        self.fast_backoff = backoff_settings
        return self

    def with_slow_backoff(self, backoff_settings):
        self.slow_backoff = backoff_settings
        return self


class YdbRetryOperationSleepOpt(object):
    def __init__(self, timeout):
        self.timeout = timeout

    def __eq__(self, other):
        return type(self) == type(other) and self.timeout == other.timeout

    def __repr__(self):
        return "YdbRetryOperationSleepOpt(%s)" % self.timeout


class YdbRetryOperationFinalResult(object):
    def __init__(self, result):
        self.result = result
        self.exc = None

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and self.result == other.result
            and self.exc == other.exc
        )

    def __repr__(self):
        return "YdbRetryOperationFinalResult(%s, exc=%s)" % (self.result, self.exc)

    def set_exception(self, exc):
        self.exc = exc


def retry_operation_impl(callee, retry_settings=None, *args, **kwargs):
    retry_settings = RetrySettings() if retry_settings is None else retry_settings
    status = None

    for attempt in six.moves.range(retry_settings.max_retries + 1):
        try:
            result = YdbRetryOperationFinalResult(callee(*args, **kwargs))
            yield result

            if result.exc is not None:
                raise result.exc

        except issues.Error as e:
            status = e
            retry_settings.on_ydb_error_callback(e)

            retriable_info = check_retriable_error(e, retry_settings, attempt)
            if not retriable_info.is_retriable:
                raise

            skip_yield_error_types = [
                issues.Aborted,
                issues.BadSession,
                issues.NotFound,
                issues.InternalError,
            ]

            yield_sleep = True
            for t in skip_yield_error_types:
                if isinstance(e, t):
                    yield_sleep = False

            if yield_sleep:
                yield YdbRetryOperationSleepOpt(retriable_info.sleep_timeout_seconds)

        except Exception as e:
            # you should provide your own handler you want
            retry_settings.unknown_error_handler(e)
            raise

    raise status


def retry_operation_sync(callee, retry_settings=None, *args, **kwargs):
    opt_generator = retry_operation_impl(callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, YdbRetryOperationSleepOpt):
            time.sleep(next_opt.timeout)
        else:
            return next_opt.result


class TableClientSettings(object):
    def __init__(self):
        self._client_query_cache_enabled = False
        self._native_datetime_in_result_sets = False
        self._native_date_in_result_sets = False
        self._make_result_sets_lazy = False
        self._native_json_in_result_sets = False
        self._native_interval_in_result_sets = False
        self._native_timestamp_in_result_sets = False
        self._allow_truncated_result = convert._default_allow_truncated_result

    def with_native_timestamp_in_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._native_timestamp_in_result_sets = enabled
        return self

    def with_native_interval_in_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._native_interval_in_result_sets = enabled
        return self

    def with_native_json_in_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._native_json_in_result_sets = enabled
        return self

    def with_native_date_in_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._native_date_in_result_sets = enabled
        return self

    def with_native_datetime_in_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._native_datetime_in_result_sets = enabled
        return self

    def with_client_query_cache(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._client_query_cache_enabled = enabled
        return self

    def with_lazy_result_sets(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._make_result_sets_lazy = enabled
        return self

    def with_allow_truncated_result(self, enabled):
        # type:(bool) -> ydb.TableClientSettings
        self._allow_truncated_result = enabled
        return self


class ScanQueryResult(object):
    def __init__(self, result, table_client_settings):
        self._result = result
        self.query_stats = result.query_stats
        self.result_set = convert.ResultSet.from_message(
            self._result.result_set, table_client_settings
        )


@enum.unique
class QueryStatsCollectionMode(enum.IntEnum):
    NONE = _apis.ydb_table.QueryStatsCollection.Mode.STATS_COLLECTION_NONE
    BASIC = _apis.ydb_table.QueryStatsCollection.Mode.STATS_COLLECTION_BASIC
    FULL = _apis.ydb_table.QueryStatsCollection.Mode.STATS_COLLECTION_FULL


class ScanQuerySettings(settings_impl.BaseRequestSettings):
    def __init__(self):
        super(ScanQuerySettings, self).__init__()
        self.collect_stats = None

    def with_collect_stats(self, collect_stats_mode):
        self.collect_stats = collect_stats_mode
        return self


class ScanQuery(object):
    def __init__(self, yql_text, parameters_types):
        self.yql_text = yql_text
        self.parameters_types = parameters_types


def _wrap_scan_query_response(response, table_client_settings):
    issues._process_response(response)
    return ScanQueryResult(response.result, table_client_settings)


def _scan_query_request_factory(query, parameters=None, settings=None):
    if not isinstance(query, ScanQuery):
        query = ScanQuery(query, {})
    parameters = {} if parameters is None else parameters
    collect_stats = getattr(
        settings,
        "collect_stats",
        _apis.ydb_table.QueryStatsCollection.Mode.STATS_COLLECTION_NONE,
    )
    return _apis.ydb_table.ExecuteScanQueryRequest(
        mode=_apis.ydb_table.ExecuteScanQueryRequest.Mode.MODE_EXEC,
        query=_apis.ydb_table.Query(yql_text=query.yql_text),
        parameters=convert.parameters_to_pb(query.parameters_types, parameters),
        collect_stats=collect_stats,
    )


@six.add_metaclass(abc.ABCMeta)
class ISession:
    @abstractmethod
    def __init__(self, driver, table_client_settings):
        pass

    @abstractmethod
    def __lt__(self, other):
        pass

    @abstractmethod
    def __eq__(self, other):
        pass

    @property
    @abstractmethod
    def session_id(self):
        pass

    @abstractmethod
    def initialized(self):
        """
        Return True if session is successfully initialized with a session_id and False otherwise.
        """
        pass

    @abstractmethod
    def pending_query(self):
        pass

    @abstractmethod
    def reset(self):
        """
        Perform session state reset (that includes cleanup of the session_id, query cache, and etc.)
        """
        pass

    @abstractmethod
    def read_table(
        self,
        path,
        key_range=None,
        columns=(),
        ordered=False,
        row_limit=None,
        settings=None,
        use_snapshot=None,
    ):
        """
        Perform an read table request.

        :param path: A path to the table
        :param key_range: (optional) A KeyRange instance that describes a range to read. The KeyRange instance\
        should include from_bound and/or to_bound. Each of the bounds (if provided) should specify a value of the\
        key bound, and type of the key prefix. See an example above.
        :param columns: (optional) An iterable with table columns to read.
        :param ordered: (optional) A flag that indicates that result should be ordered.
        :param row_limit: (optional) A number of rows to read.
        :param settings: Request settings

        :return: SyncResponseIterator instance
        """
        pass

    @abstractmethod
    def keep_alive(self, settings=None):
        pass

    @abstractmethod
    def create(self, settings=None):
        pass

    @abstractmethod
    def delete(self, settings=None):
        pass

    @abstractmethod
    def execute_scheme(self, yql_text, settings=None):
        pass

    @abstractmethod
    def transaction(
        self, tx_mode=None, allow_split_transactions=_allow_split_transaction
    ):
        pass

    @abstractmethod
    def has_prepared(self, query):
        pass

    @abstractmethod
    def prepare(self, query, settings=None):
        pass

    @abstractmethod
    def explain(self, yql_text, settings=None):
        """
        Expiremental API.

        :param yql_text:
        :param settings:

        :return:
        """
        pass

    @abstractmethod
    def create_table(self, path, table_description, settings=None):
        """
        Create a YDB table.

        :param path: A table path
        :param table_description: A description of table to create. An instance TableDescription
        :param settings: An instance of BaseRequestSettings that describes how rpc should invoked.

        :return: A description of created scheme entry or error otherwise.
        """
        pass

    @abstractmethod
    def drop_table(self, path, settings=None):
        pass

    @abstractmethod
    def alter_table(
        self,
        path,
        add_columns=None,
        drop_columns=None,
        settings=None,
        alter_attributes=None,
        add_indexes=None,
        drop_indexes=None,
        set_ttl_settings=None,
        drop_ttl_settings=None,
        add_column_families=None,
        alter_column_families=None,
        alter_storage_settings=None,
        set_compaction_policy=None,
        alter_partitioning_settings=None,
        set_key_bloom_filter=None,
        set_read_replicas_settings=None,
    ):
        pass

    @abstractmethod
    def copy_table(self, source_path, destination_path, settings=None):
        pass

    @abstractmethod
    def copy_tables(self, source_destination_pairs, settings=None):
        pass

    def describe_table(self, path, settings=None):
        """
        Returns a description of the table by provided path

        :param path: A table path
        :param settings: A request settings

        :return: Description of a table
        """
        pass


@six.add_metaclass(abc.ABCMeta)
class ITableClient:
    def __init__(self, driver, table_client_settings=None):
        pass

    @abstractmethod
    def session(self):
        pass

    @abstractmethod
    def scan_query(self, query, parameters=None, settings=None):
        pass

    @abstractmethod
    def bulk_upsert(self, table_path, rows, column_types, settings=None):
        """
        Bulk upsert data

        :param table_path: A table path.
        :param rows: A list of structures.
        :param column_types: Bulk upsert column types.

        """
        pass


class BaseTableClient(ITableClient):
    def __init__(self, driver, table_client_settings=None):
        # type:(ydb.Driver, ydb.TableClientSettings) -> None
        self._driver = driver
        self._table_client_settings = (
            TableClientSettings()
            if table_client_settings is None
            else table_client_settings
        )

    def session(self):
        # type: () -> ydb.Session
        return Session(self._driver, self._table_client_settings)

    def scan_query(self, query, parameters=None, settings=None):
        # type: (ydb.ScanQuery, tuple, ydb.BaseRequestSettings) -> ydb.SyncResponseIterator
        request = _scan_query_request_factory(query, parameters, settings)
        stream_it = self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamExecuteScanQuery,
            settings=settings,
        )
        return _utilities.SyncResponseIterator(
            stream_it,
            lambda resp: _wrap_scan_query_response(resp, self._table_client_settings),
        )

    def bulk_upsert(self, table_path, rows, column_types, settings=None):
        # type: (str, list, ydb.AbstractTypeBuilder | ydb.PrimitiveType, ydb.BaseRequestSettings) -> None
        """
        Bulk upsert data

        :param table_path: A table path.
        :param rows: A list of structures.
        :param column_types: Bulk upsert column types.

        """
        return self._driver(
            _session_impl.bulk_upsert_request_factory(table_path, rows, column_types),
            _apis.TableService.Stub,
            _apis.TableService.BulkUpsert,
            _session_impl.wrap_operation_bulk_upsert,
            settings,
            (),
        )


class TableClient(BaseTableClient):
    def async_scan_query(self, query, parameters=None, settings=None):
        # type: (ydb.ScanQuery, tuple, ydb.BaseRequestSettings) -> ydb.AsyncResponseIterator
        request = _scan_query_request_factory(query, parameters, settings)
        stream_it = self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamExecuteScanQuery,
            settings=settings,
        )
        return _utilities.AsyncResponseIterator(
            stream_it,
            lambda resp: _wrap_scan_query_response(resp, self._table_client_settings),
        )

    @_utilities.wrap_async_call_exceptions
    def async_bulk_upsert(self, table_path, rows, column_types, settings=None):
        # type: (str, list, ydb.AbstractTypeBuilder | ydb.PrimitiveType, ydb.BaseRequestSettings) -> None
        return self._driver.future(
            _session_impl.bulk_upsert_request_factory(table_path, rows, column_types),
            _apis.TableService.Stub,
            _apis.TableService.BulkUpsert,
            _session_impl.wrap_operation_bulk_upsert,
            settings,
            (),
        )


def _make_index_description(index):
    result = TableIndex(index.name).with_index_columns(
        *tuple(col for col in index.index_columns)
    )
    result.status = IndexStatus(index.status)
    return result


class TableSchemeEntry(scheme.SchemeEntry):
    def __init__(
        self,
        name,
        owner,
        type,
        effective_permissions,
        permissions,
        size_bytes,
        columns,
        primary_key,
        shard_key_bounds,
        indexes,
        table_stats,
        ttl_settings,
        attributes,
        partitioning_settings,
        column_families,
        key_bloom_filter,
        read_replicas_settings,
        storage_settings,
        *args,
        **kwargs
    ):

        super(TableSchemeEntry, self).__init__(
            name,
            owner,
            type,
            effective_permissions,
            permissions,
            size_bytes,
            *args,
            **kwargs
        )
        self.primary_key = [pk for pk in primary_key]
        self.columns = [
            Column(column.name, convert.type_to_native(column.type), column.family)
            for column in columns
        ]
        self.indexes = [_make_index_description(index) for index in indexes]
        self.shard_key_ranges = []
        self.column_families = []
        self.key_bloom_filter = FeatureFlag(key_bloom_filter)
        left_key_bound = None
        for column_family in column_families:
            self.column_families.append(
                ColumnFamily()
                .with_name(column_family.name)
                .with_keep_in_memory(FeatureFlag(column_family.keep_in_memory))
                .with_compression(Compression(column_family.compression))
            )

            if column_family.HasField("data"):
                self.column_families[-1].with_data(
                    StoragePool(column_family.data.media)
                )

        for shard_key_bound in shard_key_bounds:
            # for next key range
            key_bound_type = shard_key_bound.type
            current_bound = convert.to_native_value(shard_key_bound)
            self.shard_key_ranges.append(
                KeyRange(
                    None
                    if left_key_bound is None
                    else KeyBound.inclusive(left_key_bound, key_bound_type),
                    KeyBound.exclusive(current_bound, key_bound_type),
                )
            )
            left_key_bound = current_bound

            assert isinstance(left_key_bound, tuple)

        if len(shard_key_bounds) > 0:
            self.shard_key_ranges.append(
                KeyRange(
                    KeyBound.inclusive(left_key_bound, shard_key_bounds[-1].type),
                    None,
                )
            )

        else:
            self.shard_key_ranges.append(KeyRange(None, None))

        self.read_replicas_settings = None
        if read_replicas_settings is not None:
            self.read_replicas_settings = ReadReplicasSettings()
            for field in ("per_az_read_replicas_count", "any_az_read_replicas_count"):
                if read_replicas_settings.WhichOneof("settings") == field:
                    setattr(
                        self.read_replicas_settings,
                        field,
                        getattr(read_replicas_settings, field),
                    )

        self.storage_settings = None
        if storage_settings is not None:
            self.storage_settings = StorageSettings()
            self.storage_settings.store_external_blobs = FeatureFlag(
                self.storage_settings.store_external_blobs
            )
            if storage_settings.HasField("tablet_commit_log0"):
                self.storage_settings.with_tablet_commit_log0(
                    StoragePool(storage_settings.tablet_commit_log0.media)
                )

            if storage_settings.HasField("tablet_commit_log1"):
                self.storage_settings.with_tablet_commit_log1(
                    StoragePool(storage_settings.tablet_commit_log1.media)
                )

            if storage_settings.HasField("external"):
                self.storage_settings.with_external(
                    StoragePool(storage_settings.external.media)
                )

        self.partitioning_settings = None
        if partitioning_settings is not None:
            self.partitioning_settings = PartitioningSettings()
            for field in (
                "partitioning_by_size",
                "partitioning_by_load",
                "partition_size_mb",
                "min_partitions_count",
                "max_partitions_count",
            ):
                setattr(
                    self.partitioning_settings,
                    field,
                    getattr(partitioning_settings, field),
                )

        self.ttl_settings = None
        if ttl_settings is not None:
            if ttl_settings.HasField("date_type_column"):
                self.ttl_settings = TtlSettings().with_date_type_column(
                    ttl_settings.date_type_column.column_name,
                    ttl_settings.date_type_column.expire_after_seconds,
                )
            elif ttl_settings.HasField("value_since_unix_epoch"):
                self.ttl_settings = TtlSettings().with_value_since_unix_epoch(
                    ttl_settings.value_since_unix_epoch.column_name,
                    ColumnUnit(ttl_settings.value_since_unix_epoch.column_unit),
                    ttl_settings.value_since_unix_epoch.expire_after_seconds,
                )

        self.table_stats = None
        if table_stats is not None:
            self.table_stats = TableStats()
            if table_stats.partitions != 0:
                self.table_stats = self.table_stats.with_partitions(
                    table_stats.partitions
                )

            if table_stats.store_size != 0:
                self.table_stats = self.table_stats.with_store_size(
                    table_stats.store_size
                )

        self.attributes = attributes


class RenameItem:
    def __init__(self, source_path, destination_path, replace_destination=False):
        self._source_path = source_path
        self._destination_path = destination_path
        self._replace_destination = replace_destination

    @property
    def source_path(self):
        return self._source_path

    @property
    def destination_path(self):
        return self._destination_path

    @property
    def replace_destination(self):
        return self._replace_destination


class BaseSession(ISession):
    def __init__(self, driver, table_client_settings):
        self._driver = driver
        self._state = _session_impl.SessionState(table_client_settings)

    def __lt__(self, other):
        return self.session_id < other.session_id

    def __eq__(self, other):
        return self.session_id == other.session_id

    @property
    def session_id(self):
        """
        Return session_id.
        """
        return self._state.session_id

    def initialized(self):
        """
        Return True if session is successfully initialized with a session_id and False otherwise.
        """
        return self._state.session_id is not None

    def pending_query(self):
        return self._state.pending_query()

    def closing(self):
        """Returns True if session is closing."""
        return self._state.closing()

    def reset(self):
        """
        Perform session state reset (that includes cleanup of the session_id, query cache, and etc.)
        """
        return self._state.reset()

    def read_table(
        self,
        path,
        key_range=None,
        columns=(),
        ordered=False,
        row_limit=None,
        settings=None,
        use_snapshot=None,
    ):
        """
        Perform an read table request.

        :param path: A path to the table
        :param key_range: (optional) A KeyRange instance that describes a range to read. The KeyRange instance\
        should include from_bound and/or to_bound. Each of the bounds (if provided) should specify a value of the\
        key bound, and type of the key prefix. See an example above.
        :param columns: (optional) An iterable with table columns to read.
        :param ordered: (optional) A flag that indicates that result should be ordered.
        :param row_limit: (optional) A number of rows to read.

        :return: SyncResponseIterator instance
        """
        request = _session_impl.read_table_request_factory(
            self._state,
            path,
            key_range,
            columns,
            ordered,
            row_limit,
            use_snapshot=use_snapshot,
        )
        stream_it = self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamReadTable,
            settings=settings,
        )
        return _utilities.SyncResponseIterator(
            stream_it, _session_impl.wrap_read_table_response
        )

    def keep_alive(self, settings=None):
        return self._driver(
            _session_impl.keep_alive_request_factory(self._state),
            _apis.TableService.Stub,
            _apis.TableService.KeepAlive,
            _session_impl.wrap_keep_alive_response,
            settings,
            (self._state, self),
            self._state.endpoint,
        )

    def create(self, settings=None):
        if self._state.session_id is not None:
            return self
        create_settings = settings_impl.BaseRequestSettings()
        if settings is not None:
            create_settings = settings.make_copy()
        create_settings = create_settings.with_header(
            "x-ydb-client-capabilities", "session-balancer"
        )
        return self._driver(
            _apis.ydb_table.CreateSessionRequest(),
            _apis.TableService.Stub,
            _apis.TableService.CreateSession,
            _session_impl.initialize_session,
            create_settings,
            (self._state, self),
            self._state.endpoint,
        )

    def delete(self, settings=None):
        return self._driver(
            self._state.attach_request(_apis.ydb_table.DeleteSessionRequest()),
            _apis.TableService.Stub,
            _apis.TableService.DeleteSession,
            _session_impl.cleanup_session,
            settings,
            (self._state, self),
            self._state.endpoint,
        )

    def execute_scheme(self, yql_text, settings=None):
        return self._driver(
            _session_impl.execute_scheme_request_factory(self._state, yql_text),
            _apis.TableService.Stub,
            _apis.TableService.ExecuteSchemeQuery,
            _session_impl.wrap_execute_scheme_result,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    def transaction(
        self, tx_mode=None, allow_split_transactions=_allow_split_transaction
    ):
        return TxContext(
            self._driver,
            self._state,
            self,
            tx_mode,
            allow_split_transactions=allow_split_transactions,
        )

    def has_prepared(self, query):
        return query in self._state

    def prepare(self, query, settings=None):
        data_query, _ = self._state.lookup(query)
        if data_query is not None:
            return data_query
        return self._driver(
            _session_impl.prepare_request_factory(self._state, query),
            _apis.TableService.Stub,
            _apis.TableService.PrepareDataQuery,
            _session_impl.wrap_prepare_query_response,
            settings,
            (self._state, query),
            self._state.endpoint,
        )

    def explain(self, yql_text, settings=None):
        """
        Expiremental API.

        :param yql_text:
        :param settings:

        :return:
        """
        return self._driver(
            _session_impl.explain_data_query_request_factory(self._state, yql_text),
            _apis.TableService.Stub,
            _apis.TableService.ExplainDataQuery,
            _session_impl.wrap_explain_response,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    def create_table(self, path, table_description, settings=None):
        """
        Create a YDB table.

        :param path: A table path
        :param table_description: A description of table to create. An instance TableDescription
        :param settings: An instance of BaseRequestSettings that describes how rpc should invoked.

        :return: A description of created scheme entry or error otherwise.
        """
        return self._driver(
            _session_impl.create_table_request_factory(
                self._state, path, table_description
            ),
            _apis.TableService.Stub,
            _apis.TableService.CreateTable,
            _session_impl.wrap_operation,
            settings,
            (self._driver,),
            self._state.endpoint,
        )

    def drop_table(self, path, settings=None):
        return self._driver(
            self._state.attach_request(_apis.ydb_table.DropTableRequest(path=path)),
            _apis.TableService.Stub,
            _apis.TableService.DropTable,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    def alter_table(
        self,
        path,
        add_columns=None,
        drop_columns=None,
        settings=None,
        alter_attributes=None,
        add_indexes=None,
        drop_indexes=None,
        set_ttl_settings=None,
        drop_ttl_settings=None,
        add_column_families=None,
        alter_column_families=None,
        alter_storage_settings=None,
        set_compaction_policy=None,
        alter_partitioning_settings=None,
        set_key_bloom_filter=None,
        set_read_replicas_settings=None,
    ):
        return self._driver(
            _session_impl.alter_table_request_factory(
                self._state,
                path,
                add_columns,
                drop_columns,
                alter_attributes,
                add_indexes,
                drop_indexes,
                set_ttl_settings,
                drop_ttl_settings,
                add_column_families,
                alter_column_families,
                alter_storage_settings,
                set_compaction_policy,
                alter_partitioning_settings,
                set_key_bloom_filter,
                set_read_replicas_settings,
            ),
            _apis.TableService.Stub,
            _apis.TableService.AlterTable,
            _session_impl.AlterTableOperation,
            settings,
            (self._driver,),
            self._state.endpoint,
        )

    def describe_table(self, path, settings=None):
        """
        Returns a description of the table by provided path

        :param path: A table path
        :param settings: A request settings

        :return: Description of a table
        """
        return self._driver(
            _session_impl.describe_table_request_factory(self._state, path, settings),
            _apis.TableService.Stub,
            _apis.TableService.DescribeTable,
            _session_impl.wrap_describe_table_response,
            settings,
            (self._state, TableSchemeEntry),
            self._state.endpoint,
        )

    def copy_table(self, source_path, destination_path, settings=None):
        return self.copy_tables([(source_path, destination_path)], settings=settings)

    def copy_tables(self, source_destination_pairs, settings=None):
        return self._driver(
            _session_impl.copy_tables_request_factory(
                self._state, source_destination_pairs
            ),
            _apis.TableService.Stub,
            _apis.TableService.CopyTables,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    def rename_tables(self, rename_items, settings=None):
        return self._driver(
            _session_impl.rename_tables_request_factory(self._state, rename_items),
            _apis.TableService.Stub,
            _apis.TableService.RenameTables,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )


class Session(BaseSession):
    def async_read_table(
        self,
        path,
        key_range=None,
        columns=(),
        ordered=False,
        row_limit=None,
        settings=None,
        use_snapshot=None,
    ):
        """
        Perform an read table request.

        :param path: A path to the table
        :param key_range: (optional) A KeyRange instance that describes a range to read. The KeyRange instance\
        should include from_bound and/or to_bound. Each of the bounds (if provided) should specify a value of the\
        key bound, and type of the key prefix. See an example above.
        :param columns: (optional) An iterable with table columns to read.
        :param ordered: (optional) A flag that indicates that result should be ordered.
        :param row_limit: (optional) A number of rows to read.

        :return: AsyncResponseIterator instance
        """
        if interceptor is None:
            raise RuntimeError("Async read table is not available due to import issues")
        request = _session_impl.read_table_request_factory(
            self._state,
            path,
            key_range,
            columns,
            ordered,
            row_limit,
            use_snapshot=use_snapshot,
        )
        stream_it = self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamReadTable,
            settings=settings,
        )
        return _utilities.AsyncResponseIterator(
            stream_it, _session_impl.wrap_read_table_response
        )

    @_utilities.wrap_async_call_exceptions
    def async_keep_alive(self, settings=None):
        return self._driver.future(
            _session_impl.keep_alive_request_factory(self._state),
            _apis.TableService.Stub,
            _apis.TableService.KeepAlive,
            _session_impl.wrap_keep_alive_response,
            settings,
            (self._state, self),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_create(self, settings=None):
        if self._state.session_id is not None:
            return _utilities.wrap_result_in_future(self)
        create_settings = settings_impl.BaseRequestSettings()
        if settings is not None:
            create_settings = settings.make_copy()
        create_settings = create_settings.with_header(
            "x-ydb-client-capabilities", "session-balancer"
        )
        return self._driver.future(
            _apis.ydb_table.CreateSessionRequest(),
            _apis.TableService.Stub,
            _apis.TableService.CreateSession,
            _session_impl.initialize_session,
            create_settings,
            (self._state, self),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_delete(self, settings=None):
        return self._driver.future(
            self._state.attach_request(_apis.ydb_table.DeleteSessionRequest()),
            _apis.TableService.Stub,
            _apis.TableService.DeleteSession,
            _session_impl.cleanup_session,
            settings,
            (self._state, self),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_execute_scheme(self, yql_text, settings=None):
        return self._driver.future(
            _session_impl.execute_scheme_request_factory(self._state, yql_text),
            _apis.TableService.Stub,
            _apis.TableService.ExecuteSchemeQuery,
            _session_impl.wrap_execute_scheme_result,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_prepare(self, query, settings=None):
        data_query, _ = self._state.lookup(query)
        if data_query is not None:
            return _utilities.wrap_result_in_future(data_query)
        return self._driver.future(
            _session_impl.prepare_request_factory(self._state, query),
            _apis.TableService.Stub,
            _apis.TableService.PrepareDataQuery,
            _session_impl.wrap_prepare_query_response,
            settings,
            (
                self._state,
                query,
            ),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_create_table(self, path, table_description, settings=None):
        return self._driver.future(
            _session_impl.create_table_request_factory(
                self._state, path, table_description
            ),
            _apis.TableService.Stub,
            _apis.TableService.CreateTable,
            _session_impl.wrap_operation,
            settings,
            (self._driver,),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_drop_table(self, path, settings=None):
        return self._driver.future(
            self._state.attach_request(_apis.ydb_table.DropTableRequest(path=path)),
            _apis.TableService.Stub,
            _apis.TableService.DropTable,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_alter_table(
        self,
        path,
        add_columns=None,
        drop_columns=None,
        settings=None,
        alter_attributes=None,
        add_indexes=None,
        drop_indexes=None,
        set_ttl_settings=None,
        drop_ttl_settings=None,
        add_column_families=None,
        alter_column_families=None,
        alter_storage_settings=None,
        set_compaction_policy=None,
        alter_partitioning_settings=None,
        set_key_bloom_filter=None,
        set_read_replicas_settings=None,
    ):
        return self._driver.future(
            _session_impl.alter_table_request_factory(
                self._state,
                path,
                add_columns,
                drop_columns,
                alter_attributes,
                add_indexes,
                drop_indexes,
                set_ttl_settings,
                drop_ttl_settings,
                add_column_families,
                alter_column_families,
                alter_storage_settings,
                set_compaction_policy,
                alter_partitioning_settings,
                set_key_bloom_filter,
                set_read_replicas_settings,
            ),
            _apis.TableService.Stub,
            _apis.TableService.AlterTable,
            _session_impl.AlterTableOperation,
            settings,
            (self._driver,),
            self._state.endpoint,
        )

    def async_copy_table(self, source_path, destination_path, settings=None):
        return self.async_copy_tables(
            [(source_path, destination_path)], settings=settings
        )

    @_utilities.wrap_async_call_exceptions
    def async_copy_tables(self, source_destination_pairs, settings=None):
        return self._driver.future(
            _session_impl.copy_tables_request_factory(
                self._state, source_destination_pairs
            ),
            _apis.TableService.Stub,
            _apis.TableService.CopyTables,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_rename_tables(self, rename_tables, settings=None):
        return self._driver.future(
            _session_impl.rename_tables_request_factory(self._state, rename_tables),
            _apis.TableService.Stub,
            _apis.TableService.RenameTables,
            _session_impl.wrap_operation,
            settings,
            (self._state,),
            self._state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_describe_table(self, path, settings=None):
        return self._driver.future(
            _session_impl.describe_table_request_factory(self._state, path, settings),
            _apis.TableService.Stub,
            _apis.TableService.DescribeTable,
            _session_impl.wrap_describe_table_response,
            settings,
            (self._state, TableSchemeEntry),
            self._state.endpoint,
        )


@six.add_metaclass(abc.ABCMeta)
class ITxContext:
    @abstractmethod
    def __init__(self, driver, session_state, session, tx_mode=None):
        """
        An object that provides a simple transaction context manager that allows statements execution
        in a transaction. You don't have to open transaction explicitly, because context manager encapsulates
        transaction control logic, and opens new transaction if:
         1) By explicit .begin();
         2) On execution of a first statement, which is strictly recommended method, because that avoids
         useless round trip

        This context manager is not thread-safe, so you should not manipulate on it concurrently.

        :param driver: A driver instance
        :param session_state: A state of session
        :param tx_mode: A transaction mode, which is a one from the following choices:
         1) SerializableReadWrite() which is default mode;
         2) OnlineReadOnly();
         3) StaleReadOnly().
        """
        pass

    @abstractmethod
    def __enter__(self):
        """
        Enters a context manager and returns a session

        :return: A session instance
        """
        pass

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        """
        Closes a transaction context manager and rollbacks transaction if
        it is not rolled back explicitly
        """
        pass

    @property
    @abstractmethod
    def session_id(self):
        """
        A transaction's session id

        :return: A transaction's session id
        """
        pass

    @property
    @abstractmethod
    def tx_id(self):
        """
        Returns a id of open transaction or None otherwise

        :return: A id of open transaction or None otherwise
        """
        pass

    @abstractmethod
    def execute(self, query, parameters=None, commit_tx=False, settings=None):
        """
        Sends a query (yql text or an instance of DataQuery) to be executed with parameters.
        Execution with parameters supported only for DataQuery instances and is not supported yql text queries.

        :param query: A query, yql text or DataQuery instance.
        :param parameters: A dictionary with parameters values.
        :param commit_tx: A special flag that allows transaction commit
        :param settings: An additional request settings

        :return: A result sets or exception in case of execution errors
        """
        pass

    @abstractmethod
    def commit(self, settings=None):
        """
        Calls commit on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A committed transaction or exception if commit is failed
        """
        pass

    @abstractmethod
    def rollback(self, settings=None):
        """
        Calls rollback on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A rolled back transaction or exception if rollback is failed
        """
        pass

    @abstractmethod
    def begin(self, settings=None):
        """
        Explicitly begins a transaction

        :param settings: A request settings

        :return: An open transaction
        """
        pass


class BaseTxContext(ITxContext):
    __slots__ = (
        "_tx_state",
        "_session_state",
        "_driver",
        "session",
        "_finished",
        "_allow_split_transactions",
    )

    _COMMIT = "commit"
    _ROLLBACK = "rollback"

    def __init__(
        self,
        driver,
        session_state,
        session,
        tx_mode=None,
        allow_split_transactions=_allow_split_transaction,
    ):
        """
        An object that provides a simple transaction context manager that allows statements execution
        in a transaction. You don't have to open transaction explicitly, because context manager encapsulates
        transaction control logic, and opens new transaction if:

        1) By explicit .begin() and .async_begin() methods;
        2) On execution of a first statement, which is strictly recommended method, because that avoids useless round trip

        This context manager is not thread-safe, so you should not manipulate on it concurrently.

        :param driver: A driver instance
        :param session_state: A state of session
        :param tx_mode: A transaction mode, which is a one from the following choices:
         1) SerializableReadWrite() which is default mode;
         2) OnlineReadOnly();
         3) StaleReadOnly().
        """
        self._driver = driver
        tx_mode = SerializableReadWrite() if tx_mode is None else tx_mode
        self._tx_state = _tx_ctx_impl.TxState(tx_mode)
        self._session_state = session_state
        self.session = session
        self._finished = ""
        self._allow_split_transactions = allow_split_transactions

    def __enter__(self):
        """
        Enters a context manager and returns a session

        :return: A session instance
        """
        return self

    def __exit__(self, *args, **kwargs):
        """
        Closes a transaction context manager and rollbacks transaction if
        it is not rolled back explicitly
        """
        if self._tx_state.tx_id is not None:
            # It's strictly recommended to close transactions directly
            # by using commit_tx=True flag while executing statement or by
            # .commit() or .rollback() methods, but here we trying to do best
            # effort to avoid useless open transactions
            logger.warning("Potentially leaked tx: %s", self._tx_state.tx_id)
            try:
                self.rollback()
            except issues.Error:
                logger.warning("Failed to rollback leaked tx: %s", self._tx_state.tx_id)

            self._tx_state.tx_id = None

    @property
    def session_id(self):
        """
        A transaction's session id

        :return: A transaction's session id
        """
        return self._session_state.session_id

    @property
    def tx_id(self):
        """
        Returns a id of open transaction or None otherwise

        :return: A id of open transaction or None otherwise
        """
        return self._tx_state.tx_id

    def execute(self, query, parameters=None, commit_tx=False, settings=None):
        """
        Sends a query (yql text or an instance of DataQuery) to be executed with parameters.
        Execution with parameters supported only for DataQuery instances and is not supported yql text queries.

        :param query: A query, yql text or DataQuery instance.
        :param parameters: A dictionary with parameters values.
        :param commit_tx: A special flag that allows transaction commit
        :param settings: An additional request settings

        :return: A result sets or exception in case of execution errors
        """

        self._check_split()
        if commit_tx:
            self._set_finish(self._COMMIT)

        return self._driver(
            _tx_ctx_impl.execute_request_factory(
                self._session_state,
                self._tx_state,
                query,
                parameters,
                commit_tx,
                settings,
            ),
            _apis.TableService.Stub,
            _apis.TableService.ExecuteDataQuery,
            _tx_ctx_impl.wrap_result_and_tx_id,
            settings,
            (self._session_state, self._tx_state, query),
            self._session_state.endpoint,
        )

    def commit(self, settings=None):
        """
        Calls commit on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A committed transaction or exception if commit is failed
        """

        self._set_finish(self._COMMIT)

        if self._tx_state.tx_id is None and not self._tx_state.dead:
            return self

        return self._driver(
            _tx_ctx_impl.commit_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.CommitTransaction,
            _tx_ctx_impl.wrap_result_on_rollback_or_commit_tx,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )

    def rollback(self, settings=None):
        """
        Calls rollback on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A rolled back transaction or exception if rollback is failed
        """

        self._set_finish(self._ROLLBACK)

        if self._tx_state.tx_id is None and not self._tx_state.dead:
            return self

        return self._driver(
            _tx_ctx_impl.rollback_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.RollbackTransaction,
            _tx_ctx_impl.wrap_result_on_rollback_or_commit_tx,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )

    def begin(self, settings=None):
        """
        Explicitly begins a transaction

        :param settings: A request settings

        :return: An open transaction
        """
        if self._tx_state.tx_id is not None:
            return self

        self._check_split()

        return self._driver(
            _tx_ctx_impl.begin_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.BeginTransaction,
            _tx_ctx_impl.wrap_tx_begin_response,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )

    def _set_finish(self, val):
        self._check_split(val)
        self._finished = val

    def _check_split(self, allow=""):
        """
        Deny all operaions with transaction after commit/rollback.
        Exception: double commit and double rollbacks, because it is safe
        """
        if self._allow_split_transactions:
            return

        if self._finished != "" and self._finished != allow:
            raise RuntimeError("Any operation with finished transaction is denied")


class TxContext(BaseTxContext):
    @_utilities.wrap_async_call_exceptions
    def async_execute(self, query, parameters=None, commit_tx=False, settings=None):
        """
        Sends a query (yql text or an instance of DataQuery) to be executed with parameters.
        Execution with parameters supported only for DataQuery instances and not supported for YQL text.

        :param query: A query: YQL text or DataQuery instance. E
        :param parameters: A dictionary with parameters values.
        :param commit_tx: A special flag that allows transaction commit
        :param settings: A request settings (an instance of ExecDataQuerySettings)

        :return: A future of query execution
        """

        self._check_split()

        return self._driver.future(
            _tx_ctx_impl.execute_request_factory(
                self._session_state,
                self._tx_state,
                query,
                parameters,
                commit_tx,
                settings,
            ),
            _apis.TableService.Stub,
            _apis.TableService.ExecuteDataQuery,
            _tx_ctx_impl.wrap_result_and_tx_id,
            settings,
            (
                self._session_state,
                self._tx_state,
                query,
            ),
            self._session_state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_commit(self, settings=None):
        """
        Calls commit on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings (an instance of BaseRequestSettings)

        :return: A future of commit call
        """
        self._set_finish(self._COMMIT)

        if self._tx_state.tx_id is None and not self._tx_state.dead:
            return _utilities.wrap_result_in_future(self)

        return self._driver.future(
            _tx_ctx_impl.commit_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.CommitTransaction,
            _tx_ctx_impl.wrap_result_on_rollback_or_commit_tx,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_rollback(self, settings=None):
        """
        Calls rollback on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A future of rollback call
        """
        self._set_finish(self._ROLLBACK)

        if self._tx_state.tx_id is None and not self._tx_state.dead:
            return _utilities.wrap_result_in_future(self)

        return self._driver.future(
            _tx_ctx_impl.rollback_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.RollbackTransaction,
            _tx_ctx_impl.wrap_result_on_rollback_or_commit_tx,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )

    @_utilities.wrap_async_call_exceptions
    def async_begin(self, settings=None):
        """
        Explicitly begins a transaction

        :param settings: A request settings

        :return: A future of begin call
        """
        if self._tx_state.tx_id is not None:
            return _utilities.wrap_result_in_future(self)

        self._check_split()

        return self._driver.future(
            _tx_ctx_impl.begin_request_factory(self._session_state, self._tx_state),
            _apis.TableService.Stub,
            _apis.TableService.BeginTransaction,
            _tx_ctx_impl.wrap_tx_begin_response,
            settings,
            (self._session_state, self._tx_state, self),
            self._session_state.endpoint,
        )


class SessionPool(object):
    def __init__(
        self,
        driver,
        size=100,
        workers_threads_count=4,
        initializer=None,
        min_pool_size=0,
    ):
        """
        An object that encapsulates session creation, deletion and etc. and maintains
        a pool of active sessions of specified size

        :param driver: A Driver instance
        :param size: A maximum number of sessions to maintain in the pool
        """
        self._logger = logger.getChild(self.__class__.__name__)
        self._pool_impl = _sp_impl.SessionPoolImpl(
            self._logger,
            driver,
            size,
            workers_threads_count,
            initializer,
            min_pool_size,
        )
        if hasattr(driver, "_driver_config"):
            self.tracer = driver._driver_config.tracer
        else:
            self.tracer = ydb.Tracer(None)

    def retry_operation_sync(self, callee, retry_settings=None, *args, **kwargs):

        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        def wrapped_callee():
            with self.checkout(
                timeout=retry_settings.get_session_client_timeout
            ) as session:
                return callee(session, *args, **kwargs)

        return retry_operation_sync(wrapped_callee, retry_settings)

    @property
    def active_size(self):
        return self._pool_impl.active_size

    @property
    def free_size(self):
        return self._pool_impl.free_size

    @property
    def busy_size(self):
        return self._pool_impl.busy_size

    @property
    def max_size(self):
        return self._pool_impl.max_size

    @property
    def waiters_count(self):
        return self._pool_impl.waiters_count

    @tracing.with_trace()
    def subscribe(self):
        return self._pool_impl.subscribe()

    @tracing.with_trace()
    def unsubscribe(self, waiter):
        return self._pool_impl.unsubscribe(waiter)

    @tracing.with_trace()
    def acquire(self, blocking=True, timeout=None):
        return self._pool_impl.acquire(blocking, timeout)

    @tracing.with_trace()
    def release(self, session):
        return self._pool_impl.put(session)

    def async_checkout(self):
        """
        Returns a context manager that asynchronously checkouts a session from the pool.

        """
        return AsyncSessionCheckout(self)

    def checkout(self, blocking=True, timeout=None):
        return SessionCheckout(self, blocking, timeout)

    def stop(self, timeout=None):
        self._pool_impl.stop(timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class AsyncSessionCheckout(object):
    __slots__ = ("subscription", "pool")

    def __init__(self, pool):
        """
        A context manager that asynchronously checkouts a session for the specified pool
        and returns it on manager exit.

        :param pool: A SessionPool instance.
        """
        self.pool = pool
        self.subscription = None

    def __enter__(self):
        self.subscription = self.pool.subscribe()
        return self.subscription

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.unsubscribe(self.subscription)


class SessionCheckout(object):
    __slots__ = ("_acquired", "_pool", "_blocking", "_timeout")

    def __init__(self, pool, blocking, timeout):
        """
        A context manager that checkouts a session from the specified pool and
        returns it on manager exit.

        :param pool: A SessionPool instance
        :param blocking: A flag that specifies that session acquire method should blocks
        :param timeout: A timeout in seconds for session acquire
        """
        self._pool = pool
        self._acquired = None
        self._blocking = blocking
        self._timeout = timeout

    def __enter__(self):
        self._acquired = self._pool.acquire(self._blocking, self._timeout)
        return self._acquired

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._acquired is not None:
            self._pool.release(self._acquired)
