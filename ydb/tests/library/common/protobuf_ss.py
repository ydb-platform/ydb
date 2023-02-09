#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import string
from datetime import timedelta
from os.path import basename, dirname, join

from ydb.core.protos import msgbus_pb2
from ydb.core.protos import flat_scheme_op_pb2
from ydb.tests.library.common.protobuf import AbstractProtobufBuilder, build_protobuf_if_necessary


DEFAULT_SIZE_TO_SPLIT = 10 ** 6


class TPartitionConfig(AbstractProtobufBuilder):
    """
    See /arcadia/ydb/core/protos/flat_scheme_op_pb2.proto
    """

    def __init__(self):
        super(TPartitionConfig, self).__init__(flat_scheme_op_pb2.TPartitionConfig())
        self.with_partitioning_policy(DEFAULT_SIZE_TO_SPLIT)

    def __ensure_has_compaction_policy(self):
        if not self.protobuf.HasField('CompactionPolicy'):
            self.protobuf.CompactionPolicy.CopyFrom(flat_scheme_op_pb2.TCompactionPolicy())
            # default values
            self.protobuf.CompactionPolicy.ReadAheadHiThreshold = 67108864
            self.protobuf.CompactionPolicy.ReadAheadLoThreshold = 16777216

    def add_compaction_policy_generation(
            self, generation_id,
            size_to_compact, count_to_compact,
            force_size_to_compact, force_count_to_compact,
            compaction_broker_queue, keep_in_cache=False
    ):
        self.__ensure_has_compaction_policy()
        self.protobuf.CompactionPolicy.Generation.add(
            GenerationId=generation_id,
            SizeToCompact=size_to_compact,
            CountToCompact=count_to_compact,
            ForceCountToCompact=force_count_to_compact,
            ForceSizeToCompact=force_size_to_compact,
            CompactionBrokerQueue=compaction_broker_queue,
            KeepInCache=keep_in_cache,
        )
        return self

    def with_tx_read_size_limit(self, tx_read_size_limit):
        self.protobuf.TxReadSizeLimit = tx_read_size_limit
        return self

    def with_executor_cache_size(self, executor_cache_size):
        self.protobuf.ExecutorCacheSize = executor_cache_size
        return self

    def with_in_mem_size_to_snapshot(self, in_mem_size_to_snapshot):
        self.__ensure_has_compaction_policy()
        self.protobuf.CompactionPolicy.InMemSizeToSnapshot = in_mem_size_to_snapshot
        return self

    def with_in_mem_steps_to_snapshot(self, in_mem_steps_to_snapshot):
        self.__ensure_has_compaction_policy()
        self.protobuf.CompactionPolicy.InMemStepsToSnapshot = in_mem_steps_to_snapshot
        return self

    def with_in_mem_force_size_to_snapshot(self, in_mem_force_size_to_snapshot):
        self.__ensure_has_compaction_policy()
        self.protobuf.CompactionPolicy.InMemForceSizeToSnapshot = in_mem_force_size_to_snapshot
        return self

    def with_in_mem_force_steps_to_snapshot(self, in_mem_force_steps_to_snapshot):
        self.__ensure_has_compaction_policy()
        self.protobuf.CompactionPolicy.InMemForceStepsToSnapshot = in_mem_force_steps_to_snapshot
        return self

    def with_followers(self, follower_count, allow_follower_promotion=True):
        self.protobuf.FollowerCount = follower_count
        self.protobuf.AllowFollowerPromotion = allow_follower_promotion
        return self

    def with_partitioning_policy(self, size_to_split_bytes):
        if size_to_split_bytes is not None:
            self.protobuf.PartitioningPolicy.SizeToSplit = size_to_split_bytes
        return self


class CreatePath(AbstractProtobufBuilder):
    def __init__(self, work_dir, name=None):
        super(CreatePath, self).__init__(msgbus_pb2.TSchemeOperation())
        if name is None:
            name = basename(work_dir)
            work_dir = dirname(work_dir)
        self.protobuf.Transaction.ModifyScheme.WorkingDir = work_dir
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpMkDir
        self.protobuf.Transaction.ModifyScheme.MkDir.Name = name


class RegisterTenant(AbstractProtobufBuilder):
    class Options(object):
        def __init__(self):
            self.__plan_resolution = 10
            self.__time_cast_buckets = 2
            self.__coordinators = 1
            self.__mediators = 1

        @property
        def plan_resolution(self):
            return self.__plan_resolution

        def with_plan_resolution(self, value):
            assert value > 0
            self.__plan_resolution = value
            return self

        @property
        def time_cast_buckets(self):
            return self.__time_cast_buckets

        def with_time_cast_buckets(self, value):
            assert value > 0
            self.__time_cast_buckets = value
            return self

        @property
        def coordinators(self):
            return self.__coordinators

        def with_coordinators(self, value):
            assert value > 0
            self.__coordinators = value
            return self

        @property
        def mediators(self):
            return self.__mediators

        def with_mediators(self, value):
            assert value > 0
            self.__mediators = value
            return self

    def __init__(self, work_dir, name=None, options=None):
        super(RegisterTenant, self).__init__(msgbus_pb2.TSchemeOperation())

        if name is None:
            name = basename(work_dir)
            work_dir = dirname(work_dir)

        self.__modify_scheme.OperationType = flat_scheme_op_pb2.ESchemeOpCreateSubDomain
        self.__modify_scheme.WorkingDir = work_dir

        self.__tenant_settings.Name = name

        if options:
            self.with_options(options)

    def with_options(self, options):
        assert isinstance(options, self.Options)
        self.__tenant_settings.PlanResolution = options.plan_resolution
        self.__tenant_settings.TimeCastBucketsPerMediator = options.time_cast_buckets
        self.__tenant_settings.Coordinators = options.coordinators
        self.__tenant_settings.Mediators = options.mediators
        return self

    @property
    def __modify_scheme(self):
        return self.protobuf.Transaction.ModifyScheme

    @property
    def __tenant_settings(self):
        return self.protobuf.Transaction.ModifyScheme.SubDomain


def list_of_create_path_builders_from_full_path(full_path):
    list_of_dirs = [path for path in string.split(full_path, '/') if path]
    ret = []
    prev = ''
    for dir_name in list_of_dirs:
        current = join(prev, dir_name)
        if prev:
            ret.append(CreatePath(prev, dir_name))
        else:
            current = '/' + current
        prev = current
    return ret


class AbstractTSchemeOperationRequest(AbstractProtobufBuilder):
    """
    See
    /arcadia/ydb/core/protos/msgbus_pb2.proto
    /arcadia/ydb/core/protos/flat_scheme_op_pb2.proto
    """
    def __init__(self):
        super(AbstractTSchemeOperationRequest, self).__init__(msgbus_pb2.TSchemeOperation())


class _DropPolicyOptions(object):
    def __init__(self):
        super(_DropPolicyOptions, self).__init__()
        self.__drop_policy = flat_scheme_op_pb2.EDropFailOnChanges

    @property
    def drop_policy(self):
        return self.__drop_policy

    def with_drop_policy(self, value):
        assert value in (flat_scheme_op_pb2.EDropAbortChanges,
                         flat_scheme_op_pb2.EDropFailOnChanges,
                         flat_scheme_op_pb2.EDropWaitChanges)
        self.__drop_policy = value
        return self

    def with_abort_on_changes(self):
        self.__drop_policy = flat_scheme_op_pb2.EDropAbortChanges
        return self

    def with_fail_on_changes(self):
        self.__drop_policy = flat_scheme_op_pb2.EDropFailOnChanges
        return self

    def with_wait_changes(self):
        self.__drop_policy = flat_scheme_op_pb2.EDropWaitChanges
        return self


class DropTenantRequest(AbstractTSchemeOperationRequest):
    class Options(_DropPolicyOptions):
        pass

    def __init__(self, path, name=None, drop_policy=flat_scheme_op_pb2.EDropFailOnChanges, options=None):
        super(DropTenantRequest, self).__init__()
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpDropSubDomain

        if name is None:
            name = basename(path)
            path = dirname(path)

        self.protobuf.Transaction.ModifyScheme.WorkingDir = path
        self.protobuf.Transaction.ModifyScheme.Drop.Name = name

        fixed_options = options or self.Options()
        fixed_options.with_drop_policy(drop_policy)
        self.with_options(fixed_options)

    def with_options(self, options):
        assert isinstance(options, self.Options)

        if options.drop_policy is not None:
            self.protobuf.Transaction.ModifyScheme.Drop.WaitPolicy = options.drop_policy

        return self


class ForceDropTenantRequest(DropTenantRequest):
    def __init__(self, path, name=None, drop_policy=flat_scheme_op_pb2.EDropFailOnChanges, options=None):
        super(ForceDropTenantRequest, self).__init__(path, name, drop_policy, options)
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpForceDropSubDomain


class DropPathRequest(AbstractTSchemeOperationRequest):
    class Options(_DropPolicyOptions):
        pass

    def __init__(self, path, name=None, drop_policy=flat_scheme_op_pb2.EDropFailOnChanges, options=None):
        super(DropPathRequest, self).__init__()
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpRmDir

        if name is None:
            name = basename(path)
            path = dirname(path)

        self.protobuf.Transaction.ModifyScheme.WorkingDir = path
        self.protobuf.Transaction.ModifyScheme.Drop.Name = name

        fixed_options = options or self.Options()
        fixed_options.with_drop_policy(drop_policy)
        self.with_options(fixed_options)

    def with_options(self, options):
        assert isinstance(options, self.Options)

        if options.drop_policy is not None:
            self.protobuf.Transaction.ModifyScheme.Drop.WaitPolicy = options.drop_policy

        return self


class DropTopicRequest(AbstractTSchemeOperationRequest):
    class Options(_DropPolicyOptions):
        pass

    def __init__(self, path, topic_name=None, options=None):
        super(DropTopicRequest, self).__init__()

        if topic_name is None:
            topic_name = basename(path)
            path = dirname(path)

        self.__modify_scheme_transaction.OperationType = flat_scheme_op_pb2.ESchemeOpDropPersQueueGroup
        self.__modify_scheme_transaction.WorkingDir = path
        self.__drop.Name = topic_name
        self.with_options(options or self.Options())

    @property
    def __modify_scheme_transaction(self):
        return self.protobuf.Transaction.ModifyScheme

    @property
    def __drop(self):
        return self.__modify_scheme_transaction.Drop

    def with_options(self, options):
        assert isinstance(options, self.Options)

        if options.drop_policy is not None:
            self.__drop.WaitPolicy = options.drop_policy

        return self


class CreateTopicRequest(AbstractTSchemeOperationRequest):
    class Options(object):
        def __init__(self):
            self.__partition_count = 10
            self.__partition_per_table = None

            # PartitionConfig
            self.__important_client_ids = None
            self.__max_count_in_partition = None
            self.__max_size_in_partition = None
            self.__lifetime_seconds = int(timedelta(days=1).total_seconds())

        @property
        def partitions_count(self):
            return self.__partition_count

        def with_partitions_count(self, value):
            assert value > 0
            self.__partition_count = value
            return self

        @property
        def partition_per_table(self):
            return self.__partition_per_table

        def with_partition_per_table(self, value):
            assert value > 0
            self.__partition_per_table = value
            return self

        @property
        def important_client_ids(self):
            return self.__important_client_ids

        def with_important_client_ids(self, value):
            assert value > 0
            self.__important_client_ids = value
            return self

        @property
        def max_count_in_partition(self):
            return self.__max_count_in_partition

        def with_max_count_in_partition(self, value):
            assert value > 0
            self.__max_count_in_partition = value
            return self

        @property
        def max_size_in_partition(self):
            return self.__max_size_in_partition

        def with_max_size_in_partition(self, value):
            assert value > 0
            self.__max_size_in_partition = value
            return self

        @property
        def lifetime_seconds(self):
            return self.__lifetime_seconds

        def with_lifetime_seconds(self, value):
            assert value > 0
            self.__lifetime_seconds = value
            return self

    def __init__(self, path, topic_name=None, options=None):
        super(CreateTopicRequest, self).__init__()

        if topic_name is None:
            topic_name = basename(path)
            path = dirname(path)

        self._modify_scheme_transaction.OperationType = self._operation
        self._modify_scheme_transaction.WorkingDir = path
        self._pers_queue.Name = topic_name
        self.with_options(options or self.Options())

    @property
    def _operation(self):
        return flat_scheme_op_pb2.ESchemeOpCreatePersQueueGroup

    @property
    def _modify_scheme_transaction(self):
        return self.protobuf.Transaction.ModifyScheme

    @property
    def _pers_queue(self):
        return self._modify_scheme_transaction.CreatePersQueueGroup

    @property
    def _partition_config(self):
        return self._pers_queue.PQTabletConfig.PartitionConfig

    def with_options(self, options):
        assert isinstance(options, self.Options)

        if options.partitions_count is not None:
            self._pers_queue.TotalGroupCount = options.partitions_count

        if options.partition_per_table is not None:
            self._pers_queue.PartitionPerTablet = options.partition_per_table

        if options.max_count_in_partition is not None:
            self._partition_config.MaxCountInPartition = options.max_count_in_partition
            self._partition_config.MaxSizeInPartition = options.max_count_in_partition * 10 ** 6

        if options.max_size_in_partition is not None:
            self._partition_config.MaxSizeInPartition = options.max_size_in_partition

        if options.lifetime_seconds is not None:
            self._partition_config.LifetimeSeconds = options.lifetime_seconds

        return self


class AlterTopicRequest(CreateTopicRequest):
    def __init__(self, path, topic_name=None, options=None):
        super(AlterTopicRequest, self).__init__(path, topic_name, options)

    @property
    def _operation(self):
        return flat_scheme_op_pb2.ESchemeOpAlterPersQueueGroup

    @property
    def _pers_queue(self):
        return self._modify_scheme_transaction.AlterPersQueueGroup


class DropPath(AbstractProtobufBuilder):
    def __init__(self, work_dir, name=None, drop_policy=flat_scheme_op_pb2.EDropFailOnChanges):
        super(DropPath, self).__init__(msgbus_pb2.TSchemeOperation())
        if name is None:
            name = basename(work_dir)
            work_dir = dirname(work_dir)
        self.protobuf.Transaction.ModifyScheme.WorkingDir = work_dir
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpRmDir
        self.protobuf.Transaction.ModifyScheme.Drop.Name = name
        self.protobuf.Transaction.ModifyScheme.Drop.WaitPolicy = drop_policy


class CreateTableRequest(AbstractTSchemeOperationRequest):
    class Options(object):
        ColumnStorage1 = flat_scheme_op_pb2.ColumnStorage1
        ColumnStorage2 = flat_scheme_op_pb2.ColumnStorage2
        ColumnStorage1Ext1 = flat_scheme_op_pb2.ColumnStorage1Ext1
        ColumnStorageTest_1_2_1k = flat_scheme_op_pb2.ColumnStorageTest_1_2_1k
        ColumnCacheNone = flat_scheme_op_pb2.ColumnCacheNone

        class __StorageConfig(object):
            def __init__(self):
                self.__targets = {}
                allow_subsitude = True
                self.__defaults = {
                    'syslog': ("NotExist", allow_subsitude),
                    'log': ("NotExist", allow_subsitude)
                }
                self.__data_threshold = 0
                self.__external_threshold = 0

            def appoint_syslog(self, pool_kind, allow_subsitude=False):
                self.__targets['syslog'] = (pool_kind, allow_subsitude)
                return self

            def appoint_log(self, pool_kind, allow_subsitude=False):
                self.__targets['log'] = (pool_kind, allow_subsitude)
                return self

            def appoint_data(self, pool_kind, allow_subsitude=False, threshold=12*1024):
                self.__targets['data'] = (pool_kind, allow_subsitude)
                self.__data_threshold = threshold
                return self

            def appoint_external(self, pool_kind, allow_subsitude=False, threshold=512*1024):
                self.__targets['external'] = (pool_kind, allow_subsitude)
                self.__external_threshold = threshold
                return self

            def __apply_defaults(self):
                for key, value in self.__defaults.items():
                    if key not in self.__targets:
                        self.__targets[key] = value

            def __fill_result(self, setting, target):
                if target in self.__targets:
                    setting.PreferredPoolKind, setting.AllowOtherKinds = self.__targets[target]

            def __nonzero__(self):
                return bool(self.__targets)

            def get_proto(self):
                storage_config = AbstractProtobufBuilder(flat_scheme_op_pb2.TStorageConfig())

                self.__apply_defaults()
                self.__fill_result(storage_config.protobuf.SysLog, 'syslog')
                self.__fill_result(storage_config.protobuf.Log, 'log')
                self.__fill_result(storage_config.protobuf.Data, 'data')
                self.__fill_result(storage_config.protobuf.External, 'external')
                storage_config.protobuf.DataThreshold = self.__data_threshold
                storage_config.protobuf.ExternalThreshold = self.__external_threshold

                return storage_config.protobuf

        def __init__(self):
            self.__columns = list()
            self.__columns_family = list()

            self.__partitions_count = 1
            self.__partition_config = TPartitionConfig()
            self.__partition_config.with_followers(follower_count=1)
            self.__columns_family = list()
            self.__storage_config = {}

        def add_column(self, name, ptype, is_key=False, column_family=None):
            self.__columns.append((name, ptype, is_key, column_family))
            return self

        @property
        def columns(self):
            return self.__columns

        # deprecated: use declare_column_family and storage_config
        def add_column_family(self, family_id, storage, column_cache):
            self.__columns_family.append((family_id, storage, column_cache, 0, None))
            return self

        @property
        def columns_family(self):
            return self.__columns_family

        def declare_column_family(self, family_id,
                                  cache=0, codec=0):
            assert family_id not in self.__storage_config
            storage_config = self.__StorageConfig()
            self.__storage_config[family_id] = storage_config
            self.__columns_family.append((family_id, 0, cache, codec, storage_config))
            return storage_config

        def get_storage_config(self, family_id):
            if family_id not in self.__storage_config:
                return None
            return self.__storage_config[family_id]

        @property
        def partitions_count(self):
            return self.__partitions_count

        def with_partitions_count(self, value):
            assert value > 0
            self.__partitions_count = value
            return self

        @property
        def partition_config(self):
            return self.__partition_config

        def with_partition_config(self, value):
            assert isinstance(value, TPartitionConfig)
            self.__partition_config = value
            return self

    def __init__(self, path, table_name=None, options=None, use_options=True):
        super(CreateTableRequest, self).__init__()
        self.__column_ids = itertools.count(start=1)

        if table_name is None:
            table_name = basename(path)
            path = dirname(path)

        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpCreateTable

        self.protobuf.Transaction.ModifyScheme.WorkingDir = path
        self.__create_table_protobuf.Name = table_name

        if use_options:
            self.with_options(
                options or self.Options()
            )

    @property
    def __create_table_protobuf(self):
        return self.protobuf.Transaction.ModifyScheme.CreateTable

    def add_column(self, name, ptype, is_key=False, column_family=None):
        kwargs = {'Name': name, 'Type': str(ptype), 'Id': next(self.__column_ids)}
        if column_family is not None:
            kwargs['Family'] = column_family
        self.__create_table_protobuf.Columns.add(**kwargs)
        if is_key:
            self.__create_table_protobuf.KeyColumnNames.append(name)
        return self

    def with_partitions(self, uniform_partitions_count):
        self.__create_table_protobuf.UniformPartitionsCount = uniform_partitions_count
        return self

    def with_column_family(self, id_, storage, column_cache, codec=0,
                           storage_config=None):
        family = self.__create_table_protobuf.PartitionConfig.ColumnFamilies.add(
            Id=id_, Storage=storage, ColumnCache=column_cache, ColumnCodec=codec
        )

        if storage_config:
            family.StorageConfig.CopyFrom(
                storage_config.get_proto()
            )

        return self

    def with_partition_config(self, partition_config):
        self.__create_table_protobuf.PartitionConfig.CopyFrom(build_protobuf_if_necessary(partition_config))
        return self

    def with_options(self, options):
        assert isinstance(options, self.Options)

        self.with_partitions(options.partitions_count)

        self.with_partition_config(options.partition_config)

        for items in options.columns_family:
            self.with_column_family(*items)

        for name, ptype, is_key, column_family in options.columns:
            self.add_column(name, ptype, is_key, column_family)

        return self


class AlterTableRequest(AbstractTSchemeOperationRequest):
    def __init__(self, path, table_name):
        super(AlterTableRequest, self).__init__()
        self.__column_ids = itertools.count(start=1)

        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpAlterTable

        self.protobuf.Transaction.ModifyScheme.WorkingDir = path
        self.__alter_table_protobuf.Name = table_name

        self.with_partition_config(TPartitionConfig().with_followers(follower_count=1))

    @property
    def __alter_table_protobuf(self):
        return self.protobuf.Transaction.ModifyScheme.AlterTable

    def add_column(self, name, ptype, is_key=False):
        self.__alter_table_protobuf.Columns.add(
            Name=name,
            Type=str(ptype),
            Id=next(self.__column_ids)
        )
        if is_key:
            self.__alter_table_protobuf.KeyColumnNames.append(name)
        return self

    def drop_column(self, name):
        self.__alter_table_protobuf.DropColumns.add(
            Name=name,
        )
        return self

    def with_partitions(self, uniform_partitions_count):
        self.__alter_table_protobuf.UniformPartitionsCount = uniform_partitions_count
        return self

    def with_partition_config(self, partition_config):
        self.__alter_table_protobuf.PartitionConfig.CopyFrom(build_protobuf_if_necessary(partition_config))
        return self


class DropTableRequest(AbstractTSchemeOperationRequest):
    class Options(_DropPolicyOptions):
        pass

    def __init__(self, path, table_name=None, drop_policy=flat_scheme_op_pb2.EDropFailOnChanges, options=None):
        super(DropTableRequest, self).__init__()
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpDropTable

        if table_name is None:
            table_name = basename(path)
            path = dirname(path)

        self.protobuf.Transaction.ModifyScheme.WorkingDir = path
        self.protobuf.Transaction.ModifyScheme.Drop.Name = table_name

        fixed_options = options or self.Options()
        fixed_options.with_drop_policy(drop_policy)
        self.with_options(fixed_options)

    def with_options(self, options):
        assert isinstance(options, self.Options)

        if options.drop_policy is not None:
            self.protobuf.Transaction.ModifyScheme.Drop.WaitPolicy = options.drop_policy

        return self


class SchemeOperationStatus(AbstractProtobufBuilder):
    def __init__(self, tx_id, scheme_shard_id, timeout_seconds=120):
        super(SchemeOperationStatus, self).__init__(msgbus_pb2.TSchemeOperationStatus())
        self.protobuf.FlatTxId.TxId = tx_id
        self.protobuf.FlatTxId.SchemeShardTabletId = scheme_shard_id
        self.protobuf.PollOptions.Timeout = timeout_seconds * 1000


# make a synonym for old code
TSchemeOperationStatus = SchemeOperationStatus


class CopyTableRequest(AbstractTSchemeOperationRequest):
    def __init__(self, source_table_full_name, destination_path, destination_name):
        super(CopyTableRequest, self).__init__()
        self.protobuf.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpCreateTable
        self.__create_table_protobuf.CopyFromTable = source_table_full_name
        self.protobuf.Transaction.ModifyScheme.WorkingDir = destination_path
        self.__create_table_protobuf.Name = destination_name

    @property
    def __create_table_protobuf(self):
        return self.protobuf.Transaction.ModifyScheme.CreateTable

    def with_partition_config(self, partition_config):
        self.__create_table_protobuf.PartitionConfig.CopyFrom(build_protobuf_if_necessary(partition_config))
        return self


class SchemeDescribeRequest(AbstractProtobufBuilder):
    class Options(object):
        def __init__(self):
            self.__partition_info = False
            self.__partition_config = False
            self.__backup_info = False

        @property
        def partition_info(self):
            return self.__partition_info

        def with_partition_info(self, value=True):
            assert value > 0
            self.__partition_info = value
            return self

        @property
        def partition_config(self):
            return self.__partition_config

        def with_partition_config(self, value=True):
            assert value > 0
            self.__partition_config = value
            return self

        @property
        def backup_info(self):
            return self.__backup_info

        def with_backup_info(self, value=True):
            assert value > 0
            self.__backup_info = value
            return self

    def __init__(self, full_path, options=None):
        super(SchemeDescribeRequest, self).__init__(msgbus_pb2.TSchemeDescribe())
        self.protobuf.Path = full_path

        options = options or self.Options()
        self.with_options(options)

    @property
    def __options(self):
        return self.protobuf.Options

    def with_options(self, options):
        assert isinstance(options, self.Options)
        self.with_partition_info(options.partition_info)
        self.with_partition_config(options.partition_config)
        self.with_backup_info(options.backup_info)
        return self

    def with_partition_info(self, enable=True):
        self.__options.ReturnPartitioningInfo = enable
        return self

    def with_partition_config(self, enable=True):
        self.__options.ReturnPartitionConfig = enable
        return self

    def with_backup_info(self, enable=True):
        self.__options.BackupInfo = enable
        return self
