#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.oss.ydb_sdk_import import ydb

from enum import Enum


class QueueType(Enum):
    STD = 1
    FIFO = 2


def get_prefix_path(root, queue_type=None, common_table=True):
    subdir = ('.{}/'.format(queue_type.name)) if queue_type and common_table else ''
    return '{}/{}'.format(root, subdir)


def get_table_path(root, table_name, queue_type=None, common_table=True):
    return '{}/{}'.format(get_prefix_path(root, queue_type, common_table), table_name)


def _create_table(root, session, table_name, columns, keys_count, common_table=True, queue_type=None):
    table_path = get_table_path(root, table_name, queue_type, common_table)
    keys = [name for name, _ in columns[:keys_count]]
    columns = [ydb.Column(name, ydb.OptionalType(column_type)) for name, column_type in columns]

    description = ydb.TableDescription()
    description.with_primary_keys(*keys)
    description.with_columns(*columns)
    if queue_type and common_table:
        partitions = ydb.PartitioningSettings()
        partitions.with_min_partitions_count(10)
        partitions.with_partitioning_by_size(ydb.FeatureFlag.ENABLED)
        partitions.with_partitioning_by_load(ydb.FeatureFlag.ENABLED)

        description.with_uniform_partitions(10)
        description.with_partitioning_settings(partitions)

        ydb.retry_operation_sync(lambda: session.create_table(
            table_path,
            description
        ))
    else:
        ydb.retry_operation_sync(lambda: session.create_table(
            table_path,
            description
        ))


def get_table_keys_for_queue(common_table, with_shard=False):
    if not common_table:
        return []
    if with_shard:
        columns = [
            ('QueueIdNumberAndShardHash', ydb.PrimitiveType.Uint64),
            ('QueueIdNumber', ydb.PrimitiveType.Uint64),
            ('Shard', ydb.PrimitiveType.Uint32)
        ]
    else:
        columns = [
            ('QueueIdNumberHash', ydb.PrimitiveType.Uint64),
            ('QueueIdNumber', ydb.PrimitiveType.Uint64)
        ]
    return columns


def create_atomic_counter(root, session):
    columns = [
        ('counter_key', ydb.PrimitiveType.Uint64),
        ('value', ydb.PrimitiveType.Uint64)
    ]
    table_name = '.AtomicCounter'
    _create_table(root, session, table_name, columns, keys_count=1)

    session.transaction(ydb.SerializableReadWrite()).execute(
        "--!syntax_v1\n"
        "INSERT INTO `{}` (counter_key, value) VALUES (0, 0);".format(get_table_path(root, table_name)),
        commit_tx=True
    )


def create_queues_table(root, session):
    columns = [
        ('Account', ydb.PrimitiveType.Utf8),
        ('QueueName', ydb.PrimitiveType.Utf8),
        ('QueueId', ydb.PrimitiveType.String),
        ('QueueState', ydb.PrimitiveType.Uint64),
        ('FifoQueue', ydb.PrimitiveType.Bool),
        ('DeadLetterQueue', ydb.PrimitiveType.Bool),
        ('CreatedTimestamp', ydb.PrimitiveType.Uint64),
        ('Shards', ydb.PrimitiveType.Uint64),
        ('Partitions', ydb.PrimitiveType.Uint64),
        ('MasterTabletId', ydb.PrimitiveType.Uint64),
        ('CustomQueueName', ydb.PrimitiveType.Utf8),
        ('FolderId', ydb.PrimitiveType.Utf8),
        ('Version', ydb.PrimitiveType.Uint64),
        ('DlqName', ydb.PrimitiveType.Utf8),
        ('TablesFormat', ydb.PrimitiveType.Uint32),
        ('Tags', ydb.PrimitiveType.Utf8),
    ]
    _create_table(root, session, '.Queues', columns, keys_count=2)


def create_events_table(root, session):
    columns = [
        ('Account', ydb.PrimitiveType.Utf8),
        ('QueueName', ydb.PrimitiveType.Utf8),
        ('EventType', ydb.PrimitiveType.Uint64),
        ('CustomQueueName', ydb.PrimitiveType.Utf8),
        ('EventTimestamp', ydb.PrimitiveType.Uint64),
        ('FolderId', ydb.PrimitiveType.Utf8),
        ('Labels', ydb.PrimitiveType.Utf8),
    ]
    _create_table(root, session, '.Events', columns, keys_count=3)


def create_settings_table(root, session):
    columns = [
        ('Account', ydb.PrimitiveType.Utf8),
        ('Name', ydb.PrimitiveType.Utf8),
        ('Value', ydb.PrimitiveType.Utf8),
    ]
    _create_table(root, session, '.Settings', columns, keys_count=2)


def create_removed_queues_table(root, session):
    columns = [
        ('RemoveTimestamp', ydb.PrimitiveType.Uint64),
        ('QueueIdNumber', ydb.PrimitiveType.Uint64),
        ('Account', ydb.PrimitiveType.Utf8),
        ('QueueName', ydb.PrimitiveType.Utf8),
        ('FifoQueue', ydb.PrimitiveType.Bool),
        ('Shards', ydb.PrimitiveType.Uint32),
        ('CustomQueueName', ydb.PrimitiveType.Utf8),
        ('FolderId', ydb.PrimitiveType.Utf8),
        ('TablesFormat', ydb.PrimitiveType.Uint32),
        ('StartProcessTimestamp', ydb.PrimitiveType.Uint64),
        ('NodeProcess', ydb.PrimitiveType.Uint32),
    ]
    _create_table(root, session, '.RemovedQueues', columns, keys_count=2)


def create_attibutes_table(root, session, queue_type, common_table=True):
    queue_keys = []
    if common_table:
        queue_keys = get_table_keys_for_queue(common_table)
    else:
        queue_keys.append(('State', ydb.PrimitiveType.Uint64))

    columns = queue_keys + [
        ('ContentBasedDeduplication', ydb.PrimitiveType.Bool),
        ('DelaySeconds', ydb.PrimitiveType.Uint64),
        ('FifoQueue', ydb.PrimitiveType.Bool),
        ('MaximumMessageSize', ydb.PrimitiveType.Uint64),
        ('MessageRetentionPeriod', ydb.PrimitiveType.Uint64),
        ('ReceiveMessageWaitTime', ydb.PrimitiveType.Uint64),
        ('VisibilityTimeout', ydb.PrimitiveType.Uint64),
        ('DlqName', ydb.PrimitiveType.Utf8),
        ('DlqArn', ydb.PrimitiveType.Utf8),
        ('MaxReceiveCount', ydb.PrimitiveType.Uint64),
        ('ShowDetailedCountersDeadline', ydb.PrimitiveType.Uint64),
    ]
    _create_table(root, session, 'Attributes', columns, len(queue_keys), common_table, queue_type)


def create_state_table(root, session, queue_type, common_table=True):
    queue_keys = []
    if common_table:
        queue_keys = [
            ('QueueIdNumberHash', ydb.PrimitiveType.Uint64),
            ('QueueIdNumber', ydb.PrimitiveType.Uint64),
        ]
        if queue_type == QueueType.STD:
            queue_keys.append(('Shard', ydb.PrimitiveType.Uint32))
    else:
        queue_keys.append(('State', ydb.PrimitiveType.Uint64))

    columns = queue_keys + [
        ('CleanupTimestamp', ydb.PrimitiveType.Uint64),
        ('CreatedTimestamp', ydb.PrimitiveType.Uint64),
        ('LastModifiedTimestamp', ydb.PrimitiveType.Uint64),
        ('RetentionBoundary', ydb.PrimitiveType.Uint64),
        ('InflyCount', ydb.PrimitiveType.Int64),
        ('MessageCount', ydb.PrimitiveType.Int64),
        ('ReadOffset', ydb.PrimitiveType.Uint64),
        ('WriteOffset', ydb.PrimitiveType.Uint64),
        ('CleanupVersion', ydb.PrimitiveType.Uint64),
        ('InflyVersion', ydb.PrimitiveType.Uint64),
    ]
    _create_table(root, session, 'State', columns, len(queue_keys), common_table, queue_type)


def create_infly_table(root, session, common_table=True):
    queue_type = QueueType.STD
    queue_keys = get_table_keys_for_queue(common_table, with_shard=(queue_type == QueueType.STD))
    columns = queue_keys + [
        ('Offset', ydb.PrimitiveType.Uint64),
        ('RandomId', ydb.PrimitiveType.Uint64),
        ('LoadId', ydb.PrimitiveType.Uint64),
        ('FirstReceiveTimestamp', ydb.PrimitiveType.Uint64),
        ('LockTimestamp', ydb.PrimitiveType.Uint64),
        ('ReceiveCount', ydb.PrimitiveType.Uint32),
        ('SentTimestamp', ydb.PrimitiveType.Uint64),
        ('VisibilityDeadline', ydb.PrimitiveType.Uint64),
        ('DelayDeadline', ydb.PrimitiveType.Uint64),
    ]
    _create_table(root, session, 'Infly', columns, len(queue_keys) + 1, common_table, queue_type)


def create_message_data_table(root, session, common_table=True):
    queue_type = QueueType.STD
    queue_keys = get_table_keys_for_queue(common_table, with_shard=(queue_type == QueueType.STD))
    columns = queue_keys + [
        ('RandomId', ydb.PrimitiveType.Uint64),
        ('Offset', ydb.PrimitiveType.Uint64),
        ('Attributes', ydb.PrimitiveType.String),
        ('Data', ydb.PrimitiveType.String),
        ('MessageId', ydb.PrimitiveType.String),
        ('SenderId', ydb.PrimitiveType.String),
    ]
    _create_table(root, session, 'MessageData', columns, len(queue_keys) + 2, common_table, queue_type)


def create_message_table(root, session, queue_type, common_table=True):
    queue_keys = get_table_keys_for_queue(common_table, with_shard=(queue_type == QueueType.STD))
    columns = queue_keys + [
        ('Offset', ydb.PrimitiveType.Uint64),
        ('RandomId', ydb.PrimitiveType.Uint64),
    ]
    if queue_type == QueueType.STD:
        columns += [
            ('SentTimestamp', ydb.PrimitiveType.Uint64),
            ('DelayDeadline', ydb.PrimitiveType.Uint64),
        ]
    else:
        columns += [
            ('GroupId', ydb.PrimitiveType.String),
            ('NextOffset', ydb.PrimitiveType.Uint64),
            ('NextRandomId', ydb.PrimitiveType.Uint64),
            ('ReceiveCount', ydb.PrimitiveType.Uint32),
            ('FirstReceiveTimestamp', ydb.PrimitiveType.Uint64),
            ('SentTimestamp', ydb.PrimitiveType.Uint64),
        ]
    _create_table(root, session, 'Messages', columns, len(queue_keys) + 1, common_table, queue_type)


def create_sent_timestamp_idx_table(root, session, queue_type, common_table=True):
    queue_keys = get_table_keys_for_queue(common_table, with_shard=(queue_type == QueueType.STD))
    columns = queue_keys + [
        ('SentTimestamp', ydb.PrimitiveType.Uint64),
        ('Offset', ydb.PrimitiveType.Uint64),
        ('RandomId', ydb.PrimitiveType.Uint64),
        ('DelayDeadline', ydb.PrimitiveType.Uint64)
    ]
    if queue_type == QueueType.FIFO:
        columns.append(('GroupId', ydb.PrimitiveType.String))
    _create_table(root, session, 'SentTimestampIdx', columns, len(queue_keys) + 2, common_table, queue_type)


def create_data_table(root, session, common_table=True):
    queue_type = QueueType.FIFO
    queue_keys = get_table_keys_for_queue(common_table)
    columns = queue_keys + [
        ("RandomId", ydb.PrimitiveType.Uint64),
        ("Offset", ydb.PrimitiveType.Uint64),
        ("SenderId", ydb.PrimitiveType.String),
        ("DedupId", ydb.PrimitiveType.String),
        ("Attributes", ydb.PrimitiveType.String),
        ("Data", ydb.PrimitiveType.String),
        ("MessageId", ydb.PrimitiveType.String),
    ]
    _create_table(root, session, 'Data', columns, len(queue_keys) + 2, common_table, queue_type)


def create_deduplication_table(root, session, common_table=True):
    queue_type = QueueType.FIFO
    queue_keys = get_table_keys_for_queue(common_table)
    columns = queue_keys + [
        ("DedupId", ydb.PrimitiveType.String),
        ("Deadline", ydb.PrimitiveType.Uint64),
        ("Offset", ydb.PrimitiveType.Uint64),
        ("MessageId", ydb.PrimitiveType.String),
    ]
    _create_table(root, session, 'Deduplication', columns, len(queue_keys) + 1, common_table, queue_type)


def create_groups_table(root, session, common_table=True):
    queue_type = QueueType.FIFO
    queue_keys = get_table_keys_for_queue(common_table)
    columns = queue_keys + [
        ("GroupId", ydb.PrimitiveType.String),
        ("VisibilityDeadline", ydb.PrimitiveType.Uint64),
        ("RandomId", ydb.PrimitiveType.Uint64),
        ("Head", ydb.PrimitiveType.Uint64),
        ("Tail", ydb.PrimitiveType.Uint64),
        ("ReceiveAttemptId", ydb.PrimitiveType.Utf8),
        ("LockTimestamp", ydb.PrimitiveType.Uint64),
    ]
    _create_table(root, session, 'Groups', columns, len(queue_keys) + 1, common_table, queue_type)


def create_reads_table(root, session, common_table=True):
    queue_type = QueueType.FIFO
    queue_keys = get_table_keys_for_queue(common_table)
    columns = queue_keys + [
        ("ReceiveAttemptId", ydb.PrimitiveType.Utf8),
        ("Deadline", ydb.PrimitiveType.Uint64),
    ]
    _create_table(root, session, 'Reads', columns, len(queue_keys) + 1, common_table, queue_type)


def create_all_tables(root, driver, session):
    create_atomic_counter(root, session)
    create_queues_table(root, session)
    create_events_table(root, session)
    create_settings_table(root, session)
    create_removed_queues_table(root, session)

    for queue_type in [QueueType.STD, QueueType.FIFO]:
        create_sent_timestamp_idx_table(root, session, queue_type)
        create_message_table(root, session, queue_type)
        create_state_table(root, session, queue_type)
        create_attibutes_table(root, session, queue_type)

    # only STD
    create_infly_table(root, session)
    create_message_data_table(root, session)

    # only FIFO
    create_reads_table(root, session)
    create_groups_table(root, session)
    create_deduplication_table(root, session)
    create_data_table(root, session)


def create_queue_tables(path, if_fifo, driver, session, shards=None):
    queue_type = QueueType.FIFO if if_fifo else QueueType.STD
    create_state_table(path, session, queue_type, common_table=False)
    create_attibutes_table(path, session, queue_type, common_table=False)

    if queue_type == QueueType.STD:
        for shard in range(shards):
            shard_path = f'{path}/{shard}'
            create_infly_table(shard_path, session, common_table=False)
            create_message_data_table(shard_path, session, common_table=False)
            create_sent_timestamp_idx_table(shard_path, session, queue_type, common_table=False)
            create_message_table(shard_path, session, queue_type, common_table=False)
    else:
        create_reads_table(path, session, common_table=False)
        create_groups_table(path, session, common_table=False)
        create_deduplication_table(path, session, common_table=False)
        create_data_table(path, session, common_table=False)
        create_sent_timestamp_idx_table(path, session, queue_type, common_table=False)
        create_message_table(path, session, queue_type, common_table=False)
