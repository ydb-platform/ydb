# -*- coding: utf-8 -*-
"""
Topic cloud events audit tests.
Compare captured events with canondata/*/topic_cloud_events.json.
"""
import os
import time

import pytest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.oss.ydb_sdk_import import ydb

from helpers import (
    CanonicalCaptureCloudEventOutput,
    ydbcli_db_schema_exec,
    ydbcli_db_schema_exec_allow_failure,
)


CLUSTER_CONFIG = dict(
    additional_log_configs={
        'GRPC_PROXY': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.TRACE,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
    },
    enable_audit_log=True,
    enable_topic_cloud_events=True,
    default_users=dict((i, '') for i in ('root', 'other-user')),
)


def create_topic(cluster, database_path, topic_name):
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpCreatePersQueueGroup
        WorkingDir: "%s"
        CreatePersQueueGroup {
            Name: "%s"
            TotalGroupCount: 4
            PartitionPerTablet: 2
            PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
        }
    }''' % (database_path, topic_name)
    ydbcli_db_schema_exec(cluster, proto)


def alter_topic(cluster, database_path, topic_name):
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpAlterPersQueueGroup
        WorkingDir: "%s"
        AlterPersQueueGroup {
            Name: "%s"
            TotalGroupCount: 6
            PartitionPerTablet: 2
            PQTabletConfig { PartitionConfig { LifetimeSeconds: 42 } }
        }
    }''' % (database_path, topic_name)
    ydbcli_db_schema_exec(cluster, proto)


def drop_topic(cluster, database_path, topic_name):
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpDropPersQueueGroup
        WorkingDir: "%s"
        Drop {
            Name: "%s"
        }
    }''' % (database_path, topic_name)
    ydbcli_db_schema_exec(cluster, proto)


def create_topic_invalid(cluster, database_path, topic_name):
    """Create topic with invalid params (TotalGroupCount: 0) to provoke error."""
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpCreatePersQueueGroup
        WorkingDir: "%s"
        CreatePersQueueGroup {
            Name: "%s"
            TotalGroupCount: 0
            PartitionPerTablet: 2
            PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
        }
    }''' % (database_path, topic_name)
    return ydbcli_db_schema_exec_allow_failure(cluster, proto)


def ydb_sdk_query_exec(cluster, database_path, query):
    config = ydb.DriverConfig(
        endpoint="%s:%s" % (cluster.nodes[1].host, cluster.nodes[1].port),
        database=database_path,
    )
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(query)


def create_table(cluster, database_path, table_name):
    ydb_sdk_query_exec(
        cluster,
        database_path,
        f"CREATE TABLE `{table_name}` (id Uint64, PRIMARY KEY (id));",
    )


def create_changefeed(cluster, table_name, changefeed_name, database_path):
    ydb_sdk_query_exec(
        cluster,
        database_path,
        (
            f"ALTER TABLE `{table_name}` ADD CHANGEFEED `{changefeed_name}` "
            "WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');"
        ),
    )


def alter_topic_invalid_params(cluster, database_path, topic_name):
    """Alter topic with invalid TotalGroupCount (< current) to provoke error in alter_pq."""
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpAlterPersQueueGroup
        WorkingDir: "%s"
        AlterPersQueueGroup {
            Name: "%s"
            TotalGroupCount: 2
            PartitionPerTablet: 2
            PQTabletConfig { PartitionConfig { LifetimeSeconds: 42 } }
        }
    }''' % (database_path, topic_name)
    return ydbcli_db_schema_exec_allow_failure(cluster, proto)


def drop_topic_invalid(cluster, database_path, topic_name):
    """Drop non-existent topic to provoke error in drop_pq."""
    proto = r'''ModifyScheme {
        OperationType: ESchemeOpDropPersQueueGroup
        WorkingDir: "%s"
        Drop {
            Name: "%s"
        }
    }''' % (database_path, topic_name)
    return ydbcli_db_schema_exec_allow_failure(cluster, proto)


@pytest.fixture(scope='module')
def _database(ydb_cluster, ydb_root):
    database_path = os.path.join(ydb_root, 'TopicAuditTest')
    with ydb_database_ctx(ydb_cluster, database_path):
        yield database_path


@pytest.fixture(scope='module')
def topic_cloud_events_file_path(ydb_cluster):
    return ydb_cluster.config.topic_cloud_events_file_path


def test_create_topic(ydb_cluster, _database, topic_cloud_events_file_path):
    """Create topic and compare cloud event with canondata."""
    topic_name = 'CanonicalTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    with capture:
        create_topic(ydb_cluster, _database, topic_name)
    time.sleep(2)

    return capture.canonize()


def test_alter_topic(ydb_cluster, _database, topic_cloud_events_file_path):
    """Create, alter topic and compare AlterTopic cloud event with canondata."""
    topic_name = 'CanonicalAlterTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    create_topic(ydb_cluster, _database, topic_name)
    time.sleep(1)

    with capture:
        alter_topic(ydb_cluster, _database, topic_name)
    time.sleep(2)

    return capture.canonize()


def test_drop_topic(ydb_cluster, _database, topic_cloud_events_file_path):
    """Create, drop topic and compare DeleteTopic cloud event with canondata."""
    topic_name = 'CanonicalDropTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    create_topic(ydb_cluster, _database, topic_name)
    time.sleep(1)

    with capture:
        drop_topic(ydb_cluster, _database, topic_name)
    time.sleep(3)

    return capture.canonize()


def test_create_topic_error(ydb_cluster, _database, topic_cloud_events_file_path):
    """Create topic with invalid params, compare ERROR cloud event with canondata."""
    topic_name = 'InvalidCanonicalTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    with capture:
        create_topic_invalid(ydb_cluster, _database, topic_name)
    time.sleep(2)

    return capture.canonize()


def test_alter_topic_error(ydb_cluster, _database, topic_cloud_events_file_path):
    """Alter topic with invalid params, compare ERROR cloud event with canondata."""
    topic_name = 'AlterErrorCanonicalTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    create_topic(ydb_cluster, _database, topic_name)
    time.sleep(1)

    with capture:
        alter_topic_invalid_params(ydb_cluster, _database, topic_name)
    time.sleep(2)

    return capture.canonize()


def test_drop_topic_error(ydb_cluster, _database, topic_cloud_events_file_path):
    """Drop non-existent topic, compare ERROR cloud event with canondata."""
    topic_name = 'MissingDropCanonicalTopic'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    with capture:
        drop_topic_invalid(ydb_cluster, _database, topic_name)
    time.sleep(2)

    return capture.canonize()


def test_create_changefeed_internal_topic_event(ydb_cluster, _database, topic_cloud_events_file_path):
    """Create changefeed and capture the internal CDC topic cloud event."""
    table_name = 'CanonicalCdcTable'
    changefeed_name = 'updates'
    capture = CanonicalCaptureCloudEventOutput(topic_cloud_events_file_path, _database)

    create_table(ydb_cluster, _database, table_name)
    time.sleep(1)

    with capture:
        create_changefeed(ydb_cluster, table_name, changefeed_name, _database)
    time.sleep(3)

    return capture.canonize()
