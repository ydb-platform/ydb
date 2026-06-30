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
