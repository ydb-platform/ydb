# -*- coding: utf-8 -*-
import json
import logging
import os
import sys
import time

import pytest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.fixtures import ydb_database_ctx

from helpers import CaptureFileOutput, ydbcli_db_schema_exec

logger = logging.getLogger(__name__)


CLUSTER_CONFIG = dict(
    additional_log_configs={
        'GRPC_PROXY': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.TRACE,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
    },
    enable_audit_log=True,
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


@pytest.fixture(scope='module')
def _database(ydb_cluster, ydb_root, request):
    database_path = os.path.join(ydb_root, 'TopicAuditTest')
    with ydb_database_ctx(ydb_cluster, database_path):
        yield database_path


@pytest.fixture(scope='module')
def topic_audit_file_path(ydb_cluster):
    return ydb_cluster.config.audit_file_path


def _parse_cloud_event_from_audit(captured, event_type_suffix):
    """Extract and parse cloud event JSON from audit log output."""
    full_type = f'yandex.cloud.events.ydb.topics.{event_type_suffix}'
    for line in captured.strip().split('\n'):
        if full_type not in line or 'cloud_event_json' not in line:
            continue
        json_start = line.find(': ')
        if json_start == -1:
            continue
        outer = json.loads(line[json_start + 2:])
        inner_json = outer.get('cloud_event_json')
        assert inner_json, f'No cloud_event_json in line: {line[:200]}'
        return json.loads(inner_json)
    assert False, f'No audit line with {full_type} found in: {captured[:500]}'


def test_create_topic_cloud_event_logged(ydb_cluster, _database, topic_audit_file_path):
    """Create topic and verify CreateTopic cloud event is written to audit file."""
    topic_name = 'TestTopic'
    topic_path = os.path.join(_database, topic_name)
    capture_audit = CaptureFileOutput(topic_audit_file_path)

    with capture_audit:
        create_topic(ydb_cluster, _database, topic_name)
    time.sleep(2)

    print(capture_audit.captured, file=sys.stderr)
    assert 'cloud_event_json' in capture_audit.captured
    assert 'yandex.cloud.events.ydb.topics.CreateTopic' in capture_audit.captured
    assert topic_path in capture_audit.captured

    ev = _parse_cloud_event_from_audit(capture_audit.captured, 'CreateTopic')
    # details (TopicDetails)
    details = ev.get('details', {})
    assert details.get('path') == topic_path
    assert 'retention_period' in details
    assert details.get('retention_period') == '10s'
    # request_parameters
    req = ev.get('request_parameters', {})
    assert req.get('path') == topic_path
    assert req.get('retention_period') == '10s'
    # event_metadata
    meta = ev.get('event_metadata', {})
    assert meta.get('event_type') == 'yandex.cloud.events.ydb.topics.CreateTopic'
    assert 'event_id' in meta
    assert 'created_at' in meta
    assert 'cloud_id' in meta
    assert 'folder_id' in meta
    # event_status
    assert ev.get('event_status') == 'DONE'
    # authentication, authorization
    assert ev.get('authentication', {}).get('authenticated') is True
    assert ev.get('authorization', {}).get('authorized') is True


def test_alter_topic_cloud_event_logged(ydb_cluster, _database, topic_audit_file_path):
    """Create, alter topic and verify AlterTopic cloud event is written to audit file."""
    topic_name = 'AlterTopic'
    topic_path = os.path.join(_database, topic_name)
    capture_audit = CaptureFileOutput(topic_audit_file_path)

    create_topic(ydb_cluster, _database, topic_name)
    time.sleep(1)

    with capture_audit:
        alter_topic(ydb_cluster, _database, topic_name)
    time.sleep(2)

    print(capture_audit.captured, file=sys.stderr)
    assert 'cloud_event_json' in capture_audit.captured
    assert 'yandex.cloud.events.ydb.topics.AlterTopic' in capture_audit.captured
    assert topic_path in capture_audit.captured

    ev = _parse_cloud_event_from_audit(capture_audit.captured, 'AlterTopic')
    # details (TopicDetails)
    details = ev.get('details', {})
    assert details.get('path') == topic_path
    assert 'retention_period' in details
    assert details.get('retention_period') == '42s'
    # request_parameters
    req = ev.get('request_parameters', {})
    assert req.get('path') == topic_path
    assert req.get('retention_period') == '42s'
    # event_metadata
    meta = ev.get('event_metadata', {})
    assert meta.get('event_type') == 'yandex.cloud.events.ydb.topics.AlterTopic'
    assert 'event_id' in meta
    assert 'created_at' in meta
    # event_status
    assert ev.get('event_status') == 'DONE'
    # authentication, authorization
    assert ev.get('authentication', {}).get('authenticated') is True
    assert ev.get('authorization', {}).get('authorized') is True


def test_drop_topic_cloud_event_logged(ydb_cluster, _database, topic_audit_file_path):
    """Create, drop topic and verify DeleteTopic cloud event is written to audit file."""
    topic_name = 'DropTopic'
    topic_path = os.path.join(_database, topic_name)
    capture_audit = CaptureFileOutput(topic_audit_file_path)

    create_topic(ydb_cluster, _database, topic_name)
    time.sleep(1)

    with capture_audit:
        drop_topic(ydb_cluster, _database, topic_name)
    time.sleep(3)

    print(capture_audit.captured, file=sys.stderr)
    assert 'cloud_event_json' in capture_audit.captured
    assert 'yandex.cloud.events.ydb.topics.DeleteTopic' in capture_audit.captured
    assert topic_path in capture_audit.captured

    ev = _parse_cloud_event_from_audit(capture_audit.captured, 'DeleteTopic')
    # details (EventDetails for DeleteTopic - only path)
    details = ev.get('details', {})
    assert details.get('path') == topic_path
    # request_parameters (only path for DeleteTopic)
    req = ev.get('request_parameters', {})
    assert req.get('path') == topic_path
    # event_metadata
    meta = ev.get('event_metadata', {})
    assert meta.get('event_type') == 'yandex.cloud.events.ydb.topics.DeleteTopic'
    assert 'event_id' in meta
    assert 'created_at' in meta
    # event_status
    assert ev.get('event_status') == 'DONE'
    # authentication, authorization
    assert ev.get('authentication', {}).get('authenticated') is True
    assert ev.get('authorization', {}).get('authorized') is True
