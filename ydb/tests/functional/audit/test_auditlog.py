# -*- coding: utf-8 -*-
import json
import logging
import os
import subprocess
import sys
import time

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb

from ydb import Driver, DriverConfig, SessionPool
from ydb.draft import DynamicConfigClient
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.ydb_fixtures import ydb_database_ctx

logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'GRPC_PROXY': LogLevels.DEBUG,
        'GRPC_SERVER': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.TRACE,
        # less logs
        'KQP_PROXY': LogLevels.DEBUG,
        'KQP_GATEWAY': LogLevels.DEBUG,
        'KQP_WORKER': LogLevels.ERROR,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.ERROR,
        'KQP_COMPILE_ACTOR': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'HIVE': LogLevels.ERROR,
        'CMS_TENANTS': LogLevels.ERROR,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
        'TX_PROXY_SCHEME_CACHE': LogLevels.CRIT,
        'TX_PROXY': LogLevels.CRIT,
    },
    enable_audit_log=True,
    # extra_feature_flags=['enable_grpc_audit'],

    # two builtin users with empty passwords
    default_users=dict((i, '') for i in ('root', 'other-user')),
)


def cluster_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].port}'


def ydbcli_db_schema_exec(cluster, operation_proto):
    endpoint = cluster_endpoint(cluster)
    args = [
        cluster.nodes[1].binary_path,
        f'--server=grpc://{endpoint}',
        'db',
        'schema',
        'exec',
        operation_proto,
    ]
    r = subprocess.run(args, capture_output=True)
    assert r.returncode == 0, r.stderr.decode('utf-8')


def alter_database_audit_settings(cluster, database_path, enable_dml_audit=None, expected_subjects=None):
    assert enable_dml_audit is not None or expected_subjects is not None

    enable_dml_audit_proto = ''
    if enable_dml_audit is not None:
        enable_dml_audit_proto = '''EnableDmlAudit: %s''' % (enable_dml_audit)

    expected_subjects_proto = ''
    if expected_subjects is not None:
        expected_subjects_proto = '''ExpectedSubjects: [%s]''' % (', '.join([f'"{i}"' for i in expected_subjects]))

    alter_proto = r'''ModifyScheme {
        OperationType: ESchemeOpAlterExtSubDomain
        WorkingDir: "%s"
        SubDomain {
            Name: "%s"
            AuditSettings {
                %s
                %s
            }
        }
    }''' % (
        os.path.dirname(database_path),
        os.path.basename(database_path),
        enable_dml_audit_proto,
        expected_subjects_proto,
    )

    ydbcli_db_schema_exec(cluster, alter_proto)


def alter_user_attrs(cluster, path, **kwargs):
    if not kwargs:
        return

    user_attrs = ['''UserAttributes { Key: "%s" Value: "%s" }''' % (k, v) for k, v in kwargs.items()]

    alter_proto = r'''ModifyScheme {
        OperationType: ESchemeOpAlterUserAttributes
        WorkingDir: "%s"
        AlterUserAttributes {
            PathName: "%s"
            %s
        }
    }''' % (
        os.path.dirname(path),
        os.path.basename(path),
        '\n'.join(user_attrs),
    )

    ydbcli_db_schema_exec(cluster, alter_proto)


class CaptureFileOutput:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __exit__(self, *exc):
        # unreliable way to get all due audit records into the file
        time.sleep(0.1)
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            self.captured = f.read().decode('utf-8')


@pytest.fixture(scope='module')
def _database(ydb_cluster, ydb_root, request):
    database_path = os.path.join(ydb_root, request.node.name)
    with ydb_database_ctx(ydb_cluster, database_path):
        yield database_path


@pytest.fixture(scope='module')
def _client_session_pool_with_auth_root(ydb_cluster, _database):
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), _database, auth_token='root@builtin')) as driver:
        with SessionPool(driver) as pool:
            yield pool


@pytest.fixture(scope='module')
def _client_session_pool_with_auth_other(ydb_cluster, _database):
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), _database, auth_token='other-user@builtin')) as driver:
        with SessionPool(driver) as pool:
            yield pool


@pytest.fixture(scope='module')
def _client_session_pool_no_auth(ydb_cluster, _database):
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), _database, auth_token=None)) as driver:
        with SessionPool(driver) as pool:
            yield pool


@pytest.fixture(scope='module')
def _client_session_pool_bad_auth(ydb_cluster, _database):
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), _database, auth_token='__bad__@builtin')) as driver:
        with SessionPool(driver) as pool:
            yield pool


def create_table(pool, table_path):
    def f(s, table_path):
        s.execute_scheme(fr'''
            create table `{table_path}` (
                id int64,
                value int64,
                primary key (id)
            );
        ''')
    pool.retry_operation_sync(f, table_path=table_path, retry_settings=None)


def fill_table(pool, table_path):
    def f(s, table_path):
        s.transaction().execute(fr'''
            insert into `{table_path}` (id, value) values (1, 1), (2, 2)
        ''')
    pool.retry_operation_sync(f, table_path=table_path, retry_settings=None)


@pytest.fixture(scope='module')
def prepared_test_env(ydb_cluster, _database, _client_session_pool_no_auth):
    database_path = _database
    table_path = os.path.join(database_path, 'test-table')
    pool = _client_session_pool_no_auth

    create_table(pool, table_path)
    fill_table(pool, table_path)

    capture_audit = CaptureFileOutput(ydb_cluster.config.audit_file_path)
    print('AAA', capture_audit.filename, file=sys.stderr)
    # print('AAA', ydb_cluster.config.binary_path, file=sys.stderr)

    alter_database_audit_settings(ydb_cluster, database_path, enable_dml_audit=True)

    return table_path, capture_audit


def execute_data_query(pool, text):
    pool.retry_operation_sync(lambda s: s.transaction().execute(text, commit_tx=True))


QUERIES = [
    r'''insert into `{table_path}` (id, value) values (100, 100), (101, 101)''',
    r'''delete from `{table_path}` where id = 100 or id = 101''',
    r'''select id from `{table_path}`''',
    r'''update `{table_path}` set value = 0 where id = 1''',
    r'''replace into `{table_path}` (id, value) values (2, 3), (3, 3)''',
    r'''upsert into `{table_path}` (id, value) values (4, 4), (5, 5)''',
]


@pytest.mark.parametrize("query_template", QUERIES, ids=lambda x: x.split(maxsplit=1)[0])
def test_single_dml_query_logged(query_template, prepared_test_env, _client_session_pool_with_auth_root):
    table_path, capture_audit = prepared_test_env

    pool = _client_session_pool_with_auth_root
    query_text = query_template.format(table_path=table_path)

    with capture_audit:
        execute_data_query(pool, query_text)

    print(capture_audit.captured, file=sys.stderr)
    assert query_text in capture_audit.captured

    assert '"query_text"' in capture_audit.captured
    assert '"remote_address"' in capture_audit.captured
    assert '"subject"' in capture_audit.captured
    assert '"database"' in capture_audit.captured
    assert '"operation"' in capture_audit.captured
    assert '"start_time"' in capture_audit.captured
    assert '"end_time"' in capture_audit.captured

    # absent cloud-ids are not logged
    assert '"cloud_id"' not in capture_audit.captured
    assert '"folder_id"' not in capture_audit.captured
    assert '"resource_id"' not in capture_audit.captured


def test_dml_begin_commit_logged(prepared_test_env, _client_session_pool_with_auth_root):
    table_path, capture_audit = prepared_test_env

    pool = _client_session_pool_with_auth_root

    with pool.checkout() as session:
        with capture_audit:
            tx = session.transaction().begin()
            tx.execute(fr'''update `{table_path}` set value = 0 where id = 1''')
            tx.commit()

    print(capture_audit.captured, file=sys.stderr)
    assert 'BeginTransaction' in capture_audit.captured
    assert 'CommitTransaction' in capture_audit.captured


# TODO: fix ydbd crash on exit
# def test_dml_begin_rollback_logged(prepared_test_env, _client_session_pool_with_auth_root):
#     table_path, capture_audit = prepared_test_env
#
#     pool = _client_session_pool_with_auth_root
#
#     with pool.checkout() as session:
#         with capture_audit:
#             tx = session.transaction().begin()
#             tx.execute(fr'''update `{table_path}` set value = 0 where id = 1''')
#             tx.rollback()
#
#     print(capture_audit.captured, file=sys.stderr)
#     assert 'BeginTransaction' in capture_audit.captured
#     assert 'RollbackTransaction' in capture_audit.captured


def test_dml_requests_arent_logged_when_anonymous(prepared_test_env, _client_session_pool_no_auth):
    table_path, capture_audit = prepared_test_env
    pool = _client_session_pool_no_auth

    with capture_audit:
        for i in QUERIES:
            query_text = i.format(table_path=table_path)
            execute_data_query(pool, query_text)

    print(capture_audit.captured, file=sys.stderr)
    assert len(capture_audit.captured) == 0, capture_audit.captured


def test_dml_requests_logged_when_unauthorized(prepared_test_env, _client_session_pool_bad_auth):
    table_path, capture_audit = prepared_test_env
    pool = _client_session_pool_bad_auth

    for i in QUERIES:
        query_text = i.format(table_path=table_path)
        with pool.checkout() as session:
            tx = session.transaction()
            with capture_audit:
                with pytest.raises(ydb.issues.SchemeError):
                    tx.execute(query_text, commit_tx=True)
            print(capture_audit.captured, file=sys.stderr)
            assert query_text in capture_audit.captured


def test_dml_requests_arent_logged_when_sid_is_expected(ydb_cluster, _database, prepared_test_env, _client_session_pool_with_auth_root):
    database_path = _database
    table_path, capture_audit = prepared_test_env

    alter_database_audit_settings(ydb_cluster, database_path, enable_dml_audit=True, expected_subjects=['root@builtin'])

    pool = _client_session_pool_with_auth_root
    with capture_audit:
        for i in QUERIES:
            query_text = i.format(table_path=table_path)
            execute_data_query(pool, query_text)

    print(capture_audit.captured, file=sys.stderr)
    assert len(capture_audit.captured) == 0, capture_audit.captured


def give_use_permission_to_user(pool, database_path, user):
    def f(s, database_path):
        s.execute_scheme(fr'''
            grant 'ydb.generic.use' on `{database_path}` to `{user}`
        ''')
    pool.retry_operation_sync(f, database_path=database_path, retry_settings=None)


def test_dml_requests_logged_when_sid_is_unexpected(ydb_cluster, _database, prepared_test_env, _client_session_pool_no_auth, _client_session_pool_with_auth_other):
    database_path = _database
    table_path, capture_audit = prepared_test_env

    alter_database_audit_settings(ydb_cluster, database_path, enable_dml_audit=True, expected_subjects=['root@builtin'])
    give_use_permission_to_user(_client_session_pool_no_auth, database_path, 'other-user@builtin')

    pool = _client_session_pool_with_auth_other
    for i in QUERIES:
        query_text = i.format(table_path=table_path)
        with capture_audit:
            execute_data_query(pool, query_text)
        print(capture_audit.captured, file=sys.stderr)
        assert query_text in capture_audit.captured


@pytest.mark.parametrize('attrs', [
    dict(cloud_id='cloud-id-A', folder_id='folder-id-B', database_id='database-id-C'),
    dict(folder_id='folder-id-B'),
])
def test_cloud_ids_are_logged(ydb_cluster, _database, prepared_test_env, _client_session_pool_with_auth_root, attrs):
    database_path = _database
    table_path, capture_audit = prepared_test_env

    alter_user_attrs(ydb_cluster, database_path, **attrs)

    pool = _client_session_pool_with_auth_root
    with capture_audit:
        execute_data_query(pool, fr'''update `{table_path}` set value = 0 where id = 1''')
    print(capture_audit.captured, file=sys.stderr)

    for k, v in attrs.items():
        name = k if k != 'database_id' else 'resource_id'
        assert fr'''"{name}":"{v}"''' in capture_audit.captured


def apply_config(pool, config):
    client = DynamicConfigClient(pool._driver)
    client.set_config(config, dry_run=False, allow_unknown_fields=False)


@pytest.fixture(scope='module')
def _good_dynconfig():
    return '''
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
    '''


@pytest.fixture(scope='module')
def _bad_dynconfig():
    return '''
---
123metadata:
  kind: MainConfig
  cluster: ""
  version: %s
config:
  yaml_config_enabled: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
    '''


def test_dynconfig(ydb_cluster, prepared_test_env, _client_session_pool_with_auth_root, _good_dynconfig):
    config = _good_dynconfig
    _table_path, capture_audit = prepared_test_env
    with capture_audit:
        _client_session_pool_with_auth_root.retry_operation_sync(apply_config, config=config)

    print(capture_audit.captured, file=sys.stderr)
    assert json.dumps(config) in capture_audit.captured


@pytest.mark.parametrize('config_fixture', ["_bad_dynconfig", "_good_dynconfig"])
@pytest.mark.parametrize('pool_fixture', ["_client_session_pool_with_auth_root", "_client_session_pool_no_auth", "_client_session_pool_bad_auth", "_client_session_pool_with_auth_other"])
def test_broken_dynconfig(ydb_cluster, prepared_test_env, pool_fixture, config_fixture, request):
    pool = request.getfixturevalue(pool_fixture)
    config = request.getfixturevalue(config_fixture)
    _table_path, capture_audit = prepared_test_env
    with capture_audit:
        try:
            pool.retry_operation_sync(apply_config, config=config)
        except ydb.issues.BadRequest:
            pass

    print(capture_audit.captured, file=sys.stderr)
    assert json.dumps(config) in capture_audit.captured
