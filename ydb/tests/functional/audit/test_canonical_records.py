# -*- coding: utf-8 -*-
import logging
import json

from ydb import Driver, DriverConfig, SessionPool, TableDescription, Column, OptionalType, PrimitiveType  # , TableClient
from ydb.topic import TopicClient  # , TopicAlterConsumer
from ydb.issues import Unauthorized  # , SchemeError
from ydb.draft import DynamicConfigClient
# from ydb.query import QuerySessionPool
from ydb.tests.library.harness.util import LogLevels

import http_helpers
from helpers import cluster_endpoint, make_test_file_with_content, execute_ydbd, execute_dstool_grpc, execute_dstool_http, CanonicalCaptureAuditFileOutput

logger = logging.getLogger(__name__)


TOKEN = 'root@builtin'
OTHER_TOKEN = 'other-user@builtin'

DATABASE = '/Root'

AUTH_CONFIG = f'staff_api_user_token: {TOKEN}'

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
    audit_log_config={
        'file_backend': {
            'format': 'json',
            # File path will be generated automatically
        },
        'log_class_config': [
            {
                'log_class': 'default',
                'enable_logging': True,
                'log_phase': ['received', 'completed'],
            }
        ]
    },
    enforce_user_token_requirement=True,
    default_clusteradmin=TOKEN,
    monitoring_allowed_sids=[TOKEN],
    viewer_allowed_sids=[TOKEN],
    auth_config_path=make_test_file_with_content('auth_config.yaml', AUTH_CONFIG),
    dynamic_pdisks=[{'user_kind': 0}],
)


DYN_CONFIG = '''
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


def test_create_drop_and_alter_database(ydb_cluster):
    capture_audit_create_console = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['console'])
    capture_audit_create_schemeshard = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['schemeshard'])
    capture_audit_drop_console = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['console'])
    capture_audit_drop_schemeshard = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['schemeshard'])
    capture_audit_alter = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit_create_console, capture_audit_create_schemeshard:
        database = '/Root/Database'
        ydb_cluster.create_database(
            database,
            storage_pool_units_count={'hdd': 1},
            token=TOKEN
        )
        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database, token=TOKEN)

    with capture_audit_alter:
        alter_subdomain_enable_scheme_shard_audit = '''
            ModifyScheme {
                OperationType: ESchemeOpAlterExtSubDomain
                WorkingDir: "/Root"
                SubDomain {
                    Name: "Database"
                    AuditSettings {
                        EnableDmlAudit: true
                        ExpectedSubjects: ["expected_subject@as"]
                    }
                }
            }
        '''

        alter_subdomain_disable_scheme_shard_audit = '''
            ModifyScheme {
                OperationType: ESchemeOpAlterExtSubDomain
                WorkingDir: "/Root"
                SubDomain {
                    Name: "Database"
                    AuditSettings {
                        EnableDmlAudit: false
                        ExpectedSubjects: [""]
                    }
                }
            }
        '''

        execute_ydbd(ydb_cluster, TOKEN, ['db', 'schema', 'exec', make_test_file_with_content('enable_scheme_shard_audit.pb.txt', alter_subdomain_enable_scheme_shard_audit)])
        execute_ydbd(ydb_cluster, TOKEN, ['db', 'schema', 'exec', make_test_file_with_content('disable_scheme_shard_audit.pb.txt', alter_subdomain_disable_scheme_shard_audit)])

    with capture_audit_drop_console, capture_audit_drop_schemeshard:
        ydb_cluster.remove_database(database, token=TOKEN)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
    return capture_audit_create_console.canonize(), \
        capture_audit_create_schemeshard.canonize(), \
        capture_audit_alter.canonize(), \
        capture_audit_drop_console.canonize(), \
        capture_audit_drop_schemeshard.canonize()


def test_replace_config(ydb_cluster):
    def apply_config(pool, config):
        client = DynamicConfigClient(pool._driver)
        client.set_config(config, dry_run=False, allow_unknown_fields=False)

    def call_replace_config(token):
        with Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=token)) as driver:
            with SessionPool(driver) as pool:
                try:
                    pool.retry_operation_sync(apply_config, config=DYN_CONFIG)
                except Unauthorized:
                    pass

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit:
        call_replace_config(OTHER_TOKEN)
        call_replace_config(TOKEN)
    return capture_audit.canonize()


def test_create_drop_and_alter_table(ydb_cluster):
    capture_audit_create = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    capture_audit_alter = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    capture_audit_drop = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=TOKEN)) as driver:
        with SessionPool(driver) as pool:
            # table_client = TableClient(driver)
            description = TableDescription().with_columns(
                Column('key', OptionalType(PrimitiveType.Uint64)),
                Column('value', OptionalType(PrimitiveType.Utf8))).with_primary_key('key')

            with capture_audit_create:
                pool.retry_operation_sync(lambda session: session.create_table('/Root/Table', description))

            with capture_audit_alter:
                pool.retry_operation_sync(lambda session: session.alter_table('/Root/Table', add_columns=[Column('col', OptionalType(PrimitiveType.Uint64))]))

            with capture_audit_drop:
                pool.retry_operation_sync(lambda session: session.drop_table('/Root/Table'))
    return capture_audit_create.canonize(), capture_audit_alter.canonize(), capture_audit_drop.canonize()


# This test is commented because there is no query service implementation in python sdk
# def test_dml(ydb_cluster):
#     create_table_sql = """
#         CREATE TABLE TestTable (
#         id Int64 NOT NULL,
#         value Int64 NOT NULL,
#         PRIMARY KEY (id)
#     ) """

#     insert_sql = """
#         INSERT INTO TestTable
#             (id, value)
#         VALUES
#             (1, 2)
#     """

#     select_sql = 'SELECT * FROM TestTable'

#     def select(session):
#         with session.transaction() as transaction:
#             with transaction.execute(select_sql, commit_tx=True) as iterator:
#                 try:
#                     while iterator.next():
#                         pass
#                 except StopIteration:
#                     pass

#     capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
#     with Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=TOKEN)) as right_driver, \
#          Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=OTHER_TOKEN)) as wrong_driver:

#         right_pool = QuerySessionPool(right_driver)
#         wrong_pool = QuerySessionPool(wrong_driver)
#         with capture_audit:
#             right_pool.execute_with_retries(create_table_sql)
#             right_pool.execute_with_retries(insert_sql)
#             right_pool.retry_operation_sync(select)

#             # Request with error
#             try:
#                 wrong_pool.execute_with_retries(insert_sql)
#             except SchemeError:
#                 pass

#     return capture_audit.canonize()


def test_kill_tablet_using_developer_ui(ydb_cluster):
    # List tablets
    list_response = http_helpers.get_tablets_request(ydb_cluster, TOKEN)
    assert list_response.status_code == 200, list_response.content
    ss_tablet_id = http_helpers.extract_tablet_id(list_response.content, 'SCHEMESHARD')
    assert ss_tablet_id

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    # Kill schemeshard tablet
    with capture_audit:
        # The first request must write UNAUTHORIZED to audit log
        kill_response = http_helpers.kill_tablet_request(ydb_cluster, ss_tablet_id, OTHER_TOKEN)
        assert kill_response.status_code == 403, kill_response.content

        # The second request is done from valid user
        kill_response = http_helpers.kill_tablet_request(ydb_cluster, ss_tablet_id, TOKEN)
        assert kill_response.status_code == 200, kill_response.content
    return capture_audit.canonize()


def test_dml_through_http(ydb_cluster):
    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit:
        select_sql = 'SELECT 42'
        select_response = http_helpers.sql_request(ydb_cluster, DATABASE, select_sql, OTHER_TOKEN)
        assert select_response.status_code == 403, select_response.content

        select_response = http_helpers.sql_request(ydb_cluster, DATABASE, select_sql, TOKEN)
        assert select_response.status_code == 200, select_response.content
    return capture_audit.canonize()


def test_restart_pdisk(ydb_cluster):
    list_pdisks_response = http_helpers.list_pdisks_request(ydb_cluster, TOKEN)
    assert list_pdisks_response.status_code == 200, list_pdisks_response.content
    pdisk_subpage = http_helpers.extract_pdisk(list_pdisks_response.content)
    assert pdisk_subpage

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit:
        restart_response = http_helpers.restart_pdisk(ydb_cluster, pdisk_subpage, OTHER_TOKEN)
        assert restart_response.status_code == 403, restart_response.content

        restart_response = http_helpers.restart_pdisk(ydb_cluster, pdisk_subpage, TOKEN)
        assert restart_response.status_code == 200, restart_response.content
    return capture_audit.canonize()


def test_topic(ydb_cluster):
    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=TOKEN)) as right_driver:
        # Driver(DriverConfig(cluster_endpoint(ydb_cluster), DATABASE, auth_token=OTHER_TOKEN)) as wrong_driver:
        right_topic_client = TopicClient(driver=right_driver, settings=None)
        # wrong_topic_client = TopicClient(driver=wrong_driver, settings=None)

        topic_name = 'test_topic'
        # consumer_name = 'test_consumer'
        with capture_audit:
            # Create
            right_topic_client.create_topic(topic_name)

            # This code is commented because there is no alter_topic implementation in python sdk
            # Try to modify without rights
            # try:
            #     wrong_topic_client.alter_topic(topic_name, add_consumers=[consumer_name])
            # except SchemeError:
            #     # ydb.issues.SchemeError: message: "path 'Root/test_topic' does not exist or you do not have access rights" issue_code: 500018 severity: 1 (server_code: 400070)
            #     pass

            # Add consumer
            # right_topic_client.alter_topic(topic_name, add_consumers=[consumer_name])

            # Modify consumer
            # right_topic_client.alter_topic(topic_name, alter_consumers=[TopicAlterConsumer(name=consumer_name, alter_attributes={'x': 'y'})])

            # Write
            with right_topic_client.writer(topic_name, producer_id='test_id') as writer:
                writer.write('message')
    return capture_audit.canonize()


# def test_execute_minikql(ydb_cluster):
#     # List tablets
#     list_response = http_helpers.get_tablets_request(ydb_cluster, TOKEN)
#     assert list_response.status_code == 200, list_response.content
#     ss_tablet_id = http_helpers.extract_tablet_id(list_response.content, 'SCHEMESHARD')
#     assert ss_tablet_id

#     query = '''
#         (
#             (let row '('('TabId (Uint64 '42))))
#             (let data
#                 '(
#                     '('NextColId (Uint32 '42))
#                     '('PartitionConfig (Utf8 'Trash))
#                 )
#             )
#             (let update (UpdateRow 'Tables row data))
#             (return (
#                 AsList update
#             ))
#         )
#     '''

#     capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
#     with capture_audit:
#         execute_ydbd(ydb_cluster, TOKEN, ['admin', 'tablet', ss_tablet_id, 'execute', make_test_file_with_content('minikql.query', query)])
#     return capture_audit.canonize()


def test_dstool_evict_vdisk_grpc(ydb_cluster):
    list_result = json.loads(execute_dstool_grpc(ydb_cluster, TOKEN, ['vdisk', 'list', '--format', 'json']))
    assert len(list_result) > 0
    vdisk_id = list_result[0]["VDiskId"]
    assert vdisk_id

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit:
        execute_dstool_grpc(ydb_cluster, TOKEN, ['vdisk', 'evict', '--vdisk-ids', vdisk_id, '--ignore-degraded-group-check', '--ignore-failure-model-group-check'])
    return capture_audit.canonize()


def test_dstool_add_group_http(ydb_cluster):
    list_result = json.loads(execute_dstool_http(ydb_cluster, TOKEN, ['pool', 'list', '--format', 'json']))
    assert len(list_result) > 0
    pool_name = list_result[0]["PoolName"]
    assert pool_name

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit:
        execute_dstool_http(ydb_cluster, TOKEN, ['group', 'add', '--pool-name', pool_name, '--groups', '1'])
    return capture_audit.canonize()
