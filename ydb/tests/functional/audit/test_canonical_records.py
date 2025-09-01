# -*- coding: utf-8 -*-
from ydb import Driver, DriverConfig, SessionPool, TableClient, TableDescription, Column, OptionalType, PrimitiveType
from ydb.draft import DynamicConfigClient
from ydb.tests.library.harness.util import LogLevels

from helpers import cluster_endpoint, make_test_file_with_content, CanonicalCaptureAuditFileOutput


TOKEN = 'root@builtin'

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
    auth_config_path=make_test_file_with_content('auth_config.yaml', AUTH_CONFIG),
    # extra_feature_flags=['enable_grpc_audit'],
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


def test_create_and_drop_database(ydb_cluster):
    capture_audit_create = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['console', 'schemeshard'])
    capture_audit_drop = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, ['console', 'schemeshard'])
    with capture_audit_create:
        database = '/Root/Database'
        ydb_cluster.create_database(
            database,
            storage_pool_units_count={'hdd': 1},
            token=TOKEN
        )
        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database, token=TOKEN)

    with capture_audit_drop:
        ydb_cluster.remove_database(database, token=TOKEN)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
    return (capture_audit_create.canonize(), capture_audit_drop.canonize())


def test_replace_config(ydb_cluster):
    def apply_config(pool, config):
        client = DynamicConfigClient(pool._driver)
        client.set_config(config, dry_run=False, allow_unknown_fields=False)

    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), '/Root', auth_token=TOKEN)) as driver:
        with SessionPool(driver) as pool:
            with capture_audit:
                pool.retry_operation_sync(apply_config, config=DYN_CONFIG)
    return capture_audit.canonize()


def test_create_and_drop_table(ydb_cluster):
    capture_audit_create = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    capture_audit_drop = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with Driver(DriverConfig(cluster_endpoint(ydb_cluster), '/Root', auth_token=TOKEN)) as driver:
        table_client = TableClient(driver)
        description = TableDescription().with_columns(
            Column('key', OptionalType(PrimitiveType.Uint64)),
            Column('value', OptionalType(PrimitiveType.Utf8))).with_primary_key('key')

        with capture_audit_create:
            table_client.create_table('/Root/Table', description)

        with capture_audit_drop:
            table_client.drop_table('/Root/Table')
    return (capture_audit_create.canonize(), capture_audit_drop.canonize())
