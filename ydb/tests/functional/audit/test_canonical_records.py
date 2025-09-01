# -*- coding: utf-8 -*-
from ydb.tests.library.harness.util import LogLevels

from helpers import make_test_file_with_content, CanonicalCaptureAuditFileOutput


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
