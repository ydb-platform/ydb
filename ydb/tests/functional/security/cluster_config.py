# -*- coding: utf-8 -*-
"""Shared KiKiMR config for functional/security tests (importable without pytest conftest magic)."""

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


def create_ydb_configurator(
    certificates,
    enforce_user_token_requirement=True,
    require_counters_authentication=None,
    require_healthcheck_authentication=None,
):
    cluster_config = {
        'default_clusteradmin': 'root@builtin',
        'enforce_user_token_requirement': enforce_user_token_requirement,
    }
    config_generator = KikimrConfigGenerator(**cluster_config)

    if 'grpc_config' not in config_generator.yaml_config:
        config_generator.yaml_config['grpc_config'] = {}

    config_generator.yaml_config['grpc_config']['cert'] = certificates['server_cert']
    config_generator.yaml_config['grpc_config']['key'] = certificates['server_key']

    config_generator.monitoring_tls_cert_path = certificates['server_cert']
    config_generator.monitoring_tls_key_path = certificates['server_key']

    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['database_allowed_sids'] = ['database@builtin']
    security_config['viewer_allowed_sids'] = ['viewer@builtin']
    security_config['monitoring_allowed_sids'] = ['monitoring@builtin']

    assert (
        'administration_allowed_sids' in security_config and len(security_config['administration_allowed_sids']) > 0
    ), 'administration_allowed_sids was supposed to be set due to default_clusteradmin'

    if require_counters_authentication is not None or require_healthcheck_authentication is not None:
        if 'monitoring_config' not in config_generator.yaml_config:
            config_generator.yaml_config['monitoring_config'] = {}

        if require_counters_authentication is not None:
            config_generator.yaml_config['monitoring_config'][
                'require_counters_authentication'
            ] = require_counters_authentication

        if require_healthcheck_authentication is not None:
            config_generator.yaml_config['monitoring_config'][
                'require_healthcheck_authentication'
            ] = require_healthcheck_authentication

    return config_generator
