# -*- coding: utf-8 -*-
import copy
import logging
import pytest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'TX_PROXY': LogLevels.DEBUG,
        'TX_PROXY_SCHEME_CACHE': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        'enable_real_system_view_paths',
    ],
    # do not clutter logs with resource pools auto creation
    enable_resource_pools=False
)


# ydb_fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration():
    conf = copy.deepcopy(CLUSTER_CONFIG)
    return conf


DOMAIN_NAME = '/Root'
TENANT_NAME_1 = '/Root/Tenant1'
TENANT_NAME_2 = '/Root/Tenant2'


def test_allowed_paths_lookup(ydb_cluster):
    ydb_cluster.create_database(TENANT_NAME_1, storage_pool_units_count={'hdd': 1}, timeout_seconds=20)
    tenant_nodes_1 = ydb_cluster.register_and_start_slots(TENANT_NAME_1)
    ydb_cluster.wait_tenant_up(TENANT_NAME_1)

    ydb_cluster.create_database(TENANT_NAME_2, storage_pool_units_count={'hdd': 1}, timeout_seconds=20)
    tenant_nodes_2 = ydb_cluster.register_and_start_slots(TENANT_NAME_2)
    ydb_cluster.wait_tenant_up(TENANT_NAME_2)

    permissions_settings = ydb.ModifyPermissionsSettings()\
        .grant_permissions('user', ('ydb.generic.list',))

    # Check allowed lookups from Domain database
    domain_driver_config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DOMAIN_NAME,
    )

    with ydb.Driver(domain_driver_config) as driver:
        driver.wait(timeout=4)
        # Modify permissions on Domain root path
        driver.scheme_client.modify_permissions(DOMAIN_NAME, permissions_settings)

        # Modify permissions on Domain regular path
        driver.scheme_client.modify_permissions(DOMAIN_NAME + '/.sys', permissions_settings)

        # Modify permissions on Tenant root path
        driver.scheme_client.modify_permissions(TENANT_NAME_1, permissions_settings)

        # Modify permissions on Tenant regular path
        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(TENANT_NAME_1 + '/.sys', permissions_settings)

        assert exc_info.type is ydb.issues.SchemeError
        assert 'Path does not exist' in exc_info.value.message

    # Check allowed lookups from Tenant database
    tenant_driver_config = ydb.DriverConfig(
        endpoint=tenant_nodes_1[0].endpoint,
        database=TENANT_NAME_1,
    )

    with ydb.Driver(tenant_driver_config) as driver:
        driver.wait(timeout=4)
        # Modify permissions on Domain root path
        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(DOMAIN_NAME, permissions_settings)

        assert exc_info.type is ydb.issues.SchemeError
        assert 'Path does not exist' in exc_info.value.message

        # Modify permissions on Domain regular path
        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(DOMAIN_NAME + '/.sys', permissions_settings)

        assert exc_info.type is ydb.issues.SchemeError
        assert 'Path does not exist' in exc_info.value.message

        # Modify permissions on current Tenant root path
        driver.scheme_client.modify_permissions(TENANT_NAME_1, permissions_settings)

        # Modify permissions on current Tenant regular path
        driver.scheme_client.modify_permissions(TENANT_NAME_1 + '/.sys', permissions_settings)

        # Modify permissions on other Tenant root path
        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(TENANT_NAME_2, permissions_settings)

        assert exc_info.type is ydb.issues.SchemeError
        assert 'Path does not exist' in exc_info.value.message

        # Modify permissions on other Tenant regular path
        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(TENANT_NAME_2 + '/.sys', permissions_settings)

        assert exc_info.type is ydb.issues.SchemeError
        assert 'Path does not exist' in exc_info.value.message

    ydb_cluster.remove_database(TENANT_NAME_1)
    ydb_cluster.unregister_and_stop_slots(tenant_nodes_1)

    ydb_cluster.remove_database(TENANT_NAME_2)
    ydb_cluster.unregister_and_stop_slots(tenant_nodes_2)
