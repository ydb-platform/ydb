# -*- coding: utf-8 -*-

import logging
import pytest
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


DATABASE = '/Root/users/database'


@pytest.fixture(scope='module', params=[True, False], ids=['domain_login_only--true', 'domain_login_only--false'])
def domain_login_only(request):
    return request.param


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration, domain_login_only):
    config_generator = KikimrConfigGenerator(**ydb_cluster_configuration)
    config_generator.yaml_config['auth_config'] = {
        'domain_login_only': domain_login_only,
    }
    return config_generator


@pytest.fixture(scope='function')
def domain_admin_driver_config(ydb_endpoint):
    return ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database="/Root",
    )


@pytest.fixture(scope='function')
def tenant_admin_driver_config(ydb_endpoint):
    return ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database=DATABASE,
    )


@pytest.fixture(scope='function')
def tenant_user_driver_config(ydb_endpoint):
    return ydb.DriverConfig(
        endpoint=ydb_endpoint,
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password('user', ''),
    )


def yql_create_user(config):
    with ydb.Driver(config) as driver:
        driver.wait()
        script_client = ydb.ScriptingClient(driver)
        script_client.execute_yql('CREATE USER user;')
        script_client.execute_yql('ALTER USER user WITH PASSWORD NULL;')
        script_client.execute_yql('DROP USER user;')


def query_create_user(config):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries('CREATE USER user;')
            pool.execute_with_retries('ALTER USER user WITH PASSWORD NULL;')
            pool.execute_with_retries('DROP USER user;')


def yql_create_group(config):
    with ydb.Driver(config) as driver:
        driver.wait()
        script_client = ydb.ScriptingClient(driver)
        script_client.execute_yql('CREATE GROUP group;')
        script_client.execute_yql('CREATE USER user;')
        script_client.execute_yql('ALTER GROUP group ADD USER user;')
        script_client.execute_yql('ALTER GROUP group DROP USER user;')
        script_client.execute_yql('DROP GROUP group;')
        script_client.execute_yql('DROP USER user;')


def query_create_group(config):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries('CREATE GROUP group;')
            pool.execute_with_retries('CREATE USER user;')
            pool.execute_with_retries('ALTER GROUP group ADD USER user;')
            pool.execute_with_retries('ALTER GROUP group DROP USER user;')
            pool.execute_with_retries('DROP GROUP group;')
            pool.execute_with_retries('DROP USER user;')


def test_yql_create_user_by_domain_admin(ydb_cluster, domain_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        yql_create_user(domain_admin_driver_config)


def test_yql_create_user_by_tenant_admin(ydb_cluster, tenant_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        yql_create_user(tenant_admin_driver_config)


def test_yql_create_group_by_domain_admin(ydb_cluster, domain_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        yql_create_group(domain_admin_driver_config)


def test_yql_create_group_by_tenant_admin(ydb_cluster, tenant_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        yql_create_group(tenant_admin_driver_config)


def test_query_create_user_by_domain_admin(ydb_cluster, domain_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        query_create_user(domain_admin_driver_config)


def test_query_create_user_by_tenant_admin(ydb_cluster, tenant_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        query_create_user(tenant_admin_driver_config)


def test_query_create_group_by_domain_admin(ydb_cluster, domain_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        query_create_group(domain_admin_driver_config)


def test_query_create_group_by_tenant_admin(ydb_cluster, tenant_admin_driver_config):
    with ydb_database_ctx(ydb_cluster, DATABASE):
        query_create_group(tenant_admin_driver_config)
