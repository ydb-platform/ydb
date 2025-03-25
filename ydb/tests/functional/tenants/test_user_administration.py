# -*- coding: utf-8 -*-
import copy
import logging

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'TX_PROXY': LogLevels.DEBUG,
        # less logs
        'FLAT_TX_SCHEMESHARD': LogLevels.INFO,
        'KQP_PROXY': LogLevels.CRIT,
        'KQP_WORKER': LogLevels.CRIT,
        'KQP_GATEWAY': LogLevels.CRIT,
        'GRPC_PROXY': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,
        'GRPC_SERVER': LogLevels.CRIT,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.CRIT,
        'KQP_COMPILE_ACTOR': LogLevels.CRIT,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.CRIT,
        'SCHEME_BOARD_POPULATOR': LogLevels.CRIT,
    },
    extra_feature_flags=[
        'enable_strict_acl_check',
        'enable_strict_user_management',
        'enable_database_admin',
    ],
    # do not clutter logs with resource pools auto creation
    enable_resource_pools=False,
    enforce_user_token_requirement=True,
    # make all bootstrap and setup under this user
    default_clusteradmin='root@builtin',
)


# ydb_fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration():
    conf = copy.deepcopy(CLUSTER_CONFIG)
    return conf


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration):
    config_generator = KikimrConfigGenerator(**ydb_cluster_configuration)
    config_generator.yaml_config['auth_config'] = {
        'domain_login_only': False,
    }
    config_generator.yaml_config['domains_config']['disable_builtin_security'] = True
    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['administration_allowed_sids'].append('clusteradmin')
    return config_generator


@pytest.fixture(scope='module')
def prepared_root_db(ydb_cluster, ydb_root, ydb_endpoint):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    # prepare root database
    driver_config = ydb.DriverConfig(ydb_endpoint, ydb_root, credentials=cluster_admin)
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("create user clusteradmin password '1234'")


@pytest.fixture(scope='module')
def prepared_tenant_db(ydb_cluster, ydb_endpoint, ydb_database_module_scope):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    # prepare tenant database
    database_path = ydb_database_module_scope
    driver_config = ydb.DriverConfig(ydb_endpoint, database_path, credentials=cluster_admin)
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            # setup for database admins, first
            session.execute_scheme("create user dbadmin password '1234'")
            session.execute_scheme('create group dbadmins')
            session.execute_scheme('alter group dbadmins add user dbadmin')

            # additional setup for individual tests
            session.execute_scheme("create user ordinaryuser password '1234'")

        # setup for database admins, second
        # make dbadmin the real admin of the database
        driver.scheme_client.modify_permissions(database_path, ydb.ModifyPermissionsSettings().change_owner('dbadmins'))

    return database_path


# python sdk does not legally expose login method, so that is a crude way
# to get auth-token in exchange to credentials
def login_user(endpoint, database, user, password):
    driver_config = ydb.DriverConfig(endpoint, database)
    credentials = ydb.StaticCredentials(driver_config, user, password)
    return credentials._make_token_request()['access_token']


def test_ordinaryuser_can_change_password_for_himself(ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client):
    database_path = prepared_tenant_db

    user_auth_token = login_user(ydb_endpoint, database_path, 'ordinaryuser', '1234')
    credentials = ydb.AuthTokenCredentials(user_auth_token)

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("alter user ordinaryuser password '4321'")

    user_auth_token = login_user(ydb_endpoint, database_path, 'ordinaryuser', '4321')
