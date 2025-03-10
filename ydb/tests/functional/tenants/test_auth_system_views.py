# -*- coding: utf-8 -*-
import logging
import copy

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'TX_PROXY': LogLevels.DEBUG,
        'GRPC_SERVER': LogLevels.INFO,
        'GRPC_PROXY_NO_CONNECT_ACCESS': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.INFO,
        'SYSTEM_VIEWS': LogLevels.DEBUG,
        'TICKET_PARSER': LogLevels.DEBUG,
        # less logs
        'KQP_PROXY': LogLevels.CRIT,
        'KQP_WORKER': LogLevels.CRIT,
        'KQP_GATEWAY': LogLevels.CRIT,
        'GRPC_PROXY': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,
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


# fixtures.ydb_cluster_configuration local override
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


def stream_query_result(driver, query):
    it = driver.table_client.scan_query(query)
    result = []
    error = None

    while True:
        try:
            response = next(it)
        except StopIteration:
            break
        except ydb.issues.Error as e:
            error = e
            break

        for row in response.result_set.rows:
            result.append(row)

    return result, error


@pytest.fixture(scope='module')
def prepared_root_db(ydb_cluster, ydb_root, ydb_endpoint):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    # prepare root database
    driver_config = ydb.DriverConfig(ydb_endpoint, ydb_root, credentials=cluster_admin)
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("create user clusteradmin password '1234'")
            session.execute_scheme("create user clusteruser password '1234'")


@pytest.fixture(scope='module')
def prepared_tenant_db(ydb_cluster, ydb_endpoint, ydb_database_module_scope):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    # prepare tenant database
    database_path = ydb_database_module_scope
    driver_config = ydb.DriverConfig(ydb_endpoint, database_path, credentials=cluster_admin)
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            # add users
            session.execute_scheme("create user dbadmin password '1234'")
            session.execute_scheme("create user ordinaryuser password '1234'")

            # add group and its members
            session.execute_scheme('create group dbadmins')
            session.execute_scheme('alter group dbadmins add user dbadmin')

            # add required permissions on paths
            session.execute_scheme(f'grant "ydb.generic.use" on `{database_path}` to clusteradmin')
            # shouldn't be possible but we allow that for the admin
            session.execute_scheme(f'grant "ydb.generic.use" on `{database_path}` to clusteruser')
            session.execute_scheme(f'grant "ydb.generic.use" on `{database_path}` to dbadmin')
            session.execute_scheme(f'grant "ydb.generic.use" on `{database_path}` to ordinaryuser')

        # make dbadmin the real admin of the database
        driver.scheme_client.modify_permissions(database_path, ydb.ModifyPermissionsSettings().change_owner('dbadmin'))

    return database_path


# python sdk does not legally expose login method, so that is a crude way
# to get auth-token in exchange to credentials
def login_user(endpoint, database, user, password):
    driver_config = ydb.DriverConfig(endpoint, database)
    credentials = ydb.StaticCredentials(driver_config, user, password)
    return credentials._make_token_request()['access_token']


@pytest.mark.parametrize('user,expected_access', [
    ('clusteradmin', True),
    ('clusteruser', False),
    ('dbadmin', True),
    ('ordinaryuser', False)
])
def test_tenant_auth_groups_access(ydb_endpoint, ydb_root, prepared_root_db, prepared_tenant_db, ydb_client, user, expected_access):
    tenant_database = prepared_tenant_db

    # user could be either from the root or tenant database,
    # but they must obtain auth token by logging in the database they live in
    login_database = ydb_root if user.startswith('cluster') else tenant_database
    user_auth_token = login_user(ydb_endpoint, login_database, user, '1234')
    credentials = ydb.AuthTokenCredentials(user_auth_token)

    with ydb_client(tenant_database, credentials=credentials) as driver:
        driver.wait()

        result, error = stream_query_result(driver, f'SELECT * FROM `{tenant_database}/.sys/auth_groups`')

        # logger.debug('AAA result row count %s, error %s', len(result), error)

        if expected_access:
            assert error is None
            assert len(result) == 1
            assert result[0]['Sid'] == 'dbadmins'

        else:
            assert error is not None, result
            assert error.status == ydb.issues.StatusCode.UNAUTHORIZED
            assert 'Administrator access is required' in error.message
