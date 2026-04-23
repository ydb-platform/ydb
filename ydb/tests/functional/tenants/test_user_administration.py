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
    config_generator = KikimrConfigGenerator(
        protected_mode=True,
        **ydb_cluster_configuration,
    )
    config_generator.yaml_config['auth_config'] = {
        'domain_login_only': False,
        'enable_node_registration_by_token': False,
    }
    config_generator.yaml_config['domains_config']['disable_builtin_security'] = True
    security_config = config_generator.yaml_config['domains_config']['security_config']
    # Restore the test's expected administration_allowed_sids: protected_mode
    # widens this to include "DATABASE-ADMINS"/"ADMINS", which would let
    # dbadmin perform cluster-admin actions and break the access-denial tests.
    # Keep only the cluster bootstrap admins (root, root@builtin, clusteradmin)
    # and the cert-authenticated cluster admin SID (used for slot bootstrap).
    security_config['administration_allowed_sids'] = [
        'root', 'root@builtin', 'clusteradmin', 'clusteradmins@cert',
    ]
    # Allow mTLS-authenticated cluster admins (clusteradmins@cert) to register
    # dynamic nodes when token-based registration is disabled. Grant ONLY
    # node-registration rights (not full admin), so ordinary user/admin access
    # checks in tests stay meaningful.
    security_config['register_dynamic_node_allowed_sids'] = ['clusteradmins@cert']
    # protected_mode=True strips default_users; combined with disable_builtin_security
    # this means no root user is auto-created, but the harness `_get_token` flow
    # invokes `ydb auth get-token --user root --no-password` to fetch a session
    # token at cluster start. Re-add a root user with empty password so login works.
    security_config['default_users'] = [{'name': 'root', 'password': ''}]
    config_generator.full_config = config_generator.yaml_config
    return config_generator


def _ydb_tls_kwargs(cluster, with_client_cert=True):
    """Return DriverConfig kwargs for connecting to a TLS cluster.

    When ``with_client_cert`` is True (default), the client certificate and
    private key are included — this maps the connection to the
    ``clusteradmins@cert`` SID via the cert authorization rules. Use that for
    cluster-admin and bootstrap connections only.

    For tests that connect as a regular user (login + password), pass
    ``with_client_cert=False`` so we only set the CA root for TLS validation
    without claiming any cert-based identity.
    """
    cfg = cluster.config
    if not getattr(cfg, 'grpc_ssl_enable', False):
        return {}
    with open(cfg.grpc_tls_ca_path, 'rb') as f:
        root = f.read()
    kwargs = {'root_certificates': root}
    if with_client_cert:
        with open(cfg.grpc_tls_cert_path, 'rb') as f:
            cert = f.read()
        with open(cfg.grpc_tls_key_path, 'rb') as f:
            key = f.read()
        kwargs['certificate_chain'] = cert
        kwargs['private_key'] = key
    return kwargs


# override the shared ydb_endpoint fixture to switch to grpcs:// when protected_mode is on
@pytest.fixture(scope='module')
def ydb_endpoint(ydb_cluster):
    node = ydb_cluster.nodes[1]
    if getattr(ydb_cluster.config, 'grpc_ssl_enable', False):
        return "grpcs://%s:%s" % (node.host, node.grpc_ssl_port)
    return "%s:%s" % (node.host, node.port)


# override the shared ydb_client fixture to inject TLS material for mTLS clusters
@pytest.fixture(scope='function')
def ydb_client(ydb_cluster, ydb_endpoint, request):
    # User-level connections: TLS validation only, no client cert (so the
    # connection identity is not auto-elevated to clusteradmins@cert).
    tls_kwargs = _ydb_tls_kwargs(ydb_cluster, with_client_cert=False)

    def _make_driver(database_path, **kwargs):
        merged = dict(tls_kwargs)
        merged.update(kwargs)
        driver_config = ydb.DriverConfig(ydb_endpoint, database_path, **merged)
        driver = ydb.Driver(driver_config)

        def stop_driver():
            driver.stop()

        request.addfinalizer(stop_driver)
        return driver

    return _make_driver


@pytest.fixture(scope='module')
def prepared_root_db(ydb_cluster, ydb_root, ydb_endpoint):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    # prepare root database
    driver_config = ydb.DriverConfig(
        ydb_endpoint, ydb_root, credentials=cluster_admin,
        **_ydb_tls_kwargs(ydb_cluster),
    )
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("create user clusteradmin password '1234'")


@pytest.fixture(scope='module')
def prepared_tenant_db(ydb_cluster, ydb_endpoint, ydb_database_module_scope):
    cluster_admin = ydb.AuthTokenCredentials(ydb_cluster.config.default_clusteradmin)

    database_path = ydb_database_module_scope
    driver_config = ydb.DriverConfig(
        ydb_endpoint, database_path, credentials=cluster_admin,
        **_ydb_tls_kwargs(ydb_cluster),
    )
    with ydb.Driver(driver_config) as driver:
        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("create user dbadmin password '1234'")
            session.execute_scheme("create user dbadmin2 password '1234'")
            session.execute_scheme("create user dbadmin3 password '1234' nologin")
            session.execute_scheme("create user dbadmin4 password '1234'")
            session.execute_scheme("create user ordinaryuser password '1234'")
            session.execute_scheme("create group dbadmins")
            session.execute_scheme("create group dbsubadmins")
            session.execute_scheme("create group ordinarygroup")
            session.execute_scheme("alter group dbadmins add user dbadmin")
            session.execute_scheme("alter group dbadmins add user dbadmin2, dbadmin3, dbadmin4, dbsubadmins")
            session.execute_scheme("grant \"ydb.generic.use\" on `{}` to dbadmin".format(database_path))
            session.execute_scheme("grant \"ydb.generic.use\" on `{}` to dbadmin2".format(database_path))
            session.execute_scheme("grant \"ydb.generic.use\" on `{}` to dbadmin3".format(database_path))
            session.execute_scheme("grant \"ydb.generic.use\" on `{}` to dbadmin4".format(database_path))
            session.execute_scheme("grant \"ydb.generic.use\" on `{}` to ordinaryuser".format(database_path))

        driver.scheme_client.modify_permissions(
            database_path,
            ydb.ModifyPermissionsSettings().change_owner('dbadmins')
        )

    return database_path


def login_user_credentials(endpoint, database, user, password, tls_kwargs=None):
    driver_config = ydb.DriverConfig(endpoint, database, **(tls_kwargs or {}))
    return ydb.StaticCredentials(driver_config, user, password)


@pytest.mark.parametrize('subject_user', [
    'ordinaryuser',
    pytest.param('dbadmin4', id='dbadmin')
])
def test_user_can_change_password_for_himself(ydb_cluster, ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client, subject_user):
    database_path = prepared_tenant_db
    tls_kwargs = _ydb_tls_kwargs(ydb_cluster, with_client_cert=False)

    credentials = login_user_credentials(ydb_endpoint, database_path, subject_user, '1234', tls_kwargs=tls_kwargs)

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme(f"alter user {subject_user} password '4321'")

    credentials = login_user_credentials(ydb_endpoint, database_path, subject_user, '4321', tls_kwargs=tls_kwargs)

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            password_hash = """{
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
            }"""

            try:
                session.execute_scheme(f"alter user {subject_user} hash '{password_hash}'")
                assert subject_user == 'dbadmin4'
            except ydb.issues.Unauthorized as ex:
                assert subject_user == 'ordinaryuser'
                assert 'Access denied for' in ex.message


def test_database_admin_cant_change_database_owner(ydb_cluster, ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client):
    database_path = prepared_tenant_db

    credentials = login_user_credentials(ydb_endpoint, database_path, 'dbadmin', '1234',
                                         tls_kwargs=_ydb_tls_kwargs(ydb_cluster, with_client_cert=False))

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        with pytest.raises(ydb.issues.Error) as exc_info:
            driver.scheme_client.modify_permissions(database_path, ydb.ModifyPermissionsSettings().change_owner('ordinaryuser'))

        assert exc_info.type is ydb.issues.Unauthorized
        assert 'Access denied for dbadmin' in exc_info.value.message


@pytest.mark.parametrize('query', [
    pytest.param('alter group dbadmins add user ordinaryuser', id='add-user'),
    pytest.param('alter group dbadmins drop user dbadmin', id='remove-himself'),
    pytest.param('alter group dbadmins drop user dbadmin2', id='remove-other-admin'),
    pytest.param('alter group dbadmins add user ordinarygroup', id='add-subgroup'),
    pytest.param('alter group dbadmins drop user dbsubadmins', id='remove-subgroup'),
    pytest.param('drop group dbadmins', id='remove-admin-group'),
    pytest.param('alter group dbadmins rename to dbadminsdemoted', id='rename-admin-group'),
])
def test_database_admin_cant_change_database_admin_group(ydb_cluster, ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client, query):
    database_path = prepared_tenant_db

    credentials = login_user_credentials(ydb_endpoint, database_path, 'dbadmin', '1234',
                                         tls_kwargs=_ydb_tls_kwargs(ydb_cluster, with_client_cert=False))

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            with pytest.raises(ydb.issues.Error) as exc_info:
                session.execute_scheme(query)

            assert exc_info.type is ydb.issues.Unauthorized
            assert 'Access denied.' in exc_info.value.message


@pytest.mark.parametrize('query', [
    pytest.param('alter user dbadmin2 password "4321"', id='change-password'),
    pytest.param('alter user dbadmin2 nologin', id='block'),
    pytest.param('alter user dbadmin3 login', id='unblock'),
])
def test_database_admin_cant_change_database_admin_user(ydb_cluster, ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client, query):
    database_path = prepared_tenant_db

    credentials = login_user_credentials(ydb_endpoint, database_path, 'dbadmin', '1234',
                                         tls_kwargs=_ydb_tls_kwargs(ydb_cluster, with_client_cert=False))

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            with pytest.raises(ydb.issues.Error) as exc_info:
                session.execute_scheme(query)

        assert exc_info.type is ydb.issues.Unauthorized
        logger.debug(exc_info.value.message)
        assert 'Access denied.' in exc_info.value.message


def test_database_admin_can_create_user(ydb_cluster, ydb_endpoint, prepared_root_db, prepared_tenant_db, ydb_client):
    database_path = prepared_tenant_db

    credentials = login_user_credentials(ydb_endpoint, database_path, 'dbadmin', '1234',
                                         tls_kwargs=_ydb_tls_kwargs(ydb_cluster, with_client_cert=False))

    with ydb_client(database_path, credentials=credentials) as driver:
        driver.wait()

        pool = ydb.SessionPool(driver)
        with pool.checkout() as session:
            session.execute_scheme("create user testuser password '1234'")
