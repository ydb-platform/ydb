# -*- coding: utf-8 -*-
import copy
import logging
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

CLUSTER_ADMIN = 'cluster_admin'
DB_ADMIN = 'db_admin'
ORDINARY_USER = 'ordinary_user'

DB_CONNECT_PERMISSION = 'ydb.database.connect'
DB_ALTER_SCHEMA_PERMISSION = 'ydb.granular.alter_schema'

POSSIBLE_PERMISSIONS_SETS = [
    [],
    [DB_CONNECT_PERMISSION],
    [DB_CONNECT_PERMISSION, DB_ALTER_SCHEMA_PERMISSION]
]


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
    ],
    # do not clutter logs with resource pools auto creation
    enable_resource_pools=False,
    enforce_user_token_requirement=True,
)


@pytest.fixture(scope='module')
def builtin_users():
    # Builtin identities need no CREATE USER; root also bootstraps the cluster.
    return SimpleNamespace(
        root='root@builtin',
        cluster_admin='cluster_admin@builtin',
        db_admin='db_admin@builtin',
        ordinary_user='ordinary_user@builtin',
    )


@pytest.fixture(scope='module')
def cluster_admin_login():
    # The SID must be configured before the local user can be created in /Root.
    return 'clusteradmin'


@pytest.fixture(scope='module', params=[True, False], ids=['enabled_enforce_user_token_requirement', 'disabled_enforce_user_token_requirement'])
def enforce_user_token_requirement(request):
    return request.param


@pytest.fixture(scope='module', params=[True, False], ids=['enabled_strict_user_management', 'disabled_strict_user_management'])
def enable_strict_user_management(request):
    return request.param


@pytest.fixture(scope='module', params=[True, False], ids=['enabled_database_admin', 'disabled_database_admin'])
def enable_database_admin(request):
    return request.param


# ydb_fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration(enforce_user_token_requirement, enable_strict_user_management, enable_database_admin, builtin_users):
    conf = copy.deepcopy(CLUSTER_CONFIG)
    conf['default_clusteradmin'] = builtin_users.root
    conf['enforce_user_token_requirement'] = enforce_user_token_requirement
    if enable_strict_user_management:
        conf['extra_feature_flags'].append('enable_strict_user_management')
    if enable_database_admin:
        conf['extra_feature_flags'].append('enable_database_admin')
    return conf


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration, builtin_users, cluster_admin_login):
    config_generator = KikimrConfigGenerator(**ydb_cluster_configuration)
    config_generator.yaml_config['auth_config'] = {
        'domain_login_only': False,
    }
    config_generator.yaml_config['domains_config']['disable_builtin_security'] = True
    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['administration_allowed_sids'].append(builtin_users.cluster_admin)
    security_config['administration_allowed_sids'].append(cluster_admin_login)
    return config_generator


class YDB:
    def __init__(self, endpoint, database_path, users, groups, builtin_users):
        self.endpoint = endpoint
        self.database_path = database_path
        self.users = users
        self.groups = groups
        self.builtin_users = builtin_users

    def create_driver_config(self, creds=None):
        return ydb.DriverConfig(self.endpoint, self.database_path, credentials=creds)

    # python sdk does not legally expose login method, so that is a crude way
    # to get auth-token in exchange to credentials
    def login_user(self, user, password):
        driver_config = self.create_driver_config()
        credentials = ydb.StaticCredentials(driver_config, user, password)
        return credentials._make_token_request()['access_token']

    def get_user_token(self, user, password='passwd1234'):
        if user.endswith('@builtin'):
            return user
        else:
            return self.login_user(user, password)

    def get_user_creds(self, user, password='passwd1234'):
        user_token = self.get_user_token(user, password)
        return ydb.AuthTokenCredentials(user_token)

    @staticmethod
    def create_user(session, username):
        session.execute_scheme(f"create user {username} password 'passwd1234'")

    @staticmethod
    def drop_user(session, username, if_exist=False):
        if_exist_statement = 'if exists ' if if_exist else ''
        session.execute_scheme(f"drop user {if_exist_statement}{username}")

    @staticmethod
    def modify_user(session, username, reset_password=False):
        password = 'passwd1234' if reset_password else 'passwd1235'
        session.execute_scheme(f"alter user {username} PASSWORD '{password}'")

    @staticmethod
    def create_group(session, groupname):
        session.execute_scheme(f"create group {groupname}")

    @staticmethod
    def drop_group(session, groupname, if_exist=False):
        if_exist_statement = 'if exists ' if if_exist else ''
        session.execute_scheme(f"drop group {if_exist_statement}{groupname}")

    @staticmethod
    def rename_group(session, groupname, new_groupname):
        session.execute_scheme(f"alter group {groupname} rename to {new_groupname}")

    @staticmethod
    def add_user_to_group(session, groupname, username):
        session.execute_scheme(f"alter group {groupname} add user {username}")

    @staticmethod
    def delete_user_from_group(session, groupname, username):
        session.execute_scheme(f"alter group {groupname} drop user {username}")

    @staticmethod
    def change_db_owner(driver, new_owner):
        driver.scheme_client.modify_permissions(
            driver._driver_config.database,
            ydb.ModifyPermissionsSettings().change_owner(new_owner))

    @staticmethod
    def change_db_permissions(driver, user, new_permissions):
        driver.scheme_client.modify_permissions(
            driver._driver_config.database,
            ydb.ModifyPermissionsSettings().grant_permissions(user, new_permissions))

    @staticmethod
    def clear_db_permissions(driver):
        driver.scheme_client.modify_permissions(
            driver._driver_config.database,
            ydb.ModifyPermissionsSettings().clear_permissions())


@contextmanager
def session_with_sync_cleanup(pool):
    with pool.checkout() as session:
        try:
            yield session
        finally:
            # Pool.stop() deletes sessions asynchronously. Finish deletion here
            # so no deletion callbacks outlive fixture teardown.
            if session.initialized():
                session.delete()


@pytest.fixture
def prepared_database(ydb_endpoint, builtin_users, cluster_admin_login):
    @contextmanager
    def prepare(database_path, root_db=False):
        users = SimpleNamespace(
            cluster_admin=cluster_admin_login,
            db_admin='dbadmin',
            reserve_admin='dbadmin2',
            ordinary_user='ordinaryuser',
            existing_user='testuser1',
            new_user='testuser2',
        )
        groups = SimpleNamespace(
            admins='dbadmins',
            subadmins='dbsubadmins',
            ordinary='ordinarygroup',
            renamed_admins='dbadminsdemoted',
        )
        db = YDB(ydb_endpoint, database_path, users, groups, builtin_users)
        credentials = db.get_user_creds(builtin_users.root)
        with ydb.Driver(db.create_driver_config(credentials)) as driver:
            driver.wait()
            with ydb.SessionPool(driver) as pool:
                with session_with_sync_cleanup(pool) as session:
                    try:
                        if root_db:
                            db.create_user(session, users.cluster_admin)
                        db.create_user(session, users.db_admin)
                        db.create_user(session, users.ordinary_user)
                        db.create_user(session, users.existing_user)
                        db.create_user(session, users.reserve_admin)

                        db.create_group(session, groups.admins)
                        db.create_group(session, groups.ordinary)
                        db.create_group(session, groups.subadmins)
                        db.add_user_to_group(session, groups.admins, users.db_admin)
                        db.add_user_to_group(session, groups.admins, users.reserve_admin)
                        db.add_user_to_group(session, groups.admins, groups.subadmins)
                        yield db
                    finally:
                        db.change_db_owner(driver, builtin_users.root)
                        db.clear_db_permissions(driver)
                        if root_db:
                            db.drop_user(session, users.cluster_admin, if_exist=True)
                        db.drop_user(session, users.db_admin, if_exist=True)
                        db.drop_user(session, users.ordinary_user, if_exist=True)
                        db.drop_user(session, users.existing_user, if_exist=True)
                        db.drop_user(session, users.new_user, if_exist=True)
                        db.drop_group(session, groups.ordinary, if_exist=True)
                        db.drop_group(session, groups.subadmins, if_exist=True)
                        db.drop_group(session, groups.admins, if_exist=True)
                        db.drop_group(session, groups.renamed_admins, if_exist=True)
                        db.drop_user(session, users.reserve_admin, if_exist=True)

    return prepare


@pytest.fixture
def prepared_root_db(prepared_database, ydb_root):
    with prepared_database(ydb_root, root_db=True) as db:
        yield db


@pytest.fixture
def prepared_tenant_db(prepared_database, ydb_database_module_scope):
    with prepared_database(ydb_database_module_scope) as db:
        yield db


@contextmanager
def expect_access(allowed):
    if allowed:
        yield
    else:
        with pytest.raises(ydb.issues.Error) as exc_info:
            yield
        assert exc_info.type is ydb.issues.Unauthorized
        assert 'Access denied' in exc_info.value.message


@pytest.mark.parametrize('subject_permissions', POSSIBLE_PERMISSIONS_SETS,
                         ids=['no_permissions', 'connect_permissions', 'alter_permissions'])
@pytest.mark.parametrize('subject', [CLUSTER_ADMIN, DB_ADMIN, ORDINARY_USER])
class TestUserAdministration:
    @pytest.fixture(params=[True, False], ids=['local', 'builtin'])
    def local_domain(self, request):
        return request.param

    @pytest.fixture(params=['root', 'tenant'])
    def current_db(self, request, prepared_root_db):
        if request.param == 'root':
            return prepared_root_db
        return request.getfixturevalue('prepared_tenant_db')

    @pytest.fixture
    def subject_user(self, current_db, prepared_root_db, subject, local_domain):
        users = current_db.users if local_domain else current_db.builtin_users
        return SimpleNamespace(
            name=getattr(users, subject),
            auth_db=prepared_root_db if subject == CLUSTER_ADMIN else current_db,
        )

    @pytest.fixture(params=['user'])
    def database_owner(self, request, current_db, local_domain):
        if request.param == 'group':
            return current_db.groups.admins
        users = current_db.users if local_domain else current_db.builtin_users
        return users.db_admin

    @pytest.fixture
    def configured_db(self, current_db, database_owner, subject_user, subject_permissions, ydb_client):
        credentials = current_db.get_user_creds(current_db.builtin_users.root)
        driver = ydb_client(current_db.database_path, credentials=credentials)
        driver.wait()
        YDB.change_db_owner(driver, database_owner)
        YDB.change_db_permissions(driver, subject_user.name, subject_permissions)
        return current_db

    @pytest.fixture
    def subject_driver(self, configured_db, subject_user, ydb_client):
        credentials = subject_user.auth_db.get_user_creds(subject_user.name)
        driver = ydb_client(configured_db.database_path, credentials=credentials)
        driver.wait()
        return driver

    @pytest.fixture
    def subject_session(self, subject_driver):
        with ydb.SessionPool(subject_driver) as pool:
            with session_with_sync_cleanup(pool) as session:
                yield session

    @pytest.fixture
    def login_session(self, configured_db, subject_user, ydb_client):
        @contextmanager
        def connect(password='passwd1234'):
            # Cluster administrators change their own password in /Root,
            # even when the target of the administration test is a tenant.
            db = subject_user.auth_db
            credentials = db.get_user_creds(subject_user.name, password)
            driver = ydb_client(db.database_path, credentials=credentials)
            driver.wait()
            with ydb.SessionPool(driver) as pool:
                with session_with_sync_cleanup(pool) as session:
                    yield session

        return connect

    @pytest.fixture
    def can_manage_users(self, subject, subject_permissions, enable_strict_user_management, enable_database_admin):
        if enable_strict_user_management:
            return subject == CLUSTER_ADMIN or (subject == DB_ADMIN and enable_database_admin)
        return subject == DB_ADMIN or (
            DB_CONNECT_PERMISSION in subject_permissions and DB_ALTER_SCHEMA_PERMISSION in subject_permissions
        )

    @pytest.fixture
    def can_change_owner(self, subject, current_db, ydb_root, enable_strict_user_management, enable_database_admin):
        if subject == CLUSTER_ADMIN:
            return True
        return subject == DB_ADMIN and (
            current_db.database_path == ydb_root
            or not (enable_strict_user_management and enable_database_admin)
        )

    @pytest.fixture
    def can_modify_owner_group(self, subject, local_domain, subject_permissions, enable_strict_user_management):
        if enable_strict_user_management:
            return subject == CLUSTER_ADMIN
        return (subject == DB_ADMIN and local_domain) or (
            DB_CONNECT_PERMISSION in subject_permissions and DB_ALTER_SCHEMA_PERMISSION in subject_permissions
        )

    def test_manage_users(self, configured_db, subject_session, can_manage_users):
        users = configured_db.users
        with expect_access(can_manage_users):
            YDB.create_user(subject_session, users.new_user)
        with expect_access(can_manage_users):
            YDB.modify_user(subject_session, users.existing_user)
        with expect_access(can_manage_users):
            YDB.drop_user(subject_session, users.existing_user)

    @pytest.mark.parametrize('local_domain', [True], ids=['local'])
    def test_change_own_password(self, subject_user, login_session):
        with login_session() as session:
            YDB.modify_user(session, subject_user.name)
        with login_session(password='passwd1235') as session:
            YDB.modify_user(session, subject_user.name, reset_password=True)

    def test_change_database_owner(self, configured_db, subject_driver, can_change_owner):
        with expect_access(can_change_owner):
            YDB.change_db_owner(subject_driver, configured_db.users.reserve_admin)

    @pytest.mark.parametrize('local_domain', [True], ids=['local'])
    @pytest.mark.parametrize('database_owner', ['group'], indirect=True)
    def test_change_database_owner_from_group(self, configured_db, subject_driver, can_change_owner):
        with expect_access(can_change_owner):
            YDB.change_db_owner(subject_driver, configured_db.users.ordinary_user)

    @pytest.mark.parametrize('database_owner', ['group'], indirect=True)
    def test_modify_database_owner_group(self, configured_db, subject_session, can_modify_owner_group):
        users, groups = configured_db.users, configured_db.groups
        with expect_access(can_modify_owner_group):
            YDB.add_user_to_group(subject_session, groups.admins, users.ordinary_user)
        with expect_access(can_modify_owner_group):
            YDB.delete_user_from_group(subject_session, groups.admins, users.db_admin)
        with expect_access(can_modify_owner_group):
            YDB.delete_user_from_group(subject_session, groups.admins, users.reserve_admin)
        with expect_access(can_modify_owner_group):
            YDB.add_user_to_group(subject_session, groups.admins, groups.ordinary)
        with expect_access(can_modify_owner_group):
            YDB.delete_user_from_group(subject_session, groups.admins, groups.subadmins)
        with expect_access(can_modify_owner_group):
            YDB.rename_group(subject_session, groups.admins, groups.renamed_admins)
        if can_modify_owner_group:
            YDB.rename_group(subject_session, groups.renamed_admins, groups.admins)
        else:
            with expect_access(can_modify_owner_group):
                YDB.drop_group(subject_session, groups.admins)


@pytest.mark.parametrize('enforce_user_token_requirement', [True], indirect=True)
@pytest.mark.parametrize('enable_strict_user_management', [True], indirect=True)
@pytest.mark.parametrize('enable_database_admin', [True], indirect=True)
class TestTenantDatabaseAdmin:
    # Preserve main's group-owner regressions alongside the broader flag matrix.
    @pytest.fixture
    def prepared_admin_tenant_db(self, prepared_root_db, prepared_tenant_db):
        db = prepared_tenant_db
        db.users.blocked_admin = 'dbadmin3'
        db.users.password_admin = 'dbadmin4'
        credentials = db.get_user_creds(db.builtin_users.root)
        with ydb.Driver(db.create_driver_config(credentials)) as driver:
            driver.wait()
            with ydb.SessionPool(driver) as pool:
                with session_with_sync_cleanup(pool) as session:
                    try:
                        session.execute_scheme(f"create user {db.users.blocked_admin} password 'passwd1234' nologin")
                        YDB.create_user(session, db.users.password_admin)
                        YDB.add_user_to_group(session, db.groups.admins, db.users.blocked_admin)
                        YDB.add_user_to_group(session, db.groups.admins, db.users.password_admin)
                        YDB.change_db_owner(driver, db.groups.admins)
                        yield db
                    finally:
                        YDB.drop_user(session, db.users.blocked_admin, if_exist=True)
                        YDB.drop_user(session, db.users.password_admin, if_exist=True)

    @pytest.fixture
    def tenant_users(self, prepared_admin_tenant_db):
        return prepared_admin_tenant_db.users

    @pytest.fixture
    def tenant_groups(self, prepared_admin_tenant_db):
        return prepared_admin_tenant_db.groups

    @pytest.fixture
    def tenant_user_driver(self, prepared_admin_tenant_db, ydb_client):
        def connect(username, password='passwd1234'):
            db = prepared_admin_tenant_db
            driver = ydb_client(db.database_path, credentials=db.get_user_creds(username, password))
            driver.wait()
            return driver

        return connect

    @pytest.fixture
    def tenant_user_session(self, tenant_user_driver):
        @contextmanager
        def connect(username, password='passwd1234'):
            driver = tenant_user_driver(username, password)
            with ydb.SessionPool(driver) as pool:
                with session_with_sync_cleanup(pool) as session:
                    yield session

        return connect

    @pytest.fixture
    def db_admin_driver(self, tenant_user_driver, tenant_users):
        return tenant_user_driver(tenant_users.db_admin)

    @pytest.fixture
    def db_admin_session(self, tenant_user_session, tenant_users):
        with tenant_user_session(tenant_users.db_admin) as session:
            yield session

    @pytest.fixture
    def subject_user(self, request, tenant_users, tenant_user_session):
        name = getattr(tenant_users, request.param)
        with tenant_user_session(name) as session:
            yield SimpleNamespace(name=name, session=session)

    @pytest.mark.parametrize('subject_user, can_set_hash', [
        pytest.param('ordinary_user', False, id='ordinaryuser'),
        pytest.param('password_admin', True, id='dbadmin'),
    ], indirect=['subject_user'])
    def test_user_can_change_password_for_himself(self, tenant_user_session, subject_user, can_set_hash):
        subject_user.session.execute_scheme(f"alter user {subject_user.name} password '4321'")

        with tenant_user_session(subject_user.name, password='4321') as session:
            password_hash = """{
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                "type": "argon2id"
            }"""

            if can_set_hash:
                session.execute_scheme(f"alter user {subject_user.name} hash '{password_hash}'")
            else:
                with pytest.raises(ydb.issues.Unauthorized) as exc_info:
                    session.execute_scheme(f"alter user {subject_user.name} hash '{password_hash}'")
                assert 'Access denied for' in exc_info.value.message

    def test_database_admin_cant_change_database_owner(self, db_admin_driver, tenant_users):
        with pytest.raises(ydb.issues.Error) as exc_info:
            YDB.change_db_owner(db_admin_driver, tenant_users.ordinary_user)

        assert exc_info.type is ydb.issues.Unauthorized
        assert f'Access denied for {tenant_users.db_admin}' in exc_info.value.message

    @pytest.mark.parametrize('query', [
        pytest.param('alter group {groups.admins} add user {users.ordinary_user}', id='add-user'),
        pytest.param('alter group {groups.admins} drop user {users.db_admin}', id='remove-himself'),
        pytest.param('alter group {groups.admins} drop user {users.reserve_admin}', id='remove-other-admin'),
        pytest.param('alter group {groups.admins} add user {groups.ordinary}', id='add-subgroup'),
        pytest.param('alter group {groups.admins} drop user {groups.subadmins}', id='remove-subgroup'),
        pytest.param('drop group {groups.admins}', id='remove-admin-group'),
        pytest.param('alter group {groups.admins} rename to {groups.renamed_admins}', id='rename-admin-group'),
    ])
    def test_database_admin_cant_change_database_admin_group(self, db_admin_session, tenant_users, tenant_groups, query):
        with pytest.raises(ydb.issues.Error) as exc_info:
            db_admin_session.execute_scheme(query.format(users=tenant_users, groups=tenant_groups))

        assert exc_info.type is ydb.issues.Unauthorized
        assert 'Access denied.' in exc_info.value.message

    @pytest.mark.parametrize('query', [
        pytest.param('alter user {users.reserve_admin} password "4321"', id='change-password'),
        pytest.param('alter user {users.reserve_admin} nologin', id='block'),
        pytest.param('alter user {users.blocked_admin} login', id='unblock'),
    ])
    def test_database_admin_cant_change_database_admin_user(self, db_admin_session, tenant_users, query):
        with pytest.raises(ydb.issues.Error) as exc_info:
            db_admin_session.execute_scheme(query.format(users=tenant_users))

        assert exc_info.type is ydb.issues.Unauthorized
        logger.debug(exc_info.value.message)
        assert 'Access denied.' in exc_info.value.message

    def test_database_admin_can_create_user(self, db_admin_session, tenant_users):
        YDB.create_user(db_admin_session, tenant_users.new_user)
