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
                with pool.checkout() as session:
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


class TestUserAdministration:
    def set_db_owner(self, root_db, current_db, owner):
        root_admin_creds = root_db.get_user_creds(root_db.builtin_users.root)
        driver_config = current_db.create_driver_config(root_admin_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            YDB.change_db_owner(driver, owner)

    def set_db_subject_permissions(self, root_db, current_db, subject_login, subject_permissions):
        root_admin_creds = root_db.get_user_creds(root_db.builtin_users.root)
        driver_config = current_db.create_driver_config(root_admin_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            YDB.change_db_permissions(driver, subject_login, subject_permissions)

    def user_administration_action_wrapper(self, expected_result, func, *args):
        if not expected_result:
            with pytest.raises(ydb.issues.Error) as exc_info:
                func(*args)

            assert exc_info.type is ydb.issues.Unauthorized
            assert 'Access denied' in exc_info.value.message
        else:
            func(*args)

    def check_users_managment(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['enable_strict_user_management']:
            if test_params['subject'] == CLUSTER_ADMIN:
                expected_result = True
            elif test_params['subject'] == DB_ADMIN:
                if test_params['enable_database_admin']:
                    expected_result = True

        else:
            permissions = test_params['subject_permissions']
            if DB_ALTER_SCHEMA_PERMISSION in permissions and DB_CONNECT_PERMISSION in permissions:
                expected_result = True
            elif test_params['subject'] == DB_ADMIN:
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                self.user_administration_action_wrapper(expected_result, YDB.create_user, session, current_db.users.new_user)
                self.user_administration_action_wrapper(expected_result, YDB.modify_user, session, current_db.users.existing_user)
                self.user_administration_action_wrapper(expected_result, YDB.drop_user, session, current_db.users.existing_user)

    def check_local_user_can_change_own_password(self, auth_db, subject_login):
        expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = auth_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                self.user_administration_action_wrapper(expected_result, YDB.modify_user, session, subject_login)

        new_subject_creds = auth_db.get_user_creds(subject_login, 'passwd1235')
        driver_config = auth_db.create_driver_config(new_subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                self.user_administration_action_wrapper(expected_result, YDB.modify_user, session, subject_login, True)

    def check_changing_db_owner(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['subject'] == CLUSTER_ADMIN:
            expected_result = True
        elif test_params['subject'] == DB_ADMIN:
            if current_db.database_path == '/Root':
                expected_result = True
            elif not (test_params['enable_strict_user_management'] and test_params['enable_database_admin']):
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            self.user_administration_action_wrapper(expected_result, YDB.change_db_owner, driver, current_db.users.reserve_admin)

    def check_changing_db_owner_group(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['subject'] == CLUSTER_ADMIN:
            expected_result = True
        elif subject_login == current_db.users.db_admin:
            if current_db.database_path == '/Root':
                expected_result = True
            elif not (test_params['enable_strict_user_management'] and test_params['enable_database_admin']):
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            self.user_administration_action_wrapper(expected_result, YDB.change_db_owner, driver, current_db.users.ordinary_user)

    def check_modification_db_owner_group(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['enable_strict_user_management']:
            if test_params['subject'] == CLUSTER_ADMIN:
                expected_result = True

        else:
            permissions = test_params['subject_permissions']
            if DB_ALTER_SCHEMA_PERMISSION in permissions and DB_CONNECT_PERMISSION in permissions:
                expected_result = True
            elif subject_login == current_db.users.db_admin:
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                self.user_administration_action_wrapper(expected_result, YDB.add_user_to_group, session, current_db.groups.admins,
                                                        current_db.users.ordinary_user)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, current_db.groups.admins,
                                                        current_db.users.db_admin)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, current_db.groups.admins,
                                                        current_db.users.reserve_admin)
                self.user_administration_action_wrapper(expected_result, YDB.add_user_to_group, session, current_db.groups.admins,
                                                        current_db.groups.ordinary)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, current_db.groups.admins,
                                                        current_db.groups.subadmins)
                self.user_administration_action_wrapper(expected_result, YDB.rename_group, session, current_db.groups.admins,
                                                        current_db.groups.renamed_admins)
                if expected_result:
                    self.user_administration_action_wrapper(expected_result, YDB.rename_group, session, current_db.groups.renamed_admins,
                                                            current_db.groups.admins)
                if not expected_result:
                    self.user_administration_action_wrapper(expected_result, YDB.drop_group, session, current_db.groups.admins)

    def proceed_users_management_test(self, root_db, current_db, subject, local_domain, subject_permissions, test_params):
        users = current_db.users if local_domain else current_db.builtin_users
        subject_login = getattr(users, subject)
        db_owner = users.db_admin
        self.set_db_owner(root_db, current_db, db_owner)
        self.set_db_subject_permissions(root_db, current_db, subject_login, subject_permissions)

        subject_auth_db = root_db if subject == CLUSTER_ADMIN else current_db
        self.check_users_managment(subject_auth_db, current_db, subject_login, test_params)

        if local_domain:
            self.check_local_user_can_change_own_password(subject_auth_db, subject_login)

    def proceed_db_owners_modification_test(self, root_db, current_db, subject, local_domain, subject_permissions, test_params):
        users = current_db.users if local_domain else current_db.builtin_users
        subject_login = getattr(users, subject)
        db_owner = users.db_admin
        self.set_db_owner(root_db, current_db, db_owner)
        self.set_db_subject_permissions(root_db, current_db, subject_login, subject_permissions)

        subject_auth_db = root_db if subject == CLUSTER_ADMIN else current_db
        self.check_changing_db_owner(subject_auth_db, current_db, subject_login, test_params)

        if local_domain:
            self.set_db_owner(root_db, current_db, current_db.groups.admins)
            self.check_changing_db_owner_group(subject_auth_db, current_db, subject_login, test_params)

        self.set_db_owner(root_db, current_db, current_db.groups.admins)
        self.check_modification_db_owner_group(subject_auth_db, current_db, subject_login, test_params)

    @pytest.mark.parametrize('subject_permissions', POSSIBLE_PERMISSIONS_SETS, ids=['no_permissions', 'connect_permissions', 'alter_permissions'])
    @pytest.mark.parametrize('subject', [CLUSTER_ADMIN, DB_ADMIN, ORDINARY_USER])
    @pytest.mark.parametrize('local_domain', [True, False], ids=['local', 'builtin'])
    def test_root_db(self, request, prepared_root_db, subject, local_domain, subject_permissions):
        self.proceed_users_management_test(prepared_root_db, prepared_root_db, subject, local_domain, subject_permissions,
                                           request.node.callspec.params)
        self.proceed_db_owners_modification_test(prepared_root_db, prepared_root_db, subject, local_domain, subject_permissions,
                                                 request.node.callspec.params)

    @pytest.mark.parametrize('subject_permissions', POSSIBLE_PERMISSIONS_SETS, ids=['no_permissions', 'connect_permissions', 'alter_permissions'])
    @pytest.mark.parametrize('subject', [CLUSTER_ADMIN, DB_ADMIN, ORDINARY_USER])
    @pytest.mark.parametrize('local_domain', [True, False], ids=['local', 'builtin'])
    def test_tenant_db(self, request, prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions):
        self.proceed_users_management_test(prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions,
                                           request.node.callspec.params)

        self.proceed_db_owners_modification_test(prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions,
                                                 request.node.callspec.params)


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
                with pool.checkout() as session:
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
                with pool.checkout() as session:
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
