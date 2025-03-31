# -*- coding: utf-8 -*-
import copy
import logging

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

LOCAL_DOMAIN = 'local'
BULTIN_DOMAIN = 'builtin'

BULTIN_ROOT = 'root@builtin'
BULTIN_CLUSTER_ADMIN = 'cluster_admin@builtin'
BULTIN_DB_ADMIN = 'db_admin@builtin'
BULTIN_ORDINARY_USER = 'ordinary_user@builtin'

LOCAL_CLUSTER_ADMIN = 'clusteradmin'
LOCAL_DB_ADMIN = 'dbadmin'
LOCAL_RESERVE_DB_ADMIN = 'dbadmin2'
LOCAL_ORDINARY_USER = 'ordinaryuser'
LOCAL_TEST_USER_1 = 'testuser1'
LOCAL_TEST_USER_2 = 'testuser2'

CLUSTER_ADMIN = 'cluster_admin'
DB_ADMIN = 'db_admin'
ORDINARY_USER = 'ordinary_user'

BUILTIN_SUBJECT_LOGIN_MAP = {
    CLUSTER_ADMIN: BULTIN_CLUSTER_ADMIN,
    DB_ADMIN: BULTIN_DB_ADMIN,
    ORDINARY_USER: BULTIN_ORDINARY_USER,
}

LOCAL_SUBJECT_LOGIN_MAP = {
    CLUSTER_ADMIN: LOCAL_CLUSTER_ADMIN,
    DB_ADMIN: LOCAL_DB_ADMIN,
    ORDINARY_USER: LOCAL_ORDINARY_USER,
}


def get_subject_login(subject, local=True):
    if local:
        return LOCAL_SUBJECT_LOGIN_MAP[subject]
    else:
        return BUILTIN_SUBJECT_LOGIN_MAP[subject]


DB_ADMINS_GROUP = 'dbadmins'
DB_SUBADMINS_GROUP = 'dbsubadmins'
ORDINARY_GROUP = 'ordinarygroup'

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
    # make all bootstrap and setup under this user
    default_clusteradmin=BULTIN_ROOT,
)

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
def ydb_cluster_configuration(enforce_user_token_requirement, enable_strict_user_management, enable_database_admin):
    conf = copy.deepcopy(CLUSTER_CONFIG)
    conf['enforce_user_token_requirement'] = enforce_user_token_requirement
    if enable_strict_user_management:
        conf['extra_feature_flags'].append('enable_strict_user_management')
    if enable_database_admin:
        conf['extra_feature_flags'].append('enable_database_admin')
    return conf


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration):
    config_generator = KikimrConfigGenerator(**ydb_cluster_configuration)
    config_generator.yaml_config['auth_config'] = {
        'domain_login_only': False,
    }
    config_generator.yaml_config['domains_config']['disable_builtin_security'] = True
    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['administration_allowed_sids'].append(BULTIN_CLUSTER_ADMIN)
    security_config['administration_allowed_sids'].append(LOCAL_CLUSTER_ADMIN)
    return config_generator


class YDB:
    def __init__(self, endpoint, database_path):
        self.endpoint = endpoint
        self.database_path = database_path

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

    def prepare_context_for_tests(self, root_db=False):
        root_admin_creds = self.get_user_creds(BULTIN_ROOT)
        driver_config = self.create_driver_config(root_admin_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                if root_db:
                    self.create_user(session, LOCAL_CLUSTER_ADMIN)

                # for users management tests
                self.create_user(session, LOCAL_DB_ADMIN)
                self.create_user(session, LOCAL_ORDINARY_USER)
                self.create_user(session, LOCAL_TEST_USER_1)

                # for db owners modification tests
                self.create_user(session, LOCAL_RESERVE_DB_ADMIN)

                self.create_group(session, DB_ADMINS_GROUP)
                self.create_group(session, ORDINARY_GROUP)
                self.create_group(session, DB_SUBADMINS_GROUP)

                self.add_user_to_group(session, DB_ADMINS_GROUP, LOCAL_DB_ADMIN)
                self.add_user_to_group(session, DB_ADMINS_GROUP, LOCAL_RESERVE_DB_ADMIN)
                self.add_user_to_group(session, DB_ADMINS_GROUP, DB_SUBADMINS_GROUP)

    def clean_context_after_tests(self, root_db=False):
        root_admin_creds = self.get_user_creds(BULTIN_ROOT)
        driver_config = self.create_driver_config(root_admin_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            self.change_db_owner(driver, BULTIN_ROOT)
            self.clear_db_permissions(driver)

            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                if root_db:
                    self.drop_user(session, LOCAL_CLUSTER_ADMIN)

                # clean entities from users management tests
                self.drop_user(session, LOCAL_DB_ADMIN, if_exist=True)
                self.drop_user(session, LOCAL_ORDINARY_USER, if_exist=True)
                self.drop_user(session, LOCAL_TEST_USER_1, if_exist=True)
                self.drop_user(session, LOCAL_TEST_USER_2, if_exist=True)

                # clean entities from db owners modifications tests
                self.drop_group(session, ORDINARY_GROUP, if_exist=True)
                self.drop_group(session, DB_SUBADMINS_GROUP, if_exist=True)
                self.drop_group(session, DB_ADMINS_GROUP, if_exist=True)

                self.drop_user(session, LOCAL_RESERVE_DB_ADMIN, if_exist=True)


@pytest.fixture(scope='function')
def prepared_root_db(ydb_endpoint, ydb_root):
    db = YDB(ydb_endpoint, ydb_root)
    db.prepare_context_for_tests(True)
    yield db
    db.clean_context_after_tests(True)


@pytest.fixture(scope='function')
def prepared_tenant_db(ydb_endpoint, ydb_database_module_scope):
    db = YDB(ydb_endpoint, ydb_database_module_scope)
    db.prepare_context_for_tests()
    yield db
    db.clean_context_after_tests()

class TestUserAdministration:
    def set_db_owner(self, root_db, current_db, owner):
        root_admin_creds = root_db.get_user_creds(BULTIN_ROOT)
        driver_config = current_db.create_driver_config(root_admin_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            YDB.change_db_owner(driver, owner)

    def set_db_subject_permissions(self, root_db, current_db, subject_login, subject_permissions):
        root_admin_creds = root_db.get_user_creds(BULTIN_ROOT)
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
                self.user_administration_action_wrapper(expected_result, YDB.create_user, session, LOCAL_TEST_USER_2)
                self.user_administration_action_wrapper(expected_result, YDB.modify_user, session, LOCAL_TEST_USER_1)
                self.user_administration_action_wrapper(expected_result, YDB.drop_user, session, LOCAL_TEST_USER_1)

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
            self.user_administration_action_wrapper(expected_result, YDB.change_db_owner, driver, LOCAL_RESERVE_DB_ADMIN)

    def check_changing_db_owner_group(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['subject'] == CLUSTER_ADMIN:
            expected_result = True
        elif subject_login == LOCAL_DB_ADMIN:
            if current_db.database_path == '/Root':
                expected_result = True
            elif not (test_params['enable_strict_user_management'] and test_params['enable_database_admin']):
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            self.user_administration_action_wrapper(expected_result, YDB.change_db_owner, driver, LOCAL_ORDINARY_USER)

    def check_modification_db_owner_group(self, auth_db, current_db, subject_login, test_params):
        expected_result = False
        if test_params['enable_strict_user_management']:
            if test_params['subject'] == CLUSTER_ADMIN:
                expected_result = True

        else:
            permissions = test_params['subject_permissions']
            if DB_ALTER_SCHEMA_PERMISSION in permissions and DB_CONNECT_PERMISSION in permissions:
                expected_result = True
            elif subject_login == LOCAL_DB_ADMIN:
                expected_result = True

        subject_creds = auth_db.get_user_creds(subject_login)
        driver_config = current_db.create_driver_config(subject_creds)
        with ydb.Driver(driver_config) as driver:
            driver.wait()
            pool = ydb.SessionPool(driver)
            with pool.checkout() as session:
                self.user_administration_action_wrapper(expected_result, YDB.add_user_to_group, session, DB_ADMINS_GROUP,
                                                        LOCAL_ORDINARY_USER)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, DB_ADMINS_GROUP,
                                                        LOCAL_DB_ADMIN)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, DB_ADMINS_GROUP,
                                                        LOCAL_RESERVE_DB_ADMIN)
                self.user_administration_action_wrapper(expected_result, YDB.add_user_to_group, session, DB_ADMINS_GROUP,
                                                        ORDINARY_GROUP)
                self.user_administration_action_wrapper(expected_result, YDB.delete_user_from_group, session, DB_ADMINS_GROUP,
                                                        DB_SUBADMINS_GROUP)
                self.user_administration_action_wrapper(expected_result, YDB.rename_group, session, DB_ADMINS_GROUP,
                                                        'dbadminsdemoted')
                if expected_result:
                    self.user_administration_action_wrapper(expected_result, YDB.rename_group, session, 'dbadminsdemoted',
                                                            DB_ADMINS_GROUP)
                if not expected_result:
                    self.user_administration_action_wrapper(expected_result, YDB.drop_group, session, DB_ADMINS_GROUP)

    def proceed_users_management_test(self, root_db, current_db, subject, local_domain, subject_permissions, test_params):
        subject_login = get_subject_login(subject, local_domain)
        db_owner = LOCAL_DB_ADMIN if local_domain else BULTIN_DB_ADMIN
        self.set_db_owner(root_db, current_db, db_owner)
        self.set_db_subject_permissions(root_db, current_db, subject_login, subject_permissions)

        subject_auth_db = root_db if subject == CLUSTER_ADMIN else current_db
        self.check_users_managment(subject_auth_db, current_db, subject_login, test_params)

        if local_domain:
            self.check_local_user_can_change_own_password(subject_auth_db, subject_login)

    def proceed_db_owners_modification_test(self, root_db, current_db, subject, local_domain, subject_permissions, test_params):
        subject_login = get_subject_login(subject, local_domain)
        db_owner = LOCAL_DB_ADMIN if local_domain else BULTIN_DB_ADMIN
        self.set_db_owner(root_db, current_db, db_owner)
        self.set_db_subject_permissions(root_db, current_db, subject_login, subject_permissions)

        subject_auth_db = root_db if subject == CLUSTER_ADMIN else current_db
        self.check_changing_db_owner(subject_auth_db, current_db, subject_login, test_params)

        if local_domain:
            self.set_db_owner(root_db, current_db, DB_ADMINS_GROUP)
            self.check_changing_db_owner_group(subject_auth_db, current_db, subject_login, test_params)

        self.set_db_owner(root_db, current_db, DB_ADMINS_GROUP)
        self.check_modification_db_owner_group(subject_auth_db, current_db, subject_login, test_params)

    @pytest.mark.parametrize('subject_permissions', POSSIBLE_PERMISSIONS_SETS, ids=['no_permissions', 'connect_permissions', 'alter_permissions'])
    @pytest.mark.parametrize('subject', [CLUSTER_ADMIN, DB_ADMIN, ORDINARY_USER])
    @pytest.mark.parametrize('local_domain', [True, False], ids=[LOCAL_DOMAIN, BULTIN_DOMAIN])
    def test_root_db(self, request, prepared_root_db, subject, local_domain, subject_permissions):
        self.proceed_users_management_test(prepared_root_db, prepared_root_db, subject, local_domain, subject_permissions,
                                           request.node.callspec.params)
        self.proceed_db_owners_modification_test(prepared_root_db, prepared_root_db, subject, local_domain, subject_permissions,
                                                 request.node.callspec.params)

    @pytest.mark.parametrize('subject_permissions', POSSIBLE_PERMISSIONS_SETS, ids=['no_permissions', 'connect_permissions', 'alter_permissions'])
    @pytest.mark.parametrize('subject', [CLUSTER_ADMIN, DB_ADMIN, ORDINARY_USER])
    @pytest.mark.parametrize('local_domain', [True, False], ids=[LOCAL_DOMAIN, BULTIN_DOMAIN])
    def test_tenant_db(self, request, prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions):
        self.proceed_users_management_test(prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions,
                                           request.node.callspec.params)

        self.proceed_db_owners_modification_test(prepared_root_db, prepared_tenant_db, subject, local_domain, subject_permissions,
                                                 request.node.callspec.params)
