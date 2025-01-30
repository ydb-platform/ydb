# -*- coding: utf-8 -*-

import logging
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


class BaseCreateSubjects(object):
    @classmethod
    def setup_class(cls):
        config_generator = KikimrConfigGenerator()
        config_generator.yaml_config["auth_config"] = {
            "domain_login_only": cls.DomainLoginOnly,
        }
        cls.cluster = KiKiMR(config_generator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def setup_method(self, method=None):
        self.database = "/Root/users/{class_name}_{method_name}".format(
            class_name=self.__class__.__name__,
            method_name=method.__name__,
        )
        host = self.cluster.nodes[1].host
        port = self.cluster.nodes[1].port
        self.domain_admin_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (host, port),
            database="/Root",
        )
        self.tenant_admin_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (host, port),
            database=self.database,
        )
        self.tenant_user_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (host, port),
            database=self.database,
            credentials=ydb.StaticCredentials.from_user_password("user", ""),
        )

        logger.debug("Create database %s" % self.database)
        self.cluster.create_database(
            self.database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(self.database, count=1)
        self.cluster.wait_tenant_up(self.database)

    def teardown_method(self, method=None):
        logger.debug("Remove database %s" % self.database)
        self.cluster.remove_database(self.database)
        self.database = None

    def yql_create_user(self, admin_driver_config):
        with ydb.Driver(admin_driver_config) as driver:
            driver.wait(5)
            script_client = ydb.ScriptingClient(driver)
            script_client.execute_yql('CREATE USER user;')
            script_client.execute_yql('ALTER USER user WITH PASSWORD NULL;')
            script_client.execute_yql('DROP USER user;')

    def query_create_user(self, admin_driver_config):
        with ydb.Driver(admin_driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries("CREATE USER user;")
                pool.execute_with_retries('ALTER USER user WITH PASSWORD NULL;')
                pool.execute_with_retries("DROP USER user;")

    def yql_create_group(self, admin_driver_config):
        with ydb.Driver(admin_driver_config) as driver:
            driver.wait(5)
            script_client = ydb.ScriptingClient(driver)
            script_client.execute_yql("CREATE GROUP group;")
            script_client.execute_yql("CREATE USER user;")
            script_client.execute_yql("ALTER GROUP group ADD USER user;")
            script_client.execute_yql("ALTER GROUP group DROP USER user;")
            script_client.execute_yql("DROP GROUP group;")
            script_client.execute_yql("DROP USER user;")

    def query_create_group(self, admin_driver_config):
        with ydb.Driver(admin_driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries("CREATE GROUP group;")
                pool.execute_with_retries("CREATE USER user;")
                pool.execute_with_retries("ALTER GROUP group ADD USER user;")
                pool.execute_with_retries("ALTER GROUP group DROP USER user;")
                pool.execute_with_retries("DROP GROUP group;")
                pool.execute_with_retries("DROP USER user;")

    def test_yql_create_user_by_domain_admin(self):
        self.yql_create_user(self.domain_admin_driver_config)

    def test_yql_create_user_by_tenant_admin(self):
        self.yql_create_user(self.tenant_admin_driver_config)

    def test_yql_create_group_by_domain_admin(self):
        self.yql_create_group(self.domain_admin_driver_config)

    def test_yql_create_group_by_tenant_admin(self):
        self.yql_create_group(self.tenant_admin_driver_config)

    def test_query_create_user_by_domain_admin(self):
        self.query_create_user(self.domain_admin_driver_config)

    def test_query_create_user_by_tenant_admin(self):
        self.query_create_user(self.tenant_admin_driver_config)

    def test_query_create_group_by_domain_admin(self):
        self.query_create_group(self.domain_admin_driver_config)

    def test_query_create_group_by_tenant_admin(self):
        self.query_create_group(self.tenant_admin_driver_config)


class TestCreateSubjectsWithDomainLoginOnly(BaseCreateSubjects):
    DomainLoginOnly = True


class TestCreateSubjectsWithoutDomainLoginOnly(BaseCreateSubjects):
    DomainLoginOnly = False
