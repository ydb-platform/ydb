# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestUserManagementRollingUpgrade(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope='function')
    def setup(self):
        yield from self.setup_cluster()

    def check_login(self, username, password):
        driver_config = ydb.DriverConfig(
            database=self.database_path,
            endpoint=self.endpoints[0],
            credentials=ydb.StaticCredentials.from_user_password(username, password),
        )

        with ydb.Driver(driver_config) as test_driver:
            with ydb.QuerySessionPool(test_driver) as session_pool:
                result = session_pool.execute_with_retries("SELECT 1 as test_value")
                assert len(result) == 1
                assert result[0].rows[0]['test_value'] == 1

    def test_user_management(self):
        password1 = 'password_123'
        password2 = 'new_password_456'
        password3 = 'password1'
        password3_hash = """
            {
                "hash":"ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                "salt":"HTkpQjtVJgBoA0CZu+i3zg==",
                "type":"argon2id"
            }
        """

        passwords = [password1, password2, password3]
        password_idx = 0

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"CREATE USER user1 PASSWORD '{passwords[password_idx % len(passwords)]}'"
            session_pool.execute_with_retries(query)

        for _ in self.roll():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                self.check_login('user1', passwords[password_idx % len(passwords)])

                password_idx += 1
                if password_idx % len(passwords) == 2:
                    query = f"ALTER USER user1 HASH '{password3_hash}'"
                else:
                    query = f"ALTER USER user1 PASSWORD '{passwords[password_idx % len(passwords)]}'"
                session_pool.execute_with_retries(query)
