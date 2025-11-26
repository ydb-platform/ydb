import string
import random
import pytest
import sys

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture


class TestTinyVDisks(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")

        yield from self.setup_cluster()

    def write_data(self, table_name, count, size):
        driver = self.create_driver()

        with ydb.SessionPool(driver) as session_pool:
            with session_pool.checkout() as session:
                session.execute_scheme(
                    f"create table `{table_name}` (k Uint32, v Utf8, primary key (k));"
                )

                prepared = session.prepare(f"""
                    declare $key as Uint32;
                    declare $value as Utf8;
                    upsert into `{table_name}` (k, v) VALUES ($key, $value);
                    """)

                for _ in range(count):
                    key = random.randint(1, 2**30)
                    value = ''.join(random.choice(string.ascii_lowercase) for _ in range(size))
                    session.transaction().execute(
                        prepared, {'$key': key, '$value': value},
                        commit_tx=True
                    )

        driver.stop()

    def read_data(self, table_name):
        driver = self.create_driver()

        with ydb.QuerySessionPool(driver) as session_pool:
            result = session_pool.execute_with_retries(f"select k, v from `{table_name}`;")
            assert len(result[0].rows) == 1

        driver.stop()

    def test_change_version(self):
        self.write_data("table", 1, 4194304)
        self.change_cluster_version()
        self.read_data("table")
