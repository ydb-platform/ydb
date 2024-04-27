import os
import sys

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import YdbdSlice
import ydb


class Test(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = YdbdSlice(
            config_path=yatest_common.source_path(os.environ["YDB_CLUSTER_YAML"]),
            binary_path=yatest_common.binary_path(os.environ["YDB_DRIVER_BINARY"])
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        pass
        cls.cluster.stop()

    def test_slice_sample(self):
        driver_config = ydb.DriverConfig(
            database=self.cluster.db_path,
            endpoint="%s:%s" % (
                self.cluster.nodes[1].host, self.cluster.nodes[1].port
            )
        )
        with ydb.Driver(driver_config) as driver:
            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        "create table `{}` (key Int32, value String, primary key(key));".format(
                            "sample_table"
                        )
                    )

    def test_serializable(self):
        yatest_common.execute(
            [
                yatest_common.binary_path('ydb/tests/tools/ydb_serializable/ydb_serializable'),
                '--endpoint=%s:%d' % (self.cluster.nodes[1].host, self.cluster.nodes[1].grpc_port),
                '--database=%s' % self.cluster.db_path,
                # '--output-path=%s' % yatest_common.output_path(),
                '--output-path=%s' % "/tmp/ser",
                '--iterations=25',
                '--processes=2'
            ],
            stderr=sys.stderr,
            wait=True,
            stdout=sys.stdout,
        )
