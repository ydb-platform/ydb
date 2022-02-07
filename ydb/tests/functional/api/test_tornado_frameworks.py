# -*- coding: utf-8 -*-
import tornado.ioloop
from hamcrest import assert_that, is_, not_, none

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
import ydb
import ydb.tornado


async def raising_error(obj):
    if obj.num <= 3:
        obj.num += 1
        raise ydb.BadSession("Test bad session")
    return 7


async def cor():
    class RaisingError(object):
        num = 0

    o = RaisingError()

    return await ydb.tornado.retry_operation(lambda: raising_error(o))


class TestTornadoFrameworks(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    def test_raising_error(self):
        result = tornado.ioloop.IOLoop.current().run_sync(
            cor,
        )

        assert result == 7

    def test_retry_operation(self):
        result = tornado.ioloop.IOLoop.current().run_sync(
            lambda: ydb.tornado.retry_operation(
                lambda: ydb.tornado.as_tornado_future(
                    self.driver.table_client.session().async_create()
                )
            )
        )

        assert_that(
            result.session_id,
            is_(
                not_(
                    none()
                )
            )
        )
