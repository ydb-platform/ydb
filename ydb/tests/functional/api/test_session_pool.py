# -*- coding: utf-8 -*-
import logging
import time

from hamcrest import assert_that, is_, raises

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestSessionPool(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.logger = logger.getChild(cls.__name__)
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

    def test_session_pool_simple_acquire(self):

        with ydb.SessionPool(self.driver, size=1) as pool:
            session = pool.acquire()

            def callee():
                pool.acquire(blocking=False)

            for _ in range(10):
                assert_that(
                    callee,
                    raises(
                        ydb.SessionPoolEmpty
                    )
                )

            pool.release(session)

            session = pool.acquire()
            assert_that(
                session.initialized(),
                is_(
                    True
                )
            )

    def test_session_pool_no_race_after_future_cancel_case_1(self):

        with ydb.SessionPool(self.driver, size=1) as pool:
            session = pool.acquire()

            waiter = pool.subscribe()  # subscribe
            waiter.cancel()            # cancel subscribe
            pool.unsubscribe(waiter)   # unsubscribe
            pool.release(session)      # release session, no reply on waiter

            session = pool.acquire()   # re-acquire session
            assert_that(
                session.initialized(),
                is_(
                    True
                )
            )

    def test_session_pool_no_race_after_future_cancel_case_2(self):
        with ydb.SessionPool(self.driver, size=1) as pool:
            session = pool.acquire()

            waiter = pool.subscribe()   # subscribe
            pool.release(session)       # release prev. session

            assert_that(
                waiter.result().initialized(),   # future is signalled
                is_(
                    True
                )
            )

            waiter.cancel()              # cancelling future
            pool.unsubscribe(waiter)

    def test_session_pool_keep_alive(self):
        with ydb.SessionPool(self.driver, size=1) as pool:
            s = pool.acquire()
            pool.release(s)

            pool._pool_impl._keep_alive_threshold = 20 * 60
            pool._pool_impl.send_keep_alive()
            assert_that(
                pool.acquire().initialized(),
                is_(
                    True
                )
            )

    def test_session_pool_no_race_after_future_cancel_case_3(self):

        with ydb.SessionPool(self.driver, size=1) as pool:
            session = pool.acquire()

            waiter = pool.subscribe()
            pool.unsubscribe(waiter)

            pool.release(session)

            session = pool.acquire()
            assert_that(
                session.initialized(),
                is_(
                    True
                )
            )

    def test_session_pool_no_race_after_future_cancel_case_4(self):
        with ydb.SessionPool(self.driver, size=1) as pool:
            session = pool.acquire()

            waiter = pool.subscribe()
            waiter.cancel()
            pool.release(session)

            assert_that(
                waiter.cancelled(),
                is_(
                    True
                )
            )

            assert_that(
                pool.acquire().initialized(),
                is_(
                    True
                )
            )

    def test_session_pool_release_logic(self):

        pool = ydb.SessionPool(self.driver, size=1)

        session = pool.acquire()

        waiter = pool.subscribe()
        # broken as bad session
        session.reset()

        pool.release(session)

        assert_that(
            waiter.result(timeout=3).initialized(),
            is_(
                True
            )
        )

        pool.stop()

    def test_session_pool_close_basic_logic_case_1(self):
        pool = ydb.SessionPool(self.driver, size=1)

        session = pool.acquire()
        waiter = pool.subscribe()

        pool.stop()

        assert_that(
            waiter.result().initialized(),
            is_(
                False
            )
        )

        has_exception = False
        try:
            pool.acquire().initialized()
        except ValueError:
            has_exception = True

        assert_that(has_exception, is_(True))

        pool.unsubscribe(waiter)
        pool.release(session)

        assert_that(
            pool._pool_impl.active_size,
            is_(
                0
            )
        )

    def test_no_cluster_endpoints_no_failure(self):
        with ydb.SessionPool(self.driver, size=1) as pool:
            self.cluster.nodes[1].stop()
            waiter = pool.subscribe()
            self.cluster.nodes[1].start()

            assert_that(
                waiter.result().initialized(), is_(
                    True
                )
            )

            waiter.result().reset()
            pool.unsubscribe(waiter)

            assert_that(
                pool._pool_impl.active_size, is_(
                    0
                )
            )

    def test_session_pool_close_basic_logic_case_2(self):
        pool = ydb.SessionPool(self.driver, size=10)

        acquired = []

        for _ in range(10):
            acquired.append(pool.acquire())

        for _ in range(3):
            pool.release(acquired.pop(-1))

        pool.stop()
        assert_that(
            pool._pool_impl.active_size,
            is_(
                7
            )
        )

        while acquired:
            pool.release(acquired.pop(-1))

        assert_that(
            pool._pool_impl.active_size,
            is_(
                0
            )
        )

        has_exception = False
        try:
            pool.acquire().initialized()
        except ValueError:
            has_exception = True

        assert_that(has_exception, is_(True))

    def test_session_pool_min_size_feature(self):
        pool = ydb.SessionPool(self.driver, size=10, min_pool_size=10)

        def wait_for_size(attempts=120):
            cur_att = 0
            while cur_att < attempts:
                cur_att += 1
                cur_size = pool.active_size
                self.logger.debug("Current size is %d", cur_size)
                if cur_size < 10:
                    time.sleep(1)
                else:
                    break

        wait_for_size()
        assert_that(
            pool.active_size,
            is_(10))

        session = pool.acquire()
        session.reset()
        pool.release(session)

        wait_for_size()

        pool.stop()
