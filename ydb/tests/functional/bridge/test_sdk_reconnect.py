import unittest
from unittest.mock import MagicMock, Mock, patch

import ydb
from ydb.table import SessionPool

from test_three_piles import TestBridgeThreePiles as BridgeThreePiles, PRIMARY


class TestPinnedDriverReconnect(unittest.TestCase):
    def setUp(self):
        self.bridge = BridgeThreePiles()
        self.bridge.states = {"r1": PRIMARY}
        self.bridge.transition_timeout = 1
        self.bridge.drivers = {}
        self.bridge.pools = {}
        self.node = Mock(endpoint="localhost:1")
        self.bridge.pile_nodes = {"r1": [self.node]}
        self.bridge.stop_driver = Mock(side_effect=lambda driver: driver.stop())
        sleep = patch("test_three_piles.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    @staticmethod
    def make_pool(session):
        pool = Mock()
        checkout = MagicMock()
        checkout.__enter__.return_value = session
        pool.checkout.return_value = checkout
        # Exercise the SDK's acquisition and retry boundary, including failures
        # raised before the operation callback is entered.
        pool.retry_operation_sync.side_effect = lambda operation, retry_settings: SessionPool.retry_operation_sync(
            pool, operation, retry_settings,
        )
        return pool

    def check_reconnect(self, error, scheme=False, during_acquire=False):
        old_driver, new_driver = Mock(), Mock()
        old_session, new_session = Mock(), Mock()
        old_pool = self.make_pool(old_session)
        new_pool = self.make_pool(new_session)
        if during_acquire:
            old_pool.checkout.return_value.__enter__.side_effect = error
        elif scheme:
            old_session.execute_scheme.side_effect = error
        else:
            old_session.transaction.return_value.execute.side_effect = error
        if scheme:
            new_session.execute_scheme.return_value = "success"
        else:
            new_session.transaction.return_value.execute.return_value = "success"
        self.bridge.drivers["r1"] = old_driver
        self.bridge.pools["r1"] = old_pool

        with patch("test_three_piles.ydb.Driver", return_value=new_driver) as driver_factory:
            with patch("test_three_piles.ydb.SessionPool", return_value=new_pool):
                self.assertEqual(self.bridge.execute("query", scheme=scheme), "success")
                # Successful operations keep using the replacement client.
                self.assertEqual(self.bridge.execute("query", scheme=scheme), "success")
        old_pool.stop.assert_called_once_with()
        self.bridge.stop_driver.assert_called_once_with(old_driver)
        new_pool.stop.assert_not_called()
        driver_factory.assert_called_once()
        config = driver_factory.call_args.args[0]
        self.assertEqual(config.endpoint, "localhost:1")
        self.assertTrue(config.disable_discovery)
        self.assertIs(self.bridge.drivers["r1"], new_driver)
        self.assertIs(self.bridge.pools["r1"], new_pool)
        old_pool.retry_operation_sync.assert_called_once()
        if during_acquire:
            old_session.execute_scheme.assert_not_called()
            old_session.transaction.assert_not_called()

    def test_connection_loss_during_query(self):
        self.check_reconnect(ydb.ConnectionLost("connection closed"))

    def test_connection_loss_during_scheme_query(self):
        self.check_reconnect(ydb.ConnectionLost("connection closed"), scheme=True)

    def test_unavailable_during_query(self):
        self.check_reconnect(ydb.Unavailable("endpoint unavailable"))

    def test_connection_loss_during_session_acquisition(self):
        self.check_reconnect(ydb.ConnectionLost("connection closed"), during_acquire=True)

    def test_empty_pool_after_background_disconnect(self):
        self.check_reconnect(ydb.SessionPoolEmpty("Timeout on session acquire."), during_acquire=True)

    def test_stop_clients_before_stopping_pile(self):
        driver, pool = Mock(), Mock()
        self.bridge.drivers["r1"] = driver
        self.bridge.pools["r1"] = pool
        self.node.is_alive.return_value = False
        calls = Mock()
        calls.attach_mock(pool.stop, "pool_stop")
        calls.attach_mock(driver.stop, "driver_stop")
        calls.attach_mock(self.node.stop, "node_stop")
        self.bridge.stop_pile("r1")
        self.assertEqual([call[0] for call in calls.mock_calls], ["pool_stop", "driver_stop", "node_stop"])
        self.assertFalse(self.bridge.drivers)
        self.assertFalse(self.bridge.pools)
        self.bridge.stop_pile_clients("r1")
        driver.stop.assert_called_once_with()
        pool.stop.assert_called_once_with()
