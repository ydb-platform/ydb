"""Listener ports stay reserved until an acceptance environment closes."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from library.python.port_manager import PortManager, PortManagerException
from ydb.apps.bridge_skipper.acceptance.lib.harness import Environment


class PortReservationTest(unittest.IsolatedAsyncioTestCase):
    async def test_controller_http_skips_bad_ports_without_releasing_listener_reservations(self):
        allocated = list(range(11001, 11009)) + [6669, 10080, 18080]
        candidates = iter(allocated)
        reserved = set()
        ports = Mock(spec=PortManager)

        def reserve():
            port = next(candidates)
            self.assertNotIn(port, reserved)
            reserved.add(port)
            return port

        ports.get_port.side_effect = reserve
        ports.release_port.side_effect = reserved.remove
        ports.release.side_effect = reserved.clear
        with tempfile.TemporaryDirectory() as root:
            with patch("ydb.apps.bridge_skipper.acceptance.lib.harness.PortManager", return_value=ports):
                environment = Environment("unused", Path(root))
            try:
                self.assertEqual(environment.controller.url, "http://127.0.0.1:18080")
                self.assertEqual([pile.control_port for pile in environment.cluster.topo.piles], [11001, 11002])
                self.assertEqual([node.grpc_port for node in environment.cluster.topo.all_nodes()],
                                 list(range(11003, 11009)))
                self.assertEqual(reserved, set(allocated))
                ports.release.assert_not_called()
                ports.release_port.assert_not_called()
            finally:
                await environment.close()
            self.assertEqual(reserved, set())
            ports.release.assert_called_once_with()

    async def test_all_listener_ports_are_reserved_before_start_and_released_after_close(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"PORT_SYNC_PATH": root}):
                environment = Environment("unused", Path(root))
                try:
                    topology = environment.cluster.topo
                    ports = [pile.control_port for pile in topology.piles]
                    ports += [node.grpc_port for node in topology.all_nodes()]
                    ports.append(int(environment.controller.config["http"]["listen"].rsplit(":", 1)[1]))
                    self.assertEqual(len(set(ports)), len(ports))
                    # No listeners have started. A parallel worker must still be
                    # unable to acquire any of these ports through the shared locks.
                    for port in ports:
                        with self.subTest(port=port):
                            with patch.dict(os.environ, {"VALID_PORT_RANGE": "%d:%d" % (port, port + 1)}):
                                with PortManager() as competitor:
                                    with self.assertRaises(PortManagerException):
                                        competitor.get_port()
                finally:
                    await environment.close()
                for port in ports:
                    with patch.dict(os.environ, {"VALID_PORT_RANGE": "%d:%d" % (port, port + 1)}):
                        with PortManager() as competitor:
                            self.assertEqual(competitor.get_port(), port)
