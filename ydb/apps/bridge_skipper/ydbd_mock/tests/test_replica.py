"""Persistence failures must not publish a replica change in memory."""
import json
import os
import tempfile
import unittest
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.replica import Prepared, Replica


class ReplicaAtomicityTest(unittest.TestCase):
    def test_failed_install_preserves_memory_disk_and_prepared_state(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "replica.json")
            replica = Replica(2, path)
            replica.save()
            with open(path) as f:
                before_disk = json.load(f)
            before_state = replica.state.copy()
            prepared_state = model.derive(before_state, updates={1: model.Stored.DISCONNECTED})
            replica.prepared = Prepared("pending", prepared_state, 0, 1.0)
            proposed = model.derive(before_state, updates={1: model.Stored.DISCONNECTED})
            proposed.generation = 2
            with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                            side_effect=OSError("injected replace failure")):
                with self.assertRaisesRegex(OSError, "injected replace failure"):
                    replica.install(proposed)
            self.assertEqual(replica.state, before_state)
            self.assertEqual(replica.prepared.proposal_id, "pending")
            with open(path) as f:
                self.assertEqual(json.load(f), before_disk)

    def test_failed_force_preserves_detached_flag_and_prepared_state(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "replica.json")
            replica = Replica(2, path)
            replica.save()
            before_state = replica.state.copy()
            prepared_state = model.derive(before_state, updates={1: model.Stored.DISCONNECTED})
            replica.prepared = Prepared("pending", prepared_state, 0, 1.0)
            with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                            side_effect=OSError("injected replace failure")):
                with self.assertRaisesRegex(OSError, "injected replace failure"):
                    replica.force(prepared_state, detached=True)
            self.assertEqual(replica.state, before_state)
            self.assertFalse(replica.detached)
            self.assertEqual(replica.prepared.proposal_id, "pending")

    def test_failed_reset_preserves_current_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "replica.json")
            replica = Replica(2, path)
            replica.save()
            before_state = replica.state.copy()
            with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                            side_effect=OSError("injected replace failure")):
                with self.assertRaisesRegex(OSError, "injected replace failure"):
                    replica.reset(9)
            self.assertEqual(replica.state, before_state)

    def test_max_generation_survives_lower_force_and_reload(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "replica.json")
            replica = Replica(2, path)
            replica.save()
            high = replica.state.copy()
            high.generation = 8
            self.assertTrue(replica.install(high))
            self.assertEqual(replica.max_generation, 8)

            lower = replica.state.copy()
            lower.generation = 3
            replica.force(lower, detached=False)
            self.assertEqual(replica.max_generation, 8)

            reloaded = Replica(2, path)
            self.assertTrue(reloaded.load())
            self.assertEqual(reloaded.state.generation, 3)
            self.assertEqual(reloaded.max_generation, 8)

    def test_failed_higher_install_does_not_advance_max_generation(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "replica.json")
            replica = Replica(2, path)
            replica.save()
            high = replica.state.copy()
            high.generation = 8
            with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                            side_effect=OSError("injected replace failure")):
                with self.assertRaisesRegex(OSError, "injected replace failure"):
                    replica.install(high)
            self.assertEqual(replica.max_generation, 1)
