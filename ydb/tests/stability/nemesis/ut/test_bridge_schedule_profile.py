"""Bridge deployment uses the scheduling API available in the stable branch."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from flask import Flask

from ydb.tests.library.stability.deploy import StressUtilDeployer
from ydb.tests.stability.nemesis.internal.nemesis import catalog, cluster_context
from ydb.tests.stability.nemesis.internal.nemesis.cluster_entries import all_nemesis_type_entries
import ydb.tests.stability.nemesis.routers.orchestrator_router as router


_BRIDGE_TYPES = (
    "BridgePileStopNodesNemesis",
    "BridgePileRouteUnreachableNemesis",
    "BridgePileIptablesBlockPortsNemesis",
    "KillCoordinatorNemesis",
    "KillHiveNemesis",
    "KillBsControllerNemesis",
    "KillNodeBrokerNemesis",
    "KillSchemeShardNemesis",
    "KillMediatorNemesis",
    "KillTxAllocatorNemesis",
    "KillKeyValueNemesis",
    "KillTenantSlotBrokerNemesis",
    "KillPersQueueNemesis",
    "KillDataShardNemesis",
    "KillBlockstoreVolumeNemesis",
    "KillBlockstorePartitionNemesis",
)
_ENDPOINT = "http://orchestrator"


class _Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self):
        return json.dumps(self.body).encode()


class _NativeScheduler:
    def __init__(self):
        self.is_running = True
        self.starts = 0
        self.profiles = []

    def start(self):
        self.is_running = True
        self.starts += 1

    def stop(self):
        self.is_running = False

    def status(self):
        return {"running": self.is_running}

    def set_profile(self, **profile):
        self.profiles.append(profile)


class _PerTypeSchedules:
    def __init__(self, native_scheduler):
        self.native_scheduler = native_scheduler
        self.enabled = {"KillNodeNemesis": 200}

    def enable_schedule(self, process_type, interval):
        assert not self.native_scheduler.is_running, "native scheduler must stop before per-type scheduling"
        self.enabled[process_type] = interval
        return True

    def disable_all_schedules(self):
        stopped = list(self.enabled)
        self.enabled.clear()
        return stopped


class BridgeScheduleProfileTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        config_path = Path(directory.name) / "cluster.yaml"
        config_path.write_text(json.dumps({
            "config": {"bridge_config": {"piles": [{"name": "a"}, {"name": "b"}]}},
        }), encoding="utf-8")
        # Register the actual topology-gated entries without constructing their planners.
        with patch.object(cluster_context, "cluster_yaml_path", return_value=str(config_path)):
            entries = all_nemesis_type_entries()
        catalog_patch = patch.dict(catalog.NEMESIS_TYPES, entries, clear=True)
        catalog_patch.start()
        self.addCleanup(catalog_patch.stop)

        self.native_scheduler = _NativeScheduler()
        self.per_type_schedules = _PerTypeSchedules(self.native_scheduler)
        state_patch = patch.multiple(
            router,
            nemesis_scheduler=self.native_scheduler,
            nemesis_schedule=self.per_type_schedules,
            failure_guard=None,
            chaos_store=None,
        )
        state_patch.start()
        self.addCleanup(state_patch.stop)
        app = Flask(__name__)
        app.register_blueprint(router.blueprint)
        self.client = app.test_client()

        self.deployer = object.__new__(StressUtilDeployer)
        self.deployer.cluster_path = str(config_path)
        self.deployer.yaml_config = None
        endpoint_patch = patch.object(self.deployer, "_get_orchestrator_endpoint", return_value=_ENDPOINT)
        endpoint_patch.start()
        self.addCleanup(endpoint_patch.stop)
        self.calls = []
        transport_patch = patch("ydb.tests.library.stability.deploy.urllib.request.urlopen", side_effect=self._urlopen)
        transport_patch.start()
        self.addCleanup(transport_patch.stop)

    def _urlopen(self, request, timeout):
        body = json.loads(request.data) if request.data else None
        self.calls.append((request.get_method(), request.full_url, body))
        response = self.client.open(
            request.full_url.removeprefix(_ENDPOINT), method=request.get_method(), json=body,
        )
        if response.status_code >= 400:
            raise HTTPError(request.full_url, response.status_code, response.get_json(), {}, None)
        return _Response(response.get_json())

    def test_native_scheduler_rejects_the_bridge_pile_profile(self):
        response = self.client.post("/api/scheduler/start", json={"enabled": list(_BRIDGE_TYPES)})
        self.assertEqual(response.status_code, 400)
        message = response.get_json()["message"]
        self.assertIn("not usable by the boundary scheduler", message)
        for name in _BRIDGE_TYPES[:3]:
            self.assertIn(name, message)
        self.assertEqual(self.native_scheduler.starts, 0)

    def test_bridge_profile_uses_per_type_api_and_catalog_intervals(self):
        self.assertTrue(self.deployer._set_schedules_enabled(True, []))
        intervals = {name: catalog.NEMESIS_TYPES[name]["schedule"] for name in _BRIDGE_TYPES}
        self.assertEqual(self.calls, [
            ("GET", f"{_ENDPOINT}/api/process_types", None),
            ("POST", f"{_ENDPOINT}/api/scheduler/stop", {}),
            ("POST", f"{_ENDPOINT}/api/schedule/all", {"enabled": False}),
        ] + [
            ("POST", f"{_ENDPOINT}/api/schedule", {"type": name, "enabled": True, "interval": intervals[name]})
            for name in _BRIDGE_TYPES
        ])
        self.assertFalse(self.native_scheduler.is_running)
        self.assertEqual(self.per_type_schedules.enabled, intervals)

    def test_bridge_schedules_do_not_start_if_native_stop_fails(self):
        with patch.object(self.native_scheduler, "stop", side_effect=RuntimeError("stop failed")):
            self.assertFalse(self.deployer._set_schedules_enabled(True, []))
        self.assertEqual(self.calls, [
            ("GET", f"{_ENDPOINT}/api/process_types", None),
            ("POST", f"{_ENDPOINT}/api/scheduler/stop", {}),
        ])
        self.assertEqual(self.per_type_schedules.enabled, {"KillNodeNemesis": 200})

    def test_disabling_bridge_schedules_stops_both_schedulers(self):
        self.assertTrue(self.deployer._set_schedules_enabled(True, []))
        self.calls.clear()
        self.native_scheduler.is_running = True
        self.assertTrue(self.deployer._set_schedules_enabled(False, []))
        self.assertEqual(self.calls, [
            ("POST", f"{_ENDPOINT}/api/scheduler/stop", {}),
            ("POST", f"{_ENDPOINT}/api/schedule/all", {"enabled": False}),
        ])
        self.assertFalse(self.native_scheduler.is_running)
        self.assertEqual(self.per_type_schedules.enabled, {})

    def test_disabling_ordinary_schedules_uses_native_stop_endpoint(self):
        self.deployer.cluster_path = None
        self.assertTrue(self.deployer._set_schedules_enabled(False, []))
        self.assertEqual(self.calls, [("POST", f"{_ENDPOINT}/api/scheduler/stop", {})])
        self.assertFalse(self.native_scheduler.is_running)

    def test_ordinary_cluster_keeps_native_default_profile(self):
        self.deployer.cluster_path = None
        self.native_scheduler.is_running = False
        self.assertTrue(self.deployer._set_schedules_enabled(True, []))
        self.assertEqual(self.calls, [("POST", f"{_ENDPOINT}/api/scheduler/start", {})])
        self.assertTrue(self.native_scheduler.is_running)
        self.assertEqual(self.native_scheduler.profiles, [])
