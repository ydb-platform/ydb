"""Bridge deployment uses the scheduling API available in the stable branch."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ydb.tests.library.stability.deploy import StressUtilDeployer


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


class _Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self):
        return json.dumps(self.body).encode()


class BridgeScheduleProfileTest(unittest.TestCase):
    def test_bridge_profile_uses_per_type_api_and_catalog_intervals(self):
        catalog = [{"name": name, "schedule": 200 + index} for index, name in enumerate(_BRIDGE_TYPES)]
        catalog.append({"name": "SafelyBreakDiskNemesis", "schedule": 400})
        calls = []

        def urlopen(request, timeout):
            body = json.loads(request.data) if request.data else None
            calls.append((request.get_method(), request.full_url, body))
            return _Response(catalog if request.get_method() == "GET" else {"status": "ok"})

        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "cluster.yaml"
            config_path.write_text(json.dumps({"config": {"bridge_config": {"piles": [{"name": "a"}, {"name": "b"}]}}}))
            deployer = object.__new__(StressUtilDeployer)
            deployer.cluster_path = str(config_path)
            deployer.yaml_config = None
            with patch.object(deployer, "_get_orchestrator_endpoint", return_value="http://orchestrator"), \
                    patch("ydb.tests.library.stability.deploy.urllib.request.urlopen", side_effect=urlopen):
                self.assertTrue(deployer._set_schedules_enabled(True, []))

        self.assertEqual(calls[0], ("GET", "http://orchestrator/api/process_types", None))
        self.assertEqual(calls[1], ("POST", "http://orchestrator/api/schedule/all", {"enabled": False}))
        self.assertEqual(calls[2:], [
            ("POST", "http://orchestrator/api/schedule", {"type": name, "enabled": True, "interval": 200 + index})
            for index, name in enumerate(_BRIDGE_TYPES)
        ])

    def test_disabling_schedules_uses_the_existing_all_endpoint(self):
        deployer = object.__new__(StressUtilDeployer)
        with patch.object(deployer, "_get_orchestrator_endpoint", return_value="http://orchestrator"), \
                patch("ydb.tests.library.stability.deploy.urllib.request.urlopen", return_value=_Response({"status": "ok"})) as urlopen:
            self.assertTrue(deployer._set_schedules_enabled(False, []))
        urlopen.assert_called_once()
        request = urlopen.call_args[0][0]
        self.assertEqual(request.full_url, "http://orchestrator/api/schedule/all")
        self.assertEqual(json.loads(request.data), {"enabled": False})
