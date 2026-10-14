"""Exercise emergency mirror-3-dc recovery with real ydbd processes and disks.

The focused tests keep the control plane alive. The full-disaster test also
loses and relocates the static mirror-3-dc group and StateStorage.
"""

import base64
import bz2
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import filecmp
import hashlib
import json
import logging
from pathlib import Path
import re
import shutil
import struct
import subprocess
import time
import unittest
import uuid

import grpc
import requests
import yatest.common

from ydb.core.protos import msgbus_pb2
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, HiveClient
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.library.harness.daemon import Daemon
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


class SingleDcRecoveryFixture:
    def setUp(self):
        self.old_binary = None
        if self._testMethodName == "test_upgrade_after_two_lost_dcs_and_resume_sql":
            old_binary = yatest.common.get_param("single_dc_old_binary")
            if not old_binary:
                self.skipTest("Set single_dc_old_binary to a pre-PR ydbd executable to test disaster-time upgrade")
            self.old_binary = Path(old_binary).resolve(strict=True)
        self.artifacts = Path(yatest.common.test_output_path("single_dc"))
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.http = requests.Session()
        self.addCleanup(self.http.close)
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=9,
            use_in_memory_pdisks=False,
            dynamic_storage_pools=[],
            state_storage_rings=[1],
            n_to_select=1,
            bs_cache_file_path=str(self.artifacts / "node-warden-%n.txt"),
        )
        if self.old_binary is not None:
            self.new_binary = Path(config.get_binary_path(0)).resolve(strict=True)
            self.assertNotEqual(self.old_binary, self.new_binary)
            self.assertFalse(filecmp.cmp(str(self.old_binary), str(self.new_binary), shallow=False),
                             "The old and current executables have identical contents")
            for label, binary in (("old", self.old_binary), ("new", self.new_binary)):
                version = yatest.common.execute([str(binary), "-V"], timeout=30).std_out.decode("utf-8")
                (self.artifacts / ("upgrade-%s-version.txt" % label)).write_text(version)
                logger.info("Upgrade %s binary=%s version:\n%s", label, binary, version)
            config.set_binary_paths([str(self.old_binary)])
        # The physical test topology is 3 DCs x 3 racks. Only the management
        # group's erasure is none; the dynamic group below is mirror-3-dc.
        for index, node in enumerate(config.yaml_config["nameservice_config"]["node"]):
            dc = str(index % 3 + 1)
            node["walle_location"]["data_center"] = dc
            config.naming_config.NameserviceConfig.Node[index].WalleLocation.DataCenter = dc
        tablets = config.yaml_config["system_tablets"]
        tablets["default_node"] = [1]
        for value in tablets.values():
            if isinstance(value, list):
                for tablet in value:
                    if isinstance(tablet, dict) and "node" in tablet:
                        tablet["node"] = [1]

        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        self.pdisk_paths = {item["pdisk_path"]: item["node_id"] for item in config.pdisks_info}
        if self.old_binary is not None:
            self._assert_running_binary(set(range(1, 10)), self.old_binary)
        self.cluster.client.update_self_heal(False)
        self.cluster.add_storage_pool(
            name="single-dc-recovery", kind="hdd", erasure=Erasure.MIRROR_3_DC, num_groups=1,
        )
        base = self.cluster.client.query_base_config().BaseConfig
        groups = [group for group in base.Group if group.ErasureSpecies == "mirror-3-dc"]
        self.assertEqual(len(groups), 1, str(base))
        self.group_id = groups[0].GroupId
        slots = [slot for slot in base.VSlot if slot.GroupId == self.group_id]
        self.assertEqual(len(slots), 9, str(base))
        self.realm = next(slot.FailRealmIdx for slot in slots if slot.VSlotId.NodeId == 1)
        self.survivors = sorted(slot.VSlotId.NodeId for slot in slots if slot.FailRealmIdx == self.realm)
        self.lost = sorted(slot.VSlotId.NodeId for slot in slots if slot.FailRealmIdx != self.realm)
        self.assertEqual(len(set(self.survivors)), 3)
        self.assertEqual(len(set(self.lost)), 6)
        self._wait_ready(set(range(1, 10)))
        logger.info("group=%s surviving logical realm=%s nodes=%s lost=%s",
                    self.group_id, self.realm, self.survivors, self.lost)

    def _wait(self, description, predicate, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
                last = value
            except (requests.RequestException, grpc.RpcError, RuntimeError, AssertionError) as exc:
                last = repr(exc)
            time.sleep(0.5)
        self.fail("Timed out waiting for %s; last result: %s" % (description, last))

    def _get(self, http_node_id, path, **params):
        node = self.cluster.nodes[http_node_id]
        response = self.http.get(
            "http://%s:%s%s" % (node.host, node.mon_port, path),
            params=params, timeout=15,
        )
        return response

    def _base(self):
        return self.cluster.client.query_base_config().BaseConfig

    def _group(self):
        return next(group for group in self._base().Group if group.GroupId == self.group_id)

    def _wait_ready(self, node_ids):
        def ready():
            slots = [slot for slot in self._base().VSlot
                     if slot.GroupId == self.group_id and slot.VSlotId.NodeId in node_ids]
            return len(slots) == len(node_ids) and all(slot.Status == "READY" for slot in slots)
        self._wait("READY VDisks on %s" % sorted(node_ids), ready, timeout=180)

    def _crash(self, node_id):
        logger.info("SIGKILL node %s", node_id)
        # KiKiMRNode.kill() immediately restarts the process. The base method
        # intentionally leaves it down, and records the expected SIGKILL exit.
        Daemon.kill(self.cluster.nodes[node_id])
        self.assertFalse(self.cluster.nodes[node_id].is_alive())

    def _restart(self, node_id):
        logger.info("Start node %s using the same disk files", node_id)
        self.cluster.nodes[node_id].start()

    def _start_writer(self, tablet, size, group_id=None, node_id=1):
        # No duration: graceful load completion emits a hard GC barrier that
        # deletes all its data. A process crash preserves our durable samples.
        # No reads: a negative result later must be a failed PUT, not Discover
        # or a read of a blob that is unavailable with only one replica.
        group_id = self.group_id if group_id is None else group_id
        load_config = """
StorageLoad {
  Tablets {
    Tablets { TabletId: %d Channel: 0 GroupId: %d Generation: 1 }
    WriteSizes { Weight: 1 Min: %d Max: %d }
    WriteIntervals { Weight: 1 Uniform { MinMs: 250 MaxMs: 250 } }
    MaxInFlightWriteRequests: 1
    FlushIntervals { Weight: 1 Uniform { MinMs: 3600000 MaxMs: 3600000 } }
    PutHandleClass: UserData
    ContentType: Validated
  }
}
""" % (tablet, group_id, size, size)
        node = self.cluster.nodes[node_id]
        response = self.http.post(
            "http://%s:%s/actors/load" % (node.host, node.mon_port),
            data={"mode": "start", "all_nodes": "false", "config": load_config},
            headers={"Content-Type": "application/x-protobuf-text"}, timeout=15,
        )
        response.raise_for_status()
        result = response.json()
        self.assertEqual(result.get("status"), "ok", result)
        writer = dict(tablet=tablet, size=size, uuid=result["uuid"], node_id=node_id, group_id=group_id)
        self._wait("writer bootstrap and eight acknowledged PUTs", lambda: self._writer_has_puts(writer, 8))
        return writer

    def _writer_state(self, writer):
        response = self._get(writer["node_id"], "/actors/load", mode="results", uuid=writer["uuid"])
        response.raise_for_status()
        html = response.text
        (self.artifacts / ("load-%s.html" % writer["tablet"])).write_text(html)
        values = dict(re.findall(r"<tr><td>([^<]+)</td><td>([0-9]+)</td></tr>", html))
        return {name: int(value) for name, value in values.items()}

    def _writer_has_puts(self, writer, count):
        state = self._writer_state(writer)
        self.assertEqual(state.get("BadPutResults", 0), 0, state)
        return state if state.get("OkPutResults", 0) >= count else None

    def _sample(self, writer):
        state = self._writer_state(writer)
        # One PUT is in flight at most. Cookie denotes the next cookie to issue;
        # cookie-2 is already acknowledged. The hourly flush cannot reset it.
        self.assertGreaterEqual(state["Cookie"], 3, state)
        blob = "[%d:%d:%d:0:%d:%d:0]" % (
            writer["tablet"], state["Generation"], state["WriteStep"],
            state["Cookie"] - 2, writer["size"],
        )
        data = self._read(blob, group_id=writer["group_id"])
        self.assertEqual(len(data), writer["size"])
        logger.info("Sample %s sha256=%s", blob, hashlib.sha256(data).hexdigest())
        return blob, data

    def _read(self, blob, restore=False, group_id=None):
        group_id = self.group_id if group_id is None else group_id
        response = self._get(1, "/get_blob", groupId=group_id, blob=blob,
                             binary=1, mustRestoreFirst=int(restore))
        self.assertEqual(response.status_code, 200, response.text[:1000])
        return response.content

    def _assert_samples(self, samples):
        for blob, expected in samples:
            self.assertEqual(self._read(blob), expected, blob)
            self.assertEqual(self._read(blob, restore=True), expected, "MustRestoreFirst: " + blob)

    def _range(self, tablet, node_id=1):
        response = self._get(
            node_id, "/blob_range", groupId=self.group_id, tabletId=tablet, json=1,
            **{"from": "[%d:0:0:0:0:0:0]" % tablet,
               "to": "[%d:4294967295:4294967295:0:16777215:67108863:0]" % tablet},
        )
        response.raise_for_status()
        return response.json()

    def _wait_proxy(self, tablet, node_id):
        started = time.monotonic()

        def available():
            result = self._range(tablet, node_id)
            if result["Status"] == "OK":
                return True
            logger.info("Waiting for group %s proxy on node %s: %s", self.group_id, node_id, result)
            return None

        self._wait("group proxy on node %s" % node_id, available, timeout=90)
        logger.info("Group %s proxy on node %s answered Range after %.2fs",
                    self.group_id, node_id, time.monotonic() - started)

    def _enable_single_dc(self):
        before = self._group()
        request = msgbus_pb2.TBlobStorageConfigRequest()
        request.Domain = 1
        command = request.Request.Command.add().SetGroupSingleDcMode
        command.GroupId = self.group_id
        command.GroupGeneration = before.GroupGeneration
        command.EnableSingleDcMode = True
        command.SurvivingDc = self.realm
        response = self.cluster.client.send(request, "BlobStorageConfig").BlobStorageConfigResponse
        self.assertTrue(response.Success, str(response))
        self.assertTrue(all(status.Success for status in response.Status), str(response))
        after = self._group()
        self.assertTrue(after.EnableSingleDcMode)
        self.assertEqual(after.SurvivingDc, self.realm)
        self.assertGreater(after.GroupGeneration, before.GroupGeneration)
        self._wait_ready(set(self.survivors))

    def _tablet_info(self, tablet_id):
        response = self._get(1, "/tablets/app", TabletID=DEFAULT_HIVE_ID,
                             page="TabletInfo", tablet=tablet_id)
        response.raise_for_status()
        info = response.json()
        self.assertNotIn("error", info, info)
        return info

    def _move_tablet(self, tablet_id, node_id):
        node = self.cluster.nodes[1]
        response = self.http.post(
            "http://%s:%s/tablets/app" % (node.host, node.mon_port),
            params=dict(TabletID=DEFAULT_HIVE_ID, page="MoveTablet",
                        tablet=tablet_id, node=node_id, wait="false"),
            timeout=15,
        )
        response.raise_for_status()
        self.assertNotIn("error", response.json(), response.text)
        logger.info("Requested tablet %s restart on node %s", tablet_id, node_id)

    def _wait_tablets(self, tablet_ids, allowed_nodes, previous=None):
        def active():
            # The legacy msgbus tablet_state response has no NodeId. The
            # viewer merges whiteboard replies and records the actual node;
            # query only live candidates to avoid waiting for the dead DCs.
            response = self._get(1, "/viewer/json/tabletinfo",
                                 node_id=",".join(map(str, sorted(allowed_nodes))),
                                 enums="false", ui64="true", timeout=5000)
            response.raise_for_status()
            state = {int(item["TabletId"]): item
                     for item in response.json().get("TabletStateInfo", [])
                     if int(item["TabletId"]) in tablet_ids
                     and item.get("State") == int(TabletStates.Active)
                     and not item.get("FollowerId", 0)}
            if set(state) != set(tablet_ids):
                return None
            for tablet_id, item in state.items():
                if item["NodeId"] not in allowed_nodes:
                    return None
                if previous and item["Generation"] <= previous[tablet_id]["generation"]:
                    return None
                info = self._tablet_info(tablet_id)
                if (info["VolatileState"] != "Running" or info["NodeId"] != item["NodeId"]
                        or info["KnownGeneration"] != item["Generation"]):
                    return None
            return {tablet_id: dict(node=item["NodeId"], generation=item["Generation"])
                    for tablet_id, item in state.items()}

        state = self._wait("active user tablets on %s with new generations" % sorted(allowed_nodes),
                           active, timeout=180)
        for tablet_id in tablet_ids:
            info = self._tablet_info(tablet_id)
            channels = info["TabletStorageInfo"]["Channels"]
            self.assertTrue({0, 1}.issubset({channel["Channel"] for channel in channels}), info)
            self.assertEqual({channel["History"][-1]["GroupID"] for channel in channels},
                             {self.group_id}, info)
            # MoveTablet requests a restart destination without permanently
            # restricting the tablet to a node that will later disappear.
            self.assertFalse(info["AllowedNodes"], info)
        logger.info("Active user tablets (all channels in group %s): %s", self.group_id, state)
        return state

    def _sql(self, pool, query, parameters=None, transaction=False):
        def execute():
            try:
                retry_settings = ydb.RetrySettings(max_retries=2, max_session_acquire_timeout=5,
                                                  idempotent=True)
                settings = ydb.BaseRequestSettings().with_timeout(15)
                if transaction:
                    def committed(session):
                        with session.transaction(ydb.QuerySerializableReadWrite()) as tx:
                            with tx.execute(query, parameters=parameters, commit_tx=True,
                                            settings=settings) as results:
                                return list(results)
                    result = pool.retry_operation_sync(committed, retry_settings=retry_settings)
                else:
                    result = pool.execute_with_retries(query, parameters=parameters,
                                                       retry_settings=retry_settings, settings=settings)
                return (result,)  # DDL and UPSERT can return an empty list.
            except ydb.Error as exc:
                logger.info("SQL transient error: %s", exc)
                return None

        return self._wait("SQL request", execute, timeout=180)[0]

    def _sql_write(self, pool, expected, stage):
        declarations = []
        statements = []
        parameters = {}
        updates = {}
        for index, table in enumerate(expected):
            values = {}
            for offset, size in enumerate((4096, 1024 * 1024)):
                parameter = "$value_%s_%s" % (index, offset)
                pattern = "%s:%s:%s:" % (table, stage, size)
                # Deterministic, poorly compressible UTF-8 keeps the large
                # payload meaningful after the tablet's log compression.
                value = hashlib.shake_256(pattern.encode("ascii")).hexdigest(size // 2)
                key = stage * 10 + offset
                declarations.append("DECLARE %s AS Utf8;" % parameter)
                parameters[parameter] = (value, ydb.PrimitiveType.Utf8.proto)
                statements.append("UPSERT INTO `%s` (key, value) VALUES (%su, %s);" %
                                  (table, key, parameter))
                values[key] = value
            updates[table] = values
        # One query transaction writes all three tables, exercising the
        # surviving root coordinator as well as the migrated DataShards.
        self._sql(pool, "\n".join(declarations + statements), parameters, transaction=True)
        for table, values in updates.items():
            expected[table].update(values)
        logger.info("Committed cross-table SQL transaction at stage %s: 4KiB + 1MiB per table", stage)

    def _sql_assert_rows(self, pool, expected):
        result = self._sql(pool, "\n".join(
            "SELECT key, value FROM `%s` ORDER BY key;" % table for table in expected))
        self.assertEqual(len(result), len(expected))
        for (table, rows), actual in zip(expected.items(), result):
            values = {row["key"]: row["value"] for row in actual.rows}
            self.assertEqual(set(values), set(rows), table)
            for key, value in rows.items():
                self.assertTrue(values[key] == value, "Payload mismatch in %s key=%s" % (table, key))
        logger.info("Exact SQL payloads verified: %s", {table: len(rows) for table, rows in expected.items()})

    def _scenario_user_tablets_migrate_from_lost_dc_and_resume_sql(self):
        self._run_user_tablet_failover()

    def _assert_running_binary(self, alive_nodes, binary):
        actual_alive = {node_id for node_id, node in self.cluster.nodes.items() if node.is_alive()}
        self.assertEqual(actual_alive, set(alive_nodes))
        processes = {}
        for node_id in sorted(alive_nodes):
            node = self.cluster.nodes[node_id]
            self.assertEqual(Path(node.binary_path).resolve(), binary)
            executable = Path("/proc/%s/exe" % node.pid)
            self.assertTrue(executable.samefile(binary), "Node %s runs %s, expected %s" %
                            (node_id, executable.resolve(), binary))
            processes[node_id] = node.pid
        logger.info("Verified %s running processes from %s: node->pid=%s; stopped=%s",
                    len(processes), binary, processes, sorted(set(self.cluster.nodes) - set(alive_nodes)))
        return processes

    def _upgrade_survivors_after_dc_loss(self):
        logger.info("UPGRADE: both other DCs are already dead; replace only the three surviving processes")
        old_pids = self._assert_running_binary(set(self.survivors), self.old_binary)
        disks = {}
        for path, node_id in self.pdisk_paths.items():
            if node_id in self.survivors:
                stat = Path(path).stat()
                disks[path] = (stat.st_dev, stat.st_ino, stat.st_size)
        self.assertTrue(disks)
        # Never call cluster.start() here: it would restart all nine nodes and
        # run the initial PDisk formatting path. Reuse the existing node objects.
        for node_id in [node for node in self.survivors if node != 1] + [1]:
            self.cluster.nodes[node_id].stop()
        self.assertFalse(any(node.is_alive() for node in self.cluster.nodes.values()))
        for node_id in self.survivors:
            self.cluster.nodes[node_id].binary_path = str(self.new_binary)
            self._restart(node_id)
        new_pids = self._assert_running_binary(set(self.survivors), self.new_binary)
        for node_id in self.survivors:
            self.assertNotEqual(old_pids[node_id], new_pids[node_id])
        for path, identity in disks.items():
            stat = Path(path).stat()
            self.assertEqual((stat.st_dev, stat.st_ino, stat.st_size), identity, path)
        # Ordinary mirror-3-dc cannot become READY with two dead DCs. Only
        # wait for the independent management group before enabling recovery.
        group = self._wait("BSC after upgrading the surviving DC", self._group, timeout=180)
        self.assertFalse(group.EnableSingleDcMode, str(group))
        logger.info("UPGRADE: new binaries reopened the same PDisk files; emergency mode is still disabled")

    def _scenario_upgrade_after_two_lost_dcs_and_resume_sql(self):
        # Opt-in: --test-param=single_dc_old_binary=/absolute/path/to/old/ydbd
        pool, expected, tablet_ids, before = self._run_user_tablet_failover(
            after_dc_loss=self._upgrade_survivors_after_dc_loss)
        logger.info("UPGRADE: cold restart the recovered DC to verify persisted mode and user data")
        for node_id in [node for node in self.survivors if node != 1] + [1]:
            self._crash(node_id)
        for node_id in self.survivors:
            self._restart(node_id)
        self._assert_running_binary(set(self.survivors), self.new_binary)
        group = self._wait("BSC after the upgraded DC's cold restart", self._group, timeout=180)
        self.assertTrue(group.EnableSingleDcMode, str(group))
        self.assertEqual(group.SurvivingDc, self.realm)
        self._wait_ready(set(self.survivors))
        self._wait_tablets(tablet_ids, set(self.survivors), previous=before)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=4)
        self._sql_assert_rows(pool, expected)
        logger.info("PASS: old binary -> lose two DCs -> upgrade remaining DC -> recover tablets and SQL; "
                    "mode and old/new data survive another cold restart with the six lost nodes still down")

    def _run_user_tablet_failover(self, after_dc_loss=None):
        logger.info("SQL STAGE 1: create three user tables on mirror-3-dc storage")
        self.cluster.client.bind_storage_pools("Root", {"single-dc-recovery": "hdd"})
        node = self.cluster.nodes[1]
        HiveClient(node.host, node.mon_port).set_min_scatter_to_balance()
        driver = ydb.Driver(endpoint=node.endpoint, database="/Root")
        self.addCleanup(driver.stop)
        driver.wait(timeout=20, fail_fast=True)
        pool = ydb.QuerySessionPool(driver)
        self.addCleanup(pool.stop)
        expected = {"failover_%s" % index: {} for index in range(3)}
        for table in expected:
            self._sql(pool, """
CREATE TABLE `%s` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key))
WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = DISABLED);
""" % table)
        def created_tablets():
            tablets = self.cluster.client.tablet_state(tablet_type=TabletTypes.FLAT_DATASHARD).TabletStateInfo
            ids = sorted({tablet.TabletId for tablet in tablets})
            return ids if len(ids) == 3 else None

        tablet_ids = self._wait("three table DataShards", created_tablets)
        initial = self._wait_tablets(tablet_ids, set(range(1, 10)))

        # All user-tablet processes must actually disappear in the disaster.
        # Keep their storage on the dynamic group; only the management tablets
        # live on the independent, surviving erasure-none group.
        slots = [slot for slot in self._base().VSlot if slot.GroupId == self.group_id]
        lost_realm = next(slot.FailRealmIdx for slot in slots if slot.FailRealmIdx != self.realm)
        lost_hosts = sorted(slot.VSlotId.NodeId for slot in slots if slot.FailRealmIdx == lost_realm)
        for index, tablet_id in enumerate(tablet_ids):
            self._move_tablet(tablet_id, lost_hosts[index % len(lost_hosts)])
        before = self._wait_tablets(tablet_ids, set(lost_hosts), previous=initial)
        self._sql_write(pool, expected, stage=0)
        self._sql_assert_rows(pool, expected)

        logger.info("SQL STAGE 2: kill both other DCs, including every user-tablet host")
        for node_id in self.lost:
            self._crash(node_id)
        self.assertTrue(all(item["node"] in self.lost for item in before.values()))
        if after_dc_loss is not None:
            after_dc_loss()
        self._enable_single_dc()
        migrated = self._wait_tablets(tablet_ids, set(self.survivors), previous=before)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=1)
        self._sql_assert_rows(pool, expected)

        logger.info("SQL STAGE 3: keep user tablets live on node 1, write with only 2/3 local VDisks")
        # Complete bootstrap at 3/3 before stopping a storage peer: Discover
        # has a stronger quorum than PUT in emergency single-DC mode.
        # BSC's READY status can precede reconnection of an existing DSProxy's
        # queues after the mode change. Probe each survivor without restarting
        # it; a proxy that cannot recover will fail this bounded readiness check.
        for node_id in self.survivors:
            self._wait_proxy(tablet_ids[0], node_id)
        for tablet_id in tablet_ids:
            self._move_tablet(tablet_id, 1)
        colocated = self._wait_tablets(tablet_ids, {1}, previous=migrated)
        peer = next(node_id for node_id in self.survivors if node_id != 1)
        self._crash(peer)
        self._sql_write(pool, expected, stage=2)
        self._sql_assert_rows(pool, expected)
        self._restart(peer)
        self._wait_ready(set(self.survivors))

        logger.info("SQL STAGE 4: reboot all user tablets inside surviving DC and read persisted rows")
        self._wait_proxy(tablet_ids[0], peer)
        for tablet_id in tablet_ids:
            self._move_tablet(tablet_id, peer)
        rebooted = self._wait_tablets(tablet_ids, {peer}, previous=colocated)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=3)
        self._sql_assert_rows(pool, expected)
        self.assertTrue(all(not self.cluster.nodes[node_id].is_alive() for node_id in self.lost))
        logger.info("PASS: all three user tablets migrated from a dead DC; SQL transactions, 2/3 writes, "
                    "and persisted payloads after another tablet reboot work with six nodes still down")
        return pool, expected, tablet_ids, rebooted

    def _scenario_recover_two_lost_dcs_and_require_two_replicas(self):
        logger.info("STAGE 1: ordinary mirror-3-dc data, then stop its writer without GC")
        old_writer = self._start_writer(55826001, 1024 * 1024)
        samples = [self._sample(old_writer)]
        self._crash(1)
        self._restart(1)
        self._wait_ready(set(range(1, 10)))
        self._wait("baseline persisted blob", lambda: self._read(samples[0][0]) == samples[0][1])
        # MustRestoreFirst drives restoration before the simulated DC loss.
        self._assert_samples(samples)

        logger.info("STAGE 2: six processes lost; ordinary group cannot answer Range")
        for node_id in self.lost:
            self._crash(node_id)
        unavailable = self._range(old_writer["tablet"])
        self.assertNotEqual(unavailable["Status"], "OK", unavailable)

        logger.info("STAGE 3: enable emergency mode and recover existing payload")
        self._enable_single_dc()
        self._wait("old data in the surviving DC", lambda: self._read(samples[0][0]) == samples[0][1])
        self._assert_samples(samples)
        self.assertEqual(self._range(old_writer["tablet"])["Status"], "OK")

        # Bootstrap while all three surviving VDisks are up: Discover/Range
        # retain a stronger requirement than PUT in this emergency mode.
        writers = [self._start_writer(55826002, 4096), self._start_writer(55826003, 1024 * 1024)]
        others = [node for node in self.survivors if node != 1]

        logger.info("STAGE 4: stop one local replica; small and huge PUTs continue at 2/3")
        self._crash(others[0])
        for writer in writers:
            start = self._writer_state(writer)["OkPutResults"]
            state = self._wait("six new successful PUTs with 2/3 replicas",
                               lambda writer=writer, start=start: self._writer_has_puts(writer, start + 6))
            logger.info("2/3 tablet=%s size=%s PUT counter %s -> %s",
                        writer["tablet"], writer["size"], start, state["OkPutResults"])
            samples.append(self._sample(writer))
        self._assert_samples(samples)

        logger.info("STAGE 5: stop a second local replica; both active writers must get PUT errors")
        self._crash(others[1])
        for writer in writers:
            def failed(writer=writer):
                state = self._writer_state(writer)
                return state if state.get("BadPutResults", 0) > 0 else None
            state = self._wait("explicit failed PUT with only 1/3 replicas", failed)
            self.assertGreater(state["OkPutResults"], 0, state)
            logger.info("1/3 tablet=%s final report=%s", writer["tablet"], state)

        logger.info("STAGE 6: return to 3/3, verify data and index, then cold restart all surviving disks")
        for node_id in others:
            self._restart(node_id)
        self._wait_ready(set(self.survivors))
        self._wait("old data after restoring local replicas", lambda: self._read(samples[0][0]) == samples[0][1])
        self._assert_samples(samples)
        for writer, (blob, _) in zip([old_writer] + writers, samples):
            result = self._range(writer["tablet"])
            self.assertEqual(result["Status"], "OK", result)
            self.assertIn(blob, result["Blobs"], result)

        for node_id in others + [1]:
            self._crash(node_id)
        for node_id in self.survivors:
            self._restart(node_id)
        self._wait_ready(set(self.survivors))
        group = self._group()
        self.assertTrue(group.EnableSingleDcMode, str(group))
        self.assertEqual(group.SurvivingDc, self.realm)
        self._wait("cold-start read", lambda: self._read(samples[0][0]) == samples[0][1])
        self._assert_samples(samples)
        self.assertTrue(all(not self.cluster.nodes[node].is_alive() for node in self.lost))
        logger.info("PASS: old/new payloads survive cold restart with six original nodes still down")


class TestSingleDcRecovery(SingleDcRecoveryFixture, unittest.TestCase):
    def test_user_tablets_migrate_from_lost_dc_and_resume_sql(self):
        self._scenario_user_tablets_migrate_from_lost_dc_and_resume_sql()

    def test_upgrade_after_two_lost_dcs_and_resume_sql(self):
        self._scenario_upgrade_after_two_lost_dcs_and_resume_sql()

    def test_recover_two_lost_dcs_and_require_two_replicas(self):
        self._scenario_recover_two_lost_dcs_and_require_two_replicas()


def _static_service_set(yaml_config):
    config = yaml_config.get("config", yaml_config)
    if config.get("self_management_config", {}).get("enabled", False):
        raise ValueError("This recovery fixture requires legacy explicit YAML, not self-management")
    return config["blob_storage_config"]["service_set"]


def _static_group(service_set, group_id):
    groups = [group for group in service_set["groups"] if int(group["group_id"]) == group_id]
    if len(groups) != 1:
        raise ValueError("Expected exactly one static group %s, found %s" % (group_id, len(groups)))
    group = groups[0]
    if group["erasure_species"] not in (9, "mirror-3-dc"):
        raise ValueError("Static group must use mirror-3-dc")
    if group.get("enable_single_dc_mode", False):
        raise ValueError("Literal static relocation requires ordinary mirror-3-dc mode")
    if len(group["rings"]) != 3 or any(
        len(ring["fail_domains"]) != 3
        or any(len(domain["vdisk_locations"]) != 1 for domain in ring["fail_domains"])
        for ring in group["rings"]
    ):
        raise ValueError("Static group must have geometry 3x3x1")
    return group


def _static_location_key(location):
    return tuple(int(location[key]) for key in ("node_id", "pdisk_id", "vdisk_slot_id"))


def enumerate_static_slots(yaml_config, group_id=0):
    """Return nine slots in group order (realm * 3 + domain), validating both maps."""
    service_set = _static_service_set(yaml_config)
    group = _static_group(service_set, group_id)
    generation = int(group["group_generation"])
    pdisks = {}
    for pdisk in service_set["pdisks"]:
        key = (int(pdisk["node_id"]), int(pdisk["pdisk_id"]))
        if key in pdisks:
            raise ValueError("Duplicate explicit PDisk: %s" % (key,))
        pdisks[key] = pdisk

    records = {}
    occupied = set()
    for vdisk in service_set["vdisks"]:
        location = vdisk["vdisk_location"]
        key = _static_location_key(location)
        if key in occupied:
            raise ValueError("Duplicate explicit VSlot: %s" % (key,))
        occupied.add(key)
        vdisk_id = vdisk["vdisk_id"]
        if int(vdisk_id["group_id"]) != group_id:
            continue
        position = tuple(int(vdisk_id[key]) for key in ("ring", "domain", "vdisk"))
        if position in records:
            raise ValueError("Duplicate static logical position: %s" % (position,))
        if int(vdisk_id["group_generation"]) != generation:
            raise ValueError("Static group and VDisk generations disagree")
        records[position] = location
    if set(records) != {(realm, domain, 0) for realm in range(3) for domain in range(3)}:
        raise ValueError("Explicit static VDisks do not match geometry 3x3x1")

    slots = []
    for realm, ring in enumerate(group["rings"]):
        for domain, fail_domain in enumerate(ring["fail_domains"]):
            location = fail_domain["vdisk_locations"][0]
            node_id, pdisk_id, slot_id = _static_location_key(location)
            record = records[(realm, domain, 0)]
            if _static_location_key(record) != (node_id, pdisk_id, slot_id):
                raise ValueError("Group and VDisk location maps disagree")
            pdisk = pdisks[(node_id, pdisk_id)]
            if not pdisk.get("path"):
                raise ValueError("Explicit PDisk has no persistent path")
            if len({int(item["pdisk_guid"]) for item in (location, record, pdisk)}) != 1:
                raise ValueError("Group, VDisk and PDisk GUIDs disagree")
            slots.append(dict(group_id=group_id, node_id=node_id, pdisk_id=pdisk_id,
                              slot_id=slot_id, realm=realm, domain=domain, order=realm * 3 + domain))
    return slots


def relocate_static_group(yaml_config, survivor_node_ids, group_id=0):
    """Keep the surviving realm in place and move other realms to fresh PDisks.

    Each lost (realm, domain) goes to the retained node for the same domain.
    YAML requires distinct PDisks for the positions of a single static group.
    The new paths are adjacent to the old temporary disk files; the caller must
    create only these six new files and leave every existing disk untouched.
    The caller must have stopped all remaining processes before installing this
    complete configuration, and must restart existing disks without formatting.
    The caller must check new PDisk IDs against the pre-disaster BSC config.
    """
    survivors = set(map(int, survivor_node_ids))
    before = enumerate_static_slots(yaml_config, group_id)
    retained = [slot for slot in before if slot["node_id"] in survivors]
    if len(survivors) != 3 or len(retained) != 3 or {slot["node_id"] for slot in retained} != survivors:
        raise ValueError("Expected exactly one retained static VDisk on each of three survivors")
    realms = {slot["realm"] for slot in retained}
    if len(realms) != 1 or {slot["domain"] for slot in retained} != {0, 1, 2}:
        raise ValueError("Surviving static VDisks must form one complete logical realm")
    surviving_realm = next(iter(realms))

    result = deepcopy(yaml_config)
    service_set = _static_service_set(result)
    group = _static_group(service_set, group_id)
    generation = int(group["group_generation"]) + 1
    if generation >= 2 ** 32:
        raise ValueError("Static group generation overflows uint32")
    source_locations = {
        domain: deepcopy(fail_domain["vdisk_locations"][0])
        for domain, fail_domain in enumerate(group["rings"][surviving_realm]["fail_domains"])
    }
    occupied = {_static_location_key(vdisk["vdisk_location"]) for vdisk in service_set["vdisks"]}
    original_pdisks = list(service_set["pdisks"])
    by_id = {(int(pdisk["node_id"]), int(pdisk["pdisk_id"])): pdisk for pdisk in original_pdisks}
    replacements = {}
    lost_realms = [realm for realm in range(3) if realm != surviving_realm]
    for pdisk_id, realm in enumerate(lost_realms, start=2):
        for domain in range(3):
            location = deepcopy(source_locations[domain])
            source = by_id[(int(location["node_id"]), int(location["pdisk_id"]))]
            if (int(location["node_id"]), pdisk_id) in by_id:
                raise ValueError("Replacement PDisk ID is occupied")
            target = deepcopy(source)
            target.update(pdisk_id=pdisk_id, pdisk_guid=pdisk_id,
                          path=source["path"] + ".recovery-%s" % pdisk_id)
            service_set["pdisks"].append(target)
            location.update(pdisk_id=pdisk_id, pdisk_guid=pdisk_id, vdisk_slot_id=0)
            key = _static_location_key(location)
            if key in occupied:
                raise ValueError("Replacement static slot is already occupied: %s" % (key,))
            occupied.add(key)
            replacements[(realm, domain)] = location
            group["rings"][realm]["fail_domains"][domain]["vdisk_locations"][0] = deepcopy(location)

    group["group_generation"] = generation
    for vdisk in service_set["vdisks"]:
        vdisk_id = vdisk["vdisk_id"]
        if int(vdisk_id["group_id"]) == group_id:
            vdisk_id["group_generation"] = generation
            position = (int(vdisk_id["ring"]), int(vdisk_id["domain"]))
            if position in replacements:
                vdisk["vdisk_location"] = deepcopy(replacements[position])

    after = enumerate_static_slots(result, group_id)
    if any(slot["node_id"] not in survivors for slot in after):
        raise ValueError("Static relocation left a VDisk outside the surviving DC")
    if [slot for slot in after if slot["realm"] == surviving_realm] != retained:
        raise ValueError("Static relocation changed a retained VDisk location")
    if service_set["pdisks"][:len(original_pdisks)] != _static_service_set(yaml_config)["pdisks"]:
        raise ValueError("Static relocation changed a PDisk identity or path")
    return result


class TwoDcCopyVerificationMixin:
    @staticmethod
    def _copy_blob_fields(blob_id):
        match = re.fullmatch(r"\[(\d+):(\d+):(\d+):(\d+):(\d+):(\d+):(\d+)\]", blob_id)
        if not match:
            raise AssertionError("Invalid LogoBlobID: %r" % blob_id)
        return tuple(map(int, match.groups()))

    @classmethod
    def _copy_full_id(cls, blob_id):
        fields = cls._copy_blob_fields(blob_id)
        return "[%s]" % ":".join(map(str, fields[:-1] + (0,)))

    def _copy_url(self, node_id, path):
        node = self.cluster.nodes[node_id]
        return "http://%s:%s%s" % (node.host, node.mon_port, path)

    def _copy_json(self, session, node_id, path, params):
        response = session.get(self._copy_url(node_id, path), params=params, timeout=(3, 15))
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get("error_msg") or result.get("Error"):
            raise AssertionError("%s node=%s: %r" % (path, node_id, result))
        return result

    def _copy_snapshot(self, group_id, slots, directory, blobsan_path):
        """Read every stream to its explicit empty terminator, then analyze files."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)

        def fetch_slot(slot):
            path = directory / ("vdisk-%d.snapshot" % slot["order"])
            chunks = 0
            stream_actor = None
            raw_bytes = 0
            deadline = time.monotonic() + 120
            session_id = str(uuid.uuid4())
            with requests.Session() as session, path.open("wb") as output:
                while True:
                    if time.monotonic() > deadline:
                        raise AssertionError("Snapshot deadline: %r" % slot)
                    response = session.get(self._copy_url(slot["node_id"], "/vdisk_stream"), params={
                        "pdiskId": slot["pdisk_id"], "vdiskSlotId": slot["slot_id"], "sessionId": session_id,
                    }, timeout=(3, 20))
                    response.raise_for_status()
                    data = response.content
                    if not data:
                        break  # valid stream EOF, including an empty VDisk
                    if data == b"ERROR":
                        raise AssertionError("VDisk stream returned ERROR: %r" % slot)
                    payload = bz2.decompress(data)
                    if len(payload) < 32:
                        raise AssertionError("Truncated snapshot header: %r" % slot)
                    actor1, actor2, sequence, blocks = struct.unpack_from("<QQQQ", payload)
                    actor = (actor1, actor2)
                    if stream_actor is not None and actor != stream_actor:
                        raise AssertionError("Snapshot actor changed: %r" % slot)
                    stream_actor = actor
                    if sequence != chunks + 1 or not blocks:
                        raise AssertionError("Snapshot sequence/blocks invalid: %r %r" % (slot, (sequence, blocks)))
                    output.write(struct.pack("<III", group_id, slot["order"], len(data)))
                    output.write(data)
                    chunks += 1
                    raw_bytes += len(data)
            return dict(slot, chunks=chunks, bytes=raw_bytes, completed=True, file=str(path))

        with ThreadPoolExecutor(max_workers=6) as executor:
            streams = list(executor.map(fetch_slot, slots))
        self.assertEqual(len(streams), 6)
        snapshot = directory / "group.snapshot"
        with snapshot.open("wb") as output:
            for stream in sorted(streams, key=lambda item: item["order"]):
                with Path(stream["file"]).open("rb") as source:
                    shutil.copyfileobj(source, output)
        (directory / "streams.json").write_text(json.dumps(streams, indent=2))
        stdout_path, stderr_path = directory / "blobsan.stdout", directory / "blobsan.stderr"
        with snapshot.open("rb") as source, stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            result = subprocess.run([str(blobsan_path), "mirror-3-dc"], stdin=source,
                                    stdout=stdout, stderr=stderr, timeout=120, check=False)
        self.assertEqual(result.returncode, 0, "blobsan failed; inspect %s" % stderr_path)
        blob_ids, observed_orders = set(), set()
        record_pattern = re.compile(r"^(\d+) (\d+) A .*?Key# (\[[^\]]+\]) Ingress#")
        with stdout_path.open() as source:
            for line in source:
                match = record_pattern.search(line)
                if match:
                    self.assertEqual(int(match[1]), group_id)
                    observed_orders.add(int(match[2]))
                    blob_ids.add(self._copy_full_id(match[3]))
        candidates = set()
        with stderr_path.open() as source:
            for line in source:
                match = re.search(r"GroupId#\s+(\d+)\s+BlobId#\s+(\[[^\]]+\])", line)
                if match:
                    self.assertEqual(int(match[1]), group_id)
                    candidates.add(self._copy_full_id(match[2]))
        self.assertTrue(blob_ids, "Snapshot/analyzer contained no LogoBlob records")
        self.assertTrue(candidates <= blob_ids, "Analyzer candidates absent from its stdout")
        self.assertTrue(observed_orders <= {slot["order"] for slot in slots})
        report = {"group_id": group_id, "blob_ids": sorted(blob_ids), "candidates": sorted(candidates),
                  "streams": streams, "observed_logo_orders": sorted(observed_orders)}
        (directory / "inventory.json").write_text(json.dumps(report, indent=2))
        logger.info("Group %s snapshot: complete streams=%s blobs=%s candidates=%s",
                         group_id, len(streams), len(blob_ids), len(candidates))
        return report

    def _copy_restore_blob(self, session, node_id, group_id, blob_id):
        """Same request as alexvru/recovery.py, with explicit routing and JSON parsing."""
        last = None
        for attempt in range(3):
            try:
                result = self._copy_json(session, node_id, "/blob_range", {
                    "groupId": group_id, "tabletId": self._copy_blob_fields(blob_id)[0],
                    "from": blob_id, "to": blob_id, "mustRestoreFirst": 1, "json": 1,
                })
                if result.get("Status") == "OK":
                    blobs = result.get("Blobs")
                    if blobs not in ([], [blob_id]):
                        raise AssertionError("Unexpected exact Range result: %r" % result)
                    return bool(blobs), result
                last = result
            except requests.RequestException as exc:
                last = repr(exc)
            if attempt != 2:
                time.sleep(0.5)
        raise AssertionError("Restore failed group=%s blob=%s: %r" % (group_id, blob_id, last))

    def _copy_local_blob(self, session, slot, blob_id):
        """Direct VDisk read, not a group read; persist hashes rather than payloads."""
        result = self._copy_json(session, slot["node_id"], "/vdisk/json/getblob", {
            "node_id": slot["node_id"], "pdisk_id": slot["pdisk_id"], "vslot_id": slot["slot_id"],
            "from": blob_id, "to": blob_id, "internals": 1, "need_data": 1,
        })
        if "logoblobs" not in result:
            raise AssertionError("Missing local VGet results: %r" % result)
        parts = []
        for item in result["logoblobs"]:
            if self._copy_full_id(item["id"]) != blob_id:
                raise AssertionError("Unexpected local blob: %r" % item)
            status = item["status"]
            part = {"status": status, "id": item["id"], "ingress": item.get("ingress")}
            if status == "OK":
                data = base64.b64decode(item.get("data_base64", ""), validate=True)
                expected_size = self._copy_blob_fields(blob_id)[5]
                if len(data) != expected_size:
                    raise AssertionError("Local payload size %s != %s: %r" % (len(data), expected_size, item["id"]))
                part_id = self._copy_blob_fields(item["id"])[6]
                subgroup = re.search(r"nodeId:\s*(\d+)", item.get("ingress", ""))
                if subgroup is None or part_id != int(subgroup[1]) % 3 + 1:
                    raise AssertionError("Noncanonical local part: %r" % part)
                part.update(size=len(data), sha256=hashlib.sha256(data).hexdigest())
            elif status not in ("NODATA", "NOT_YET"):
                raise AssertionError("Local VGet error slot=%r result=%r" % (slot, item))
            parts.append(part)
        return dict(slot, parts=parts)

    def _restore_and_verify_group_copies(self, group_id, live_slots, label, blobsan_path,
                                        required_blobs=(), workers=4):
        """Restore every snapshot ID; prove >=2 physical copies in both live DCs.

        Return a JSON-serializable manifest. A Range OK/empty is explicitly recorded
        as absent/collected, never counted as restored. Concurrent GC of a restored
        blob needs a second successful empty Range. Required blobs cannot disappear;
        pass a mapping ID -> bytes (or SHA256 hex) to also check a known payload.
        """
        slots = [dict(slot) for slot in live_slots]
        self.assertEqual(len(slots), 6)
        self.assertEqual(len({slot["order"] for slot in slots}), 6)
        self.assertEqual(len({(slot["node_id"], slot["pdisk_id"], slot["slot_id"]) for slot in slots}), 6)
        realms = {slot["realm"] for slot in slots}
        self.assertEqual(len(realms), 2)
        for realm in realms:
            self.assertEqual({slot["domain"] for slot in slots if slot["realm"] == realm}, {0, 1, 2})
        for slot in slots:
            self.assertTrue(self.cluster.nodes[slot["node_id"]].is_alive(), slot)
        directory = Path(self.artifacts) / ("copies-%s-group-%s" % (label, group_id))
        directory.mkdir(parents=True, exist_ok=False)
        inventory = self._copy_snapshot(group_id, slots, directory / "before", blobsan_path)
        required = {self._copy_full_id(blob) for blob in required_blobs}
        expected_hashes = {}
        if isinstance(required_blobs, dict):
            for blob, value in required_blobs.items():
                if value is not None:
                    expected_hashes[self._copy_full_id(blob)] = (hashlib.sha256(value).hexdigest()
                                                              if isinstance(value, bytes) else value)
        self.assertTrue(required <= set(inventory["blob_ids"]), "Required blobs missing from full snapshot")
        deadline = time.monotonic() + 900

        def restore_and_check(blob_id):
            if time.monotonic() > deadline:
                raise AssertionError("Group copy verification deadline exceeded")
            node_id = slots[0]["node_id"]
            with requests.Session() as session:
                present, response = self._copy_restore_blob(session, node_id, group_id, blob_id)
                if not present:
                    if blob_id in required:
                        raise AssertionError("Required blob disappeared before restoration: %s" % blob_id)
                    return {"blob_id": blob_id, "outcome": "range_absent_before_cohort", "range": response}
                history = []
                for attempt in range(3):
                    local = [self._copy_local_blob(session, slot, blob_id) for slot in slots]
                    copies = [item for item in local if any(part["status"] == "OK" for part in item["parts"])]
                    hashes = {part["sha256"] for item in copies for part in item["parts"] if part["status"] == "OK"}
                    if len(hashes) > 1:
                        raise AssertionError("Copies disagree for %s: %r" % (blob_id, local))
                    if hashes and blob_id in expected_hashes and hashes != {expected_hashes[blob_id]}:
                        raise AssertionError("Known payload mismatch for %s: %r" % (blob_id, hashes))
                    counts = {realm: sum(item["realm"] == realm for item in copies) for realm in realms}
                    history.append({"counts": counts, "local": local})
                    if all(count >= 2 for count in counts.values()):
                        return {"blob_id": blob_id, "outcome": "verified_2x2", "sha256": next(iter(hashes)),
                                "size": self._copy_blob_fields(blob_id)[5], "counts": counts, "local": local}
                    present, response = self._copy_restore_blob(session, node_id, group_id, blob_id)
                    if not present:
                        if blob_id in required:
                            raise AssertionError("Required blob disappeared after restoration: %s" % blob_id)
                        return {"blob_id": blob_id, "outcome": "range_absent_after_restoration",
                                "range": response, "local_history": history}
                    if attempt != 2:
                        time.sleep(0.5)
                raise AssertionError("Missing physical 2x2 after successful restore: %s %r" % (blob_id, history))

        results, failures = [], []
        ids = inventory["blob_ids"]
        with (directory / "copies.jsonl").open("w") as output, ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {executor.submit(restore_and_check, blob): blob for blob in ids}
            for future in as_completed(pending):
                blob = pending[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"blob_id": blob, "outcome": "failure", "error": repr(exc)}
                    failures.append(result)
                results.append(result)
                output.write(json.dumps(result, sort_keys=True) + "\n")
                output.flush()
                if len(results) % 100 == 0 or len(results) == len(ids):
                    logger.info("Group %s copy verification: %s/%s, failures=%s",
                                     group_id, len(results), len(ids), len(failures))
        verified = {item["blob_id"]: item for item in results if item["outcome"] == "verified_2x2"}
        counts = {outcome: sum(item["outcome"] == outcome for item in results) for outcome in
                  ("verified_2x2", "range_absent_before_cohort", "range_absent_after_restoration", "failure")}
        summary = {"group_id": group_id, "inventory_count": len(ids), "candidate_count": len(inventory["candidates"]),
                   "counts": counts, "required_blobs": sorted(required), "verified": verified, "slots": slots}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2))
        self.assertFalse(failures, "Group %s copy failures: %r; artifact=%s" % (group_id, failures[:5], directory))
        self.assertTrue(verified, "No live blob was independently verified")
        self.assertTrue(required <= set(verified), "Required blobs not verified")
        after = self._copy_snapshot(group_id, slots, directory / "after", blobsan_path)
        summary["after_candidate_count"] = len(after["candidates"])
        summary["verified_candidates_after"] = sorted(set(after["candidates"]) & set(verified))
        (directory / "summary.json").write_text(json.dumps(summary, indent=2))
        logger.info("Group %s COPY PROOF: %s; after candidates=%s", group_id, counts, len(after["candidates"]))
        return summary


class StateStorageDisasterHelpers:
    def _config_for_surviving_state_storage(self, full_config, survivors):
        import copy

        result = copy.deepcopy(full_config)
        config = result.get("config", result)
        nodes = sorted(set(survivors))
        self.assertEqual(len(nodes), 3, nodes)
        domains = config["domains_config"]
        self.assertFalse(any(name.startswith("explicit_") and "storage" in name
                             for name in domains), domains)
        self.assertNotIn("explicit_scheme_board_config", domains)
        storage = domains["state_storage"]
        self.assertEqual(len(storage), 1, storage)
        self.assertEqual(storage[0].get("ssid", 1), 1, storage)
        self.assertNotIn("ring_groups", storage[0])
        # This legacy field configures SS, StateStorageBoard and SchemeBoard.
        storage[0]["ring"] = {
            "nto_select": 3,
            "ring": [{"node": [node], "use_ring_specific_node_selection": True}
                     for node in nodes],
        }
        tablets = config["system_tablets"]
        tablets["default_node"] = nodes[:]
        for entries in tablets.values():
            if isinstance(entries, list):
                for tablet in entries:
                    if isinstance(tablet, dict):
                        tablet["node"] = nodes[:]
        # Some configs contain normalized bootstrap records as well.
        for tablet in config.get("bootstrap_config", {}).get("tablet", []):
            tablet["node"] = nodes[:]
        return result

    def _configured_system_tablets(self, full_config):
        config = full_config.get("config", full_config)
        normalized = config.get("bootstrap_config", {}).get("tablet", [])
        if normalized:
            result = {int(tablet["info"]["tablet_id"]): str(tablet["type"]) for tablet in normalized}
            self.assertEqual(len(result), len(normalized), normalized)
            return result
        # The regular harness uses legacy bootstrappers: /actors/bootstrapper
        # requires EnableConfiguredBootstrapper, which defaults to false. Resolve
        # the fixed YAML defaults instead, then independently require each real
        # tablet to appear Active in whiteboard. Constants and counts are from
        # yaml_config/core_constants.h and core/base/domain.h::Make*IDFixed.
        defaults = {
            "flat_hive": (0xA001, 1),
            "flat_bs_controller": (0x1001, 1),
            "flat_schemeshard": (0x8587A0, 1),
            "flat_tx_coordinator": (0x800000, 3),
            "tx_mediator": (0x810000, 3),
            "tx_allocator": (0x820000, 3),
            "cms": (0x2000, 1),
            "node_broker": (0x2001, 1),
            "tenant_slot_broker": (0x2002, 1),
            "console": (0x2003, 1),
        }
        tablets = config["system_tablets"]
        if tablets.get("dbs_controller"):
            defaults["dbs_controller"] = (0x2004, 1)
        self.assertFalse(set(tablets) - set(defaults) - {"default_node"}, tablets)
        result = {}
        for name, (base, count) in defaults.items():
            entries = tablets.get(name) or [{} for _ in range(count)]
            for index, tablet in enumerate(entries, 1):
                default_id = (1 << 56) | (base + index if count > 1 else base)
                tablet_id = int(tablet.get("info", {}).get("tablet_id", default_id))
                self.assertNotIn(tablet_id, result, tablet)
                result[tablet_id] = name
        return result

    def _wait_system_tablets(self, configured, allowed_nodes, previous=None, stage="system", timeout=240):
        import json

        allowed_nodes = set(allowed_nodes)
        tablet_ids = set(configured)
        self.assertTrue(tablet_ids)
        if previous is not None:
            self.assertEqual(set(previous), tablet_ids)

        def active():
            response = self._get(
                min(allowed_nodes), "/viewer/json/tabletinfo",
                node_id=",".join(map(str, sorted(allowed_nodes))),
                enums="false", ui64="true", fields_required="all", timeout=5000,
            )
            response.raise_for_status()
            payload = response.json()
            (self.artifacts / ("%s-system-tablets.json" % stage)).write_text(
                json.dumps(payload, indent=2, sort_keys=True))
            result = {}
            for item in payload.get("TabletStateInfo", []):
                tablet_id = int(item["TabletId"])
                if tablet_id not in tablet_ids or int(item.get("FollowerId", 0)):
                    continue
                if int(item.get("State", -1)) != 10 or not item.get("Leader", False):
                    continue
                node = int(item.get("NodeId", 0))
                generation = int(item.get("Generation", 0))
                if node not in allowed_nodes or not generation:
                    continue
                if previous is not None and generation <= previous[tablet_id]["generation"]:
                    continue
                # FieldsRequired=all exposes actual storage-info groups. Requiring
                # at least channels 0 and 1 prevents vacuous success on missing data.
                groups = [int(group) for group in item.get("ChannelGroupIDs", [])]
                self.assertGreaterEqual(len(groups), 2, item)
                self.assertEqual(set(groups), {0}, item)
                if previous is not None:
                    self.assertEqual(groups, previous[tablet_id]["channel_groups"], item)
                self.assertNotIn(tablet_id, result, payload)
                result[tablet_id] = dict(node=node, generation=generation, channel_groups=groups)
            return result if set(result) == tablet_ids else None

        return self._wait("all configured system tablets Active on %s (%s), group 0, new generations"
                          % (sorted(allowed_nodes), stage), active, timeout=timeout)

    @staticmethod
    def _state_storage_proto_block(text, name):
        import re

        match = re.search(r"\b" + re.escape(name) + r"\s*\{", text)
        if match is None:
            raise AssertionError("Missing running %s in NodeWarden page" % name)
        start = match.end()
        depth = 1
        for offset in range(start, len(text)):
            depth += (text[offset] == "{") - (text[offset] == "}")
            if not depth:
                return text[start:offset]
        raise AssertionError("Unbalanced running %s in NodeWarden page" % name)

    def _wait_running_state_storage(self, ring_nodes, query_nodes=None, stage="system"):
        import html
        import re

        ring_nodes = sorted(set(ring_nodes))
        query_nodes = sorted(set(query_nodes if query_nodes is not None else ring_nodes))

        def configured():
            for node in query_nodes:
                response = self._get(node, "/actors/nodewarden")
                response.raise_for_status()
                (self.artifacts / ("%s-nodewarden-%s.html" % (stage, node))).write_text(response.text)
                # NodeWarden renders its applied StorageConfig, rather than the
                # input file. All three directories must have moved together.
                text = html.unescape(re.sub(r"<[^>]+>", " ", response.text))
                for name in ("StateStorageConfig", "StateStorageBoardConfig", "SchemeBoardConfig"):
                    block = self._state_storage_proto_block(text, name)
                    selected = [int(value) for value in re.findall(r"\bNToSelect\s*:\s*(\d+)", block)]
                    replicas = [int(value) for value in re.findall(r"\bNode\s*:\s*(\d+)", block)]
                    self.assertEqual(selected, [len(ring_nodes)], (node, name, block))
                    self.assertEqual(sorted(replicas), ring_nodes, (node, name, block))
                    self.assertNotRegex(block, r"\b(?:WriteOnly|IsDisabled)\s*:\s*true")
            return True

        self._wait("applied SS, StateStorageBoard and SchemeBoard on %s (%s)"
                   % (query_nodes, stage), configured, timeout=120)


class TestFullDcRecovery(SingleDcRecoveryFixture, StateStorageDisasterHelpers,
                         TwoDcCopyVerificationMixin, unittest.TestCase):
    def setUp(self):
        old_binary = yatest.common.get_param("single_dc_old_binary")
        tools_dir = yatest.common.get_param("single_dc_alexvru_tools")
        if not old_binary or not tools_dir:
            self.skipTest("Set single_dc_old_binary and single_dc_alexvru_tools for the full disaster drill")
        self.old_binary = Path(old_binary).resolve(strict=True)
        self.blobsan = (Path(tools_dir) / "blobsan").resolve(strict=True)
        self.static_recovery = yatest.common.get_param("single_dc_static_recovery", "single-dc")
        self.assertIn(self.static_recovery, ("single-dc", "relocate"))
        self.artifacts = Path(yatest.common.test_output_path("full_dc_recovery"))
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.http = requests.Session()
        self.addCleanup(self.http.close)
        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC, nodes=9,
            use_in_memory_pdisks=False, dynamic_storage_pools=[],
            state_storage_rings=list(range(1, 10)), n_to_select=9,
            bs_cache_file_path=str(self.artifacts / "node-warden-%n.txt"),
        )
        self.new_binary = Path(config.get_binary_path(0)).resolve(strict=True)
        self.assertNotEqual(self.old_binary, self.new_binary)
        self.assertFalse(filecmp.cmp(str(self.old_binary), str(self.new_binary), shallow=False))
        config.set_binary_paths([str(self.old_binary)])
        # The initial control plane really lives in a DC that will disappear.
        # The first failed DC is 3/6/9, the second is 2/5/8, survivors are 1/4/7.
        self.survivors, self.second_dc, self.first_dc = [1, 4, 7], [2, 5, 8], [3, 6, 9]
        self.lost = sorted(self.first_dc + self.second_dc)
        self._place_bootstrap_tablets(config.yaml_config, self.second_dc)
        self.original_config = deepcopy(config.full_config)
        (self.artifacts / "initial-config.json").write_text(json.dumps(self.original_config, indent=2))
        self.static_slots = enumerate_static_slots(self.original_config)
        self.assertEqual({slot["node_id"] for slot in self.static_slots}, set(range(1, 10)))
        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        node = self.cluster.nodes[1]
        self.bs_client = kikimr_client_factory(node.host, node.grpc_port, retry_count=1, timeout=5)
        self.addCleanup(self.bs_client.close)
        self.pdisk_paths = {item["pdisk_path"]: item["node_id"] for item in config.pdisks_info}
        self._assert_running_binary(set(range(1, 10)), self.old_binary)
        self.cluster.client.update_self_heal(False)
        self.cluster.add_storage_pool(name="/Root:hdd", kind="hdd", erasure=Erasure.MIRROR_3_DC,
                                      num_groups=1)
        base = self._base()
        groups = [group for group in base.Group if group.GroupId != 0]
        self.assertEqual(len(groups), 1, str(base))
        self.group_id = groups[0].GroupId
        self.dynamic_slots = [dict(group_id=slot.GroupId, node_id=slot.VSlotId.NodeId,
                                   pdisk_id=slot.VSlotId.PDiskId, slot_id=slot.VSlotId.VSlotId,
                                   realm=slot.FailRealmIdx, domain=slot.FailDomainIdx,
                                   order=slot.FailRealmIdx * 3 + slot.FailDomainIdx)
                              for slot in base.VSlot if slot.GroupId == self.group_id]
        self.assertEqual(len(self.dynamic_slots), 9)
        self.realm = next(slot["realm"] for slot in self.dynamic_slots if slot["node_id"] == 1)
        self.assertEqual({slot["node_id"] for slot in self.dynamic_slots if slot["realm"] == self.realm},
                         set(self.survivors))
        self.assertFalse(any(pdisk.NodeId in self.survivors and pdisk.PDiskId in (2, 3)
                             for pdisk in base.PDisk), "Static replacement PDisk IDs must be unused")
        self._wait_ready(set(range(1, 10)))
        logger.info("FULL DISASTER: old binary, static group0 and dynamic group%s are mirror-3-dc; "
                    "StateStorage selects9 replicas (quorum5), bootstrap tablet hosts=%s",
                    self.group_id, self.second_dc)

    def _base(self):
        # The normal harness retries each RPC ten times. Keep polling bounded
        # when the entire control plane is deliberately unavailable.
        return self.bs_client.query_base_config().BaseConfig

    @staticmethod
    def _place_bootstrap_tablets(config, nodes):
        tablets = config["system_tablets"]
        tablets["default_node"] = list(nodes)
        for value in tablets.values():
            if isinstance(value, list):
                for tablet in value:
                    if isinstance(tablet, dict):
                        tablet["node"] = list(nodes)

    def _make_sql_data(self):
        self.cluster.client.bind_storage_pools("Root", {"/Root:hdd": "hdd"})
        node = self.cluster.nodes[1]
        HiveClient(node.host, node.mon_port).set_min_scatter_to_balance()
        driver = ydb.Driver(endpoint=node.endpoint, database="/Root")
        self.addCleanup(driver.stop)
        driver.wait(timeout=20, fail_fast=True)
        pool = ydb.QuerySessionPool(driver)
        self.addCleanup(pool.stop)
        expected = {"full_disaster_%s" % index: {} for index in range(3)}
        for table in expected:
            self._sql(pool, """
CREATE TABLE `%s` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key))
WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = DISABLED);
""" % table)
        def created():
            state = self.cluster.client.tablet_state(tablet_type=TabletTypes.FLAT_DATASHARD).TabletStateInfo
            ids = sorted({tablet.TabletId for tablet in state})
            return ids if len(ids) == 3 else None
        tablet_ids = self._wait("three full-disaster DataShards", created)
        initial = self._wait_tablets(tablet_ids, set(range(1, 10)))
        for tablet_id, node_id in zip(tablet_ids, self.second_dc):
            self._move_tablet(tablet_id, node_id)
        before = self._wait_tablets(tablet_ids, set(self.second_dc), previous=initial)
        self._sql_write(pool, expected, stage=0)
        self._sql_assert_rows(pool, expected)
        return pool, expected, tablet_ids, before

    def _wait_static_replicated(self):
        def replicated():
            response = self._get(1, "/viewer/json/vdiskinfo", node_id=",".join(map(str, self.survivors)),
                                 enums="true", ui64="true", timeout=5000)
            response.raise_for_status()
            state = response.json()
            (self.artifacts / "static-vdisk-state.json").write_text(json.dumps(state, indent=2))
            slots = [item for item in state.get("VDiskStateInfo", [])
                     if int(item.get("VDiskId", {}).get("GroupID", -1)) == 0]
            expected = {(slot["realm"], slot["domain"], slot["node_id"], slot["pdisk_id"], slot["slot_id"])
                        for slot in self.relocated_static_slots}
            actual = {(int(item["VDiskId"]["Ring"]), int(item["VDiskId"]["Domain"]),
                       int(item["NodeId"]), int(item["PDiskId"]), int(item["VDiskSlotId"])) for item in slots}
            return slots if (len(slots) == len(expected) and actual == expected and all(
                item.get("Replicated") and item.get("VDiskState") == "OK"
                and int(item["VDiskId"]["GroupGeneration"]) == self.static_generation
                for item in slots)) else None
        result = self._wait("all expected static VDisks replicated", replicated, timeout=240)
        logger.info("Static group0: %s replicated logical VDisks on nodes%s, recovery=%s",
                    len(result), self.survivors, self.static_recovery)
        return result

    def _relocate_control_plane_and_upgrade(self):
        self._assert_running_binary(set(self.survivors), self.old_binary)
        disks = {path: (Path(path).stat().st_dev, Path(path).stat().st_ino, Path(path).stat().st_size)
                 for path, node in self.pdisk_paths.items() if node in self.survivors}
        for node_id in self.survivors:
            self.cluster.nodes[node_id].stop()
        self.assertFalse(any(node.is_alive() for node in self.cluster.nodes.values()))
        if self.static_recovery == "relocate":
            # Diagnostic opt-in: replacing six VDisks at once currently stalls
            # in LostData recovery, which requires a normal multi-DC sync quorum.
            emergency = relocate_static_group(self.original_config, self.survivors)
            self.relocated_static_slots = enumerate_static_slots(emergency)
        else:
            # BSC itself uses group0, so enable static emergency mode in the
            # startup YAML. The dynamic BSC command cannot bootstrap group0.
            emergency = deepcopy(self.original_config)
            service_set = emergency["blob_storage_config"]["service_set"]
            group = service_set["groups"][0]
            retained = [slot for slot in self.static_slots if slot["node_id"] in self.survivors]
            realms = {slot["realm"] for slot in retained}
            self.assertEqual(len(realms), 1)
            group.update(enable_single_dc_mode=True, surviving_dc=next(iter(realms)),
                         group_generation=group["group_generation"] + 1)
            for vdisk in service_set["vdisks"]:
                if vdisk["vdisk_id"]["group_id"] == 0:
                    vdisk["vdisk_id"]["group_generation"] = group["group_generation"]
            self.relocated_static_slots = retained
        emergency = self._config_for_surviving_state_storage(emergency, self.survivors)
        old_pdisks = self.original_config["blob_storage_config"]["service_set"]["pdisks"]
        old_paths = {pdisk["path"] for pdisk in old_pdisks}
        sizes = {pdisk["node_id"]: Path(pdisk["path"]).stat().st_size for pdisk in old_pdisks
                 if pdisk["node_id"] in self.survivors}
        for pdisk in emergency["blob_storage_config"]["service_set"]["pdisks"]:
            if pdisk["path"] not in old_paths:
                # Exclusive creation cannot truncate an existing data file.
                with Path(pdisk["path"]).open("xb") as disk:
                    disk.truncate(sizes[pdisk["node_id"]])
                logger.info("Created empty replacement static PDisk node=%s id=%s path=%s",
                            pdisk["node_id"], pdisk["pdisk_id"], pdisk["path"])
        (self.artifacts / "emergency-config.json").write_text(json.dumps(emergency, indent=2))
        self.static_generation = emergency["blob_storage_config"]["service_set"]["groups"][0]["group_generation"]
        self.cluster.overwrite_configs(emergency)
        for node_id in self.survivors:
            self.cluster.nodes[node_id].binary_path = str(self.new_binary)
            self._restart(node_id)
        self._assert_running_binary(set(self.survivors), self.new_binary)
        for path, identity in disks.items():
            stat = Path(path).stat()
            self.assertEqual((stat.st_dev, stat.st_ino, stat.st_size), identity, path)
        group = self._wait("BSC recovered from group0", self._group, timeout=240)
        self.assertFalse(group.EnableSingleDcMode, str(group))
        self._wait_static_replicated()
        return emergency

    def test_static_and_dynamic_groups_after_sequential_dc_loss(self):
        logger.info("FULL STAGE1: populate SQL and both storage groups on the pre-PR binary")
        pool, expected, tablet_ids, before = self._make_sql_data()
        configured = self._configured_system_tablets(self.original_config)
        self._wait_system_tablets(configured, set(self.second_dc), stage="before")
        self._wait_running_state_storage(range(1, 10), query_nodes=self.survivors, stage="before")
        # Raw probes remain live forever unless GC is explicitly requested.
        # Run their writers on the first lost DC, so SIGKILL stops writes without GC.
        samples = {0: [], self.group_id: []}
        for group_index, group_id in enumerate(samples):
            for offset, size in enumerate((4096, 1024 * 1024)):
                writer = self._start_writer(55826900 + group_index * 10 + offset, size,
                                            group_id=group_id, node_id=self.first_dc[0])
                samples[group_id].append(self._sample(writer))

        logger.info("FULL STAGE2: lose the first DC; explicitly restore old blobs to2+2 in both groups")
        for node_id in self.first_dc:
            self._crash(node_id)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=1)
        live = set(self.survivors + self.second_dc)
        for group_id, slots in ((0, self.static_slots), (self.group_id, self.dynamic_slots)):
            self._restore_and_verify_group_copies(
                group_id, [slot for slot in slots if slot["node_id"] in live],
                "group-%s" % group_id, self.blobsan,
                required_blobs=dict(samples[group_id]),
            )
        self._sql_assert_rows(pool, expected)
        before_second_loss = self._wait_tablets(tablet_ids, set(self.second_dc))
        system_before = self._wait_system_tablets(configured, set(self.second_dc), stage="before-second-loss")

        logger.info("FULL STAGE3: lose the second DC, including system and user tablet hosts")
        for node_id in self.second_dc:
            self._crash(node_id)
        for group_id, group_samples in samples.items():
            blob = group_samples[0][0]
            response = self._get(1, "/blob_range", groupId=group_id, tabletId=blob[1:-1].split(":")[0],
                                 json=1, **{"from": blob, "to": blob})
            response.raise_for_status()
            self.assertNotEqual(response.json().get("Status"), "OK", response.text)
        logger.info("Both ordinary groups are unavailable with one DC; StateStorage has3/9 replicas")

        logger.info("FULL STAGE4: static recovery=%s, relocate StateStorage, upgrade and restart surviving DC",
                    self.static_recovery)
        emergency = self._relocate_control_plane_and_upgrade()
        self._wait_running_state_storage(self.survivors, stage="recovered")
        self.assertEqual(self._configured_system_tablets(emergency), configured)
        self._wait_system_tablets(configured, set(self.survivors), previous=system_before, stage="recovered")
        self._enable_single_dc()
        recovered = self._wait_tablets(tablet_ids, set(self.survivors), previous=before_second_loss)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=2)
        self._sql_assert_rows(pool, expected)
        for group_id, group_samples in samples.items():
            for blob, data in group_samples:
                self.assertEqual(self._read(blob, group_id=group_id), data, blob)

        logger.info("FULL STAGE5: cold restart all three survivors with relocated configuration and persistent mode")
        system_recovered = self._wait_system_tablets(configured, set(self.survivors), stage="before-cold-restart")
        for node_id in self.survivors:
            self._crash(node_id)
        for node_id in self.survivors:
            self._restart(node_id)
        group = self._wait("BSC after cold restart", self._group, timeout=240)
        self.assertTrue(group.EnableSingleDcMode, str(group))
        self.assertEqual(group.SurvivingDc, self.realm)
        self._wait_ready(set(self.survivors))
        self._wait_static_replicated()
        self._wait_running_state_storage(self.survivors, stage="cold-restart")
        self.assertEqual(self._configured_system_tablets(emergency), configured)
        self._wait_system_tablets(configured, set(self.survivors), previous=system_recovered, stage="cold-restart")
        self._wait_tablets(tablet_ids, set(self.survivors), previous=recovered)
        self._sql_assert_rows(pool, expected)
        self._sql_write(pool, expected, stage=3)
        self._sql_assert_rows(pool, expected)
        for group_id, group_samples in samples.items():
            for blob, data in group_samples:
                self.assertEqual(self._read(blob, group_id=group_id), data, blob)
        self._assert_running_binary(set(self.survivors), self.new_binary)
        logger.info("PASS FULL DISASTER: old3DC -> minus1DC -> verified2+2 static+dynamic -> minus2DC -> "
                    "static recovery=%s, StateStorage relocation+upgrade -> same system/user tablets, SQL read/write -> "
                    "cold restart, exact old/new payloads; all six lost nodes stayed down", self.static_recovery)
