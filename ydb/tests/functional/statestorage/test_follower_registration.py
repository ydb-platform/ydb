import copy
import json
import logging
from pathlib import Path
import time
import unittest

import requests
import yaml
import yatest.common

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


class TestFollowerRegistration(unittest.TestCase):
    database = "/Root/follower_recovery"
    table = database + "/data"

    def setUp(self):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=4,
            state_storage_rings=[2, 3, 4],
            n_to_select=3,
            separate_node_configs=True,
            dynamic_pdisks=[{"user_kind": 42}],
            dynamic_storage_pools=[{
                "name": "data", "kind": "hdd", "pdisk_user_kind": 42,
            }],
            additional_log_configs={
                "STATESTORAGE": LogLevels.DEBUG,
                "TABLET_RESOLVER": LogLevels.DEBUG,
            },
        )
        # Neither the static data group nor the dynamic data pool uses the
        # StateStorage-only nodes whose services/processes fail in this test.
        for disk in config.pdisks_info:
            if disk["pdisk_user_kind"] == 42 and disk["node_id"] != 1:
                disk["pdisk_user_kind"] = 43
        for pool_type in config.yaml_config["domains_config"]["domain"][0]["storage_pool_types"]:
            if pool_type["kind"] == "hdd":
                pool_type["pool_config"]["pdisk_filter"] = [{
                    "property": [{"type": "ROT"}, {"kind": 42}],
                }]
        config.yaml_config["system_tablets"]["default_node"] = [1]
        for kind in ("flat_tx_coordinator", "tx_allocator", "tx_mediator"):
            for tablet in config.yaml_config["system_tablets"][kind]:
                tablet["node"] = [1]

        self.cluster = KiKiMR(configurator=config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        # Dynamic slots use the shared config directory, while static nodes
        # need individual files for the temporary service reconfiguration.
        config.write_proto_configs(self.cluster.config_path)
        self.a, self.b, self.c = (self.cluster.nodes[index] for index in (2, 3, 4))
        self.evidence = {}

    def wait(self, check, description, timeout=30):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                return check()
            except (AssertionError, requests.RequestException, KeyError, ydb.Error) as error:
                last_error = error
                time.sleep(0.1)
        raise AssertionError("Timed out waiting for " + description) from last_error

    def get(self, node, route, **params):
        response = requests.get(
            f"http://{node.host}:{node.mon_port}{route}", params=params, timeout=5,
        )
        response.raise_for_status()
        return response

    def session(self, node):
        driver = ydb.Driver(ydb.DriverConfig(
            endpoint=f"grpc://{node.host}:{node.port}",
            database=self.database,
            disable_discovery=True,
        ))
        self.addCleanup(driver.stop)
        driver.wait(timeout=30)
        session = driver.table_client.session().create()
        self.addCleanup(session.delete)
        return session

    def tablet_id(self):
        description = self.get(
            self.cluster.nodes[1], "/viewer/json/describe",
            database=self.database, path=self.table,
        ).json()
        partitions = description["PathDescription"]["TablePartitions"]
        self.assertEqual(len(partitions), 1)
        return int(partitions[0]["DatashardId"])

    def incarnations(self, tablet_id):
        result = {}
        for slot in self.slots:
            tablets = self.get(
                slot, "/viewer/json/tabletinfo", node_id=".", enums="true",
                filter=f"(TabletId={tablet_id})",
            ).json().get("TabletStateInfo", [])
            for tablet in tablets:
                if int(tablet["TabletId"]) == tablet_id and tablet["State"] == "Active":
                    role = "follower" if int(tablet.get("FollowerId", 0)) else "leader"
                    self.assertNotIn(role, result)
                    result[role] = (slot, tablet)
        self.assertEqual(set(result), {"leader", "follower"})
        self.assertIsNot(result["leader"][0], result["follower"][0])
        return result

    def save_evidence(self):
        Path(yatest.common.test_output_path("follower_recovery.json")).write_text(
            json.dumps(self.evidence, indent=2),
        )

    def test_stale_read_survives_state_storage_replica_recovery(self):
        # Inject absence of A's StateStorage service while its node and IC
        # session remain alive. This asymmetric config is only fault injection;
        # normal reconfiguration uses WriteOnly ring groups. A node disconnect
        # alone does not establish Missing=true.
        config_path = Path(next(
            argument.split("=", 1)[1] for argument in self.a.command
            if argument.startswith("--yaml-config=")
        ))
        original = yaml.safe_load(config_path.read_text())
        without_a = copy.deepcopy(original)
        without_a["domains_config"]["state_storage"][0]["ring"]["ring"][0]["node"] = [1]
        self.a.stop()
        config_path.write_text(yaml.safe_dump(without_a))
        self.a.start()

        self.cluster.create_database(self.database, {"hdd": 1})
        self.slots = self.cluster.register_and_start_slots(self.database, count=2)
        self.cluster.wait_tenant_up(self.database, endpoints_timeout_seconds=60)
        writer = self.session(self.cluster.nodes[1])
        creation_started = time.monotonic()
        writer.execute_scheme(
            f"CREATE TABLE `{self.table}` ("
            "id Uint64 NOT NULL, value Utf8, PRIMARY KEY(id)) "
            'WITH (READ_REPLICAS_SETTINGS = "ANY_AZ:1");'
        )
        writer.transaction().execute(
            f'UPSERT INTO `{self.table}` (id, value) VALUES (1u, "committed");',
            commit_tx=True,
        )
        tablet_id = self.wait(self.tablet_id, "the DataShard ID")
        before = self.wait(lambda: self.incarnations(tablet_id), "active leader and follower")
        self.evidence["tablet_id"] = tablet_id
        self.evidence["initial"] = {role: item[1] for role, item in before.items()}

        # A is missing long enough for its notification grace, but the same
        # follower and its guardians must survive the service recovery.
        time.sleep(4)
        recovery_started = time.monotonic()
        self.a.stop()
        config_path.write_text(yaml.safe_dump(original))
        self.a.start()
        self.assertLess(time.monotonic() - recovery_started, 10)

        def recovered():
            html = self.get(
                self.cluster.nodes[1], "/tablets", SsId=tablet_id,
            ).text
            # A recovered registration must be observable before B fails.
            self.assertIn("complete", html)
            self.assertNotIn("timeout", html)
            registrations = [
                line for line in Path(self.a.ydbd_log_file_path).read_text().splitlines()
                if "received TEvReplicaRegFollower" in line and f"tabletId={tablet_id}" in line
            ]
            self.assertTrue(registrations, "A has not received the recovered follower registration")
            self.evidence["registration_on_a"] = registrations[-1]
            current = self.incarnations(tablet_id)
            for role in before:
                self.assertEqual(current[role][1]["NodeId"], before[role][1]["NodeId"])
                self.assertEqual(current[role][1]["Generation"], before[role][1]["Generation"])
                self.assertEqual(current[role][1]["ChangeTime"], before[role][1]["ChangeTime"])
            return html

        self.evidence["state_storage_after_recovery"] = self.wait(
            recovered, "recovered StateStorage service and unchanged tablet incarnations",
        )
        # Allow the registration ping to reach the existing guardian before
        # the second failure, without crossing its 15-second downtime limit.
        time.sleep(0.5)
        self.assertLess(
            time.monotonic() - creation_started, 14,
            "setup exceeded the lifetime of the original missing-replica guardian",
        )
        self.save_evidence()

        # Only B is down now. A and C provide the StateStorage majority, and
        # neither B nor A carries user data or either DataShard incarnation.
        self.b.stop()
        time.sleep(18)
        after = self.incarnations(tablet_id)
        self.evidence["after_b_failure"] = {role: item[1] for role, item in after.items()}
        for role in before:
            self.assertEqual(after[role][1]["NodeId"], before[role][1]["NodeId"])
            self.assertEqual(after[role][1]["Generation"], before[role][1]["Generation"])
            self.assertEqual(after[role][1]["ChangeTime"], before[role][1]["ChangeTime"])

        leader, follower = after["leader"][0], after["follower"][0]
        reader = self.session(follower)
        query = f"SELECT value FROM `{self.table}` WHERE id = 1u;"
        settings = ydb.ExecDataQuerySettings().with_timeout(5).with_keep_in_cache(True)
        control = reader.transaction().execute(query, commit_tx=True, settings=settings)
        self.assertEqual(control[0].rows[0].value, "committed")
        self.evidence["leader_read_before_pause"] = "committed"
        # Warm the stale query plan while all metadata tablets are available.
        # The failure below must occur during data access, not compilation.
        warmup = reader.transaction(ydb.StaleReadOnly()).execute(
            query, commit_tx=True, settings=settings,
        )
        self.assertEqual(warmup[0].rows[0].value, "committed")
        # The first stale lookup may use the leader while the resolver refreshes
        # its follower list. Let the normal 30-second follower pipe cache expire
        # before warming it again, so pausing the leader tests follower access.
        time.sleep(31)
        warmup = reader.transaction(ydb.StaleReadOnly()).execute(
            query, commit_tx=True, settings=settings,
        )
        self.assertEqual(warmup[0].rows[0].value, "committed")
        current = self.incarnations(tablet_id)
        for role in before:
            for field in ("NodeId", "Generation", "ChangeTime"):
                self.assertEqual(current[role][1][field], before[role][1][field])
        self.evidence["before_leader_pause"] = {role: item[1] for role, item in current.items()}
        self.evidence["stale_read_before_pause"] = "committed"
        self.save_evidence()

        # This is a real SDK stale read, without ForceFollower or mocked
        # replies. Its session and the follower run on the unpaused process.
        leader.freeze()
        self.addCleanup(leader.unfreeze)
        started = time.monotonic()
        try:
            result = reader.transaction(ydb.StaleReadOnly()).execute(
                query, commit_tx=True, settings=settings,
            )
        except ydb.Error as error:
            self.evidence["stale_read_error"] = repr(error)
            self.evidence["stale_read_seconds"] = time.monotonic() - started
            self.save_evidence()
            raise
        self.evidence["stale_read_seconds"] = time.monotonic() - started
        self.evidence["stale_read_value"] = result[0].rows[0].value
        self.save_evidence()
        self.assertEqual(result[0].rows[0].value, "committed")
        logger.info("Follower read remained available: %s", self.evidence)
