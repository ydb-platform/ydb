# -*- coding: utf-8 -*-
import os
import time

import pytest
import requests

import yatest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.library.compatibility import fixtures as compatibility
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


class TestRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1, because of enable_column_store flag")

        yield from self.setup_cluster(
            extra_feature_flags=["enable_column_store"],
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
        )

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_kv(self, store_type):
        init_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "kv",
            "init",
            "--min-partitions",
            "10",
            "--partition-size",
            "10",
            "--auto-partition",
            "0",
            "--init-upserts",
            "0",
            "--cols",
            "5",
            "--int-cols",
            "2",
            "--key-cols",
            "3",
        ]

        run_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "kv",
            "run",
            "mixed",
            "--seconds",
            "10000000",  # infinity
            "--threads",
            "10",
            "--cols",
            "5",
            "--len",
            "200",
            "--int-cols",
            "2",
            "--key-cols",
            "3",
        ]

        init_command = init_command_prefix
        init_command.extend(
            [
                "--path",
                store_type,
                "--store",
                store_type,
            ]
        )
        run_command = run_command_prefix
        run_command.extend(
            [
                "--path",
                store_type,
            ]
        )
        yatest.common.execute(init_command, wait=True)
        run = yatest.common.execute(run_command, wait=False)

        for _ in self.roll():
            time.sleep(5)

        run.kill()


class TestReplicationForceDropRolling(compatibility.RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = [
            compatibility.current_binary_path,
            yatest.common.get_param("force_drop_previous_binary", compatibility.inter_stable_binary_path),
        ]

    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(nodes=3, erasure=Erasure.NONE)

    def scheme(self, query):
        with ydb.SessionPool(self.driver) as pool:
            with pool.checkout() as session:
                session.execute_scheme(query, settings=ydb.BaseRequestSettings().with_timeout(30))

    def describe(self, name):
        response = requests.get(
            f"http://localhost:{self.cluster.nodes[1].mon_port}/viewer/json/describe",
            params={"database": "/Root", "path": f"/Root/{name}", "enums": "true"}, timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def existing_names(self):
        return {entry.name for entry in self.driver.scheme_client.list_directory("/Root").children}

    def wait_state(self, name, state):
        deadline = time.monotonic() + 60
        description = None
        while time.monotonic() < deadline:
            response = requests.get(
                f"http://localhost:{self.cluster.nodes[1].mon_port}/viewer/json/describe_replication",
                params={"database": "/Root", "path": f"/Root/{name}"}, timeout=10,
            )
            response.raise_for_status()
            description = response.json()
            if state in description:
                return
            time.sleep(0.2)
        pytest.fail(f"{name} did not reach {state}: {description}")

    @staticmethod
    def identity(description):
        entry = description["PathDescription"]["Self"]
        return entry["SchemeshardId"], entry["PathId"]

    def test_completed_drop_survives_rolling_downgrade_and_upgrade(self):
        identities = {}
        for mode in ("retain", "cascade", "ordinary"):
            self.scheme(f"CREATE TABLE source_{mode} (key Uint64, PRIMARY KEY (key));")
            self.scheme(f"""
                CREATE ASYNC REPLICATION replication_{mode}
                FOR source_{mode} AS destination_{mode}
                WITH (CONNECTION_STRING = '{self.endpoint}/?database=/Root', TOKEN = 'root@builtin');
            """)
            deadline = time.monotonic() + 60
            destination, streams = None, []
            while time.monotonic() < deadline:
                if f"destination_{mode}" not in self.existing_names():
                    time.sleep(0.2)
                    continue
                destination = self.describe(f"destination_{mode}")
                streams = self.describe(f"source_{mode}").get("PathDescription", {}).get("Table", {}).get("CdcStreams", [])
                if destination.get("Status") == "StatusSuccess" and streams:
                    break
                time.sleep(0.2)
            assert destination and destination.get("Status") == "StatusSuccess" and len(streams) == 1
            identities[mode] = self.identity(destination)
            if mode == "ordinary":
                self.scheme(f"DROP ASYNC REPLICATION replication_{mode};")
                continue
            self.scheme(f"ALTER ASYNC REPLICATION replication_{mode} SET (STATE = 'PAUSED');")
            self.wait_state(f"replication_{mode}", "paused")
            self.scheme(f"""
                ALTER ASYNC REPLICATION replication_{mode} SET (
                    ENDPOINT = 'localhost:1', TOKEN_SECRET_PATH = '/Root/missing-source-secret'
                );
            """)
            self.scheme(f"ALTER ASYNC REPLICATION replication_{mode} SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');")
            self.wait_state(f"replication_{mode}", "done")
            cascade = " CASCADE" if mode == "cascade" else ""
            self.scheme(f"DROP ASYNC REPLICATION replication_{mode}{cascade} FORCE;")

        stages = 0
        for stage, _ in enumerate(self.roll()):
            names = self.existing_names()
            for mode in ("retain", "cascade", "ordinary"):
                assert f"replication_{mode}" not in names
                streams = self.describe(f"source_{mode}")["PathDescription"]["Table"].get("CdcStreams", [])
                assert len(streams) == (0 if mode == "ordinary" else 1)
                if mode == "cascade":
                    assert f"destination_{mode}" not in names
                else:
                    destination = self.describe(f"destination_{mode}")
                    assert self.identity(destination) == identities[mode]
                    if mode == "ordinary":
                        assert destination["PathDescription"]["Table"]["ReplicationConfig"]["Mode"] == "REPLICATION_MODE_READ_ONLY"
            with ydb.QuerySessionPool(self.driver) as pool:
                pool.execute_with_retries(f"UPSERT INTO destination_retain (key) VALUES ({stage});")
                rows = pool.execute_with_retries("SELECT key FROM destination_retain ORDER BY key;")[0].rows
                assert [row.key for row in rows] == list(range(stage + 1))
            stages += 1
        assert stages == 2 * len(self.cluster.nodes) + 1
