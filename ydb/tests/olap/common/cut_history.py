import requests

from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, HiveClient
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb

CUT_HISTORY_FLAGS = [
    "enable_cut_history",
    "enable_columnshard_cut_history",
    "enable_columnshard_cut_history_sending",
]
HIVE_CONFIG = {"cut_history_deny_list": "KeyValue,PersQueue,BlobDepot", "cut_history_allow_list": "ColumnShard"}


class CutHistoryScenario:
    """One shard with an occupied closed interval followed by an empty one."""

    table = "cut_history"

    def __init__(self, cluster):
        self.cluster = cluster
        node = cluster.nodes[1]
        self.mon_url = f"http://{node.host}:{node.mon_port}"
        self.hive = HiveClient(node.host, node.mon_port)
        self.tablet_id = None

    def execute(self, driver, query):
        with ydb.QuerySessionPool(driver) as pool:
            return pool.execute_with_retries(query, retry_settings=ydb.RetrySettings(idempotent=True))

    def create_table(self, driver):
        self.execute(
            driver,
            f"""
            CREATE TABLE `{self.table}` (
                id Uint64 NOT NULL,
                payload Utf8,
                PRIMARY KEY (id)
            ) PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        """,
        )
        description = self.cluster.client.describe(f"/Root/{self.table}", "")
        shards = description.PathDescription.ColumnTableDescription.Sharding.ColumnShards
        assert len(shards) == 1, description
        self.tablet_id = shards[0]

    def write_rows(self, driver, offset=0):
        rows = {i: f"payload_{i}" for i in range(offset, offset + 64)}
        values = ",".join(f'({key}, "{value}")' for key, value in rows.items())
        self.execute(driver, f"UPSERT INTO `{self.table}` (id, payload) VALUES {values};")
        return rows

    def assert_rows(self, driver, expected):
        result = self.execute(driver, f"SELECT id, payload FROM `{self.table}` ORDER BY id;")
        actual = [(row["id"], row["payload"]) for row in result[0].rows]
        assert actual == sorted(expected.items())

    def tablet_info(self):
        response = requests.get(
            f"{self.mon_url}/tablets/app",
            params={"TabletID": DEFAULT_HIVE_ID, "page": "TabletInfo", "tablet": self.tablet_id},
            timeout=10,
        )
        response.raise_for_status()
        info = response.json()
        assert "error" not in info, info
        return info

    def history(self):
        channels = self.tablet_info()["TabletStorageInfo"]["Channels"]
        history = {
            int(channel["Channel"]): [
                (int(entry["FromGeneration"]), int(entry["GroupID"])) for entry in channel["History"]
            ]
            for channel in channels
            if int(channel["Channel"]) >= 2
        }
        assert history and all(history.values()), channels
        return history

    def latest_entries(self):
        return {(channel, *entries[-1]) for channel, entries in self.history().items()}

    def entries(self):
        return {
            (channel, generation, group) for channel, entries in self.history().items() for generation, group in entries
        }

    def reassign(self):
        before = self.latest_entries()
        self.hive.change_tablet_group(self.tablet_id, channels=sorted(channel for channel, _, _ in before))
        assert wait_for(lambda: self.latest_entries().isdisjoint(before), timeout_seconds=120), self.tablet_info()

    def restart(self):
        generation = int(self.tablet_info()["KnownGeneration"])
        self.cluster.client.tablet_kill(self.tablet_id, assert_success=True)
        assert wait_for(
            lambda: int(self.tablet_info()["KnownGeneration"]) > generation, timeout_seconds=120
        ), self.tablet_info()

    def prepare(self, driver):
        self.create_table(driver)
        rows = self.write_rows(driver)
        occupied = self.latest_entries()
        self.reassign()
        empty = self.latest_entries()
        # Closing this interval last makes it eligible only after the next boot.
        self.reassign()
        self.assert_rows(driver, rows)
        return empty, occupied, rows

    def wait_for_cut(self, empty, occupied):
        assert empty and occupied and empty.isdisjoint(occupied)
        assert wait_for(lambda: self.entries().isdisjoint(empty), timeout_seconds=120), self.tablet_info()
        # A write can use only a subset of the tablet's data channels.
        assert self.entries() & occupied, self.tablet_info()
