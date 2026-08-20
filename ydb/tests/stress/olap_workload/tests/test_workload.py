# -*- coding: utf-8 -*-
import json
import time
import urllib.request
import os
import pytest
import yatest
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_move_column_table": True,
                "enable_columnshard_bool": True,
                "enable_cs_dictionary_encoding": True,
                "enable_cut_history": True,
                "enable_columnshard_interval": True,
                "enable_columnshard_uuid": True,
                "enable_columnshard_dy_number": True,
            },
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
                "generate_internal_path_id": True,
            },
            # Hive's gate needs both sides: the type must be absent from the deny
            # list (ColumnShard is on it by default) AND present in the allow list
            # (which defaults to DataShard only) — with either half missing Hive
            # refuses the TEvCutTabletHistory this test ultimately waits for.
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
                "cut_history_allow_list": "DataShard,ColumnShard",
            },
        )

    def _cut_history_sensors(self):
        totals = {}
        for node in self.cluster.nodes.values():
            url = f"http://localhost:{node.mon_port}/counters/counters=tablets/json"
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8", "replace"))
            except Exception:
                continue
            for item in payload.get("sensors", []):
                labels = item.get("labels", {})
                if labels.get("component") != "CutHistory":
                    continue
                name = labels.get("sensor", "")
                for prefix in ("Deriviative/", "Value/"):
                    if name.startswith(prefix):
                        name = name[len(prefix):]
                        break
                try:
                    totals[name] = totals.get(name, 0) + int(item.get("value") or 0)
                except (TypeError, ValueError):
                    continue
        return totals

    def _mon_json(self, path):
        for node in self.cluster.nodes.values():
            url = f"http://localhost:{node.mon_port}{path}"
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8", "replace"))
            except Exception:
                continue
        return {}

    def _mon_post(self, path):
        """Returns (error, body): error is None on HTTP success.

        POST, because Hive's mutating monitoring pages (ReassignTablet) reject GET
        with 400 "Must use POST request". The body is returned for diagnostics —
        the ReassignTablet page answers 200 with its operations list, so an empty
        list or an embedded error is only visible there.
        """
        # The tablet monitoring proxy takes params from the BODY (not the URL) for
        # form-urlencoded POSTs — and urllib always stamps that content type when
        # data is given. So the query must ride in the body or it is invisible.
        base, _, query = path.partition("?")
        errors = []
        for node in self.cluster.nodes.values():
            url = f"http://localhost:{node.mon_port}{base}"
            try:
                with urllib.request.urlopen(url, data=query.encode(), timeout=60) as response:
                    return None, response.read().decode("utf-8", "replace")
            except Exception as e:
                errors.append(f"{url}: {e}")
        return "; ".join(errors) or "no cluster nodes", ""

    def _prepare_cut_history_candidate(self, pool):
        """Make one deterministically cuttable history entry and return the probe path.

        Channel history grows only on a Hive channel (re)assignment
        (TTxUpdateTabletGroups) — plain tablet restarts never add entries, which is
        why gating on the workload's restarts alone cannot work. And an entry is
        nominated only when its generation range holds no blobs. Hence the order
        here: create an EMPTY column table, force-reassign its data channel (the
        pre-reassign entry then covers a range with no blobs at all), and only then
        write rows — the new blobs belong to the active entry, and the writes give
        the tablet the GC completions on which TryNominate runs.
        """
        path = f"{self.database}/cut_history_probe"
        # Compilation can time out while the host digests the workload run's load;
        # that is environment, not subject matter — retry for up to two minutes.
        deadline = time.time() + 120
        while True:
            try:
                pool.execute_with_retries(
                    f"CREATE TABLE `{path}` (k Uint64 NOT NULL, v Uint64, PRIMARY KEY(k)) "
                    "WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)"
                )
                break
            except ydb.issues.Error:
                if time.time() >= deadline:
                    raise
                time.sleep(5)
        described = self._mon_json(f"/viewer/json/describe?path={path}")
        shards = ((((described.get("PathDescription") or {}).get("ColumnTableDescription") or {})
                   .get("Sharding") or {}).get("ColumnShards") or [])
        assert shards, f"no ColumnShards behind {path}: {described}"
        # Tenant databases expose their Hive via describe (ProcessingParams.Hive);
        # the root domain used here does not, so resolve the Hive tablet directly
        # and keep the well-known root Hive id as the fallback.
        tablets = self._mon_json("/viewer/json/tabletinfo?filter=(Type=Hive)")
        hives = [t.get("TabletId") for t in tablets.get("TabletStateInfo", []) if t.get("TabletId")]
        hive_id = hives[0] if hives else 72057594037968897
        # forcedGroup is REQUIRED here, with the channel's own current group: a manual
        # reassign (HIVE_REASSIGN_REASON_NO) excludes the current group from selection
        # (FindFreeAllocationUnit filters newGroup.Id != currentGroup->Id), so on this
        # single-group cluster an unforced reassign is silently a no-op — the tablet
        # never restarts and no history entry appears. A forced group bypasses the
        # selection and is applied verbatim, appending the entry even for the same
        # group (MaySkipChannelReassign only skips same-group for BALANCE).
        # Hive's TabletInfo page is the authoritative source for the channel's
        # current group (whiteboard's ChannelGroupIDs can lag on young tablets).

        def channel2_history():
            info = self._mon_json(
                f"/tablets/app?TabletID={hive_id}&page=TabletInfo&tablet={shards[0]}")
            channels = ((info.get("TabletStorageInfo") or {}).get("Channels") or [])
            for ch in channels:
                if ch.get("Channel") == 2:
                    return ch.get("History") or []
            return []

        history = channel2_history()
        assert history, f"no channel 2 history in Hive TabletInfo for tablet {shards[0]}"
        force_group = history[-1]["GroupID"]
        history_before = len(history)
        # wait=0: fire-and-return — the sensor poll below is the real confirmation.
        err, body = self._mon_post(
            f"/tablets/app?TabletID={hive_id}&page=ReassignTablet"
            f"&tablet={shards[0]}&channel=2&forcedGroup={force_group}&wait=0"
        )
        assert err is None, f"Hive ReassignTablet monitoring call failed: {err}"
        # The reassignment restarts the tablet; wait until Hive's history actually
        # grew — the crisp intermediate signal that the entry to cut now exists.
        deadline = time.time() + 60
        while time.time() < deadline and len(channel2_history()) <= history_before:
            time.sleep(3)
        assert len(channel2_history()) > history_before, (
            f"forced reassign did not grow channel 2 history; "
            f"POST response: {body[:2000]}; history now: {channel2_history()}"
        )
        return path

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ])
        # A run in which the cutter never engaged proves nothing about CutHistory,
        # so manufacture a guaranteed-cuttable entry and require the full pipeline:
        # nomination, sweep, hard barrier, TEvCutTabletHistory — i.e. Entries/Cut
        # must grow, not merely "no errors". The polling window is sized to the
        # one-minute nomination cadence plus barrier round-trip; the suite is
        # SIZE(MEDIUM), so the whole run must still fit in ten minutes under asan.
        pool = ydb.QuerySessionPool(self.driver)
        try:
            probe = self._prepare_cut_history_candidate(pool)
            deadline = time.time() + 150
            sensors = {}
            row = 0
            while time.time() < deadline:
                # Each write is a fresh chance for a GC completion, which is the
                # only place TryNominate is evaluated. The write is a means, not an
                # assertion target — a compile timeout under host load is not a
                # verdict on the cutter, so tolerate it and keep polling.
                try:
                    pool.execute_with_retries(f"UPSERT INTO `{probe}` (k, v) VALUES ({row}, {row})")
                    row += 1
                except ydb.issues.Error:
                    pass
                sensors = self._cut_history_sensors()
                if sensors.get("Entries/Cut/Count", 0) > 0:
                    break
                time.sleep(5)
        finally:
            pool.stop()
        assert sensors.get("Nominations/Count", 0) > 0, (
            f"CutHistory never nominated the probe entry: {sensors}"
        )
        assert sensors.get("Entries/Cut/Count", 0) > 0, (
            f"CutHistory nominated but never cut the probe entry: {sensors}"
        )
        assert sensors.get("Channels/Poisoned", 0) == 0, f"cutter poisoned a channel: {sensors}"
        assert sensors.get("Barriers/Failed/Count", 0) == 0, f"barrier send failed: {sensors}"
