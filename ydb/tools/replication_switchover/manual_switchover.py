#!/usr/bin/env python3
"""Manual switchover of an async replication between two YDB clusters, and a rehearsal that flips a
pair back and forth under writes.

The flip demotes every replicated table on the primary to a read-only replica (an internal
AlterTable through the legacy `ydbd db schema exec` service), waits until the replication consumer has
committed every change record, finishes the replication with STATE = 'DONE' (the replica becomes
writable), creates the reverse replication with INITIAL_SCAN = FALSE, attaches it with
STATE = 'STANDBY', probes it with one row and drops the old replication object. Each flip after the
first also drops the stale changefeeds of the previous direction by the names recorded earlier.

A table that becomes a replica for the second time keeps the deduplication offsets of the earlier
stream in its shards; the reverse replication reads the new stream from offset 0 and the shards would
skip its records. Attach-mode replications therefore qualify the deduplication source id with the
changefeed name (see TRemoteTopicReaderSettings.SourceIdPrefix); both clusters need that change.

Usage:
  manual_switchover.py run --source-endpoint grpc://a:2135 --source-database /a \
      --target-endpoint grpc://b:2135 --target-database /b [--flips 4] [--index] [--consistency ROW]
  Endpoints default to YDB_SOURCE_ENDPOINT/YDB_SOURCE_DATABASE and YDB_TARGET_*; the `ydbd` and `ydb`
  binaries default to YDBD/YDB_CLI or PATH. The clusters are expected to accept anonymous access, as
  the test clusters do; both must expose the legacy gRPC service for `ydbd db schema exec`.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

import ydb

YDBD = os.environ.get("YDBD", "ydbd")
YDB_CLI = os.environ.get("YDB_CLI", "ydb")
CONSUMER = "replicationConsumer"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


class Cluster:
    def __init__(self, name, endpoint, database):
        self.name, self.endpoint, self.database = name, endpoint, database
        self.driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.AnonymousCredentials())
        self.driver.wait(timeout=120, fail_fast=False)
        self.pool = ydb.QuerySessionPool(self.driver)

    def __repr__(self):
        return self.name

    def sql(self, text, retries=5):
        return self.pool.execute_with_retries(text, retry_settings=ydb.RetrySettings(max_retries=retries),
                                              settings=ydb.BaseRequestSettings().with_timeout(60))

    def try_sql(self, text):
        try:
            self.sql(text)
            return None
        except ydb.Error as e:
            return str(e)

    def rows(self, table):
        rs = self.sql(f"SELECT key, value FROM `{table}` ORDER BY key")
        return [(r.key, r.value) for r in rs[0].rows]

    def digest(self, table):
        rs = self.sql(f"SELECT COUNT(*) AS c, SUM(key) AS s FROM `{table}`")
        return (rs[0].rows[0].c, rs[0].rows[0].s)

    # legacy service calls
    def ydbd(self, *args):
        return subprocess.run([YDBD, "-s", self.endpoint, *args], capture_output=True, text=True)

    def schema_exec(self, proto_text, tag):
        path = f"/tmp/mswitch_{self.name}_{tag}.pb.txt"
        open(path, "w").write(proto_text)
        out = self.ydbd("db", "schema", "exec", path)
        if out.returncode != 0:
            raise RuntimeError(f"{self}: schema exec {tag} failed rc={out.returncode}\n"
                               f"{out.stdout[-800:]}\n{out.stderr[-800:]}")
        return out.stdout

    def describe_proto(self, path):
        out = self.ydbd("db", "schema", "describe", "-P", path)
        if out.returncode != 0:
            raise RuntimeError(f"{self}: describe {path} failed: {out.stderr[-500:]}")
        return out.stdout

    def replica_mode(self, path):
        text = self.describe_proto(path)
        m = re.search(r"ReplicationConfig \{([^}]*)\}", text, re.S)
        if not m:
            return "NONE"
        mm = re.search(r"Mode: (\w+)", m.group(1))
        return mm.group(1).replace("REPLICATION_MODE_", "") if mm else "NONE"

    def cli(self, *args):
        out = subprocess.run([YDB_CLI, "-e", self.endpoint, "-d", self.database, *args], capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(f"{self}: ydb {' '.join(args)} failed: {out.stderr[-500:]}")
        return out.stdout

    def describe_replication(self, path):
        return json.loads(self.cli("scheme", "describe", path, "--format", "proto-json-base64"))

    def consumer_offsets(self, topic):
        d = self.driver.topic_client.describe_consumer(topic, CONSUMER, include_stats=True)
        res = []
        for p in d.partitions:
            end = p.partition_stats.partition_end if p.partition_stats else None
            committed = p.partition_consumer_stats.committed_offset if p.partition_consumer_stats else None
            res.append((p.partition_id, end, committed))
        return res

    def changefeeds(self, table):
        return [c.name for c in self.driver.table_client.session().create().describe_table(table).changefeeds]


def wait_for(pred, timeout, what, interval=1.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = pred()
            if last:
                return last
        except ydb.Error as e:
            last = f"error: {str(e)[:120]}"
        time.sleep(interval)
    raise TimeoutError(f"timeout waiting for {what}; last: {last!r}")


def changefeed_paths(src_cluster, desc, src_table):
    """Full topic paths of every changefeed of a replication, from its describe (plan step 0)."""
    paths = []
    for item in desc.get("items", []):
        sp, cf = item["source_path"], item.get("source_changefeed_name")
        if not cf:
            raise RuntimeError(f"item without changefeed yet: {item}")
        if sp == src_table:
            paths.append(f"{sp}/{cf}")
        else:  # index target: source_path is <T>/<index>
            paths.append(f"{sp}/indexImplTable/{cf}")
    return paths


def demote_proto(working_dir, name, mode, consistency):
    cfg = f"Mode: {mode}"
    if mode == "REPLICATION_MODE_READ_ONLY":
        cfg += f"\n      ConsistencyLevel: CONSISTENCY_LEVEL_{consistency}\n      ImportState: IMPORTED"
    return f"""ModifyScheme {{
  OperationType: ESchemeOpAlterTable
  WorkingDir: "{working_dir}"
  Internal: true
  AlterTable {{
    Name: "{name}"
    ReplicationConfig {{
      {cfg}
    }}
  }}
}}
"""


class Writer(threading.Thread):
    """Writes rows with unique keys to the current primary until stopped; records acknowledged keys."""

    def __init__(self, cluster, table, start_key, value):
        super().__init__(daemon=True)
        self.cluster, self.table, self.key, self.value = cluster, table, start_key, value
        self.acked = []
        self.rejected = 0
        self.stop_flag = threading.Event()

    def run(self):
        while not self.stop_flag.is_set():
            k = self.key
            err = self.cluster.try_sql(f"UPSERT INTO `{self.table}` (key, value) VALUES ({k}u, '{self.value}{k}'u)")
            if err is None:
                self.acked.append(k)
                self.key += 1
            else:
                self.rejected += 1
                time.sleep(0.2)
            time.sleep(0.02)

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=30)
        return self.key


class Switchover:
    def __init__(self, a, b, table, consistency, with_index, state):
        self.a, self.b, self.table, self.state = a, b, table, state
        self.consistency, self.with_index = consistency, with_index
        self.index = "by_value" if with_index else None

    def table_path(self, cluster):
        return f"{cluster.database}/{self.table}"

    def impl_tables(self, cluster):
        """(working_dir, name, path) of the base table and each synchronous index impl table."""
        base = self.table_path(cluster)
        res = [(cluster.database, self.table, base)]
        if self.index:
            res.append((f"{base}/{self.index}", "indexImplTable", f"{base}/{self.index}/indexImplTable"))
        return res

    # ---- setup -------------------------------------------------------------------------------
    def setup(self, rows):
        a, b = self.a, self.b
        t = self.table_path(a)
        for c in (a, b):
            for n in range(1, 10):  # leftovers of an earlier run on the same pair
                c.try_sql(f"DROP ASYNC REPLICATION `{c.database}/R{n}` FORCE")
            c.try_sql(f"DROP TABLE `{self.table_path(c)}`")
        idx = f", INDEX {self.index} GLOBAL SYNC ON (value)" if self.index else ""
        a.sql(f"CREATE TABLE `{t}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key){idx})")
        values = ", ".join(f"({k}u, 'init{k}'u)" for k in range(rows))
        a.sql(f"UPSERT INTO `{t}` (key, value) VALUES {values}")
        rep = f"{b.database}/R1"
        extra = ""
        if self.consistency == "GLOBAL":
            extra = ", CONSISTENCY_LEVEL = 'GLOBAL', COMMIT_INTERVAL = Interval('PT1S')"
        b.sql(f"CREATE ASYNC REPLICATION `{rep}` FOR `{t}` AS `{self.table_path(b)}` "
              f"WITH (CONNECTION_STRING = '{a.endpoint}/?database={a.database}'{extra})")
        wait_for(lambda: b.digest(self.table_path(b)) == a.digest(t), 240, "initial copy")
        wait_for(lambda: "running" in b.describe_replication(rep), 120, "R1 running")
        self.state.update({"primary": a.name, "replication": rep, "replication_cluster": b.name,
                           "stale": {a.name: [], b.name: []}, "flips": 0, "next_rep": 2})
        log("setup done: primary", a, "replica", b, "replication", rep)

    # ---- the flip ----------------------------------------------------------------------------
    def flip(self):
        st = self.state
        src = self.a if st["primary"] == self.a.name else self.b      # current primary, to be demoted
        dst = self.b if src is self.a else self.a                     # current replica, to be promoted
        rep = st["replication"]
        src_t, dst_t = self.table_path(src), self.table_path(dst)
        log(f"=== flip #{st['flips'] + 1}: {src} -> {dst}, replication {rep} on {dst}")

        # step 0: preflight
        desc = dst.describe_replication(rep)
        if "running" not in desc or "error" in desc:
            raise RuntimeError(f"preflight: replication not healthy: {json.dumps(desc)[:400]}")
        cf_paths = changefeed_paths(src, desc, src_t)
        log("step 0: changefeeds on", src, cf_paths)

        # step 2: fence by demoting every table on src
        for wd, name, path in self.impl_tables(src):
            src.schema_exec(demote_proto(wd, name, "REPLICATION_MODE_READ_ONLY", self.consistency), f"demote_{name}")
            mode = wait_for(lambda: src.replica_mode(path) == "READ_ONLY" and "READ_ONLY", 60, f"{path} read-only")
            log("step 2: demoted", path, mode)
        err = src.try_sql(f"UPSERT INTO `{src_t}` (key, value) VALUES (9000000000ul, 'fence-probe'u)")
        if err is None:
            raise RuntimeError("fence check: a write to the demoted table was accepted")
        if "replica" not in err.lower():
            raise RuntimeError(f"fence check: write failed for another reason: {err[:300]}")
        log("step 2: write rejected on", src, "->", " ".join(err.split())[:120])

        # step 3: drain
        def drained():
            stable = 0
            last = None
            for _ in range(60):
                snap = [(p, e, c) for path in cf_paths for (p, e, c) in src.consumer_offsets(path)]
                ok = all(e is not None and c is not None and c >= e for _, e, c in snap)
                if ok and snap == last:
                    stable += 1
                    if stable >= 3:
                        return snap
                else:
                    stable = 0
                last = snap
                time.sleep(2)
            raise TimeoutError(f"drain: last offsets {last}")
        if self.consistency == "GLOBAL":
            time.sleep(3)  # two commit intervals of 1s plus slack
        snap = drained()
        log("step 3: drained", snap)
        if self.consistency == "GLOBAL":
            def tx_idle():
                d = dst.describe_replication(rep)
                tx = d.get("transactions")
                return tx is not None and not tx.get("assigned") and not tx.get("pending")
            wait_for(tx_idle, 60, "global transactions idle", interval=1.0)
            time.sleep(1.5)
            if not tx_idle():
                raise RuntimeError("global transactions not idle on second poll")
            log("step 3: global transactions idle")

        # step 4: promote dst (point of no return)
        for _, _, path in self.impl_tables(src):
            assert src.replica_mode(path) == "READ_ONLY", path
        dst.sql(f"ALTER ASYNC REPLICATION `{rep}` SET (STATE = 'DONE')")
        for _, _, path in self.impl_tables(dst):
            wait_for(lambda: dst.replica_mode(path) == "NONE" and "NONE", 120, f"{path} promoted")
        log("step 4: promoted", dst)

        # step 5: compare
        ds, dd = src.digest(src_t), dst.digest(dst_t)
        if ds != dd:
            raise RuntimeError(f"step 5: digests differ: {src}={ds} {dst}={dd}")
        log("step 5: digests equal", ds)

        # step 6: drop stale changefeeds on the promoted side (recorded at an earlier flip)
        for path in st["stale"][dst.name]:
            table, cf = path.rsplit("/", 1)
            if table.endswith("/indexImplTable"):
                # No public statement drops a changefeed of an index implementation table; the
                # legacy DropCdcStream does, by the recorded name only (never by listing: the live
                # changefeed of the next replication sits next to the stale one).
                wd, name = table.rsplit("/", 1)
                dst.schema_exec(f"""ModifyScheme {{
  OperationType: ESchemeOpDropCdcStream
  WorkingDir: "{wd}"
  DropCdcStream {{
    TableName: "{name}"
    StreamName: "{cf}"
  }}
}}
""", f"dropcdc_{cf}")
            else:
                dst.sql(f"ALTER TABLE `{table}` DROP CHANGEFEED `{cf}`")
            log("step 6: dropped stale changefeed", path)
        st["stale"][dst.name] = []
        st["stale"][src.name] = cf_paths  # the forward changefeeds now sit on a replica; dropped next flip

        # step 7: reverse replication on src
        new_rep = f"{src.database}/R{st['next_rep']}"
        st["next_rep"] += 1
        extra = ", CONSISTENCY_LEVEL = 'GLOBAL', COMMIT_INTERVAL = Interval('PT1S')" if self.consistency == "GLOBAL" \
            else ", CONSISTENCY_LEVEL = 'ROW'"
        src.sql(f"CREATE ASYNC REPLICATION `{new_rep}` FOR `{dst_t}` AS `{src_t}` "
                f"WITH (CONNECTION_STRING = '{dst.endpoint}/?database={dst.database}', INITIAL_SCAN = FALSE{extra})")

        def new_cfs_ready():
            d = src.describe_replication(new_rep)
            if "error" in d:
                raise RuntimeError(f"step 7: {new_rep} error: {d['error']}")
            items = d.get("items", [])
            return items and all(i.get("source_changefeed_name") for i in items) and d
        d = wait_for(new_cfs_ready, 120, "reverse changefeeds")
        new_cf_paths = changefeed_paths(dst, d, dst_t)
        for p in new_cf_paths:
            wait_for(lambda p=p: dst.consumer_offsets(p) is not None, 60, f"topic {p}")
        log("step 7: created", new_rep, "changefeeds", new_cf_paths)

        # step 8: attach
        src.sql(f"ALTER ASYNC REPLICATION `{new_rep}` SET (STATE = 'STANDBY')")

        def attached():
            d = src.describe_replication(new_rep)
            if "error" in d:
                raise RuntimeError(f"step 8: {new_rep} error: {d['error']}")
            return "running" in d
        wait_for(attached, 120, "reverse replication running")
        log("step 8: attached", new_rep)

        # step 9: probe
        probe_key = 10_000_000 + st["flips"]
        dst.sql(f"UPSERT INTO `{dst_t}` (key, value) VALUES ({probe_key}u, 'probe'u)")
        try:
            wait_for(lambda: (probe_key, "probe") in src.rows(src_t), 90, "probe row on the new replica")
            log("step 9: probe row replicated", dst, "->", src)
            st["probe_ok"] = True
        except TimeoutError as e:
            log("step 9: PROBE FAILED (rows written on the new primary do not reach the new replica):", e)
            st["probe_ok"] = False

        # step 10: cleanup
        dst.sql(f"DROP ASYNC REPLICATION `{rep}` FORCE")
        log("step 10: dropped", rep)

        st.update({"primary": dst.name, "replication": new_rep, "replication_cluster": src.name,
                   "flips": st["flips"] + 1})
        return src, dst


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--rows", type=int, default=50)
    ap.add_argument("--flips", type=int, default=2)
    ap.add_argument("--consistency", choices=["ROW", "GLOBAL"], default="ROW")
    ap.add_argument("--index", action="store_true")
    ap.add_argument("--table", default="switch_T")
    ap.add_argument("--write-seconds", type=float, default=5.0, help="writes before each flip")
    ap.add_argument("--state-file", default="/tmp/mswitch_state.json")
    ap.add_argument("--source-endpoint", default=os.environ.get("YDB_SOURCE_ENDPOINT"))
    ap.add_argument("--source-database", default=os.environ.get("YDB_SOURCE_DATABASE"))
    ap.add_argument("--target-endpoint", default=os.environ.get("YDB_TARGET_ENDPOINT"))
    ap.add_argument("--target-database", default=os.environ.get("YDB_TARGET_DATABASE"))
    args = ap.parse_args()
    for name in ("source_endpoint", "source_database", "target_endpoint", "target_database"):
        if not getattr(args, name):
            ap.error(f"--{name.replace('_', '-')} is required (or its YDB_* environment variable)")

    a = Cluster("A", args.source_endpoint, args.source_database)
    b = Cluster("B", args.target_endpoint, args.target_database)
    state = {}
    sw = Switchover(a, b, args.table, args.consistency, args.index, state)
    sw.setup(args.rows)
    next_key = args.rows
    results = []
    for i in range(args.flips):
        primary = a if state["primary"] == "A" else b
        w = Writer(primary, sw.table_path(primary), next_key, f"w{i}_")
        w.start()
        time.sleep(args.write_seconds)
        next_key = w.stop()
        log(f"writer on {primary}: {len(w.acked)} rows acknowledged, {w.rejected} rejected")
        src, dst = sw.flip()
        json.dump(state, open(args.state_file, "w"), indent=1)
        # every acknowledged write is on the new primary
        missing = [k for k in w.acked if (k, f"w{i}_{k}") not in dst.rows(sw.table_path(dst))]
        log(f"after flip #{i + 1}: {len(missing)} of {len(w.acked)} pre-fence rows missing on {dst}")
        # writes on the new primary reach the new replica? (L3 shows here from the second flip on)
        keys = list(range(next_key, next_key + 20))
        next_key += 20
        values = ", ".join(f"({k}u, 'post{i}_{k}'u)" for k in keys)
        dst.sql(f"UPSERT INTO `{sw.table_path(dst)}` (key, value) VALUES {values}")
        try:
            wait_for(lambda: all((k, f"post{i}_{k}") in src.rows(sw.table_path(src)) for k in keys), 60,
                     "post-flip rows on the new replica", interval=2.0)
            arrived = True
        except TimeoutError as e:
            arrived = False
            log("post-flip rows did NOT arrive:", e)
        results.append({"flip": i + 1, "src": src.name, "dst": dst.name, "pre_fence_missing": len(missing),
                        "acked": len(w.acked), "probe_ok": state.get("probe_ok"), "post_flip_rows_arrived": arrived})
        log("RESULT", results[-1])
    print(json.dumps(results, indent=1))
    ok = all(r["pre_fence_missing"] == 0 and r["post_flip_rows_arrived"] for r in results)
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
