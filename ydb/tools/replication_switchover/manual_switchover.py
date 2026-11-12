#!/usr/bin/env python3
"""Manual switchover of an async replication between two YDB clusters.

Two clusters A and B hold the same row tables; one is the primary, the other a replica fed by an
async replication object that lives on the replica side. `flip` swaps the roles without copying the
data: it demotes the primary's tables to read-only replicas, drains the changefeeds, finishes the
replication (the replica becomes writable), creates and attaches the reverse replication, probes it
and drops the old object. Every step is a command of its own, safe to repeat, and `flip` runs the
ones a state file does not yet record as done, so an interrupted flip is resumed by running it again.

    init      record the pair, the tables, the current primary and its replication object
    preflight step 0: health, TTL/vector-index refusal, changefeed paths of every target
    fence     step 2: demote every table (and sync index impl table) on the primary
    rollback  undo fence (only before promote)
    drain     step 3: every changefeed consumer has committed its end offset
    promote   step 4: STATE = 'DONE' on the replica side, tables writable; then step 5, compare
    cleanup-changefeeds  step 6: drop the previous direction's changefeeds on the promoted side
    create-reverse       step 7: CREATE ASYNC REPLICATION ... INITIAL_SCAN = FALSE on the old primary
    attach    step 8: STATE = 'STANDBY'
    probe     step 9: one row per table from the new primary to the new replica
    drop-old  step 10: DROP ASYNC REPLICATION ... FORCE; the roles are swapped in the state file
    flip      all of the above, resumable
    failover  unplanned: the primary is lost; finish the replication on the replica side
    fence-lost  the lost primary came back: demote its tables before anything writes to them
    reseed    drop the lost side's tables and re-create them with an initial-scan replication
    status    print the state file and what the clusters say
    rehearse  create tables and a replication, then flip back and forth under writes and check

Requirements: both clusters on a build with TRemoteTopicReaderSettings.SourceIdPrefix (a table that
becomes a replica for the second time would otherwise skip the new stream's records); the legacy
gRPC service on at least one node per cluster (`ydbd db schema exec` sends the demote); row tables
with synchronous, unique or async indexes; no TTL; no DDL on the tables during a flip.
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
PROBE_BASE = 10_000_000  # probe keys live above every rehearsal key
FENCE_PROBE_KEY = 9_000_000_000


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


class StepError(RuntimeError):
    pass


# ---- clusters ---------------------------------------------------------------------------------

class Cluster:
    def __init__(self, name, endpoint, database, extra_create_settings=""):
        self.name, self.endpoint, self.database = name, endpoint, database
        self.extra_create_settings = extra_create_settings
        self.driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.AnonymousCredentials())
        self.driver.wait(timeout=120, fail_fast=False)
        self.pool = ydb.QuerySessionPool(self.driver)

    def __repr__(self):
        return self.name

    def path(self, table):
        return table if table.startswith("/") else f"{self.database}/{table}"

    def sql(self, text, retries=5):
        return self.pool.execute_with_retries(text, retry_settings=ydb.RetrySettings(max_retries=retries),
                                              settings=ydb.BaseRequestSettings().with_timeout(60))

    def try_sql(self, text):
        try:
            self.sql(text)
            return None
        except ydb.Error as e:
            return str(e)

    def digest(self, table):
        rs = self.sql(f"SELECT COUNT(*) AS c, SUM(key) AS s FROM `{self.path(table)}`")
        return (rs[0].rows[0].c, rs[0].rows[0].s)

    def has_row(self, table, key, value):
        rs = self.sql(f"SELECT value FROM `{self.path(table)}` WHERE key = {key}ul")
        return bool(rs[0].rows) and rs[0].rows[0].value == value

    # legacy service calls
    def ydbd(self, *args):
        return subprocess.run([YDBD, "-s", self.endpoint, *args], capture_output=True, text=True)

    def schema_exec(self, proto_text, tag):
        path = f"/tmp/switchover_{self.name}_{tag}.pb.txt"
        with open(path, "w") as f:
            f.write(proto_text)
        out = self.ydbd("db", "schema", "exec", path)
        if out.returncode != 0:
            raise StepError(f"{self}: schema exec {tag} failed rc={out.returncode}\n"
                            f"{out.stdout[-800:]}\n{out.stderr[-800:]}")
        return out.stdout

    def describe_proto(self, path):
        out = self.ydbd("db", "schema", "describe", "-P", path)
        if out.returncode != 0:
            raise StepError(f"{self}: describe {path} failed: {out.stderr[-500:]}")
        return out.stdout

    def path_exists(self, path):
        out = self.ydbd("db", "schema", "describe", "-P", path)
        # the legacy describe answers "Path not found" with exit code 0
        return out.returncode == 0 and "Self {" in out.stdout and "Path not found" not in out.stdout

    def table_info(self, table):
        """Replica mode, TTL, and indexes (name, type) from the full path description."""
        text = self.describe_proto(self.path(table))
        info = {"mode": "NONE", "ttl": False, "indexes": []}
        m = re.search(r"ReplicationConfig \{([^}]*)\}", text)
        if m:
            mm = re.search(r"Mode: (\w+)", m.group(1))
            if mm:
                info["mode"] = mm.group(1).replace("REPLICATION_MODE_", "")
        if re.search(r"TTLSettings \{\s*Enabled \{", text):
            info["ttl"] = True
        for block in re.finditer(r"TableIndexes \{(.*?)\n    \}", text, re.S):
            name = re.search(r'Name: "([^"]+)"', block.group(1))
            kind = re.search(r"Type: (\w+)", block.group(1))
            info["indexes"].append((name.group(1), kind.group(1) if kind else "EIndexTypeGlobal"))
        return info

    def cli(self, *args):
        out = subprocess.run([YDB_CLI, "-e", self.endpoint, "-d", self.database, *args], capture_output=True, text=True)
        if out.returncode != 0:
            raise StepError(f"{self}: ydb {' '.join(args)} failed: {out.stderr[-500:]}")
        return out.stdout

    def describe_replication(self, path):
        return json.loads(self.cli("scheme", "describe", path, "--format", "proto-json-base64"))

    def replication_exists(self, path):
        return self.path_exists(path)

    def consumer_offsets(self, topic):
        d = self.driver.topic_client.describe_consumer(topic, CONSUMER, include_stats=True)
        res = []
        for p in d.partitions:
            end = p.partition_stats.partition_end if p.partition_stats else None
            committed = p.partition_consumer_stats.committed_offset if p.partition_consumer_stats else None
            res.append((p.partition_id, end, committed))
        return res


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
    raise StepError(f"timeout waiting for {what}; last: {last!r}")


def replication_config_proto(working_dir, name, mode, consistency):
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


def drop_cdc_proto(working_dir, name, stream):
    return f"""ModifyScheme {{
  OperationType: ESchemeOpDropCdcStream
  WorkingDir: "{working_dir}"
  DropCdcStream {{
    TableName: "{name}"
    StreamName: "{stream}"
  }}
}}
"""


def parse_duration(text):
    """'10s', '1.5s', '2m' -> seconds."""
    m = re.fullmatch(r"([0-9.]+)(ms|s|m|h)?", text or "")
    if not m:
        return 10.0
    v = float(m.group(1))
    return {"ms": v / 1000, "s": v, "m": v * 60, "h": v * 3600, None: v}[m.group(2)]


# ---- seeding from a backup ----------------------------------------------------------------------

class S3:
    """S3 connection for `ydb export s3` / `ydb import s3`; endpoint is host[:port], scheme separate."""

    def __init__(self, endpoint, bucket, access_key, secret_key, scheme="http", virtual_addressing=False):
        if "://" in endpoint:
            scheme, endpoint = endpoint.split("://", 1)
        self.endpoint, self.bucket, self.scheme = endpoint.rstrip("/"), bucket, scheme
        self.access_key, self.secret_key = access_key, secret_key
        self.virtual_addressing = virtual_addressing

    def args(self):
        return ["--s3-endpoint", self.endpoint, "--scheme", self.scheme, "--bucket", self.bucket,
                "--access-key", self.access_key, "--secret-key", self.secret_key,
                "--use-virtual-addressing", "true" if self.virtual_addressing else "false"]

    def env(self):
        return {}

    @staticmethod
    def from_args(args):
        if not getattr(args, "s3_endpoint", None):
            return None
        return S3(args.s3_endpoint, args.s3_bucket, args.s3_access_key, args.s3_secret_key)


def s3_operation(cluster, operation, s3, items, extra=()):
    """Start an export or import and wait for it; items are (source, destination) pairs."""
    cmd = [operation, "s3", *s3.args()]
    for src, dst in items:
        cmd += ["--item", f"Source={src},Destination={dst}"]
    cmd += list(extra) + ["--format", "proto-json-base64"]
    result = json.loads(cluster.cli(*cmd))
    op_id = result["id"]

    def done():
        r = json.loads(cluster.cli("operation", "get", op_id, "--format", "proto-json-base64"))
        if not r.get("ready"):
            return False
        if r.get("status") != "SUCCESS":
            raise StepError(f"{cluster}: {operation} {op_id} failed: {json.dumps(r)[:600]}")
        return True
    wait_for(done, 900, f"{operation} {op_id} on {cluster}", 2.0)
    cluster.cli("operation", "forget", op_id)
    log(f"{operation} ok on {cluster}: {op_id}")


def seed_from_backup(src, dst, tables, rep, s3, consistency, extra_create_settings, prefix):
    """Create the replication without an initial scan, back the source tables up to S3, restore them
    into the placeholders and attach. This is the documented initial-sync procedure for large data."""
    pairs = ", ".join(f"`{src.path(t)}` AS `{dst.path(t)}`" for t in tables)
    level = ", CONSISTENCY_LEVEL = 'GLOBAL', COMMIT_INTERVAL = Interval('PT1S')" if consistency == "GLOBAL" else ""
    if not dst.replication_exists(rep):
        dst.sql(f"CREATE ASYNC REPLICATION `{rep}` FOR {pairs} "
                f"WITH (CONNECTION_STRING = '{src.endpoint}/?database={src.database}', INITIAL_SCAN = FALSE{level}"
                f"{extra_create_settings})")

    # Every changefeed must exist before the backup is taken, or changes between the backup and the
    # changefeed's creation would be lost; the placeholders must exist for the import to replace.
    def ready():
        d = dst.describe_replication(rep)
        if "error" in d:
            raise StepError(f"seed: {rep}: {d['error']}")
        items = d.get("items", [])
        if not items or not all(i.get("source_changefeed_name") for i in items):
            return False
        return all(dst.table_info(t)["mode"] == "READ_ONLY" for t in tables)
    wait_for(ready, 300, "changefeeds and placeholders for the seed", 2.0)
    log("seed: changefeeds and placeholders ready on", src, "/", dst)

    s3_operation(src, "export", s3, [(src.path(t), f"{prefix}/{t}") for t in tables])
    s3_operation(dst, "import", s3, [(f"{prefix}/{t}", dst.path(t)) for t in tables],
                 extra=["--index-population-mode", "Build"])
    dst.sql(f"ALTER ASYNC REPLICATION `{rep}` SET (STATE = 'STANDBY')")

    def running():
        d = dst.describe_replication(rep)
        if "error" in d:
            raise StepError(f"seed: {rep}: {d['error']}")
        return "running" in d
    wait_for(running, 300, "seeded replication running", 2.0)
    wait_for(lambda: all(dst.digest(t) == src.digest(t) for t in tables), 600, "seeded replica caught up", 2.0)
    log("seed ok:", rep, "attached; replica equals the source")


# ---- state ------------------------------------------------------------------------------------

class State:
    def __init__(self, path):
        self.path = path
        self.d = json.load(open(path)) if os.path.exists(path) else {}

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.d, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)

    def require(self, *keys):
        for k in keys:
            if k not in self.d:
                raise StepError(f"state file {self.path} has no '{k}'; run `init` first")

    @property
    def flip(self):
        return self.d.setdefault("flip", {"n": self.d.get("flips_done", 0) + 1, "steps": {}})

    def step_done(self, name):
        return bool(self.flip["steps"].get(name))

    def mark(self, name, value=True):
        self.flip["steps"][name] = value
        self.save()


# ---- the switchover ----------------------------------------------------------------------------

class Switchover:
    def __init__(self, state, extra_create_settings="", s3=None):
        self.st = state
        self.s3 = s3
        state.require("clusters", "tables", "primary")
        self.clusters = {n: Cluster(n, c["endpoint"], c["database"], extra_create_settings)
                         for n, c in state.d["clusters"].items()}
        self.tables = state.d["tables"]

    # roles
    @property
    def primary(self):
        return self.clusters[self.st.d["primary"]]

    @property
    def replica(self):
        return self.clusters[[n for n in self.clusters if n != self.st.d["primary"]][0]]

    @property
    def consistency(self):
        return self.st.d.get("consistency", "ROW")

    def demotable(self, cluster, table):
        """(working_dir, name, path) of a table and each sync/unique index impl table on a cluster."""
        base = cluster.path(table)
        res = [(os.path.dirname(base), os.path.basename(base), base)]
        for name, kind in cluster.table_info(table)["indexes"]:
            if kind in ("EIndexTypeGlobal", "EIndexTypeGlobalUnique"):
                res.append((f"{base}/{name}", "indexImplTable", f"{base}/{name}/indexImplTable"))
        return res

    def changefeed_paths(self, cluster, desc):
        """Full topic paths of every changefeed of a replication (base and index targets)."""
        paths = []
        tables = {cluster.path(t) for t in self.tables}
        for item in desc.get("items", []):
            sp, cf = item["source_path"], item.get("source_changefeed_name")
            if not cf:
                raise StepError(f"replication item has no changefeed yet: {item}")
            paths.append(f"{sp}/{cf}" if sp in tables else f"{sp}/indexImplTable/{cf}")
        return paths

    # ---- steps ---------------------------------------------------------------------------------
    def preflight(self):
        p, r, st = self.primary, self.replica, self.st
        rep = st.d.get("replication")
        if not rep:
            raise StepError("no replication object recorded; after a failover run `reseed` first")
        desc = r.describe_replication(rep)
        if "running" not in desc or "error" in desc:
            raise StepError(f"replication {rep} is not healthy: {json.dumps(desc)[:400]}")
        if (desc.get("running", {}).get("stats") or {}).get("lag") is None:
            log("preflight: lag is not reported yet; the drain will still wait for the offsets")
        gc = desc.get("global_consistency")
        st.d["consistency"] = "GLOBAL" if gc is not None else "ROW"
        st.d["commit_interval"] = parse_duration((gc or {}).get("commit_interval", "10s")) if gc else None
        for t in self.tables:
            info = p.table_info(t)
            if info["ttl"]:
                raise StepError(f"{p}: table {t} has TTL; not supported by the manual switchover")
            for name, kind in info["indexes"]:
                if kind == "EIndexTypeGlobalVectorKmeansTree":
                    raise StepError(f"{p}: table {t} has vector index {name}; it would be dropped at attach")
            if info["mode"] != "NONE" and not st.step_done("fence"):
                raise StepError(f"{p}: table {t} is already a replica ({info['mode']})")
        paths = self.changefeed_paths(p, desc)
        expected = sum(len(self.demotable(p, t)) for t in self.tables)
        if len(paths) != expected:
            raise StepError(f"replication has {len(paths)} changefeeds, expected {expected} (tables and sync indexes)")
        self.st.flip["changefeeds"] = paths
        self.st.flip["old_replication"] = rep
        self.st.flip["started"] = time.time()
        st.mark("preflight", {"consistency": st.d["consistency"], "changefeeds": paths})
        log("preflight ok:", st.d["consistency"], "changefeeds", paths)

    def fence(self, cluster=None):
        p = cluster or self.primary
        for t in self.tables:
            for wd, name, path in self.demotable(p, t):
                if p.table_info(path)["mode"] == "READ_ONLY":
                    continue
                p.schema_exec(replication_config_proto(wd, name, "REPLICATION_MODE_READ_ONLY", self.consistency),
                              f"demote_{name}_{abs(hash(path))}")
                wait_for(lambda path=path: p.table_info(path)["mode"] == "READ_ONLY", 60, f"{path} read-only")
                log("fence: demoted", path)
        for t in self.tables:
            err = p.try_sql(f"UPSERT INTO `{p.path(t)}` (key, value) VALUES ({FENCE_PROBE_KEY}ul, 'fence-probe'u)")
            if err is None:
                raise StepError(f"fence: a write to {p.path(t)} was accepted after the demote")
            if "replica" not in err.lower():
                raise StepError(f"fence: write to {p.path(t)} failed for another reason: {err[:300]}")
        self.st.flip["fenced_at"] = time.time()
        self.st.mark("fence")
        log("fence ok on", p)

    def rollback(self):
        if self.st.step_done("promote"):
            raise StepError("rollback is only possible before promote; the replica is already writable")
        p = self.primary
        for t in self.tables:
            for wd, name, path in self.demotable(p, t):
                p.schema_exec(replication_config_proto(wd, name, "REPLICATION_MODE_NONE", self.consistency),
                              f"undo_{name}_{abs(hash(path))}")
                wait_for(lambda path=path: p.table_info(path)["mode"] == "NONE", 60, f"{path} writable")
                log("rollback: writable again", path)
        for k in ("fence", "drain"):
            self.st.flip["steps"].pop(k, None)
        self.st.save()
        log("rollback done; the replication keeps running")

    def drain(self, successes_needed=3, poll_interval=2.0, max_polls=150):
        p, r, st = self.primary, self.replica, self.st
        paths = st.flip["changefeeds"]
        is_global = self.consistency == "GLOBAL"
        if is_global:
            time.sleep(2 * (st.d.get("commit_interval") or 10.0))
        successes, last = 0, None
        for _ in range(max_polls):
            snap = [(path, pid, e, c) for path in paths for (pid, e, c) in p.consumer_offsets(path)]
            ok = all(e is not None and c is not None and c >= e for _, _, e, c in snap)
            if ok and (is_global or snap == last):
                successes += 1
                if successes >= successes_needed:
                    break
            elif not is_global:
                successes = 0
            last = snap
            time.sleep(poll_interval)
        else:
            raise StepError(f"drain: offsets did not settle; last {last}")
        log("drain: offsets committed", [(e, c) for _, _, e, c in last])
        if is_global and st.d.get("skip_global_tx_wait"):
            log("drain: skipping the global transaction wait (the server's consistent DONE waits instead)")
        elif is_global:
            # Staged GLOBAL rows are invisible until their transaction commits, and the source is
            # fenced, so equal digests on both sides prove that every interval was committed. The
            # describe's transaction list, when the server reports it, must be empty as well.
            rep = st.flip["old_replication"]
            interval = st.d.get("commit_interval") or 10.0

            def settled():
                tx = r.describe_replication(rep).get("transactions")
                if tx is not None and (tx.get("assigned") or tx.get("pending")):
                    return False
                return all(p.digest(t) == r.digest(t) for t in self.tables)
            wait_for(settled, 300, "global replica equal to the fenced source", interval=1.0)
            time.sleep(interval)
            if not settled():
                raise StepError("drain: the replica changed again after it had caught up")
            log("drain: global replica equal to the source over one commit interval")
        st.mark("drain")

    def promote(self):
        p, r, st = self.primary, self.replica, self.st
        for t in self.tables:
            for _, _, path in self.demotable(p, t):
                if p.table_info(path)["mode"] != "READ_ONLY":
                    raise StepError(f"refusing DONE: {path} on {p} is not a replica (run fence first)")
        rep = st.flip["old_replication"]
        if r.replication_exists(rep) and "done" not in r.describe_replication(rep):
            r.sql(f"ALTER ASYNC REPLICATION `{rep}` SET (STATE = 'DONE', FAILOVER_MODE = 'CONSISTENT')")
        targets = [path for t in self.tables for _, _, path in self.demotable(r, t)]
        try:
            wait_for(lambda: all(r.table_info(x)["mode"] == "NONE" for x in targets), 180, "tables writable after DONE")
        except StepError:
            desc = r.describe_replication(rep) if r.replication_exists(rep) else {}
            log("promote: DONE did not finish in time:", json.dumps(desc)[:300], "; clearing replica mode by hand")
            for t in self.tables:
                for wd, name, path in self.demotable(r, t):
                    if r.table_info(path)["mode"] != "NONE":
                        r.schema_exec(replication_config_proto(wd, name, "REPLICATION_MODE_NONE", self.consistency),
                                      f"clear_{name}_{abs(hash(path))}")
            wait_for(lambda: all(r.table_info(x)["mode"] == "NONE" for x in targets), 60, "tables writable")
        st.flip["promoted_at"] = time.time()
        st.mark("promote")
        downtime = st.flip["promoted_at"] - st.flip.get("fenced_at", st.flip["promoted_at"])
        log("promote ok on", r, "downtime %.1fs" % downtime)
        self.compare()

    def compare(self):
        p, r = self.primary, self.replica
        for t in self.tables:
            dp, dr = p.digest(t), r.digest(t)
            if dp != dr:
                raise StepError(f"compare: {t} differs: {p}={dp} {r}={dr}")
        log("compare ok:", {t: p.digest(t) for t in self.tables})
        self.st.mark("compare")

    def cleanup_changefeeds(self):
        """Drop, on the promoted side, the changefeeds recorded at the previous flip for that side."""
        r, st = self.replica, self.st
        for path in st.d.get("stale", {}).get(r.name, []):
            table, cf = path.rsplit("/", 1)
            if table.endswith("/indexImplTable"):
                wd, name = table.rsplit("/", 1)
                r.schema_exec(drop_cdc_proto(wd, name, cf), f"dropcdc_{cf}")
            else:
                err = r.try_sql(f"ALTER TABLE `{table}` DROP CHANGEFEED `{cf}`")
                if err and "not found" not in err.lower() and "does not exist" not in err.lower():
                    raise StepError(f"cleanup: {path}: {err[:200]}")
            log("cleanup: dropped stale changefeed", path)
        st.d.setdefault("stale", {})[r.name] = []
        st.mark("cleanup-changefeeds")

    def create_reverse(self):
        p, r, st = self.primary, self.replica, self.st
        n = st.d.get("next_rep", 2)
        new_rep = st.flip.get("new_replication") or f"{p.database}/R{n}"
        if not p.replication_exists(new_rep):
            pairs = ", ".join(f"`{r.path(t)}` AS `{p.path(t)}`" for t in self.tables)
            level = ", CONSISTENCY_LEVEL = 'GLOBAL'" if self.consistency == "GLOBAL" else ", CONSISTENCY_LEVEL = 'ROW'"
            if self.consistency == "GLOBAL" and st.d.get("commit_interval"):
                level += f", COMMIT_INTERVAL = Interval('PT{int(st.d['commit_interval'])}S')"
            p.sql(f"CREATE ASYNC REPLICATION `{new_rep}` FOR {pairs} "
                  f"WITH (CONNECTION_STRING = '{r.endpoint}/?database={r.database}', INITIAL_SCAN = FALSE{level}"
                  f"{p.extra_create_settings})")
            st.d["next_rep"] = n + 1
        st.flip["new_replication"] = new_rep
        st.save()

        def ready():
            d = p.describe_replication(new_rep)
            if "error" in d:
                raise StepError(f"{new_rep}: {d['error']}")
            items = d.get("items", [])
            return items and all(i.get("source_changefeed_name") for i in items) and d
        d = wait_for(ready, 180, "reverse changefeeds")
        paths = self.changefeed_paths(r, d)
        for path in paths:
            wait_for(lambda path=path: r.consumer_offsets(path) is not None, 60, f"topic {path}")
        st.flip["new_changefeeds"] = paths
        st.mark("create-reverse")
        log("create-reverse ok:", new_rep, paths)

    def attach(self):
        p, st = self.primary, self.st
        new_rep = st.flip["new_replication"]
        d = p.describe_replication(new_rep)
        if "running" not in d:
            p.sql(f"ALTER ASYNC REPLICATION `{new_rep}` SET (STATE = 'STANDBY')")

        def running():
            d = p.describe_replication(new_rep)
            if "error" in d:
                raise StepError(f"attach: {new_rep}: {d['error']}")
            return "running" in d
        wait_for(running, 180, "reverse replication running")
        st.mark("attach")
        log("attach ok:", new_rep)

    def probe(self):
        p, r, st = self.primary, self.replica, self.st
        key = PROBE_BASE + st.flip["n"]
        for t in self.tables:
            r.sql(f"UPSERT INTO `{r.path(t)}` (key, value) VALUES ({key}ul, 'probe'u)")
        for t in self.tables:
            wait_for(lambda t=t: p.has_row(t, key, "probe"), 120, f"probe row of {t} on {p}", interval=0.5)
        st.mark("probe")
        log("probe ok: every table replicates", r, "->", p)

    def drop_old(self):
        p, r, st = self.primary, self.replica, self.st
        rep = st.flip["old_replication"]
        if r.replication_exists(rep):
            r.sql(f"DROP ASYNC REPLICATION `{rep}` FORCE")
        log("drop-old ok:", rep)
        # swap roles: the old primary's forward changefeeds now sit on a replica, dropped next flip
        st.d.setdefault("stale", {})[p.name] = st.flip["changefeeds"]
        st.d["replication"] = st.flip["new_replication"]
        st.d["replication_cluster"] = p.name
        st.d["primary"] = r.name
        st.d["flips_done"] = st.flip["n"]
        st.d["last_flip"] = st.d.pop("flip")
        st.save()
        log(f"flip #{st.d['flips_done']} complete: primary is now {r}")

    ORDER = ["preflight", "fence", "drain", "promote", "cleanup-changefeeds", "create-reverse", "attach", "probe",
             "drop-old"]

    def flip(self):
        for name in self.ORDER:
            if self.st.step_done(name):
                log("flip: skipping completed step", name)
                continue
            getattr(self, name.replace("-", "_"))()

    # ---- unplanned failover ------------------------------------------------------------------
    def failover(self):
        """The primary is lost. On the replica side: DONE, then DROP ... FORCE."""
        p, r, st = self.primary, self.replica, self.st
        rep = st.d["replication"]
        if r.replication_exists(rep) and "done" not in r.describe_replication(rep):
            r.sql(f"ALTER ASYNC REPLICATION `{rep}` SET (STATE = 'DONE')")
        targets = [path for t in self.tables for _, _, path in self.demotable(r, t)]
        wait_for(lambda: all(r.table_info(x)["mode"] == "NONE" for x in targets), 180, "tables writable after DONE")
        if r.replication_exists(rep):
            r.sql(f"DROP ASYNC REPLICATION `{rep}` FORCE")
        st.d.update({"primary": r.name, "replication": None, "replication_cluster": None, "lost": p.name})
        st.d.setdefault("stale", {})[p.name] = []  # the lost side is re-seeded, nothing to keep
        st.d.pop("flip", None)
        st.save()
        log(f"failover done: {r} is the primary; {p} is lost and must be fenced when it comes back")

    def fence_lost(self):
        lost = self.st.d.get("lost")
        if not lost:
            raise StepError("no lost side recorded")
        self.fence(self.clusters[lost])
        self.st.flip["steps"].pop("fence", None)
        self.st.d.pop("flip", None)
        self.st.save()

    def reseed(self):
        """Drop the lost side's tables and re-create them from the primary with an initial scan."""
        p, st = self.primary, self.st
        lost = st.d.get("lost")
        if not lost:
            raise StepError("no lost side recorded")
        lc = self.clusters[lost]
        for n in range(1, 50):
            lc.try_sql(f"DROP ASYNC REPLICATION `{lc.database}/R{n}` FORCE")
        for t in self.tables:
            lc.try_sql(f"DROP TABLE `{lc.path(t)}`")
        n = st.d.get("next_rep", 2)
        rep = f"{lc.database}/R{n}"
        st.d["next_rep"] = n + 1
        if self.s3:
            seed_from_backup(p, lc, self.tables, rep, self.s3, self.consistency, lc.extra_create_settings,
                             f"reseed-{n}-{int(time.time())}")
        else:
            pairs = ", ".join(f"`{p.path(t)}` AS `{lc.path(t)}`" for t in self.tables)
            level = ", CONSISTENCY_LEVEL = 'GLOBAL'" if self.consistency == "GLOBAL" else ""
            lc.sql(f"CREATE ASYNC REPLICATION `{rep}` FOR {pairs} "
                   f"WITH (CONNECTION_STRING = '{p.endpoint}/?database={p.database}'{level}{lc.extra_create_settings})")
            wait_for(lambda: "running" in lc.describe_replication(rep), 180, "reseed replication running")
            wait_for(lambda: all(lc.digest(t) == p.digest(t) for t in self.tables), 600, "reseed initial scan", 2.0)
        st.d.update({"replication": rep, "replication_cluster": lost, "lost": None})
        st.save()
        log(f"reseed done: {lc} is a replica of {p} again through {rep}")

    def status(self):
        st = self.st
        print(json.dumps(st.d, indent=1, sort_keys=True))
        for n, c in self.clusters.items():
            for t in self.tables:
                print(f"{n} {t}: {c.table_info(t)}")
        rep, rc = st.d.get("replication"), st.d.get("replication_cluster")
        if rep and rc:
            d = self.clusters[rc].describe_replication(rep)
            print(f"{rc} {rep}: {json.dumps({k: d[k] for k in d if k in ('running', 'paused', 'error', 'done')})}")


# ---- rehearsal ---------------------------------------------------------------------------------

class Writer(threading.Thread):
    def __init__(self, cluster, tables, start_key, tag):
        super().__init__(daemon=True)
        self.cluster, self.tables, self.key, self.tag = cluster, tables, start_key, tag
        self.acked = {t: [] for t in tables}
        self.rejected = 0
        self.stop_flag = threading.Event()

    def run(self):
        while not self.stop_flag.is_set():
            k = self.key
            for t in self.tables:
                err = self.cluster.try_sql(f"UPSERT INTO `{self.cluster.path(t)}` (key, value) "
                                           f"VALUES ({k}ul, '{self.tag}{k}'u)")
                if err is None:
                    self.acked[t].append(k)
                else:
                    self.rejected += 1
            self.key += 1
            time.sleep(0.02)

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=30)
        return self.key


def rehearse(args, state_path):
    a = Cluster("A", args.a_endpoint, args.a_database, args.extra_create_settings)
    b = Cluster("B", args.b_endpoint, args.b_database, args.extra_create_settings)
    tables = [f"{args.table_prefix}{i}" for i in range(args.tables)]
    for c in (a, b):
        for n in range(1, 50):
            c.try_sql(f"DROP ASYNC REPLICATION `{c.database}/R{n}` FORCE")
        for t in tables:
            c.try_sql(f"DROP TABLE `{c.path(t)}`")
    for i, t in enumerate(tables):
        idx = ""
        kind = args.index if i % 2 == 0 or args.tables == 1 else "none"
        if kind == "sync":
            idx = ", INDEX by_value GLOBAL SYNC ON (value)"
        elif kind == "async":
            idx = ", INDEX by_value GLOBAL ASYNC ON (value)"
        parts = f" WITH (UNIFORM_PARTITIONS = {args.partitions})" if args.partitions > 1 else ""
        a.sql(f"CREATE TABLE `{a.path(t)}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key){idx}){parts}")
        for off in range(0, args.rows, 500):
            values = ", ".join(f"({k}ul, 'init{k}'u)" for k in range(off, min(off + 500, args.rows)))
            a.sql(f"UPSERT INTO `{a.path(t)}` (key, value) VALUES {values}")
    rep = f"{b.database}/R1"
    s3 = S3.from_args(args)
    if args.seed == "backup":
        if not s3:
            raise StepError("--seed backup needs --s3-endpoint, --s3-bucket, --s3-access-key, --s3-secret-key")
        seed_from_backup(a, b, tables, rep, s3, args.consistency, args.extra_create_settings,
                         f"rehearse-{int(time.time())}")
    else:
        pairs = ", ".join(f"`{a.path(t)}` AS `{b.path(t)}`" for t in tables)
        level = ""
        if args.consistency == "GLOBAL":
            level = ", CONSISTENCY_LEVEL = 'GLOBAL', COMMIT_INTERVAL = Interval('PT1S')"
        b.sql(f"CREATE ASYNC REPLICATION `{rep}` FOR {pairs} "
              f"WITH (CONNECTION_STRING = '{a.endpoint}/?database={a.database}'{level}{args.extra_create_settings})")
        wait_for(lambda: all(b.digest(t) == a.digest(t) for t in tables), 600, "initial copy", 2.0)
        wait_for(lambda: "running" in b.describe_replication(rep), 120, "R1 running")
    log("rehearse: setup done:", len(tables), "tables,", args.rows, "rows each, primary A, replication", rep,
        "seed", args.seed)

    st = State(state_path)
    st.d = {"clusters": {"A": {"endpoint": a.endpoint, "database": a.database},
                         "B": {"endpoint": b.endpoint, "database": b.database}},
            "tables": tables, "primary": "A", "replication": rep, "replication_cluster": "B",
            "stale": {"A": [], "B": []}, "next_rep": 2, "flips_done": 0,
            "skip_global_tx_wait": bool(args.skip_global_tx_wait)}
    st.save()
    sw = Switchover(st, args.extra_create_settings, s3)
    next_key = args.rows
    results = []
    for i in range(args.flips):
        primary, replica = sw.primary, sw.replica
        w = Writer(primary, tables, next_key, f"w{i}_")
        w.start()
        time.sleep(args.write_seconds)
        next_key = w.stop()
        acked = sum(len(v) for v in w.acked.values())
        log(f"rehearse: writer on {primary}: {acked} rows acknowledged, {w.rejected} rejected")
        sw.flip()
        src, dst = replica, primary  # roles after the flip: dst is the new primary
        dst, src = sw.primary, sw.replica
        missing = sum(1 for t in tables for k in w.acked[t] if not dst.has_row(t, k, f"w{i}_{k}"))
        keys = list(range(next_key, next_key + 20))
        next_key += 20
        for t in tables:
            values = ", ".join(f"({k}ul, 'post{i}_{k}'u)" for k in keys)
            dst.sql(f"UPSERT INTO `{dst.path(t)}` (key, value) VALUES {values}")
        try:
            wait_for(lambda: all(src.has_row(t, k, f"post{i}_{k}") for t in tables for k in keys), 120,
                     "post-flip rows on the new replica", 1.0)
            arrived = True
        except StepError as e:
            arrived = False
            log("rehearse: post-flip rows did NOT arrive:", e)
        lf = st.d["last_flip"]
        res = {"flip": i + 1, "src": src.name, "dst": dst.name, "acked": acked, "pre_fence_missing": missing,
               "post_flip_rows_arrived": arrived, "downtime_s": round(lf["promoted_at"] - lf["fenced_at"], 1)}
        results.append(res)
        log("RESULT", res)
        if args.failover_after and i + 1 == args.failover_after:
            # the primary is "lost": stop writing to it, finish on the replica, then fence and re-seed it
            log("rehearse: unplanned failover of", sw.primary)
            lost = sw.primary
            sw.failover()
            sw.fence_lost()
            err = lost.try_sql(f"UPSERT INTO `{lost.path(tables[0])}` (key, value) VALUES ({FENCE_PROBE_KEY}ul, 'x'u)")
            assert err is not None and "replica" in err.lower(), f"lost side accepted a write: {err}"
            sw.reseed()
            results.append({"failover": True, "new_primary": sw.primary.name, "reseeded": sw.replica.name,
                            "equal": all(sw.primary.digest(t) == sw.replica.digest(t) for t in tables)})
            log("RESULT", results[-1])
    print(json.dumps(results, indent=1))
    ok = all((r.get("failover") and r["equal"])
             or (r.get("pre_fence_missing") == 0 and r.get("post_flip_rows_arrived"))
             for r in results)
    return 0 if ok else 2


# ---- main ----------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state-file", default="switchover_state.json")
    ap.add_argument("--extra-create-settings", default="",
                    help="appended inside WITH (...) of every CREATE ASYNC REPLICATION, "
                         "e.g. \", TOKEN_SECRET_PATH = 'x'\"")
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"),
                    help="host[:port] or scheme://host[:port]")
    ap.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    ap.add_argument("--s3-access-key", default=os.environ.get("S3_ACCESS_KEY", "test"))
    ap.add_argument("--s3-secret-key", default=os.environ.get("S3_SECRET_KEY", "test"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    def pair_args(sp):
        sp.add_argument("--a-endpoint", default=os.environ.get("YDB_SOURCE_ENDPOINT"))
        sp.add_argument("--a-database", default=os.environ.get("YDB_SOURCE_DATABASE"))
        sp.add_argument("--b-endpoint", default=os.environ.get("YDB_TARGET_ENDPOINT"))
        sp.add_argument("--b-database", default=os.environ.get("YDB_TARGET_DATABASE"))

    sp = sub.add_parser("init", help="record the pair, tables, primary and replication object")
    pair_args(sp)
    sp.add_argument("--primary", choices=["A", "B"], required=True)
    sp.add_argument("--replication", required=True, help="path of the replication object on the replica side")
    sp.add_argument("--table", action="append", required=True, help="table name relative to the database; repeatable")
    for name in ("preflight", "fence", "rollback", "drain", "promote", "compare", "cleanup-changefeeds",
                 "create-reverse", "attach", "probe", "drop-old", "flip", "failover", "fence-lost", "reseed", "status"):
        sub.add_parser(name)
    sp = sub.add_parser("seed", help="create the replication from the primary to the replica side from an S3 backup")
    sp.add_argument("--prefix", default=None, help="S3 key prefix for the backup (default: seed-<time>)")
    sp = sub.add_parser("rehearse", help="create tables and a replication, flip back and forth under writes")
    pair_args(sp)
    sp.add_argument("--tables", type=int, default=1)
    sp.add_argument("--table-prefix", default="switch_")
    sp.add_argument("--rows", type=int, default=50)
    sp.add_argument("--partitions", type=int, default=1)
    sp.add_argument("--index", choices=["none", "sync", "async"], default="none")
    sp.add_argument("--flips", type=int, default=2)
    sp.add_argument("--consistency", choices=["ROW", "GLOBAL"], default="ROW")
    sp.add_argument("--write-seconds", type=float, default=5.0)
    sp.add_argument("--failover-after", type=int, default=0, help="after this flip, rehearse an unplanned failover")
    sp.add_argument("--seed", choices=["scan", "backup"], default="scan",
                    help="initial sync: CDC initial scan, or S3 backup and restore into placeholders")
    sp.add_argument("--skip-global-tx-wait", action="store_true",
                    help="rely on the server's consistent DONE (EnableConsistentReplicationDone) "
                         "for the GLOBAL drain")
    args = ap.parse_args()

    if args.cmd in ("init", "rehearse"):
        for name in ("a_endpoint", "a_database", "b_endpoint", "b_database"):
            if not getattr(args, name):
                ap.error(f"--{name.replace('_', '-')} is required (or the YDB_SOURCE_*/YDB_TARGET_* variables)")
    if args.cmd == "rehearse":
        return rehearse(args, args.state_file)
    st = State(args.state_file)
    if args.cmd == "init":
        st.d = {"clusters": {"A": {"endpoint": args.a_endpoint, "database": args.a_database},
                             "B": {"endpoint": args.b_endpoint, "database": args.b_database}},
                "tables": args.table, "primary": args.primary, "replication": args.replication,
                "replication_cluster": "B" if args.primary == "A" else "A",
                "stale": {"A": [], "B": []}, "next_rep": 2, "flips_done": 0}
        st.save()
        sw = Switchover(st, args.extra_create_settings, S3.from_args(args))
        sw.status()
        return 0
    sw = Switchover(st, args.extra_create_settings, S3.from_args(args))
    try:
        if args.cmd == "seed":
            if not sw.s3:
                raise StepError("seed needs --s3-endpoint and --s3-bucket")
            n = st.d.get("next_rep", 2)
            rep = f"{sw.replica.database}/R{n}"
            seed_from_backup(sw.primary, sw.replica, sw.tables, rep, sw.s3, sw.consistency,
                             sw.replica.extra_create_settings, args.prefix or f"seed-{int(time.time())}")
            st.d.update({"replication": rep, "replication_cluster": sw.replica.name, "next_rep": n + 1, "lost": None})
            st.save()
            return 0
        getattr(sw, args.cmd.replace("-", "_"))()
    except StepError as e:
        log("FAILED:", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
