"""Authenticated, disk-backed replication recovery checks shared by version tests."""

from contextlib import contextmanager
from enum import Enum
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import grpc
import requests
import yatest.common

from ydb.public.api.grpc import ydb_auth_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_replication_v1_pb2_grpc
from ydb.public.api.protos import ydb_auth_pb2, ydb_status_codes_pb2
from ydb.public.api.protos.draft import ydb_replication_pb2
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root"
USER = "sourceauthuser"
P1 = "ReplicationOriginalPassword27516"
P2 = "ReplicationUpdatedPassword27516"
ADMIN = "root@builtin"
PHASE_TIMEOUT = 120
RPC_TIMEOUT = 10


class SecretKind(Enum):
    LEGACY = "legacy"
    SCHEMA = "schema"


def remaining(deadline):
    value = min(RPC_TIMEOUT, deadline - time.monotonic())
    if value <= 0:
        raise TimeoutError("Phase deadline exceeded")
    return value


def transient(error):
    if isinstance(error, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(error, grpc.RpcError):
        return error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
                                grpc.StatusCode.RESOURCE_EXHAUSTED)
    return isinstance(error, (ydb.Unavailable, ydb.Overloaded, ydb.Timeout, ydb.ConnectionFailure,
                              ydb.ConnectionLost, ydb.DeadlineExceed))


@lru_cache(maxsize=None)
def executable_manifest(path, size, modified_ns):
    # Include stat identity in the key so a rebuilt executable is never confused
    # with an earlier process using the same harness path.
    digest = hashlib.sha256()
    with open(path, "rb") as executable:
        for chunk in iter(lambda: executable.read(8 * 1024**2), b""):
            digest.update(chunk)
    version = subprocess.run([path, "-V"], capture_output=True, text=True, timeout=30, check=True)
    return {"resolved_binary": path, "binary_sha256": digest.hexdigest(),
            "binary_version": version.stdout + version.stderr}


@contextmanager
def rejecting_source_endpoint(source):
    """Observe only a gRPC connection preface, then cause a transport failure."""
    preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stopped = threading.Event()
    lock = threading.Lock()
    observations = []
    errors = []
    thread = None

    def accept_connections():
        while not stopped.is_set():
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError as error:
                if not stopped.is_set():
                    errors.append(type(error).__name__)
                return
            with connection:
                connection.settimeout(1)
                received = b""
                try:
                    while len(received) < len(preface):
                        chunk = connection.recv(len(preface) - len(received))
                        if not chunk:
                            break
                        received += chunk
                except (socket.timeout, OSError):
                    continue
                if received == preface:
                    with lock:
                        observations.append(time.time())
                # Read no request headers or payload. Closing this connection
                # produces a real transport error for the destination SDK.

    def snapshot():
        assert not errors, f"Cold source listener failed: {errors}"
        with lock:
            return {"connection_count": len(observations), "timestamps": list(observations)}

    try:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((source.cluster.nodes[1].host, source.cluster.nodes[1].port))
        listener.listen()
        listener.settimeout(0.2)
        thread = threading.Thread(target=accept_connections, name="cold-source-preface", daemon=True)
        thread.start()
        yield snapshot
    finally:
        stopped.set()
        listener.close()
        if thread is not None:
            thread.join(timeout=2)
            assert not thread.is_alive(), "Cold source listener did not stop"


class Database:
    def __init__(self, name, *, binary_paths=None, nodes=1, extra_flags=(), disabled_flags=(), tenant_slots=0, replication_log_level=7, account_lockout_attempt_threshold=None, enable_audit_log=False):
        self.name = name
        self.database_path = f"{DATABASE}/tenant" if tenant_slots else DATABASE
        self.tenant_slots = tenant_slots
        self.directory = Path(yatest.common.output_path(name))
        self.directory.mkdir(exist_ok=True)
        self.config = KikimrConfigGenerator(
            nodes=nodes, erasure=Erasure.NONE, binary_paths=binary_paths,
            output_path=str(self.directory), pdisk_store_path=str(self.directory),
            static_pdisk_size=16 * 1024**3, use_in_memory_pdisks=False,
            extra_grpc_services=["replication"], default_clusteradmin=ADMIN,
            additional_log_configs={"REPLICATION_SERVICE": replication_log_level,
                                    "REPLICATION_CONTROLLER": replication_log_level},
            enforce_user_token_requirement=True, enable_audit_log=enable_audit_log,
            extra_feature_flags=["enable_schema_secrets", *extra_flags],
            disabled_feature_flags=["disable_old_secret_creation", "disable_old_secrets", *disabled_flags],
        )
        if account_lockout_attempt_threshold is not None:
            self.config.yaml_config.setdefault("auth_config", {})["account_lockout"] = {
                "attempt_threshold": account_lockout_attempt_threshold}
        if tenant_slots:
            # Dynamic nodes authenticate their Console config fetch separately
            # from source replication. Match the existing security fixtures.
            self.config.yaml_config.setdefault("auth_config", {})["staff_api_user_token"] = ADMIN
        self.cluster = KiKiMR(self.config, cluster_name=name)
        self.disk_paths = [Path(disk["pdisk_path"]) for disk in self.config.pdisks_info]
        assert self.disk_paths and all(path.parent.resolve() == self.directory.resolve() for path in self.disk_paths)
        self.driver = None
        self.pool = None
        self.channel = None
        self.tokens = []
        self.last_state = None
        self.last_issue = ""
        self.last_rows = None
        self.phase = "setup"
        self.saved_logs = set()
        self.restart_sequence = 0

    def processes(self):
        return [*self.cluster.nodes.values(), *self.cluster.slots.values()]

    @property
    def endpoint(self):
        return self.cluster.nodes[1].endpoint

    def redact(self, value):
        text = str(value)
        for secret in [P1, P2, *self.tokens]:
            text = text.replace(secret, "<redacted>")
        return text

    def metadata(self):
        return (("x-ydb-database", self.database_path), ("x-ydb-auth-ticket", ADMIN))

    def connect(self):
        self.driver = ydb.Driver(endpoint=self.endpoint, database=self.database_path,
                                 credentials=ydb.AccessTokenCredentials(ADMIN))
        self.driver.wait(timeout=30)
        self.pool = ydb.QuerySessionPool(self.driver)
        self.channel = grpc.insecure_channel(self.endpoint)
        self.poll("query service readiness", lambda timeout: self.query("SELECT 1;", timeout))

    def disconnect(self):
        if self.pool is not None:
            self.pool.stop(timeout=10)
            self.pool = None
        if self.driver is not None:
            self.driver.stop(timeout=10)
            self.driver = None
        if self.channel is not None:
            self.channel.close()
            self.channel = None

    def stop_nodes(self):
        self.saved_logs.update(self.log_offsets())
        self.disconnect()
        for node in self.processes():
            node.stop()

    def start_nodes(self):
        # Restart existing node objects; KiKiMR.start() would format their disks.
        # The harness truncates the ydbd auxiliary log on start, so use a fresh
        # file and retain all prior paths for the final credential scan.
        self.restart_sequence += 1
        for node in self.processes():
            node.set_log_file_prefix(f"restart_{self.restart_sequence}_")
            node.start()
        self.connect()

    def restart(self):
        disks = self.storage_identity()
        self.stop_nodes()
        self.start_nodes()
        assert self.storage_identity() == disks, "Restart changed destination PDisks"

    def storage_identity(self):
        return [(str(path), path.stat().st_ino, path.stat().st_size) for path in self.disk_paths]

    def query(self, text, timeout=RPC_TIMEOUT):
        return self.pool.execute_with_retries(
            text, retry_settings=ydb.RetrySettings(max_retries=0, max_session_acquire_timeout=timeout),
            settings=ydb.BaseRequestSettings().with_timeout(timeout),
        )

    def rows(self, timeout=RPC_TIMEOUT):
        self.last_rows = [(int(row["id"]), row["value"])
                          for row in self.query("SELECT id, value FROM t ORDER BY id;", timeout)[0].rows]
        return self.last_rows

    def replication(self, timeout=RPC_TIMEOUT):
        response = ydb_replication_v1_pb2_grpc.ReplicationServiceStub(self.channel).DescribeReplication(
            ydb_replication_pb2.DescribeReplicationRequest(path=f"{self.database_path}/r", include_stats=True),
            metadata=self.metadata(), timeout=timeout,
        )
        assert response.operation.ready
        ydb.issues._process_response(response.operation)
        result = ydb_replication_pb2.DescribeReplicationResult()
        assert response.operation.result.Unpack(result)
        self.last_state = result.WhichOneof("state")
        self.last_issue = self.redact(result.error.issues)
        return result

    def poll(self, phase, predicate, *, allow_missing=False):
        self.phase = phase
        deadline = time.monotonic() + PHASE_TIMEOUT
        last_error = None
        while time.monotonic() < deadline:
            try:
                value = predicate(remaining(deadline))
                if value:
                    return value
            except (ydb.Error, grpc.RpcError, requests.ConnectionError, requests.Timeout) as error:
                if not transient(error) and not (allow_missing and isinstance(error, ydb.SchemeError)):
                    raise AssertionError(self.redact(error)) from None
                last_error = self.redact(error)
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        raise AssertionError(f"{self.name}: {phase} timed out; last_observed_state={self.last_state}; "
                             f"issue={self.last_issue}; rows={self.last_rows}; error={last_error}; "
                             f"nodes/logs={self.node_manifest()}")

    def wait_state(self, state):
        def ready(timeout):
            result = self.replication(timeout)
            return result if result.WhichOneof("state") == state else None
        return self.poll(f"public {state}", ready)

    def wait_rows(self, expected):
        self.poll(f"exact rows {expected}", lambda timeout: self.rows(timeout) == expected, allow_missing=True)

    def login(self, password, expected):
        with grpc.insecure_channel(self.endpoint) as channel:
            response = ydb_auth_v1_pb2_grpc.AuthServiceStub(channel).Login(
                ydb_auth_pb2.LoginRequest(user=USER, password=password),
                metadata=(("x-ydb-database", self.database_path),), timeout=RPC_TIMEOUT,
            )
        assert response.operation.status == expected, "Unexpected fresh source Login status"
        if expected == ydb_status_codes_pb2.StatusIds.SUCCESS:
            result = ydb_auth_pb2.LoginResult()
            assert response.operation.result.Unpack(result) and result.token
            self.tokens.append(result.token)

    def describe(self, path, timeout=RPC_TIMEOUT):
        node = self.cluster.nodes[1]
        result = requests.get(f"http://{node.host}:{node.mon_port}/viewer/json/describe",
                              params={"database": self.database_path, "path": path, "enums": "true"},
                              headers={"Authorization": ADMIN}, timeout=timeout)
        result.raise_for_status()
        description = result.json()
        assert description["Status"] == "StatusSuccess", self.redact(description)
        return description["PathDescription"]

    def identities(self):
        replication = self.describe(f"{self.database_path}/r")
        destination = self.describe(f"{self.database_path}/t")

        def identity(description):
            return (int(description["Self"]["SchemeshardId"]), int(description["Self"]["PathId"]))
        return {"replication": identity(replication), "destination": identity(destination),
                "controller": int(replication["ReplicationDescription"]["ControllerId"])}

    def node_manifest(self):
        result = []
        for node in self.processes():
            binary = Path(node.binary_path).resolve()
            stat = binary.stat()
            pid = node.pid if node.is_alive() else None
            live_executable = None
            if pid is not None and sys.platform.startswith("linux"):
                proc_exe = Path(f"/proc/{pid}/exe")
                live_executable = os.readlink(proc_exe)
                assert os.path.samefile(proc_exe, binary), "Live executable differs from configured binary"
            result.append({"harness_node": node.node_id, "binary": node.binary_path,
                           **executable_manifest(str(binary), stat.st_size, stat.st_mtime_ns),
                           "alive": pid is not None, "pid": pid, "live_executable": live_executable,
                           "stdout": node.stdout_file_name, "stderr": node.stderr_file_name,
                           "log": node.ydbd_log_file_path})
        return result

    def record(self, phase, **values):
        with (self.directory / "credential-stages.jsonl").open("a") as output:
            output.write(json.dumps({"phase": phase, "time": time.time(), "nodes": self.node_manifest(),
                                     "storage": self.storage_identity(), **values}) + "\n")

    def log_offsets(self):
        return {path: Path(path).stat().st_size
                for node in self.processes()
                for path in (node.stdout_file_name, node.stderr_file_name, node.ydbd_log_file_path) if path and Path(path).exists()}

    def log_lines_after(self, offsets):
        for node in self.processes():
            for path in (node.stdout_file_name, node.stderr_file_name, node.ydbd_log_file_path):
                if path and Path(path).exists():
                    with open(path, "rb") as log:
                        log.seek(offsets.get(path, 0))
                        for line in log:
                            yield path, line.decode(errors="replace")

    def wait_cold_source_attempt(self, offsets, connection_probe):
        def observed(timeout):
            assert self.replication(timeout).WhichOneof("state") != "error"
            connections = connection_probe()
            if not connections["connection_count"]:
                return False
            for path, line in self.log_lines_after(offsets):
                if "Handshake with reader" in line and "actorClassName=Worker " in line:
                    self.record("cold-replication-attempt", log=path, event=self.redact(line.strip()),
                                **connections)
                    return True
            return False
        self.poll("cold worker reader handshake and source connection", observed)

    def assert_no_secrets(self, issue="", extra_tokens=()):
        secrets = [P1, P2, *self.tokens, *extra_tokens]
        assert not any(secret in str(issue) for secret in secrets), "Credential leaked in replication issue"
        paths = self.saved_logs | set(self.log_offsets())
        for path in paths:
            if Path(path).exists():
                text = Path(path).read_text(errors="replace")
                assert not any(secret in text for secret in secrets), f"Credential leaked in {path}"


@contextmanager
def database(name, **options):
    instance = Database(name, **options)
    try:
        instance.cluster.start()
        if instance.tenant_slots:
            instance.cluster.create_database(instance.database_path, storage_pool_units_count={"hdd": 1},
                                             timeout_seconds=60, token=ADMIN)
            instance.cluster.register_and_start_slots(instance.database_path, instance.tenant_slots)
            instance.cluster.wait_tenant_up(instance.database_path, token=ADMIN)
        instance.connect()
        yield instance
    finally:
        try:
            instance.disconnect()
        finally:
            instance.cluster.stop()
            for path in instance.disk_paths:
                path.unlink(missing_ok=True)


def secret_option(kind):
    return "PASSWORD_SECRET_NAME" if kind == SecretKind.LEGACY else "PASSWORD_SECRET_PATH"


def secret_name(kind, name="pw", database_path=DATABASE):
    return name if kind == SecretKind.LEGACY else f"{database_path}/{name}"


def write_secret(destination, kind, password, *, create=False, name="pw"):
    if kind == SecretKind.LEGACY:
        statement = f"CREATE OBJECT {name} (TYPE SECRET) WITH" if create else f"ALTER OBJECT {name} (TYPE SECRET) SET"
        destination.query(f"{statement} value='{password}';")
    else:
        path = f"{destination.database_path}/{name}"
        before = None if create else destination.describe(path)
        statement = "CREATE" if create else "ALTER"
        destination.query(f"{statement} SECRET `{path}` WITH (value='{password}');")

        def changed(timeout):
            after = destination.describe(path, timeout)
            version = int(after["SecretDescription"]["Version"])
            identity = (after["Self"]["SchemeshardId"], after["Self"]["PathId"])
            if before is not None:
                assert identity == (before["Self"]["SchemeshardId"], before["Self"]["PathId"])
                if version <= int(before["SecretDescription"]["Version"]):
                    return False
            destination.record("schema-secret-visible", path=path, identity=identity, version=version)
            return True
        destination.poll("completed schema secret version visible", changed)


def create_replication(source, destination, kind):
    source.query(f"CREATE USER {USER} PASSWORD '{P1}';")
    source.query(f"GRANT ALL ON `{source.database_path}` TO {USER};")
    source.query("CREATE TABLE t (id Uint64 NOT NULL, value Utf8, PRIMARY KEY(id));")
    source.login(P1, ydb_status_codes_pb2.StatusIds.SUCCESS)
    write_secret(destination, kind, P1, create=True)
    write_secret(destination, kind, P1, create=True, name="alternatepw")
    destination.query(
        'CREATE ASYNC REPLICATION r FOR t AS t WITH ('
        f'CONNECTION_STRING="grpc://{source.endpoint}/?database={source.database_path}", '
        f'USER="{USER}", {secret_option(kind)}="{secret_name(kind, database_path=destination.database_path)}", '
        'CONSISTENCY_LEVEL="GLOBAL", COMMIT_INTERVAL=Interval("PT1S"));'
    )
    source.query('UPSERT INTO t (id, value) VALUES (1u, "initial"), (2u, "remove-me");')
    destination.wait_rows([(1, "initial"), (2, "remove-me")])
    destination.wait_state("running")
    identities = destination.identities()
    destination.record("initial", identities=identities)
    return identities


def cleanup_objects(source, destination, kind):
    # Shutdown remains unconditional even if a broken cluster cannot execute DDL.
    commands = [(destination, "DROP ASYNC REPLICATION r CASCADE;"), (source, "DROP TABLE t;")]
    for name in ("pw", "alternatepw"):
        command = f"DROP OBJECT {name} (TYPE SECRET);" if kind == SecretKind.LEGACY else f"DROP SECRET `{destination.database_path}/{name}`;"
        commands.append((destination, command))
    for db, command in commands:
        if db.pool is not None:
            try:
                db.query(command, timeout=5)
            except (ydb.Error, grpc.RpcError):
                pass


def rotate_to_error(source, destination, identities):
    source.query(f"ALTER USER {USER} PASSWORD '{P2}';")
    source.login(P2, ydb_status_codes_pb2.StatusIds.SUCCESS)
    source.login(P1, ydb_status_codes_pb2.StatusIds.UNAUTHORIZED)
    destination.restart()
    source.query('UPSERT INTO t (id, value) VALUES (1002u, "during-error");')
    result = destination.wait_state("error")
    issue = str(result.error.issues)
    assert issue and "auth" in issue.lower(), "Public Error lacks useful authentication cause"
    destination.assert_no_secrets(issue, source.tokens)
    assert destination.identities() == identities
    destination.record("error", identities=identities, issue=destination.redact(issue))
    destination.restart()
    result = destination.wait_state("error")
    assert result.error.issues, "Persisted Error lost its issue"
    assert destination.identities() == identities
    destination.record("error-after-restart", identities=identities)


def recover(source, destination, kind, identities, extra_rows=()):
    initial_state = destination.replication().WhichOneof("state")
    assert initial_state in ("error", "paused"), f"Unexpected recovery starting state: {initial_state}"
    write_secret(destination, kind, P2)
    write_secret(destination, kind, P2, name="alternatepw")
    # ALTER completion precedes credential reset. Legacy TEvAskSnapshot forces a
    # refresh and waits for snapshot actuality >= request time. Schema ALTER is
    # followed by a visible version increment with the same secret path identity;
    # successful data recovery independently proves the resolver used the value.
    if initial_state == "error":
        destination.query('ALTER ASYNC REPLICATION r SET (STATE="PAUSED");')
    destination.wait_state("paused")
    destination.record("paused", identities=destination.identities())
    for name in ("alternatepw", "pw"):
        destination.query(f'ALTER ASYNC REPLICATION r SET ({secret_option(kind)}="{secret_name(kind, name, destination.database_path)}");')
    destination.query('ALTER ASYNC REPLICATION r SET (STATE="STANDBY");')
    destination.wait_state("running")
    destination.wait_rows(sorted([(1, "initial"), (2, "remove-me"), (1002, "during-error"), *extra_rows]))
    source.query('UPSERT INTO t (id, value) VALUES (1u, "updated"), (1004u, "after-recovery");')
    source.query('DELETE FROM t WHERE id=2u;')
    expected = sorted([(1, "updated"), (1002, "during-error"), (1004, "after-recovery"), *extra_rows])
    destination.wait_rows(expected)
    assert destination.identities() == identities
    destination.restart()
    destination.wait_rows(expected)
    destination.wait_state("running")
    source.query('UPSERT INTO t (id, value) VALUES (1005u, "after-restart");')
    destination.wait_rows(sorted(expected + [(1005, "after-restart")]))
    assert destination.identities() == identities
    destination.assert_no_secrets(extra_tokens=source.tokens)
    source.assert_no_secrets()
    destination.record("recovered-after-restart", identities=identities, rows=destination.last_rows)
