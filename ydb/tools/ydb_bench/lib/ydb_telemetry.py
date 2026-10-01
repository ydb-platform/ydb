"""Bounded, best-effort executor-pool telemetry from local YDB monitoring ports."""

import json
import gzip
import logging
import math
import socket
import threading
import time
import uuid
from pathlib import Path
from collections import deque
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

COUNTERS = (
    "CurrentThreadCountPercent",
    "DefaultThreadCountPercent",
    "MaxThreadCountPercent",
    "PossibleMaxThreadCountPercent",
    "PotentialMaxThreadCountPercent",
    "ElapsedMicrosec",
    "CpuMicrosec",
)
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_VIEW_BYTES = 2 * 1024 * 1024
MAX_ARCHIVE_RESPONSE_BYTES = 16 * 1024 * 1024
ARCHIVE_PART_BYTES = 16 * 1024 * 1024
ARCHIVE_STATE_BYTES = 32 * 1024 * 1024
ARCHIVE_FORMAT = "ydb-counters-delta-v1"


def _archive_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class CountersArchiveEncoder:
    """Dictionary and last successful snapshot, scoped to one archive part."""

    def __init__(self):
        self.definitions = {}
        self.nodes = {}
        self.state_bytes = 0
        self.sequence = 0

    def encode(self, record):
        result = {key: value for key, value in record.items() if key != "counters"}
        result["format"] = ARCHIVE_FORMAT
        result["sequence"] = self.sequence
        self.sequence += 1
        if "counters" not in record:
            return result
        node = tuple(record[key] for key in ("host", "role", "index", "port"))
        old_order, old_values = self.nodes.get(node, (None, {}))
        order, values, definitions, changes = [], {}, [], []
        occurrences = {}
        for sensor in record["counters"]["sensors"]:
            definition = {key: value for key, value in sensor.items() if key not in ("value", "hist")}
            key = _archive_json(definition)
            occurrence = occurrences.get(key, 0)
            occurrences[key] = occurrence + 1
            identity = (key, occurrence)
            if identity not in self.definitions:
                self.definitions[identity] = len(self.definitions)
                self.state_bytes += len(key)
                definitions.append([self.definitions[identity], definition])
            metric = self.definitions[identity]
            value = {key: value for key, value in sensor.items() if key in ("value", "hist")}
            encoded = _archive_json(value)
            order.append(metric)
            values[metric] = encoded
            # A newly present scalar defaults to integer zero, never a missing sample.
            if encoded != old_values.get(metric, '{"value":0}'):
                changes.append([metric, value])
        self.state_bytes += sum(map(len, values.values())) - sum(map(len, old_values.values()))
        self.nodes[node] = (order, values)
        result.update(definitions=definitions, changes=changes)
        if order != old_order:
            result["present"] = order
        result["counter_metadata"] = {key: value for key, value in record["counters"].items() if key != "sensors"}
        return result


def read_counters_archive(path):
    """Yield reconstructed snapshots from one part, including legacy full snapshots."""
    definitions, nodes = {}, {}
    sequence = 0
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            if "format" not in record:
                yield record
                continue
            if record["format"] != ARCHIVE_FORMAT:
                raise ValueError("unsupported YDB counters archive format")
            if record.get("sequence") != sequence:
                raise ValueError("missing or reordered counter record")
            sequence += 1
            result = {
                key: value
                for key, value in record.items()
                if key not in ("format", "sequence", "definitions", "changes", "present", "counter_metadata")
            }
            if "error" in record:
                yield result
                continue
            for metric, definition in record["definitions"]:
                if metric in definitions:
                    raise ValueError("duplicate counter definition")
                definitions[metric] = definition
            node = tuple(record[key] for key in ("host", "role", "index", "port"))
            old_order, old_values = nodes.get(node, (None, {}))
            order = record.get("present", old_order)
            if order is None or len(set(order)) != len(order):
                raise ValueError("missing or invalid counter checkpoint")
            values = {metric: old_values.get(metric, {"value": 0}) for metric in order}
            for metric, value in record["changes"]:
                if metric not in values:
                    raise ValueError("change for absent counter")
                values[metric] = value
            if any(metric not in definitions for metric in order):
                raise ValueError("undefined counter")
            # Copy through JSON so callers cannot mutate subsequent decoded snapshots.
            result["counters"] = json.loads(
                _archive_json(
                    dict(
                        record["counter_metadata"],
                        sensors=[dict(definitions[metric], **values[metric]) for metric in order],
                    )
                )
            )
            nodes[node] = (order, values)
            yield result


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def parse_counters(payload):
    pools = {}
    if not isinstance(payload, dict) or not isinstance(payload.get("sensors"), list):
        raise ValueError("monitoring response has no sensors list")
    for sensor in payload["sensors"]:
        if not isinstance(sensor, dict):
            continue
        labels = sensor.get("labels")
        if not isinstance(labels, dict):
            continue
        pool, name, value = labels.get("execpool"), labels.get("sensor"), sensor.get("value")
        if not isinstance(pool, str) or not pool or len(pool) > 128 or name not in COUNTERS:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            continue
        try:
            if not math.isfinite(value):
                continue
        except OverflowError:
            continue
        if pool not in pools and len(pools) >= 32:
            raise ValueError("too many executor pools")
        values = pools.setdefault(pool, {})
        if name in values:
            raise ValueError("ambiguous executor-pool counter labels")
        values[name] = value
    return pools


class YdbCountersMonitor:
    def __init__(self, path, nodes, context, interval=2.0, *, collect_archive=True):
        self.path = Path(path) if path is not None else None
        self.nodes = nodes
        self.context = dict(context)
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self._previous = {}
        self._write_lock = threading.Lock()
        self.error = None
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())
        self.archive = None
        if collect_archive and self.path is not None:
            self.archive = YdbCountersArchive(self.path.parent / "ydb-counters", nodes, context, interval=5.0)

    def start(self):
        if self.archive is not None:
            self.archive.start()
        if self.path is not None:
            self._thread = threading.Thread(target=self._run, name="ydb-bench-ydb-counters", daemon=True)
            try:
                self._thread.start()
            except RuntimeError as error:
                self._thread = None
                self.error = str(error)[:300]
                logging.warning("YDB counters collection could not start: %s", self.error)
        return self

    def stop(self):
        if self.archive is not None:
            self.archive.stop()
            if self.archive.error:
                self.error = self.archive.error
        with self._write_lock:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _fetch(self, port):
        return parse_counters(
            json.loads(self._fetch_payload(port, "/counters/counters=utils/json", MAX_RESPONSE_BYTES))
        )

    def _fetch_payload(self, port, endpoint, limit):
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("invalid local monitoring port")
        url = "http://127.0.0.1:{}{}".format(port, endpoint)
        deadline = time.monotonic() + 2.0
        with self._opener.open(url, timeout=1.0) as response:
            payload = bytearray()
            while len(payload) <= limit:
                if self._stop.is_set() or time.monotonic() >= deadline:
                    raise ValueError("monitoring request cancelled or timed out")
                chunk = response.read1(min(65536, limit + 1 - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
        if len(payload) > limit:
            raise ValueError("monitoring response is too large")
        return payload

    def _sample(self):
        record = {"timestamp_unix": time.time(), "context": self.context, "nodes": []}
        nodes = self.nodes()
        if not isinstance(nodes, (tuple, list)):
            raise ValueError("monitoring node list is unavailable")
        if len(nodes) > 64:
            record["error"] = "Only the first 64 nodes are sampled"
        for role, index, port in nodes[:64]:
            if self._stop.is_set():
                break
            node = {"role": role, "index": index, "port": port}
            key = (role, index, port)
            try:
                pools = self._fetch(port)
                now = time.monotonic()
                node.update(timestamp_unix=time.time(), pools=pools, rates={})
                if not pools:
                    node["error"] = "Requested executor-pool counters are unavailable"
                previous = self._previous.get(key)
                if previous and now > previous[0]:
                    for pool, counters in pools.items():
                        rates = {}
                        for name in ("ElapsedMicrosec", "CpuMicrosec"):
                            old = previous[1].get(pool, {}).get(name)
                            current = counters.get(name)
                            if old is not None and current is not None and current >= old:
                                rate = (current - old) / (now - previous[0])
                                if math.isfinite(rate):
                                    rates[name] = rate
                        node["rates"][pool] = rates
                self._previous[key] = (now, pools)
            except (OSError, ValueError, TypeError) as error:
                node["error"] = str(error)[:300]
                self._previous.pop(key, None)
            record["nodes"].append(node)
        return record

    def _run(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            size = self.path.stat().st_size if self.path.exists() else 0
            if size >= MAX_FILE_BYTES:
                return
            if size:
                with self.path.open("rb") as previous:
                    previous.seek(max(0, size - 1024))
                    if b'"truncated":true' in previous.read():
                        return
            with self.path.open("ab") as stream:
                while not self._stop.is_set():
                    record = self._sample()
                    payload = (json.dumps(record, allow_nan=False, separators=(",", ":")) + "\n").encode()
                    with self._write_lock:
                        if self._stop.is_set():
                            break
                        if size + len(payload) > MAX_FILE_BYTES - 1024:
                            stream.write(b'{"error":"YDB metrics storage limit reached","truncated":true,"nodes":[]}\n')
                            break
                        stream.write(payload)
                        stream.flush()
                    size += len(payload)
                    if self._stop.wait(self.interval):
                        break
        except Exception as error:
            self.error = str(error)[:300]
            logging.warning("YDB counters collection stopped: %s", self.error)


class YdbCountersArchive(YdbCountersMonitor):
    """Complete monitoring snapshots, stored only as concatenated gzip members."""

    def __init__(self, path, nodes, context, interval=5.0):
        super().__init__(path, nodes, context, interval, collect_archive=False)

    def stop(self):
        super().stop()
        # Artifact hashing must not race with the last archive write.
        if self._thread is not None:
            self._thread.join()

    def _run(self):
        try:
            self.path.mkdir(parents=True, exist_ok=True)
            session = uuid.uuid4().hex
            part, size = 0, 0
            encoder = CountersArchiveEncoder()
            while not self._stop.is_set():
                started = time.monotonic()
                nodes = self.nodes()
                if not isinstance(nodes, (tuple, list)):
                    raise ValueError("monitoring node list is unavailable")
                for role, index, port in nodes:
                    if self._stop.is_set():
                        break
                    record = {
                        "timestamp_unix": time.time(),
                        "context": self.context,
                        "host": socket.gethostname(),
                        "role": role,
                        "index": index,
                        "port": port,
                    }
                    try:
                        payload = self._fetch_payload(port, "/counters/json?@private=1", MAX_ARCHIVE_RESPONSE_BYTES)
                        value = json.loads(payload)
                        if not isinstance(value, dict) or not isinstance(value.get("sensors"), list):
                            raise ValueError("monitoring response has no sensors list")
                        if any(not isinstance(sensor, dict) for sensor in value["sensors"]):
                            raise ValueError("monitoring response has invalid sensors")
                        record["counters"] = value
                    except (OSError, ValueError, TypeError) as error:
                        record["error"] = str(error)[:300]
                    if size and encoder.state_bytes >= ARCHIVE_STATE_BYTES:
                        part, size = part + 1, 0
                        encoder = CountersArchiveEncoder()
                    compressed = gzip.compress((_archive_json(encoder.encode(record)) + "\n").encode(), compresslevel=1)
                    if size and size + len(compressed) > ARCHIVE_PART_BYTES:
                        part, size = part + 1, 0
                        encoder = CountersArchiveEncoder()
                        compressed = gzip.compress(
                            (_archive_json(encoder.encode(record)) + "\n").encode(), compresslevel=1
                        )
                    with self._write_lock:
                        if self._stop.is_set():
                            break
                        with (self.path / (session + "-{:06d}.jsonl.gz".format(part))).open("ab") as stream:
                            stream.write(compressed)
                    size += len(compressed)
                if self._stop.wait(max(0, self.interval - (time.monotonic() - started))):
                    break
        except Exception as error:
            self.error = str(error)[:300]
            logging.warning("Full YDB counters collection stopped: %s", self.error)


def read_metrics(path, attempt):
    """Read one attempt, including early attempts; bound both scanning and response size."""
    path = Path(path)
    if not path.is_file():
        return {"samples": [], "truncated": False}
    records = deque()
    retained_bytes = 0
    invalid = 0
    truncated = False
    with path.open("rb") as stream:
        scanned = 0
        while scanned < MAX_FILE_BYTES:
            line = stream.readline(min(MAX_VIEW_BYTES + 1, MAX_FILE_BYTES - scanned))
            if not line:
                break
            scanned += len(line)
            if not line.endswith(b"\n"):
                truncated = truncated or len(line) > MAX_VIEW_BYTES or scanned >= MAX_FILE_BYTES
                break
            try:
                record = json.loads(line)
                if not isinstance(record, dict) or not isinstance(record.get("nodes"), list):
                    raise ValueError("invalid metrics record")
                truncated = truncated or bool(record.get("truncated"))
                context = record.get("context", {})
                if not isinstance(context, dict):
                    raise ValueError("invalid metrics context")
                selected = "verification" if context.get("verification") is True else str(context.get("attempt"))
                if selected != str(attempt):
                    continue
                record = _project_record(record)
                encoded_size = len(json.dumps(record).encode())
                records.append((record, encoded_size))
                retained_bytes += encoded_size
                while len(records) > 300 or retained_bytes > MAX_VIEW_BYTES:
                    retained_bytes -= records.popleft()[1]
                    truncated = True
            except (ValueError, UnicodeError, TypeError, RecursionError):
                invalid += 1
        truncated = truncated or bool(stream.read(1))
    return {
        "samples": [record for record, _ in records],
        "truncated": truncated,
        "invalid_records": invalid,
    }


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or value < 0:
        return None
    try:
        return value if math.isfinite(value) else None
    except OverflowError:
        return None


def _project_record(record):
    context = record["context"]
    result = {
        "timestamp_unix": _number(record.get("timestamp_unix")),
        "context": {key: _number(context.get(key)) for key in ("attempt", "repetition", "load", "dynamic_nodes")},
        "nodes": [],
    }
    result["context"]["verification"] = context.get("verification") is True
    if record.get("error"):
        result["error"] = str(record["error"])[:300]
    for node in record["nodes"][:64]:
        if not isinstance(node, dict) or node.get("role") not in ("static", "dynamic"):
            continue
        projected = {
            "role": node["role"],
            "index": _number(node.get("index")),
            "timestamp_unix": _number(node.get("timestamp_unix")),
        }
        if node.get("error"):
            projected["error"] = str(node["error"])[:300]
        for field in ("pools", "rates"):
            pools = node.get(field, {})
            projected[field] = {}
            if not isinstance(pools, dict):
                continue
            for pool, counters in list(pools.items())[:32]:
                if not isinstance(pool, str) or len(pool) > 128 or not isinstance(counters, dict):
                    continue
                projected[field][pool] = {name: _number(counters[name]) for name in COUNTERS if name in counters}
        result["nodes"].append(projected)
    return result
