"""Persistent host directory and bounded, authenticated read-only peer access."""

import hmac
import json
import secrets
import socket
import threading
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_bytes, atomic_write_text

PROTOCOL = 1
MAX_RESPONSE = 32 * 1024 * 1024
MAX_HOSTS = 16


def cluster_request(record, operation, members=None):
    request = Request(
        validate_endpoint(record['endpoint']) + '/peer/cluster/' + operation,
        data=json.dumps({'members': members} if members is not None else {}).encode(),
        headers={'Authorization': 'Bearer ' + record['token'], 'Content-Type': 'application/json'},
    )
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=5) as response:
            body = response.read(MAX_RESPONSE + 1)
            if len(body) > MAX_RESPONSE:
                raise BenchmarkError('cluster response exceeds 32 MiB')
            return json.loads(body)
    except (OSError, ValueError) as error:
        raise BenchmarkError('cluster exchange failed; check peer reachability, token and version') from error


def member_records(values):
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_HOSTS + 1:
        raise BenchmarkError('invalid cluster size')
    result = {}
    for value in values:
        if not isinstance(value, dict):
            raise BenchmarkError('invalid cluster member')
        try:
            host_id = str(uuid.UUID(value['id']))
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            raise BenchmarkError('invalid member id') from error
        token, name, port = value.get('token'), value.get('name'), value.get('port')
        if (
            not isinstance(token, str)
            or not token.isascii()
            or not 20 <= len(token) <= 256
            or any(c.isspace() for c in token)
        ):
            raise BenchmarkError('invalid member token')
        if not isinstance(name, str) or len(name) > 120 or type(port) is not int or not 1 <= port <= 65535:
            raise BenchmarkError('invalid member name or port')
        if host_id in result:
            raise BenchmarkError('duplicate cluster member')
        result[host_id] = dict(
            id=host_id, name=name, port=port, token=token, endpoint=validate_endpoint(value.get('endpoint'))
        )
    return result


def union_members(current, incoming):
    result = dict(current)
    for host_id, record in incoming.items():
        old = result.get(host_id)
        if old and any(old[key] != record[key] for key in ('endpoint', 'token', 'port')):
            raise BenchmarkError('conflicting address or credentials for host ' + host_id)
        result.setdefault(host_id, record)
    if len(result) > MAX_HOSTS + 1:
        raise BenchmarkError('cluster host limit reached')
    return result


def allowed_path(path):
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or parsed.fragment:
        return False
    return parsed.path in (
        "/api/host-info",
        "/api/activity-status",
        "/api/system-topology",
        "/api/cpu-usage",
        "/api/runs",
        "/api/settings",
        "/api/benchmarks",
        "/api/saved-comparisons",
        "/api/comparisons",
        "/api/chart-data",
        "/api/local-ydb-comparison",
    ) or parsed.path.startswith("/api/runs/")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_endpoint(value):
    if not isinstance(value, str):
        raise BenchmarkError("host endpoint must be a URL")
    parsed = urlsplit(value)
    if parsed.username or parsed.password or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise BenchmarkError("host endpoint must contain only scheme, host and port")
    if not parsed.hostname or parsed.scheme not in ("http", "https"):
        raise BenchmarkError("host endpoint requires HTTP or HTTPS")
    try:
        port = parsed.port
    except ValueError as error:
        raise BenchmarkError("invalid host port") from error
    if not port or not 0 < port < 65536:
        raise BenchmarkError("host endpoint requires a valid explicit port")
    return value.rstrip("/")


def open_peer(record, path):
    if not allowed_path(path):
        raise BenchmarkError("peer route is not allowed")
    endpoint = validate_endpoint(record["endpoint"])
    request = Request(endpoint + "/peer" + path, headers={"Authorization": "Bearer " + record["token"]})
    try:
        response = build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=5)
    except HTTPError as error:
        response = error
    except (OSError, URLError) as error:
        raise BenchmarkError("host is unreachable") from error
    return response


def request_peer(record, path):
    with open_peer(record, path) as response:
        body = response.read(MAX_RESPONSE + 1)
        if len(body) > MAX_RESPONSE:
            raise BenchmarkError("peer metadata exceeds 32 MiB")
        return response.status, response.headers.get("Content-Type", "application/octet-stream"), body


class HostDirectory:
    def __init__(self, output):
        self.root = Path(output)
        self.lock = threading.RLock()
        self.join_lock = threading.Lock()
        self.endpoint = None
        self.port = None
        self.path = self.root / ".hosts.json"
        identity = self.root / ".host-id"
        token = self.root / ".peer-token"
        if not identity.exists():
            atomic_write_text(identity, str(uuid.uuid4()))
        if not token.exists():
            atomic_write_bytes(token, secrets.token_urlsafe(32).encode(), mode=0o600)
        self.id = identity.read_text().strip()
        self.token = token.read_text().strip()
        self.records = json.loads(self.path.read_text()) if self.path.exists() else []

    def authorized(self, header):
        return bool(not header or header.isascii()) and hmac.compare_digest(header or "", "Bearer " + self.token)

    def identity(self, port=None):
        return {
            "id": self.id,
            "name": socket.getfqdn(),
            "port": port,
            "protocol": PROTOCOL,
            "capabilities": ["read-only", "cluster-membership-v1"],
        }

    def get(self, host_id):
        with self.lock:
            for record in self.records:
                if record["id"] == host_id:
                    return dict(record)
        raise BenchmarkError("host not found")

    def resolve(self, options):
        if not isinstance(options, dict):
            raise BenchmarkError("expected host object")
        endpoint = validate_endpoint(options.get("endpoint"))
        token = options.get("token", "")
        name = options.get("name", "")
        if not isinstance(token, str) or not 20 <= len(token) <= 256 or any(c.isspace() for c in token):
            raise BenchmarkError("invalid peer token")
        if not isinstance(name, str) or len(name) > 120:
            raise BenchmarkError("invalid host name")
        record = {"endpoint": endpoint, "token": token}
        status, _, body = request_peer(record, "/api/host-info")
        try:
            info = json.loads(body)
            host_id = str(uuid.UUID(info["id"]))
        except (ValueError, KeyError, TypeError) as error:
            raise BenchmarkError("invalid peer identity") from error
        if status != 200 or info.get("protocol") != PROTOCOL:
            raise BenchmarkError("peer authentication or protocol mismatch")
        if host_id == self.id:
            raise BenchmarkError("cannot add this server as a remote host")
        port = info.get("port")
        if port is not None and (type(port) is not int or not 1 <= port <= 65535):
            raise BenchmarkError("invalid peer port")
        record.update(id=host_id, name=name.strip() or info["name"], port=port)
        return record

    def add(self, options):
        record = self.resolve(options)
        with self.lock:
            if len(self.records) >= MAX_HOSTS or any(r["id"] == record['id'] for r in self.records):
                raise BenchmarkError("host already registered or host limit reached")
            updated = self.records + [record]
            atomic_write_bytes(self.path, json.dumps(updated).encode(), mode=0o600)
            self.records = updated
        return self.public(record)

    def snapshot(self):
        if not self.endpoint:
            raise BenchmarkError('local peer endpoint is not configured')
        with self.lock:
            return [
                dict(id=self.id, name=socket.getfqdn(), port=self.port, endpoint=self.endpoint, token=self.token)
            ] + [dict(record) for record in self.records]

    def merge(self, values):
        incoming = member_records(values)
        with self.lock:
            combined = union_members(member_records(self.snapshot()), incoming)
            updated = [record for host_id, record in combined.items() if host_id != self.id]
            atomic_write_bytes(self.path, json.dumps(updated).encode(), mode=0o600)
            self.records = updated
        return {'members': len(combined)}

    def validate_merge(self, values):
        incoming = member_records(values)
        with self.lock:
            union_members(member_records(self.snapshot()), incoming)

    def join(self, options):
        # Network calls must not hold the directory lock: peers may call back concurrently.
        with self.join_lock:
            seed = self.resolve(options)
            members = union_members(member_records(self.snapshot()), member_records([seed]))
            checked = {self.id}
            while set(members) - checked:
                host_id = next(iter(set(members) - checked))
                response = cluster_request(members[host_id], 'snapshot')
                incoming = member_records(response)
                if host_id not in incoming:
                    raise BenchmarkError('peer snapshot does not contain its identity')
                members = union_members(members, incoming)
                checked.add(host_id)
            payload = list(members.values())
            # Validate every participant before changing any directory.
            for host_id, record in members.items():
                if host_id != self.id:
                    cluster_request(record, 'validate', payload)
            failures = []
            self.merge(payload)
            for host_id, record in members.items():
                if host_id != self.id:
                    try:
                        cluster_request(record, 'merge', payload)
                    except BenchmarkError:
                        failures.append(host_id)
            if failures:
                raise BenchmarkError(
                    'cluster synchronization partially applied; retry Add host. Unreachable hosts: '
                    + ', '.join(failures)
                )
            return self.public(seed)

    def remove(self, host_id):
        with self.lock:
            record = self.get(host_id)
            updated = [r for r in self.records if r["id"] != record["id"]]
            atomic_write_bytes(self.path, json.dumps(updated).encode(), mode=0o600)
            self.records = updated
        return {"removed": host_id}

    @staticmethod
    def public(record):
        return {**{k: record[k] for k in ("id", "name", "endpoint")}, "port": record.get("port")}

    def list(self):
        with self.lock:
            return [self.public(r) for r in self.records]
