from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import logging
import math
import re
import ssl
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence, TypeVar


LOGGER = logging.getLogger("icv2-load")

_Item = TypeVar("_Item")
_Result = TypeVar("_Result")

VIEWER_NODES_PATH = "/viewer/json/nodes"
INTERCONNECT_COUNTERS_PATH = "/counters/counters=interconnect/json"
DISCOVERY_FALLBACK_LIMIT = 3
ICV2_COUNTER_NAMES = frozenset(
    {
        "SessionsRegistered",
        "SessionsUnregistered",
        "EventsSent",
        "BytesSent",
    }
)


@dataclasses.dataclass(frozen=True)
class Node:
    node_id: int
    grpc_endpoints: tuple[str, ...]
    monitor_url: str
    start_time: str

    @property
    def grpc_endpoint(self) -> str:
        return self.grpc_endpoints[0]

    def with_grpc_endpoint(self, endpoint: str) -> Node:
        return dataclasses.replace(
            self,
            grpc_endpoints=(endpoint, *(item for item in self.grpc_endpoints if item != endpoint)),
        )

    @property
    def instance(self) -> tuple[int, str]:
        return self.node_id, self.start_time or "|".join(sorted(self.grpc_endpoints))


@dataclasses.dataclass(frozen=True)
class LoadProfile:
    name: str
    target_offset: int
    size_min: int
    size_max: int
    in_fly: int
    interval_min: str
    interval_max: str
    rope: bool = False


LOAD_PROFILES = (
    LoadProfile(
        name="inline-small",
        target_offset=1,
        size_min=1,
        size_max=512,
        in_fly=64,
        interval_min="100us",
        interval_max="1ms",
    ),
    # Cross the default 256 KiB ICv2 serialization window to exercise partial rope writes.
    LoadProfile(
        name="rope-fragmenting",
        target_offset=-1,
        size_min=64 * 1024,
        size_max=512 * 1024,
        in_fly=4,
        interval_min="5ms",
        interval_max="20ms",
        rope=True,
    ),
)


@dataclasses.dataclass(frozen=True)
class Settings:
    ydbd_path: str
    seed_monitor_url: str
    duration: float
    refresh_interval: float
    burst_duration: float
    preflight_timeout: float
    http_timeout: float
    command_timeout: float
    max_workers: int
    min_load_coverage: float = 0.5
    max_load_gap: float = 120.0
    min_rtt_per_second: float = 1.0
    max_rtt_gap: float = 5.0
    token_file: str | None = None
    ca_file: str | None = None
    client_cert_file: str | None = None
    client_key_file: str | None = None


@dataclasses.dataclass
class ActiveBurst:
    source: Node
    target_node_id: int
    profile: LoadProfile
    started_at: float
    planned_duration: float
    process: subprocess.Popen[str]
    v2_healthy: bool = True
    completed: threading.Event = dataclasses.field(default_factory=threading.Event)
    completed_at: float | None = None
    stdout: str = ""
    stderr: str = ""
    wait_error: str | None = None
    timed_out: bool = False
    waiter: threading.Thread | None = None

    @property
    def route_key(self) -> tuple[tuple[int, str], str, int]:
        return self.source.instance, self.profile.name, self.target_node_id

    @property
    def coverage_key(self) -> tuple[int, str]:
        return self.source.node_id, self.profile.name


class LoadCoverage:
    def __init__(self, expected: Iterable[tuple[int, str]], started_at: float) -> None:
        self.started_at = started_at
        self.intervals: dict[tuple[int, str], list[tuple[float, float]]] = {
            key: [] for key in expected
        }

    def record(self, key: tuple[int, str], started_at: float, finished_at: float) -> None:
        if key in self.intervals and finished_at > started_at:
            self.intervals[key].append((started_at, finished_at))

    def failures(
        self,
        finished_at: float,
        min_coverage: float,
        max_gap: float,
    ) -> list[str]:
        measurements = self.measurements(finished_at)
        failures = []
        for (node_id, profile_name), (coverage, observed_max_gap) in measurements.items():
            if coverage < min_coverage:
                failures.append(
                    f"node {node_id} profile {profile_name} covered {coverage:.1%} of the Nemesis window, "
                    f"need at least {min_coverage:.1%}"
                )
            if observed_max_gap > max_gap:
                failures.append(
                    f"node {node_id} profile {profile_name} had a {observed_max_gap:.3f}s load/V2 gap, "
                    f"maximum allowed is {max_gap:.3f}s"
                )
        return failures

    def measurements(self, finished_at: float) -> dict[tuple[int, str], tuple[float, float]]:
        duration = max(0.0, finished_at - self.started_at)
        measurements = {}
        for key, intervals in sorted(self.intervals.items()):
            clipped = sorted(
                (max(start, self.started_at), min(end, finished_at))
                for start, end in intervals
                if end > self.started_at and start < finished_at
            )
            merged = []
            for start, end in clipped:
                if merged and start <= merged[-1][1]:
                    merged[-1] = merged[-1][0], max(merged[-1][1], end)
                else:
                    merged.append((start, end))

            covered = sum(end - start for start, end in merged)
            gaps = []
            previous_end = self.started_at
            for start, end in merged:
                gaps.append(max(0.0, start - previous_end))
                previous_end = end
            gaps.append(max(0.0, finished_at - previous_end))
            measurements[key] = (
                covered / duration if duration else 0.0,
                max(gaps, default=duration),
            )
        return measurements


class HttpClient:
    def __init__(
        self,
        timeout: float,
        token_file: str | None = None,
        ca_file: str | None = None,
        client_cert_file: str | None = None,
        client_key_file: str | None = None,
    ) -> None:
        self.timeout = timeout
        self.token_file = token_file
        self.ssl_context = ssl.create_default_context(cafile=ca_file)
        if client_cert_file:
            self.ssl_context.load_cert_chain(client_cert_file, client_key_file)

    def _request(self, url: str, accept: str):
        headers = {"Accept": accept}
        if self.token_file:
            token = Path(self.token_file).read_text().strip()
            if token:
                headers["Authorization"] = token
        request = urllib.request.Request(url, headers=headers)
        return urllib.request.urlopen(request, timeout=self.timeout, context=self.ssl_context)

    def get_json(self, url: str) -> Any:
        with self._request(url, "application/json") as response:
            return json.load(response)

    def get_text(self, url: str) -> str:
        with self._request(url, "text/html") as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")


def _field(value: Any, *names: str, default: Any = None) -> Any:
    if not isinstance(value, Mapping):
        return default
    for name in names:
        if name in value:
            return value[name]
    lower = {str(key).lower(): item for key, item in value.items()}
    for name in names:
        if name.lower() in lower:
            return lower[name.lower()]
    return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _normalize_monitor_url(url: str) -> str:
    if "://" not in url:
        url = "http://" + url
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"invalid monitoring URL: {url!r}")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _host_port(default_host: str, address: Any) -> str | None:
    text = str(address or "").strip()
    if not text:
        return None

    endpoint_host = ""
    port: int | None = None
    try:
        parsed = urllib.parse.urlsplit(text if "://" in text else "//" + text)
        port = parsed.port
        if parsed.hostname and parsed.hostname not in {"0.0.0.0", "::", "*"}:
            endpoint_host = parsed.hostname
    except ValueError:
        pass

    if not endpoint_host:
        endpoint_host = default_host.strip("[]")

    if port is None:
        _, separator, suffix = text.rpartition(":")
        if separator and suffix.isdigit():
            port = int(suffix)
        elif text.isdigit():
            port = int(text)

    if not endpoint_host or port is None:
        return None
    if ":" in endpoint_host:
        endpoint_host = f"[{endpoint_host}]"
    return f"{endpoint_host}:{port}"


def _endpoint_map(
    description: Mapping[str, Any],
    system_state: Mapping[str, Any],
) -> dict[str, tuple[Any, ...]]:
    raw = _field(system_state, "Endpoints", default=_field(description, "Endpoints", default=[]))
    if isinstance(raw, Mapping):
        return {str(name).lower(): (address,) for name, address in raw.items()}

    result: dict[str, list[Any]] = {}
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for endpoint in raw:
            name = _field(endpoint, "Name")
            address = _field(endpoint, "Address")
            if name and address:
                result.setdefault(str(name).lower(), []).append(address)
    return {name: tuple(addresses) for name, addresses in result.items()}


def _choose_endpoints(
    endpoints: Mapping[str, Sequence[Any]],
    names: Iterable[str],
) -> list[Any]:
    result = []
    for name in names:
        result.extend(endpoints.get(name, ()))
    return result


def parse_nodes(payload: Any, monitor_scheme: str) -> list[Node]:
    raw_nodes = _field(payload, "Nodes", default=payload if isinstance(payload, list) else [])
    if not isinstance(raw_nodes, list):
        raise ValueError("viewer nodes response has no Nodes array")

    result: dict[int, Node] = {}
    for description in raw_nodes:
        if not isinstance(description, Mapping) or _as_bool(_field(description, "Disconnected", default=False)):
            continue
        try:
            node_id = int(_field(description, "NodeId"))
        except (TypeError, ValueError):
            continue

        system_state = _field(description, "SystemState", default={})
        if not isinstance(system_state, Mapping):
            system_state = {}
        host = str(_field(system_state, "Host", default=_field(description, "Host", default="")))
        endpoints = _endpoint_map(description, system_state)

        # TLS monitoring also advertises "http-mon"; use the seed URL's scheme.
        monitor_names = ("http-mon", "monitoring", "mon")
        grpc_endpoints = tuple(
            dict.fromkeys(
                endpoint
                for address in _choose_endpoints(endpoints, ("grpc",))
                if (endpoint := _host_port(host, address)) is not None
            )
        )
        monitor_endpoint = next(
            (
                endpoint
                for address in _choose_endpoints(endpoints, monitor_names)
                if (endpoint := _host_port(host, address)) is not None
            ),
            None,
        )
        if not grpc_endpoints or not monitor_endpoint:
            continue

        start_time = str(_field(system_state, "StartTime", default=_field(description, "StartTime", default="")))
        result[node_id] = Node(
            node_id=node_id,
            grpc_endpoints=grpc_endpoints,
            monitor_url=f"{monitor_scheme}://{monitor_endpoint}",
            start_time=start_time,
        )
    return sorted(result.values(), key=lambda node: node.node_id)


def parse_icv2_counters(payload: Any) -> dict[str, float]:
    sensors = _field(payload, "sensors", "Sensors", default=[])
    if not isinstance(sensors, list):
        return {}

    result: dict[str, float] = {}
    for entry in sensors:
        labels = _field(entry, "labels", "Labels", default={})
        if str(_field(labels, "subsystem", default="")).lower() != "uring":
            continue
        name = str(_field(labels, "sensor", default=_field(entry, "name", "Name", default=""))).rsplit("/", 1)[-1]
        if name not in ICV2_COUNTER_NAMES:
            continue
        try:
            result[name] = result.get(name, 0.0) + float(_field(entry, "value", "Value", default=0))
        except (TypeError, ValueError):
            continue
    return result


def load_routes(
    nodes: Sequence[Node],
    profiles: Sequence[LoadProfile] = LOAD_PROFILES,
) -> list[tuple[Node, Node, LoadProfile]]:
    routes = []
    for index, source in enumerate(nodes):
        for profile in profiles:
            target_index = (index + profile.target_offset) % len(nodes)
            routes.append((source, nodes[target_index], profile))
    return routes


def build_load_command(
    settings: Settings,
    source: Node,
    hops: Sequence[int],
    profile: LoadProfile,
    duration: float,
    generation: int,
    wait_for_completion: bool = False,
) -> list[str]:
    command = [settings.ydbd_path, "-s", f"grpc://{source.grpc_endpoint}"]
    if settings.token_file:
        command += ["--token-file", settings.token_file]

    duration_arg = f"{duration:.6f}".rstrip("0").rstrip(".") + "s"
    command += [
        "admin",
        "debug",
        "interconnect",
        "load",
        "--name",
        f"icv2-nemesis-{profile.name}-n{source.node_id}-g{generation}",
        "--hops",
        ",".join(map(str, hops)),
        "--size-min",
        str(profile.size_min),
        "--size-max",
        str(profile.size_max),
        "--infly",
        str(profile.in_fly),
        "--interval-min",
        profile.interval_min,
        "--interval-max",
        profile.interval_max,
        "--soft",
        "--duration",
        duration_arg,
        "--num",
        "1",
    ]
    if profile.rope:
        command.append("--rope")
    if wait_for_completion:
        command.append("--wait")
    return command


def parse_load_result(output: str) -> dict[str, int] | None:
    required = (
        "throughput_bytes",
        "throughput_samples",
        "rtt_samples",
        "duration_us",
        "max_rtt_gap_us",
    )
    for line in reversed(output.splitlines()):
        try:
            value = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(value, Mapping) or any(name not in value for name in required):
            continue

        result = {}
        for name in required:
            item = value[name]
            if isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0 or int(item) != item:
                break
            result[name] = int(item)
        else:
            return result
    return None


def _validate_load_progress(
    result: Mapping[str, int],
    settings: Settings,
    planned_duration: float,
) -> float:
    if result["duration_us"] < 1:
        raise ValueError(f"actor returned no measured duration: {result}")

    reported_duration = result["duration_us"] / 1_000_000
    measured_duration = min(planned_duration, reported_duration)
    required_rtt_samples = max(
        1,
        math.ceil(reported_duration * settings.min_rtt_per_second),
    )
    if result["rtt_samples"] < required_rtt_samples:
        raise ValueError(
            f"actor returned {result['rtt_samples']} RTT samples, "
            f"need at least {required_rtt_samples} over {reported_duration:.3f}s"
        )

    max_rtt_gap = result["max_rtt_gap_us"] / 1_000_000
    if max_rtt_gap > settings.max_rtt_gap:
        raise ValueError(
            f"actor max RTT gap was {max_rtt_gap:.3f}s, "
            f"maximum allowed is {settings.max_rtt_gap:.3f}s"
        )
    return measured_duration


class Supervisor:
    def __init__(self, settings: Settings, http_client: HttpClient) -> None:
        self.settings = settings
        self.http_client = http_client
        self.monitor_urls = [settings.seed_monitor_url]
        self.selected_grpc_endpoints: dict[int, str] = {}
        self.verified_instances: set[tuple[int, str]] = set()
        self.window_bursts: dict[tuple[tuple[int, str], str, int], ActiveBurst] = {}
        self.baseline_node_ids: frozenset[int] = frozenset()
        self.discovery_fallback_offset = 0

    def _parallel(self, function: Callable[[_Item], _Result], items: Iterable[_Item]) -> Iterator[tuple[_Item, _Result]]:
        items = list(items)
        if not items:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.settings.max_workers, len(items))) as executor:
            futures = {executor.submit(function, item): item for item in items}
            for future in concurrent.futures.as_completed(futures):
                yield futures[future], future.result()

    def _discover(self) -> list[Node]:
        query = urllib.parse.urlencode({"fields_required": "NodeId,SystemState"})
        monitor_urls = list(self.monitor_urls)
        monitor_scheme = urllib.parse.urlsplit(self.settings.seed_monitor_url).scheme

        def fetch(monitor_url: str) -> list[Node]:
            payload = self.http_client.get_json(f"{monitor_url}{VIEWER_NODES_PATH}?{query}")
            nodes = parse_nodes(payload, monitor_scheme=monitor_scheme)
            if not nodes:
                raise RuntimeError("no connected nodes with compatible gRPC and monitoring endpoints")
            return [
                node.with_grpc_endpoint(selected)
                if (selected := self.selected_grpc_endpoints.get(node.node_id)) in node.grpc_endpoints
                else node
                for node in nodes
            ]

        def remember(preferred_url: str, nodes: Sequence[Node]) -> None:
            self.monitor_urls = _unique(
                [
                    preferred_url,
                    self.settings.seed_monitor_url,
                    *(node.monitor_url for node in nodes),
                    *monitor_urls,
                ]
            )

        results: list[tuple[int, str, list[Node]]] = []
        errors = []
        preferred_url = monitor_urls[0]
        try:
            preferred_nodes = fetch(preferred_url)
            preferred_node_ids = frozenset(node.node_id for node in preferred_nodes)
            results.append((0, preferred_url, preferred_nodes))
            if not self.baseline_node_ids or self.baseline_node_ids.issubset(preferred_node_ids):
                remember(preferred_url, preferred_nodes)
                return preferred_nodes
        except Exception as error:
            errors.append(f"{preferred_url}: {error}")

        fallback_urls = monitor_urls[1:]
        selected_urls = []
        if fallback_urls:
            start = self.discovery_fallback_offset % len(fallback_urls)
            count = min(DISCOVERY_FALLBACK_LIMIT, self.settings.max_workers, len(fallback_urls))
            selected_urls = [
                fallback_urls[(start + index) % len(fallback_urls)]
                for index in range(count)
            ]
            self.discovery_fallback_offset = (start + count) % len(fallback_urls)

        workers = len(selected_urls)
        if workers:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(fetch, monitor_url): (index, monitor_url)
                    for index, monitor_url in enumerate(selected_urls, start=1)
                }
                for future in concurrent.futures.as_completed(futures):
                    index, monitor_url = futures[future]
                    try:
                        results.append((index, monitor_url, future.result()))
                    except Exception as error:
                        errors.append(f"{monitor_url}: {error}")

        if not results:
            raise RuntimeError("viewer discovery failed: " + "; ".join(errors))

        _, best_url, best_nodes = max(results, key=lambda item: (len(item[2]), -item[0]))
        remember(best_url, best_nodes)
        return best_nodes

    def _verify_node(self, node: Node) -> tuple[bool, str, dict[str, float]]:
        try:
            payload = self.http_client.get_json(node.monitor_url + INTERCONNECT_COUNTERS_PATH)
            counters = parse_icv2_counters(payload)
        except Exception as error:
            return False, str(error), {}

        missing_counters = sorted(ICV2_COUNTER_NAMES - counters.keys())
        if missing_counters:
            return False, "missing ICv2 counters: " + ",".join(missing_counters), counters
        detail = ", ".join(f"{name}={value:g}" for name, value in sorted(counters.items()))
        return True, detail, counters

    def _verify_nodes(self, nodes: Sequence[Node]) -> dict[tuple[int, str], tuple[bool, str, dict[str, float]]]:
        return {node.instance: result for node, result in self._parallel(self._verify_node, nodes)}

    def _checked_counters(
        self,
        nodes: Sequence[Node],
    ) -> tuple[dict[tuple[int, str], dict[str, float]], dict[tuple[int, str], str], list[str]]:
        checks = self._verify_nodes(nodes)
        counters_by_instance = {}
        details_by_instance = {}
        failures = []
        for node in nodes:
            success, detail, counters = checks.get(
                node.instance,
                (False, "counter check did not finish", {}),
            )
            if success:
                counters_by_instance[node.instance] = counters
                details_by_instance[node.instance] = detail
            else:
                failures.append(f"node {node.node_id}: {detail}")
        return counters_by_instance, details_by_instance, failures

    def _verify_route(self, source: Node, target: Node) -> tuple[bool, str]:
        try:
            page = self.http_client.get_text(
                source.monitor_url + f"/actors/interconnect/peer{target.node_id:04d}"
            )
        except Exception as error:
            return False, str(error)
        if "Session (v2)" not in page:
            return False, "peer page has no current Session (v2)"
        return True, "Session (v2)"

    def _check_routes(
        self,
        nodes: Sequence[Node],
        profiles: Sequence[LoadProfile] = LOAD_PROFILES,
    ) -> dict[tuple[tuple[int, str], int], tuple[bool, str]]:
        unique_routes = {
            (source.instance, target.node_id): (source, target)
            for source, target, _ in load_routes(nodes, profiles)
        }
        return {
            (source.instance, target.node_id): result
            for (source, target), result in self._parallel(lambda route: self._verify_route(*route), unique_routes.values())
        }

    def _verify_routes(
        self,
        nodes: Sequence[Node],
        profiles: Sequence[LoadProfile] = LOAD_PROFILES,
    ) -> list[str]:
        checks = self._check_routes(nodes, profiles)
        if not checks:
            return ["no ICv2 routes to verify"]

        failures = []
        nodes_by_instance = {node.instance: node for node in nodes}
        for (source_instance, target_node_id), (success, detail) in checks.items():
            if not success:
                source = nodes_by_instance[source_instance]
                failures.append(f"{source.node_id}->{target_node_id}: {detail}")
        return failures

    def _refresh_verified_nodes(self, nodes: Sequence[Node]) -> tuple[list[Node], list[str]]:
        counters_by_instance, details, failures = self._checked_counters(nodes)
        self.verified_instances.intersection_update(node.instance for node in nodes)
        for node in nodes:
            if node.instance in counters_by_instance:
                if node.instance not in self.verified_instances:
                    LOGGER.info("verified node %s: %s", node.node_id, details[node.instance])
                self.verified_instances.add(node.instance)
            else:
                self.verified_instances.discard(node.instance)
        ready = [node for node in nodes if node.instance in self.verified_instances]
        return ready, failures

    def _preflight(self) -> list[Node]:
        deadline = time.monotonic() + self.settings.preflight_timeout
        last_error = "preflight was not attempted"
        while True:
            try:
                nodes = self._discover()
                if len(nodes) < 2:
                    raise RuntimeError("real interconnect load requires at least two connected nodes")
                _, _, failures = self._checked_counters(nodes)
                if failures:
                    raise RuntimeError("ICv2 preflight failed: " + "; ".join(failures))
                self.baseline_node_ids = frozenset(node.node_id for node in nodes)
                LOGGER.info(
                    "ICv2 counter preflight passed on nodes %s",
                    ",".join(str(node.node_id) for node in nodes),
                )
                return nodes
            except Exception as error:
                last_error = str(error)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(last_error)
            LOGGER.warning("%s; retrying preflight", last_error)
            time.sleep(min(2.0, remaining))

    def _run_command(
        self,
        command: Sequence[str],
        timeout: float,
    ) -> tuple[bool, str, str]:
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            return False, "", str(error)
        if result.returncode:
            detail = (result.stderr or result.stdout).strip()
            return False, result.stdout, detail[-1000:] or f"exit code {result.returncode}"
        return True, result.stdout, ""

    def _launch_proof(
        self,
        source: Node,
        target: Node,
        profile: LoadProfile,
        generation: int,
    ) -> tuple[bool, dict[str, int], str]:
        command = build_load_command(
            self.settings,
            source,
            [target.node_id],
            profile,
            self.settings.burst_duration,
            generation,
            wait_for_completion=True,
        )
        success, stdout, detail = self._run_command(
            command,
            self.settings.burst_duration + self.settings.command_timeout,
        )
        if not success:
            return False, {}, detail
        result = parse_load_result(stdout)
        if result is None:
            return False, {}, "load command returned no valid completion statistics"
        return True, result, ""

    def _start_window_bursts(
        self,
        nodes: Sequence[Node],
        route_checks: Mapping[tuple[tuple[int, str], int], tuple[bool, str]],
        remaining: float,
        generation: int,
    ) -> int:
        burst_duration = min(self.settings.burst_duration, remaining)
        if burst_duration <= 0:
            return 0

        started = 0
        for source, target, profile in load_routes(nodes):
            route_key = source.instance, profile.name, target.node_id
            route_ok, _ = route_checks.get((source.instance, target.node_id), (False, "not checked"))
            if route_key in self.window_bursts:
                continue

            command = build_load_command(
                self.settings,
                source,
                [target.node_id],
                profile,
                burst_duration,
                generation,
                wait_for_completion=True,
            )
            started_at = time.monotonic()
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except OSError as error:
                LOGGER.warning(
                    "failed to start waiting %s burst on node %s via %s: %s",
                    profile.name,
                    source.node_id,
                    source.grpc_endpoint,
                    error,
                )
                continue

            burst = ActiveBurst(
                source=source,
                target_node_id=target.node_id,
                profile=profile,
                started_at=started_at,
                planned_duration=burst_duration,
                process=process,
                # Unverified bursts can establish the session, but cannot prove V2 coverage.
                v2_healthy=route_ok,
            )
            burst.waiter = threading.Thread(
                target=self._wait_window_burst,
                args=(burst,),
                name=f"icv2-load-{source.node_id}-{profile.name}",
                daemon=True,
            )
            burst.waiter.start()
            self.window_bursts[burst.route_key] = burst
            started += 1
            LOGGER.info(
                "started waiting %s burst: source=%s target=%s duration=%.3fs",
                profile.name,
                source.node_id,
                target.node_id,
                burst_duration,
            )
        return started

    @staticmethod
    def _wait_window_burst(burst: ActiveBurst) -> None:
        try:
            burst.stdout, burst.stderr = burst.process.communicate()
        except OSError as error:
            burst.wait_error = str(error)
        finally:
            burst.completed_at = time.monotonic()
            burst.completed.set()

    def _mark_window_bursts(
        self,
        nodes: Sequence[Node],
        route_checks: Mapping[tuple[tuple[int, str], int], tuple[bool, str]],
    ) -> None:
        ready_instances = {node.instance for node in nodes}
        for burst in self.window_bursts.values():
            route_ok, _ = route_checks.get(
                (burst.source.instance, burst.target_node_id),
                (False, "not checked"),
            )
            if burst.source.instance not in ready_instances or not route_ok:
                burst.v2_healthy = False

    def _observe_window_health(
        self,
        nodes: Sequence[Node],
    ) -> tuple[
        list[Node],
        dict[tuple[tuple[int, str], int], tuple[bool, str]],
    ]:
        ready, counter_failures = self._refresh_verified_nodes(nodes)
        for failure in counter_failures:
            LOGGER.warning("ICv2 verification failed: %s", failure)
        if len(ready) < 2:
            LOGGER.warning("fewer than two verified ICv2 nodes are currently reachable")
            self._mark_window_bursts((), {})
            return ready, {}

        route_checks = self._check_routes(ready)
        self._mark_window_bursts(ready, route_checks)
        nodes_by_instance = {node.instance: node for node in ready}
        for (source_instance, target_node_id), (success, detail) in route_checks.items():
            if not success:
                source = nodes_by_instance[source_instance]
                LOGGER.warning(
                    "ICv2 route verification failed: %s->%s: %s",
                    source.node_id,
                    target_node_id,
                    detail,
                )
        return ready, route_checks

    def _collect_window_bursts(self, coverage: LoadCoverage, observed_at: float) -> int:
        completed = 0
        for route_key, burst in list(self.window_bursts.items()):
            timeout_at = (
                burst.started_at
                + burst.planned_duration
                + self.settings.command_timeout
            )
            if (
                not burst.completed.is_set()
                and observed_at >= timeout_at
                and not burst.timed_out
                and burst.process.poll() is None
            ):
                burst.timed_out = True
                try:
                    burst.process.kill()
                except ProcessLookupError:
                    pass
            if not burst.completed.is_set():
                continue

            completed_at = burst.completed_at if burst.completed_at is not None else observed_at
            return_code = burst.process.returncode
            del self.window_bursts[route_key]

            try:
                if burst.timed_out:
                    raise ValueError(f"timed out after {burst.planned_duration + self.settings.command_timeout:.3f}s")
                if burst.wait_error:
                    raise ValueError(f"failed to collect load command output: {burst.wait_error}")
                if return_code:
                    raise ValueError((burst.stderr or burst.stdout).strip()[-1000:] or f"exit code {return_code}")
                result = parse_load_result(burst.stdout)
                if result is None:
                    raise ValueError("load command returned no valid completion statistics")
                if not burst.v2_healthy:
                    raise ValueError("V2 route/session validation failed while the actor was running")
                measured_duration = _validate_load_progress(result, self.settings, burst.planned_duration)
                # Unknown CLI/RPC overhead leaves only [completed_at - D, started_at + D]
                # guaranteed inside the actor's run, where D is its measured duration.
                proof_started_at = completed_at - measured_duration
                proof_finished_at = burst.started_at + measured_duration
                if proof_finished_at <= proof_started_at:
                    raise ValueError(
                        "CLI/RPC overhead consumed the whole provable actor interval: "
                        f"process_elapsed={completed_at - burst.started_at:.3f}s "
                        f"actor_duration={measured_duration:.3f}s"
                    )
            except ValueError as error:
                LOGGER.warning(
                    "discarded %s burst proof on node %s: %s",
                    burst.profile.name,
                    burst.source.node_id,
                    error,
                )
                continue

            coverage.record(burst.coverage_key, proof_started_at, proof_finished_at)
            completed += 1
            LOGGER.info(
                "completed %s burst: source=%s target=%s actor_duration=%.3fs "
                "credited_duration=%.3fs rtt_samples=%s max_rtt_gap=%.3fs",
                burst.profile.name,
                burst.source.node_id,
                burst.target_node_id,
                measured_duration,
                proof_finished_at - proof_started_at,
                result["rtt_samples"],
                result["max_rtt_gap_us"] / 1_000_000,
            )
        return completed

    def _drain_window_bursts(self, coverage: LoadCoverage) -> tuple[int, list[str]]:
        completed = 0
        failures = []
        hard_deadline = max(
            (
                burst.started_at
                + burst.planned_duration
                + 2 * self.settings.command_timeout
                for burst in self.window_bursts.values()
            ),
            default=time.monotonic(),
        )
        while self.window_bursts:
            now = time.monotonic()
            completed += self._collect_window_bursts(coverage, now)
            if not self.window_bursts:
                break
            if now >= hard_deadline:
                failure = (
                    f"{len(self.window_bursts)} waiting load commands did not stop "
                    "by the hard reap deadline"
                )
                LOGGER.warning("%s", failure)
                self.close()
                failures.append(failure)
                break
            next_timeout = min(
                burst.started_at
                + burst.planned_duration
                + self.settings.command_timeout
                for burst in self.window_bursts.values()
            )
            time.sleep(min(0.2, max(0.01, next_timeout - now), hard_deadline - now))
        return completed, failures

    def close(self) -> None:
        bursts = list(self.window_bursts.values())
        for burst in bursts:
            try:
                if burst.process.poll() is None:
                    burst.process.kill()
            except OSError as error:
                LOGGER.warning("failed to stop load command for node %s: %s", burst.source.node_id, error)
        deadline = time.monotonic() + self.settings.command_timeout
        for burst in bursts:
            if burst.waiter is None:
                try:
                    burst.process.communicate(timeout=max(0.0, deadline - time.monotonic()))
                except (OSError, subprocess.TimeoutExpired) as error:
                    LOGGER.warning("failed to reap load command for node %s: %s", burst.source.node_id, error)
                continue
            burst.waiter.join(timeout=max(0.0, deadline - time.monotonic()))
            if burst.waiter.is_alive():
                LOGGER.warning("load command waiter for node %s did not stop", burst.source.node_id)
        self.window_bursts.clear()

    def _arm_preflight_bursts(self, nodes: Sequence[Node]) -> float:
        burst_duration = self.settings.burst_duration
        plans_by_source: dict[Node, list[tuple[int, LoadProfile]]] = {}
        for source, target, profile in load_routes(nodes):
            plans_by_source.setdefault(source, []).append((target.node_id, profile))

        def arm_source(source: Node) -> tuple[str | None, int, float]:
            endpoint_errors = []
            successes = 0
            active_until = 0.0
            for endpoint in source.grpc_endpoints:
                selected_source = source.with_grpc_endpoint(endpoint)
                for target, profile in plans_by_source[source]:
                    command = build_load_command(
                        self.settings,
                        selected_source,
                        [target],
                        profile,
                        burst_duration,
                        0,
                    )
                    success, _, detail = self._run_command(
                        command,
                        self.settings.command_timeout,
                    )
                    if not success:
                        if not successes:
                            endpoint_errors.append(f"{endpoint}: {detail}")
                            break
                        LOGGER.warning(
                            "failed to arm %s burst on node %s via %s: %s",
                            profile.name,
                            source.node_id,
                            endpoint,
                            detail,
                        )
                        return endpoint, successes, active_until
                    successes += 1
                    active_until = time.monotonic() + burst_duration
                    LOGGER.info(
                        "armed %s burst: source=%s target=%s duration=%.3fs",
                        profile.name,
                        source.node_id,
                        target,
                        burst_duration,
                    )
                else:
                    return endpoint, successes, active_until

            LOGGER.warning(
                "no gRPC endpoint accepted InterconnectDebug on node %s: %s",
                source.node_id,
                "; ".join(endpoint_errors),
            )
            return None, 0, 0.0

        successes = 0
        active_until = 0.0
        for source, (endpoint, armed, deadline) in self._parallel(arm_source, plans_by_source):
            if endpoint is not None:
                self.selected_grpc_endpoints[source.node_id] = endpoint
            successes += armed
            active_until = max(active_until, deadline)
        planned = sum(map(len, plans_by_source.values()))
        if successes != planned:
            raise RuntimeError(
                f"ICv2 route preflight armed only {successes} of {planned} bursts"
            )
        return active_until

    def _baseline_topology_failures(self, nodes: Sequence[Node]) -> list[str]:
        failures = []
        current_node_ids = frozenset(node.node_id for node in nodes)
        missing_node_ids = sorted(self.baseline_node_ids - current_node_ids)
        if missing_node_ids:
            failures.append("baseline nodes are missing: " + ",".join(map(str, missing_node_ids)))
        if len(current_node_ids) < len(self.baseline_node_ids):
            failures.append(
                f"only {len(current_node_ids)} of {len(self.baseline_node_ids)} baseline nodes are visible"
            )
        if len(nodes) < 2:
            failures.append("fewer than two nodes are visible")
        return failures

    def _preflight_routes(self, nodes: Sequence[Node]) -> list[Node]:
        expected_topology = {node.node_id: node.instance for node in nodes}
        active_until = self._arm_preflight_bursts(nodes)

        deadline = time.monotonic() + min(
            self.settings.preflight_timeout,
            self.settings.burst_duration,
        )
        last_failures = ["route checks were not attempted"]
        while True:
            try:
                current_nodes = self._discover()
                last_failures = self._baseline_topology_failures(current_nodes)
                current_topology = {node.node_id: node.instance for node in current_nodes}
                if current_topology != expected_topology:
                    last_failures.append("node topology or process generation changed during preflight")

                _, counter_failures = self._refresh_verified_nodes(current_nodes)
                last_failures.extend(counter_failures)
                last_failures.extend(
                    f"route {failure}"
                    for failure in self._verify_routes(current_nodes)
                )
                if not last_failures:
                    LOGGER.info("ICv2 route preflight passed for both load profiles")
                    remaining = active_until - time.monotonic()
                    if remaining > 0:
                        LOGGER.info("waiting %.3fs for preflight load bursts to finish", remaining)
                        time.sleep(remaining)
                    return current_nodes
            except Exception as error:
                last_failures = [str(error)]

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("ICv2 route preflight failed: " + "; ".join(last_failures))
            LOGGER.warning("ICv2 route preflight is not complete: %s", "; ".join(last_failures))
            time.sleep(min(self.settings.refresh_interval, remaining))

    def _prove_profile(self, profile: LoadProfile, generation: int) -> list[str]:
        try:
            nodes = self._discover()
        except Exception as error:
            return [f"topology discovery failed: {error}"]

        failures = self._baseline_topology_failures(nodes)
        if failures:
            return failures

        expected_topology = {node.node_id: node.instance for node in nodes}
        proof_baseline, _, counter_failures = self._checked_counters(nodes)
        if counter_failures:
            return counter_failures

        routes = load_routes(nodes, (profile,))
        for (source, target, _), (success, result, detail) in self._parallel(
            lambda route: self._launch_proof(*route, generation), routes
        ):
            if not success:
                failures.append(
                    f"actor {source.node_id}->{target.node_id} did not complete: {detail}"
                )
                continue
            try:
                _validate_load_progress(result, self.settings, self.settings.burst_duration)
            except ValueError as error:
                failures.append(f"actor {source.node_id}->{target.node_id}: {error}")
                continue
            LOGGER.info(
                "proved %s actor %s->%s: rtt_samples=%s max_rtt_gap=%.3fs "
                "throughput_bytes=%s",
                profile.name,
                source.node_id,
                target.node_id,
                result["rtt_samples"],
                result["max_rtt_gap_us"] / 1_000_000,
                result["throughput_bytes"],
            )

        if failures:
            return failures

        try:
            nodes = self._discover()
        except Exception as error:
            return [f"topology rediscovery failed: {error}"]

        failures.extend(self._baseline_topology_failures(nodes))
        current_topology = {node.node_id: node.instance for node in nodes}
        if current_topology != expected_topology:
            failures.append("node topology or process generation changed during the final proof")

        current_counters, _, counter_failures = self._checked_counters(nodes)
        failures.extend(counter_failures)
        if len(current_counters) != len(nodes):
            failures.append(
                f"only {len(current_counters)} of {len(nodes)} visible nodes expose all ICv2 counters"
            )
        failures.extend(
            f"route {failure}"
            for failure in self._verify_routes(nodes, (profile,))
        )

        for node in nodes:
            baseline = proof_baseline.get(node.instance)
            current = current_counters.get(node.instance)
            if baseline is None:
                failures.append(f"node {node.node_id} changed process generation during the final proof")
                continue
            if current is None:
                continue

            required_deltas = {
                "EventsSent": 1,
                "BytesSent": max(1, profile.size_min),
            }
            for counter_name, required_delta in required_deltas.items():
                actual_delta = current[counter_name] - baseline[counter_name]
                if actual_delta < required_delta:
                    failures.append(
                        f"node {node.node_id} uring counter {counter_name} advanced by "
                        f"{actual_delta:g}, need at least {required_delta}"
                    )
        return failures

    def _postflight(self, generation: int) -> list[str]:
        for profile_index, profile in enumerate(LOAD_PROFILES):
            failures = self._prove_profile(profile, generation + profile_index)
            if failures:
                return [f"{profile.name}: {failure}" for failure in failures]
        return []

    def run(self) -> int:
        nodes = self._preflight()
        nodes = self._preflight_routes(nodes)
        started_at = time.monotonic()
        deadline = started_at + self.settings.duration
        coverage = LoadCoverage(
            (
                (node_id, profile.name)
                for node_id in self.baseline_node_ids
                for profile in LOAD_PROFILES
            ),
            started_at,
        )
        previous_topology: dict[int, tuple[int, str]] = {}
        generation = 0
        started_bursts = 0
        completed_bursts = 0
        use_preflight_nodes = True

        while (remaining := deadline - time.monotonic()) > 0:
            if not use_preflight_nodes:
                try:
                    nodes = self._discover()
                except Exception as error:
                    LOGGER.warning("%s", error)
                    self._mark_window_bursts((), {})
                    completed_bursts += self._collect_window_bursts(coverage, time.monotonic())
                    retry_remaining = deadline - time.monotonic()
                    if retry_remaining > 0:
                        time.sleep(min(self.settings.refresh_interval, retry_remaining))
                    continue
            use_preflight_nodes = False

            topology = {node.node_id: node.instance for node in nodes}
            if previous_topology and topology != previous_topology:
                LOGGER.info(
                    "node topology or process generation changed; validating old bursts and arming new routes"
                )
            previous_topology = topology

            ready, route_checks = self._observe_window_health(nodes)
            completed_bursts += self._collect_window_bursts(coverage, time.monotonic())
            if len(ready) >= 2:
                generation += 1
                launch_remaining = max(0.0, deadline - time.monotonic())
                if launch_remaining >= self.settings.refresh_interval:
                    started_bursts += self._start_window_bursts(
                        ready,
                        route_checks,
                        launch_remaining,
                        generation,
                    )

            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(self.settings.refresh_interval, remaining))

        try:
            nodes = self._discover()
            self._observe_window_health(nodes)
        except Exception as error:
            LOGGER.warning("final ICv2 window verification failed: %s", error)
            self._mark_window_bursts((), {})
        drained_bursts, lifecycle_failures = self._drain_window_bursts(coverage)
        completed_bursts += drained_bursts
        for (node_id, profile_name), (covered, max_gap) in coverage.measurements(deadline).items():
            LOGGER.info(
                "Nemesis-window coverage: node=%s profile=%s covered=%.1f%% max_gap=%.3fs",
                node_id,
                profile_name,
                covered * 100,
                max_gap,
            )

        failures = [
            "Nemesis window: " + failure
            for failure in coverage.failures(
                deadline,
                self.settings.min_load_coverage,
                self.settings.max_load_gap,
            )
        ]
        failures.extend(
            "Nemesis window: " + failure
            for failure in lifecycle_failures
        )
        if not lifecycle_failures:
            failures.extend(self._postflight(generation + 1))
        if failures:
            for failure in failures:
                LOGGER.error("acceptance failed: %s", failure)
            LOGGER.error(
                "finished with %s acceptance failures after starting %s and proving %s bursts",
                len(failures),
                started_bursts,
                completed_bursts,
            )
            return 1

        LOGGER.info(
            "Nemesis-window coverage and postflight passed after starting %s and proving %s bursts",
            started_bursts,
            completed_bursts,
        )
        return 0


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


_DURATION_RE = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ms|s|m|h|d)?$")


def parse_duration(value: str) -> float:
    match = _DURATION_RE.fullmatch(value.strip())
    if not match:
        raise argparse.ArgumentTypeError(f"invalid duration: {value!r}")
    multiplier = {None: 1.0, "ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
    seconds = float(match.group("value")) * multiplier[match.group("unit")]
    if seconds <= 0:
        raise argparse.ArgumentTypeError("duration must be positive")
    return seconds


def parse_args(argv: Sequence[str] | None = None) -> Settings:
    parser = argparse.ArgumentParser(
        description="Keep real-cluster ICv2 load actors alive while Nemesis restarts or isolates YDB nodes."
    )
    parser.add_argument("--ydbd-path", required=True, help="path to the ydbd binary matching the cluster")
    parser.add_argument("--seed-monitor-url", required=True, help="seed node monitoring base URL, for example http://host:8765")
    parser.add_argument("--duration", type=parse_duration, default=3600.0, help="total supervisor duration (default: 1h)")
    parser.add_argument("--refresh-interval", type=parse_duration, default=10.0, help="viewer refresh interval (default: 10s)")
    parser.add_argument("--burst-duration", type=parse_duration, default=30.0, help="duration of every bounded load actor (default: 30s)")
    parser.add_argument("--preflight-timeout", type=parse_duration, default=60.0, help="initial discovery/counter deadline (default: 1m)")
    parser.add_argument("--http-timeout", type=parse_duration, default=5.0, help="timeout for each monitoring request (default: 5s)")
    parser.add_argument("--command-timeout", type=parse_duration, default=20.0, help="timeout for each ydbd admin invocation (default: 20s)")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="maximum parallel monitoring and preflight/final admin requests",
    )
    parser.add_argument(
        "--min-load-coverage",
        type=float,
        default=0.5,
        help="minimum healthy load/V2 fraction for every source and profile (default: 0.5)",
    )
    parser.add_argument(
        "--max-load-gap",
        type=parse_duration,
        default=120.0,
        help="maximum load/V2 gap for every source and profile (default: 2m)",
    )
    parser.add_argument(
        "--min-rtt-per-second",
        type=float,
        default=1.0,
        help="minimum returned RTT samples per measured actor second (default: 1)",
    )
    parser.add_argument(
        "--max-rtt-gap",
        type=parse_duration,
        default=5.0,
        help="maximum actor-local gap between returned RTT samples (default: 5s)",
    )
    parser.add_argument("--token-file", help="admin token file, also used for monitoring Authorization")
    parser.add_argument("--ca-file", help="CA certificate for HTTPS monitoring")
    parser.add_argument("--client-cert-file", help="client certificate for HTTPS monitoring")
    parser.add_argument("--client-key-file", help="client certificate key for HTTPS monitoring")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    args = parser.parse_args(argv)

    if args.burst_duration <= args.refresh_interval:
        parser.error("--burst-duration must be greater than --refresh-interval to avoid planned load gaps")
    if args.max_workers <= 0:
        parser.error("--max-workers must be positive")
    if not 0 < args.min_load_coverage <= 1:
        parser.error("--min-load-coverage must be in the (0, 1] range")
    if not math.isfinite(args.min_rtt_per_second) or args.min_rtt_per_second <= 0:
        parser.error("--min-rtt-per-second must be a finite positive number")
    if bool(args.client_cert_file) != bool(args.client_key_file):
        parser.error("--client-cert-file and --client-key-file must be specified together")
    for option in ("ydbd_path", "token_file", "ca_file", "client_cert_file", "client_key_file"):
        path = getattr(args, option)
        if path and not Path(path).is_file():
            parser.error(f"--{option.replace('_', '-')} does not exist: {path}")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return Settings(
        ydbd_path=args.ydbd_path,
        seed_monitor_url=_normalize_monitor_url(args.seed_monitor_url),
        duration=args.duration,
        refresh_interval=args.refresh_interval,
        burst_duration=args.burst_duration,
        preflight_timeout=args.preflight_timeout,
        http_timeout=args.http_timeout,
        command_timeout=args.command_timeout,
        max_workers=args.max_workers,
        min_load_coverage=args.min_load_coverage,
        max_load_gap=args.max_load_gap,
        min_rtt_per_second=args.min_rtt_per_second,
        max_rtt_gap=args.max_rtt_gap,
        token_file=args.token_file,
        ca_file=args.ca_file,
        client_cert_file=args.client_cert_file,
        client_key_file=args.client_key_file,
    )


def main(argv: Sequence[str] | None = None) -> int:
    settings = parse_args(argv)
    http_client = HttpClient(
        timeout=settings.http_timeout,
        token_file=settings.token_file,
        ca_file=settings.ca_file,
        client_cert_file=settings.client_cert_file,
        client_key_file=settings.client_key_file,
    )
    supervisor = Supervisor(settings, http_client)
    try:
        return supervisor.run()
    except KeyboardInterrupt:
        LOGGER.info("interrupted; waiting CLI processes will stop and armed actors remain bounded by their deadline")
        return 130
    except Exception as error:
        LOGGER.error("%s", error)
        return 2
    finally:
        supervisor.close()
