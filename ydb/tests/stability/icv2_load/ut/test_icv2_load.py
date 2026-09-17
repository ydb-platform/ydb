import dataclasses
import json
from unittest.mock import Mock

import pytest

import ydb.tests.stability.icv2_load.icv2_load as icv2_load_module
from ydb.tests.stability.icv2_load.icv2_load import (
    ActiveBurst,
    HttpClient,
    LOAD_PROFILES,
    LoadCoverage,
    Node,
    Settings,
    Supervisor,
    build_load_command,
    load_routes,
    parse_duration,
    parse_icv2_counters,
    parse_load_result,
    parse_nodes,
)


def _settings(**overrides):
    values = {
        "ydbd_path": "/bin/ydbd",
        "seed_monitor_url": "http://seed:8765",
        "duration": 60,
        "refresh_interval": 10,
        "burst_duration": 30,
        "preflight_timeout": 60,
        "http_timeout": 5,
        "command_timeout": 20,
        "max_workers": 4,
    }
    values.update(overrides)
    return Settings(**values)


def _viewer_node(node_id, host=None):
    host = host or f"node-{node_id}.example"
    return {
        "NodeId": node_id,
        "SystemState": {
            "Host": host,
            "StartTime": str(node_id * 100),
            "Endpoints": [
                {"Name": "grpc", "Address": ":2135"},
                {"Name": "http-mon", "Address": ":8765"},
            ],
        },
    }


def _node(node_id, *, host=None, grpc_endpoints=None, monitor_scheme="http", start_time=None):
    host = host or f"node-{node_id}"
    return Node(
        node_id=node_id,
        grpc_endpoints=grpc_endpoints or (f"{host}:2135",),
        monitor_url=f"{monitor_scheme}://{host}:8765",
        start_time=str(node_id * 100) if start_time is None else start_time,
    )


def _nodes():
    return [_node(1), _node(2)]


def _load_result(**overrides):
    result = {
        "throughput_bytes": 100000,
        "throughput_samples": 100,
        "rtt_samples": 30,
        "duration_us": 30_000_000,
        "max_rtt_gap_us": 1_000_000,
    }
    result.update(overrides)
    return result


def _window_burst(process):
    burst = ActiveBurst(
        source=_node(1, start_time="1"),
        target_node_id=2,
        profile=LOAD_PROFILES[0],
        started_at=0,
        planned_duration=1,
        process=process,
    )
    supervisor = Supervisor(_settings(command_timeout=1), object())
    supervisor.window_bursts[burst.route_key] = burst
    coverage = LoadCoverage([burst.coverage_key], started_at=0)
    return supervisor, burst, coverage


def test_monitoring_authorization_preserves_compact_login_jwt(tmp_path, monkeypatch):
    token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature"
    token_file = tmp_path / "token"
    token_file.write_text(token, encoding="utf-8")
    authorization = None

    def urlopen(request, **_kwargs):
        nonlocal authorization
        authorization = request.get_header("Authorization")
        return object()

    monkeypatch.setattr(icv2_load_module.urllib.request, "urlopen", urlopen)

    HttpClient(timeout=1, token_file=str(token_file))._request(
        "http://node.example:8765/viewer/json/nodes",
        "application/json",
    )

    assert authorization == token


def test_parse_nodes_uses_node_specific_endpoints_and_skips_disconnected():
    payload = {
        "Nodes": [
            {
                "NodeId": "1",
                "SystemState": {
                    "Host": "node-1.example",
                    "StartTime": "123",
                    "Endpoints": [
                        {"Name": "grpc", "Address": ":2135"},
                        {"Name": "http-mon", "Address": ":8765"},
                    ],
                },
            },
            {
                "NodeId": 2,
                "Disconnected": True,
                "SystemState": {
                    "Host": "node-2.example",
                    "Endpoints": [
                        {"Name": "grpc", "Address": ":2135"},
                        {"Name": "http-mon", "Address": ":8765"},
                    ],
                },
            },
        ]
    }

    assert parse_nodes(payload, "http") == [
        _node(1, host="node-1.example", start_time="123")
    ]


def test_parse_nodes_preserves_grpc_candidates_and_advertised_hosts():
    node = _viewer_node(1)
    node["SystemState"]["Endpoints"] = [
        {"Name": "grpc", "Address": "10.1.2.3:2135"},
        {"Name": "grpc", "Address": ":3135"},
        {"Name": "http-mon", "Address": ":8765"},
    ]

    nodes = parse_nodes({"Nodes": [node]}, "http")

    assert nodes[0].grpc_endpoints == ("10.1.2.3:2135", "node-1.example:3135")


def test_parse_nodes_uses_http_mon_endpoint_name_for_https_transport():
    nodes = parse_nodes({"Nodes": [_viewer_node(1)]}, "https")

    assert nodes[0].monitor_url == "https://node-1.example:8765"


def test_parse_icv2_counters_filters_subsystem_and_sums_shards():
    payload = {
        "sensors": [
            {"labels": {"subsystem": "uring", "shard": "0", "sensor": "SessionsRegistered"}, "value": 3},
            {"labels": {"subsystem": "uring", "shard": "0", "sensor": "SessionsUnregistered"}, "value": 1},
            {"labels": {"subsystem": "uring", "shard": "1", "sensor": "EventsSent"}, "value": "7"},
            {"labels": {"subsystem": "uring", "shard": "1", "sensor": "BytesSent"}, "value": "1024"},
            {"labels": {"subsystem": "tcp", "sensor": "EventsSent"}, "value": 100},
        ]
    }

    assert parse_icv2_counters(payload) == {
        "SessionsRegistered": 3.0,
        "SessionsUnregistered": 1.0,
        "EventsSent": 7.0,
        "BytesSent": 1024.0,
    }


def test_discovery_bounds_and_rotates_fault_path_fallbacks():
    class FakeHttpClient:
        def __init__(self):
            self.requests = []

        def get_json(self, url):
            parsed = icv2_load_module.urllib.parse.urlsplit(url)
            fields = icv2_load_module.urllib.parse.parse_qs(parsed.query)["fields_required"][0]
            host = parsed.netloc
            self.requests.append((host, fields))
            assert fields == "NodeId,SystemState"
            node_ids = {
                "node-1.example:8765": (1,),
                "node-2.example:8765": (2, 3),
                "node-3.example:8765": (2, 3, 4),
                "node-4.example:8765": (2, 3, 4, 5),
                "node-5.example:8765": (2, 3, 4, 5, 6),
            }[host]
            return {"Nodes": [_viewer_node(node_id) for node_id in node_ids]}

    http_client = FakeHttpClient()
    supervisor = Supervisor(
        _settings(seed_monitor_url="http://node-1.example:8765"),
        http_client,
    )
    supervisor.monitor_urls = [
        "http://node-1.example:8765",
        "http://node-2.example:8765",
        "http://node-3.example:8765",
        "http://node-4.example:8765",
        "http://node-5.example:8765",
    ]
    supervisor.baseline_node_ids = frozenset(range(1, 6))

    first_nodes = supervisor._discover()
    first_requests = list(http_client.requests)
    second_nodes = supervisor._discover()

    assert [node.node_id for node in first_nodes] == [2, 3, 4, 5]
    assert len(first_requests) == 4
    assert all(fields == "NodeId,SystemState" for _, fields in first_requests)
    assert ("node-5.example:8765", "NodeId,SystemState") not in first_requests
    assert [node.node_id for node in second_nodes] == [2, 3, 4, 5, 6]
    assert ("node-5.example:8765", "NodeId,SystemState") in http_client.requests[4:]


def test_discovery_uses_only_preferred_endpoint_for_complete_topology():
    node_1 = _viewer_node(1)
    node_1["SystemState"]["Endpoints"].insert(
        1,
        {"Name": "grpc", "Address": ":3135"},
    )

    class FakeHttpClient:
        def __init__(self):
            self.urls = []

        def get_json(self, url):
            self.urls.append(url)
            return {"Nodes": [node_1, _viewer_node(2)]}

    http_client = FakeHttpClient()
    supervisor = Supervisor(_settings(), http_client)
    supervisor.monitor_urls = ["http://node-1.example:8765", "http://node-2.example:8765"]
    supervisor.baseline_node_ids = frozenset((1, 2))
    supervisor.selected_grpc_endpoints[1] = "node-1.example:3135"

    nodes = supervisor._discover()

    assert [node.node_id for node in nodes] == [1, 2]
    assert nodes[0].grpc_endpoint == "node-1.example:3135"
    assert len(http_client.urls) == 1


def test_counter_check_does_not_infer_session_liveness_from_lifecycle_counters():
    class FakeHttpClient:
        def get_json(self, _url):
            return {
                "sensors": [
                    {"labels": {"subsystem": "uring", "sensor": "SessionsRegistered"}, "value": 0},
                    {
                        "labels": {"subsystem": "uring", "sensor": "SessionsUnregistered"},
                        "value": 0,
                    },
                    {"labels": {"subsystem": "uring", "sensor": "EventsSent"}, "value": 10},
                    {"labels": {"subsystem": "uring", "sensor": "BytesSent"}, "value": 100},
                ]
            }

    node = _node(1, start_time="123")

    assert Supervisor(_settings(), FakeHttpClient())._verify_node(node)[0]


def test_load_routes_cover_both_ring_directions():
    nodes = [_node(node_id, start_time="1") for node_id in (1, 2, 3)]

    routes = [
        (source.node_id, target.node_id, profile.name)
        for source, target, profile in load_routes(nodes)
    ]

    assert routes == [
        (1, 2, "inline-small"),
        (1, 3, "rope-fragmenting"),
        (2, 3, "inline-small"),
        (2, 1, "rope-fragmenting"),
        (3, 1, "inline-small"),
        (3, 2, "rope-fragmenting"),
    ]


def test_coverage_requires_the_actor_for_the_current_target():
    nodes = [_node(node_id, start_time="1") for node_id in (1, 2, 3)]
    supervisor = Supervisor(_settings(), object())
    source = nodes[0]
    target = nodes[1]
    wrong_target_burst = ActiveBurst(
        source=source,
        target_node_id=nodes[2].node_id,
        profile=LOAD_PROFILES[0],
        started_at=0,
        planned_duration=30,
        process=object(),
    )
    supervisor.window_bursts[wrong_target_burst.route_key] = wrong_target_burst

    supervisor._mark_window_bursts(
        nodes,
        {(source.instance, target.node_id): (True, "Session (v2)")},
    )

    assert not wrong_target_burst.v2_healthy

    exact_burst = dataclasses.replace(
        wrong_target_burst,
        target_node_id=target.node_id,
        v2_healthy=True,
    )
    supervisor.window_bursts = {exact_burst.route_key: exact_burst}
    supervisor._mark_window_bursts(
        nodes,
        {(source.instance, target.node_id): (True, "Session (v2)")},
    )

    assert exact_burst.v2_healthy


@pytest.mark.parametrize(
    ("rtt_samples", "max_rtt_gap_us", "failure_fragment"),
    [
        (30, 1_000_000, None),
        (0, 1_000_000, "need at least 30"),
        (30, 29_000_000, "max RTT gap"),
    ],
)
def test_profile_proof_requires_distributed_actor_rtt_even_when_global_counters_advance(
    monkeypatch,
    rtt_samples,
    max_rtt_gap_us,
    failure_fragment,
):
    nodes = _nodes()
    supervisor = Supervisor(_settings(), object())
    supervisor.baseline_node_ids = frozenset(node.node_id for node in nodes)
    refresh_count = 0

    def checked_counters(current_nodes):
        nonlocal refresh_count
        counters = {
            "SessionsRegistered": 1,
            "SessionsUnregistered": 0,
            "EventsSent": (10, 100)[refresh_count],
            "BytesSent": (1000, 100000)[refresh_count],
        }
        refresh_count += 1
        return {node.instance: counters for node in current_nodes}, {}, []

    profile = LOAD_PROFILES[0]

    monkeypatch.setattr(supervisor, "_discover", lambda: nodes)
    monkeypatch.setattr(supervisor, "_checked_counters", checked_counters)
    monkeypatch.setattr(
        supervisor,
        "_run_command",
        lambda _command, _timeout: (
            True,
            json.dumps(_load_result(rtt_samples=rtt_samples, max_rtt_gap_us=max_rtt_gap_us)),
            "",
        ),
    )
    monkeypatch.setattr(supervisor, "_verify_routes", lambda _nodes, _profiles=LOAD_PROFILES: [])

    failures = supervisor._prove_profile(profile, generation=7)

    assert bool(failures) == bool(failure_fragment)
    if failure_fragment:
        assert all(failure_fragment in failure for failure in failures)


def test_profile_proof_keeps_uring_progress_as_supplemental_evidence(monkeypatch):
    nodes = _nodes()
    supervisor = Supervisor(_settings(), object())
    supervisor.baseline_node_ids = frozenset(node.node_id for node in nodes)

    counters = {
        "SessionsRegistered": 1,
        "SessionsUnregistered": 0,
        "EventsSent": 10,
        "BytesSent": 1000,
    }
    monkeypatch.setattr(supervisor, "_discover", lambda: nodes)
    monkeypatch.setattr(
        supervisor,
        "_checked_counters",
        lambda current_nodes: ({node.instance: counters for node in current_nodes}, {}, []),
    )
    monkeypatch.setattr(
        supervisor,
        "_run_command",
        lambda _command, _timeout: (True, json.dumps(_load_result()), ""),
    )
    monkeypatch.setattr(supervisor, "_verify_routes", lambda _nodes, _profiles=LOAD_PROFILES: [])

    failures = supervisor._prove_profile(LOAD_PROFILES[0], generation=7)

    assert failures
    assert all("uring counter" in failure for failure in failures)


@pytest.mark.parametrize(
    ("route_failures", "expect_failure"),
    [
        ([], False),
        (["1->2: peer page has no current Session (v2)"], True),
    ],
)
def test_route_preflight_requires_v2_on_every_planned_route(
    monkeypatch,
    route_failures,
    expect_failure,
):
    nodes = _nodes()
    supervisor = Supervisor(_settings(burst_duration=0), object())
    supervisor.baseline_node_ids = frozenset(node.node_id for node in nodes)

    monkeypatch.setattr(supervisor, "_discover", lambda: nodes)
    monkeypatch.setattr(
        supervisor,
        "_arm_preflight_bursts",
        lambda _nodes: 0.0,
    )
    monkeypatch.setattr(
        supervisor,
        "_refresh_verified_nodes",
        lambda current_nodes: (list(current_nodes), []),
    )
    monkeypatch.setattr(supervisor, "_verify_routes", lambda _nodes: route_failures)

    if expect_failure:
        with pytest.raises(RuntimeError, match="route preflight failed"):
            supervisor._preflight_routes(nodes)
    else:
        assert supervisor._preflight_routes(nodes) == nodes


def test_preflight_arms_every_route_and_returns_last_actor_deadline(monkeypatch):
    nodes = _nodes()
    supervisor = Supervisor(_settings(burst_duration=30), object())
    commands = []

    def run_command(command, timeout):
        commands.append(command)
        assert timeout == supervisor.settings.command_timeout
        return True, "", ""

    monkeypatch.setattr(supervisor, "_run_command", run_command)
    monkeypatch.setattr(icv2_load_module.time, "monotonic", lambda: 100)

    active_until = supervisor._arm_preflight_bursts(nodes)

    assert active_until == 130
    assert len(commands) == len(load_routes(nodes))
    assert all("--wait" not in command for command in commands)


def test_preflight_selects_endpoint_that_serves_interconnect_debug(monkeypatch):
    nodes = [
        _node(1, grpc_endpoints=("node-1-ext:2135", "node-1:2135")),
        _node(2),
    ]
    supervisor = Supervisor(_settings(burst_duration=30), object())
    commands = []

    def run_command(command, _timeout):
        commands.append(command)
        if command[2] == "grpc://node-1-ext:2135":
            return False, "", "UNIMPLEMENTED"
        return True, "", ""

    monkeypatch.setattr(supervisor, "_run_command", run_command)
    monkeypatch.setattr(icv2_load_module.time, "monotonic", lambda: 100)

    supervisor._arm_preflight_bursts(nodes)

    assert supervisor.selected_grpc_endpoints[1] == "node-1:2135"
    assert len(commands) == len(load_routes(nodes)) + 1


@pytest.mark.parametrize(("fail_first_profile", "armed"), [(True, 2), (False, 3)])
def test_preflight_rejects_failed_routes_without_rearming_accepted_load(monkeypatch, fail_first_profile, armed):
    endpoints = ("node-1:2135", "node-1-alt:2135")
    nodes = [_node(1, grpc_endpoints=endpoints), _node(2)]
    supervisor = Supervisor(_settings(), object())
    commands = []

    def run_command(command, _timeout):
        commands.append(command)
        if command[2] != "grpc://node-2:2135" and (fail_first_profile or "--rope" in command):
            return False, "", "UNAVAILABLE"
        return True, "", ""

    monkeypatch.setattr(supervisor, "_run_command", run_command)

    with pytest.raises(RuntimeError, match=f"preflight armed only {armed} of 4 bursts"):
        supervisor._arm_preflight_bursts(nodes)

    node1_endpoints = [command[2] for command in commands if command[2] != "grpc://node-2:2135"]
    expected_endpoints = endpoints if fail_first_profile else (endpoints[0], endpoints[0])
    assert node1_endpoints == [f"grpc://{endpoint}" for endpoint in expected_endpoints]
    assert supervisor.selected_grpc_endpoints.get(1) == (None if fail_first_profile else endpoints[0])


def test_postflight_proves_profiles_separately_and_reports_rope_failure(monkeypatch):
    supervisor = Supervisor(_settings(), object())
    proved_profiles = []

    def prove(profile, _generation):
        proved_profiles.append(profile.name)
        return ["traffic stalled"] if profile.rope else []

    monkeypatch.setattr(supervisor, "_prove_profile", prove)

    failures = supervisor._postflight(generation=7)

    assert proved_profiles == ["inline-small", "rope-fragmenting"]
    assert failures == ["rope-fragmenting: traffic stalled"]


def test_build_rope_load_command_targets_source_endpoint():
    settings = _settings(token_file="/tmp/token")
    source = _node(1, start_time="123")

    command = build_load_command(settings, source, [2], LOAD_PROFILES[1], 12.5, 4)

    assert command[:5] == ["/bin/ydbd", "-s", "grpc://node-1:2135", "--token-file", "/tmp/token"]
    assert command[command.index("--hops") + 1] == "2"
    assert command[command.index("--duration") + 1] == "12.5s"
    assert "--rope" in command


def test_build_waiting_load_command_and_parse_completion_result():
    source = _node(1, start_time="123")
    command = build_load_command(
        _settings(),
        source,
        [2],
        LOAD_PROFILES[0],
        12.5,
        4,
        wait_for_completion=True,
    )

    assert "--wait" in command
    assert parse_load_result(
        'diagnostic\n{"throughput_bytes":12,"throughput_samples":2,"rtt_samples":1,'
        '"duration_us":1000000,"max_rtt_gap_us":500000}\n'
    ) == {
        "throughput_bytes": 12,
        "throughput_samples": 2,
        "rtt_samples": 1,
        "duration_us": 1_000_000,
        "max_rtt_gap_us": 500_000,
    }
    assert parse_load_result('{"rtt_samples":1}') is None


def test_grpc_command_does_not_receive_monitoring_tls_flags():
    source = _node(1, monitor_scheme="https", start_time="123")
    tls_files = {
        "ca_file": "/tmp/ca.pem",
        "client_cert_file": "/tmp/client.pem",
        "client_key_file": "/tmp/client.key",
    }

    command = build_load_command(_settings(**tls_files), source, [2], LOAD_PROFILES[0], 1, 1)

    assert command[2] == "grpc://node-1:2135"
    assert "--ca-file" not in command
    assert "--client-cert-file" not in command
    assert "--client-cert-key-file" not in command


def test_load_coverage_enforces_ratio_and_longest_gap_per_source_profile():
    key = (1, "inline-small")
    healthy = LoadCoverage([key], started_at=0)
    healthy.record(key, 10, 50)
    healthy.record(key, 50, 100)

    assert healthy.failures(100, min_coverage=0.8, max_gap=20) == []

    stalled = LoadCoverage([key], started_at=0)
    failures = stalled.failures(100, min_coverage=0.5, max_gap=20)

    assert len(failures) == 2
    assert any("covered 0.0%" in failure for failure in failures)
    assert any("100.000s load/V2 gap" in failure for failure in failures)


@pytest.mark.parametrize(
    ("rtt_samples", "max_rtt_gap_us", "v2_healthy", "expect_coverage"),
    [
        (20, 1_000_000, True, True),
        (0, 1_000_000, True, False),
        (20, 19_000_000, True, False),
        (20, 1_000_000, False, False),
    ],
)
def test_window_coverage_uses_completed_actor_rtt_and_v2_observations(
    rtt_samples,
    max_rtt_gap_us,
    v2_healthy,
    expect_coverage,
):
    class FinishedProcess:
        returncode = 0

    key = (1, LOAD_PROFILES[0].name)
    source = _node(1, start_time="1")
    burst = ActiveBurst(
        source=source,
        target_node_id=2,
        profile=LOAD_PROFILES[0],
        started_at=0,
        planned_duration=30,
        process=FinishedProcess(),
        v2_healthy=v2_healthy,
    )
    burst.stdout = json.dumps(
        _load_result(
            throughput_bytes=12,
            throughput_samples=2,
            rtt_samples=rtt_samples,
            duration_us=20_000_000,
            max_rtt_gap_us=max_rtt_gap_us,
        )
    )
    burst.completed_at = 21
    burst.completed.set()
    supervisor = Supervisor(_settings(), object())
    supervisor.window_bursts[burst.route_key] = burst
    coverage = LoadCoverage([key], started_at=0)

    completed = supervisor._collect_window_bursts(coverage, observed_at=30)
    covered, _ = coverage.measurements(30)[key]

    assert completed == int(expect_coverage)
    assert (covered > 0) is expect_coverage
    if expect_coverage:
        assert coverage.intervals[key] == [(1, 20)]


@pytest.mark.parametrize(
    ("overrides", "return_code", "failure"),
    [
        ({"timed_out": True}, 0, "timed out"),
        ({"wait_error": "read failed"}, 0, "failed to collect"),
        ({"stderr": "RPC failed"}, 1, "RPC failed"),
        ({"stdout": "diagnostics only"}, 0, "no valid completion statistics"),
        ({"stdout": json.dumps(_load_result(duration_us=0))}, 0, "no measured duration"),
        ({"completed_at": 2}, 0, "overhead consumed the whole provable actor interval"),
    ],
)
def test_window_coverage_discards_failed_or_unprovable_bursts(overrides, return_code, failure, caplog):
    supervisor, burst, coverage = _window_burst(Mock(returncode=return_code))
    burst.stdout = json.dumps(_load_result(duration_us=1_000_000))
    burst.completed_at = 1
    for name, value in overrides.items():
        setattr(burst, name, value)
    burst.completed.set()

    assert supervisor._collect_window_bursts(coverage, observed_at=2) == 0
    assert coverage.intervals[burst.coverage_key] == []
    assert not supervisor.window_bursts
    assert failure in caplog.text


@pytest.mark.parametrize("route_recovers", [False, True])
def test_window_bursts_reconnect_routes_without_crediting_unverified_load(monkeypatch, route_recovers):
    nodes = _nodes()
    supervisor = Supervisor(_settings(), object())
    coverage = LoadCoverage(
        ((node.node_id, profile.name) for node in nodes for profile in LOAD_PROFILES),
        started_at=0,
    )
    now = 0
    commands = []
    waiters = []

    def popen(command, **_kwargs):
        commands.append(command)
        duration = parse_duration(command[command.index("--duration") + 1])
        output = json.dumps(_load_result(duration_us=int(duration * 1_000_000)))
        return Mock(returncode=0, communicate=Mock(return_value=(output, "")))

    def thread(*, target, args, **_kwargs):
        waiters.append(lambda: target(*args))
        return Mock()

    def finish_bursts():
        for wait in waiters:
            wait()
        waiters.clear()
        return supervisor._collect_window_bursts(coverage, observed_at=now)

    monkeypatch.setattr(icv2_load_module.subprocess, "Popen", popen)
    monkeypatch.setattr(icv2_load_module.threading, "Thread", thread)
    monkeypatch.setattr(icv2_load_module.time, "monotonic", lambda: now)
    route_checks = {
        (source.instance, target.node_id): (False, "no session")
        for source, target, _ in load_routes(nodes)
    }
    num_routes = len(load_routes(nodes))

    assert supervisor._start_window_bursts(nodes, route_checks, remaining=45, generation=1) == num_routes
    assert supervisor._start_window_bursts(nodes, route_checks, remaining=45, generation=2) == 0
    assert len(commands) == num_routes
    assert all(command[command.index("--duration") + 1] == "30s" for command in commands)

    now = 30
    route_checks = {key: (route_recovers, "Session (v2)" if route_recovers else "no session") for key in route_checks}
    supervisor._mark_window_bursts(nodes, route_checks)
    assert finish_bursts() == 0
    assert all(intervals == [] for intervals in coverage.intervals.values())

    commands.clear()
    assert supervisor._start_window_bursts(nodes, route_checks, remaining=15, generation=3) == num_routes
    assert len(commands) == num_routes
    assert all(command[command.index("--duration") + 1] == "15s" for command in commands)
    now = 45
    assert finish_bursts() == (num_routes if route_recovers else 0)
    expected_intervals = [(30, 45)] if route_recovers else []
    assert all(intervals == expected_intervals for intervals in coverage.intervals.values())


def test_window_drain_has_a_hard_reap_deadline(monkeypatch):
    class StuckProcess:
        returncode = None
        killed = False

        def poll(self):
            return None

        def kill(self):
            self.killed = True

    process = StuckProcess()
    supervisor, _, coverage = _window_burst(process)
    closed = False

    def close():
        nonlocal closed
        closed = True
        supervisor.window_bursts.clear()

    monkeypatch.setattr(supervisor, "close", close)
    monkeypatch.setattr(icv2_load_module.time, "monotonic", lambda: 4)

    completed, failures = supervisor._drain_window_bursts(coverage)

    assert completed == 0
    assert failures == ["1 waiting load commands did not stop by the hard reap deadline"]
    assert process.killed
    assert closed


def test_completed_process_is_not_misclassified_as_timeout():
    class ExitedProcess:
        returncode = 0
        killed = False

        def poll(self):
            return self.returncode

        def kill(self):
            self.killed = True

    process = ExitedProcess()
    supervisor, burst, coverage = _window_burst(process)

    assert supervisor._collect_window_bursts(coverage, observed_at=2) == 0
    assert not burst.timed_out
    assert not process.killed

    burst.stdout = json.dumps(
        _load_result(
            rtt_samples=1,
            duration_us=1_000_000,
            max_rtt_gap_us=500_000,
        )
    )
    burst.completed_at = 1
    burst.completed.set()

    assert supervisor._collect_window_bursts(coverage, observed_at=2) == 1
    assert coverage.measurements(2)[burst.coverage_key][0] > 0


def test_parse_duration():
    assert parse_duration("250ms") == 0.25
    assert parse_duration("1.5m") == 90
