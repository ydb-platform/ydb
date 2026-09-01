"""Orchestrator endpoints this scheduler needs, through a Flask test client."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml
from flask import Flask

import ydb.tests.stability.nemesis.routers.orchestrator_router as router
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import ChaosProblemStore
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import StuckFault


class FakeScheduler:
    def __init__(self) -> None:
        self.profiles: list[dict] = []
        self.started = 0
        self._running = False

    def running(self) -> bool:
        return self._running

    def status(self) -> dict:
        return {"running": self._running, "enabled_types": ["KillNodeNemesis"], "max_per_tick": 3}

    def set_profile(self, **kwargs) -> None:
        self.profiles.append(kwargs)

    def start(self) -> None:
        self.started += 1
        self._running = True

    def stop(self) -> None:
        self._running = False


@pytest.fixture
def client():
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({
            "static_erasure": "block-4-2",
            "hosts": [
                {"name": f"h{i}", "location": {"rack": f"r{i}", "data_center": "dc1"}}
                for i in (1, 2, 3)
            ],
        }),
        encoding="utf-8",
    )
    app = Flask(__name__)
    app.register_blueprint(router.blueprint)

    router.hosts = ["h1", "h2", "h3"]
    router.chaos_problems = ChaosProblemStore()
    router.failure_guard = FailureModelGuard(ClusterTopologyModel(path), total_slots=10)
    router.nemesis_scheduler = FakeScheduler()
    router.cluster_inventory = None
    with app.test_client() as c:
        yield c


def test_problems_reports_stuck_faults_and_the_budget(client):
    body = client.get("/api/problems").get_json()
    assert body["count"] == 0 and body["guard_enabled"] is True and "failure_budget" in body

    router.chaos_problems.record_stuck_fault(
        StuckFault(
            lease_id="l1",
            nemesis_type="StopStartNodeNemesis",
            target=ChaosTarget.for_node("h1", node_id=1),
            held_sec=400.0,
            timeout_sec=300.0,
        )
    )
    target = ChaosTarget.for_node("h1", node_id=1)
    router.failure_guard.reserve(
        router.failure_guard.footprint_for(target, ImpactScope.NODE),
        identity_key=target.identity_key(),
    )

    body = client.get("/api/problems").get_json()
    assert body["by_kind"] == {"stuck_fault": 1}
    assert body["problems"][0]["target"] == "node:1:h1"
    assert body["failure_budget"]["impaired_racks"] == ["dc1/r1"]


def test_scheduler_start_and_stop(client):
    body = client.post("/api/scheduler/start", json={}).get_json()
    assert body["status"] == "ok" and body["scheduler"]["running"] is True
    assert router.nemesis_scheduler.profiles == [], "an empty body keeps the current profile"

    body = client.post("/api/scheduler/stop", json={}).get_json()
    assert body["status"] == "ok" and body["scheduler"]["running"] is False


def test_scheduler_start_applies_a_valid_profile(client):
    resp = client.post(
        "/api/scheduler/start",
        json={
            "enabled": ["KillNodeNemesis"],
            "base_interval": 30,
            "max_per_tick": 2,
            "max_bypass_per_tick": 2,
        },
    )
    assert resp.status_code == 200, resp.data
    assert router.nemesis_scheduler.profiles == [
        {
            "enabled": ["KillNodeNemesis"],
            "base_interval": 30.0,
            "max_per_tick": 2,
            "max_bypass_per_tick": 2,
        }
    ]


@pytest.mark.parametrize(
    "body",
    [
        {"enabled": "KillNodeNemesis"},                 # a bare string would split into letters
        {"enabled": ["NoSuchNemesis"]},
        {"enabled": ["ClusterRollingRestartNemesis"]},   # planner keeps its own targets
        {"jitter": 3},
        {"max_per_tick": 0},
        {"max_bypass_per_tick": 0},
        {"base_intervall": 30},                         # a typo must not be ignored
    ],
)
def test_scheduler_start_rejects_bad_profiles(client, body):
    resp = client.post("/api/scheduler/start", json=body)
    assert resp.status_code == 400, resp.data
    assert resp.get_json()["message"]
    assert router.nemesis_scheduler.started == 0, "a rejected profile must not start chaos"


def test_endpoints_survive_missing_wiring(client):
    router.chaos_problems = None
    assert client.get("/api/problems").get_json()["problems"] == []
    router.nemesis_scheduler = None
    assert client.post("/api/scheduler/start", json={}).status_code == 500
    assert client.get("/api/scheduler").get_json() == {"available": False}
