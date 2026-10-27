"""Active local kikimr units are snapshotted and restarted without slot_cfg."""

from __future__ import annotations

import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.runners.local_kikimr import (
    active_local_kikimr,
    start_local_kikimr,
    stop_local_kikimr,
)


_LIST_UNITS_SAMPLE = """\
UNIT LOAD ACTIVE SUB DESCRIPTION
kikimr-multi@31003.service loaded active running YDB slot 31003
kikimr-multi@31013.service loaded active running YDB slot 31013
  kikimr-multi@31023.service loaded active running YDB slot 31023
kikimr-multi@31033.service loaded active running YDB slot 31033
kikimr-multi@31999.service loaded inactive dead YDB slot 31999
kikimr-multi@31998.service loaded failed failed YDB slot 31998
kikimr.service loaded active running static kikimr
nginx.service loaded active running nginx
"""


def _completed(cmd, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(list(cmd), returncode, stdout=stdout, stderr=stderr)


def _recording_run(calls, stdout_for=None):
    def run(cmd):
        recorded = list(cmd)
        calls.append(recorded)
        stdout = ""
        if stdout_for is not None:
            stdout = stdout_for(recorded)
        return _completed(recorded, stdout=stdout)

    return run


def test_parser_yields_only_active_kikimr_multi_ids():
    calls = []

    def stdout_for(cmd):
        if "is-active" in cmd:
            return "active\n"
        if "list-units" in cmd:
            return _LIST_UNITS_SAMPLE
        return ""

    static, slots = active_local_kikimr(run=_recording_run(calls, stdout_for))
    assert static is True
    assert slots == ["31003", "31013", "31023", "31033"]
    assert ["systemctl", "list-units", "kikimr-multi@*", "--state=active", "--no-legend"] in calls


def test_start_issues_only_systemctl_start():
    calls = []
    start_local_kikimr(True, ["31003", "31013"], run=_recording_run(calls))
    assert calls == [
        ["sudo", "systemctl", "start", "kikimr"],
        ["sudo", "systemctl", "start", "kikimr-multi@31003"],
        ["sudo", "systemctl", "start", "kikimr-multi@31013"],
    ]
    flat = " ".join(" ".join(cmd) for cmd in calls)
    assert "tee" not in flat
    assert "mkdir" not in flat
    assert "slot_cfg" not in flat
    assert all("systemctl" in cmd and "start" in cmd for cmd in calls)


def test_stop_slots_then_static_and_start_static_then_slots():
    stop_calls = []
    stop_local_kikimr(True, ["31003", "31013", "31023"], run=_recording_run(stop_calls))
    assert stop_calls == [
        ["sudo", "systemctl", "stop", "kikimr-multi@31003"],
        ["sudo", "systemctl", "stop", "kikimr-multi@31013"],
        ["sudo", "systemctl", "stop", "kikimr-multi@31023"],
        ["sudo", "systemctl", "stop", "kikimr"],
    ]

    start_calls = []
    start_local_kikimr(True, ["31003", "31013", "31023"], run=_recording_run(start_calls))
    assert [cmd[-1] for cmd in start_calls] == [
        "kikimr",
        "kikimr-multi@31003",
        "kikimr-multi@31013",
        "kikimr-multi@31023",
    ]
    assert [cmd[-2] for cmd in start_calls] == ["start", "start", "start", "start"]
