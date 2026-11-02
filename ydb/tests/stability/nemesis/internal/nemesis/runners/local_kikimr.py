# -*- coding: utf-8 -*-
"""Start and stop the kikimr units that are actually running on this host.

Inventory in ``ExternalKiKiMRCluster.slots`` lists every synthetic slot from
databases.yaml.  Restarting that list creates units that were not running and,
via a slot_cfg rewrite, drops settings such as ``kafka_port``.  These helpers
only touch units reported by systemd, and they never write ``slot_cfg``.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence

_LIST_ACTIVE_SLOTS: tuple[str, ...] = (
    "systemctl",
    "list-units",
    "kikimr-multi@*",
    "--state=active",
    "--no-legend",
)
_IS_ACTIVE_STATIC: tuple[str, ...] = ("systemctl", "is-active", "kikimr")

# stdout of `systemctl is-active` for a unit that exists or is simply down.
_KNOWN_IS_ACTIVE = frozenset({
    "active",
    "inactive",
    "failed",
    "activating",
    "deactivating",
    "unknown",
    "dead",
})

_MULTI_PREFIX = "kikimr-multi@"


def _default_run(cmd: Sequence[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(cmd),
        check=False,
        capture_output=True,
        text=True,
    )


def _run(run: Callable | None, cmd: Sequence[str]) -> subprocess.CompletedProcess:
    return (run or _default_run)(cmd)


def _check(proc: subprocess.CompletedProcess, cmd: Sequence[str]) -> None:
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, list(cmd), proc.stdout, proc.stderr)


def parse_active_kikimr_multi(text: str) -> list[str]:
    """Instance ids of active ``kikimr-multi@`` units in ``list-units`` output.

    Lines for other units, or for units whose ACTIVE column is not ``active``,
    are ignored.  Nothing is invented when the text does not name a unit.
    """
    ids: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("●"):
            line = line[1:].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        unit, active = parts[0], parts[2]
        if active != "active" or not unit.startswith(_MULTI_PREFIX):
            continue
        instance = unit[len(_MULTI_PREFIX):]
        if instance.endswith(".service"):
            instance = instance[: -len(".service")]
        if not instance or instance == "*":
            continue
        ids.append(instance)
    return ids


def active_local_kikimr(run: Callable | None = None) -> tuple[bool, list[str]]:
    """Return whether static ``kikimr`` is active, and active slot instance ids.

    Slot ids come only from
    ``systemctl list-units 'kikimr-multi@*' --state=active --no-legend``.
    """
    static_cmd = list(_IS_ACTIVE_STATIC)
    static_proc = _run(run, static_cmd)
    static_out = (static_proc.stdout or "").strip()
    if static_out not in _KNOWN_IS_ACTIVE:
        raise subprocess.CalledProcessError(
            static_proc.returncode, static_cmd, static_proc.stdout, static_proc.stderr
        )
    slots_cmd = list(_LIST_ACTIVE_SLOTS)
    slots_proc = _run(run, slots_cmd)
    _check(slots_proc, slots_cmd)
    return static_out == "active", parse_active_kikimr_multi(slots_proc.stdout or "")


def _run_all(run: Callable | None, commands: list[list[str]]) -> None:
    first_error: subprocess.CalledProcessError | None = None
    for cmd in commands:
        proc = _run(run, cmd)
        if proc.returncode != 0 and first_error is None:
            first_error = subprocess.CalledProcessError(
                proc.returncode, cmd, proc.stdout, proc.stderr
            )
    if first_error is not None:
        raise first_error


def stop_local_kikimr(static: bool, slots: Sequence[str], run: Callable | None = None) -> None:
    """Stop slot units first, then static ``kikimr`` when it was active."""
    commands = [["sudo", "systemctl", "stop", "kikimr-multi@{}".format(slot)] for slot in slots]
    if static:
        commands.append(["sudo", "systemctl", "stop", "kikimr"])
    _run_all(run, commands)


def start_local_kikimr(static: bool, slots: Sequence[str], run: Callable | None = None) -> None:
    """Start static ``kikimr`` first, then the same slot units.

    Issues ``systemctl start`` only.  Does not create directories or write
    ``slot_cfg``.
    """
    commands: list[list[str]] = []
    if static:
        commands.append(["sudo", "systemctl", "start", "kikimr"])
    commands.extend(
        ["sudo", "systemctl", "start", "kikimr-multi@{}".format(slot)] for slot in slots
    )
    _run_all(run, commands)
