"""Shared dispatch types and helpers (orchestrator → agent RPC shape)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget


@dataclass(frozen=True)
class DispatchCommand:
    execution_id: str
    scenario_id: str
    nemesis_type: str
    action: str
    target: ChaosTarget
    payload: dict[str, Any]

    @property
    def host(self) -> str:
        """Agent address (backward-compatible alias)."""
        return self.target.host


def dispatch(
    nemesis_type: str,
    host_or_target: str | ChaosTarget,
    action: str,
    payload: dict[str, Any],
    *,
    scenario_id: str | None = None,
    target: ChaosTarget | None = None,
) -> DispatchCommand:
    """Build a dispatch command.

    ``host_or_target`` may be a hostname (legacy) or a :class:`ChaosTarget`.
    Explicit ``target=`` overrides when provided.
    """
    if target is not None:
        chaos_target = target
    elif isinstance(host_or_target, ChaosTarget):
        chaos_target = host_or_target
    else:
        chaos_target = ChaosTarget.for_host(str(host_or_target))
    return DispatchCommand(
        execution_id=str(uuid.uuid4()),
        scenario_id=scenario_id or str(uuid.uuid4()),
        nemesis_type=nemesis_type,
        action=action,
        target=chaos_target,
        payload=payload,
    )


def fanout(
    nemesis_type: str,
    hosts_or_targets: list[str] | list[ChaosTarget],
    action: str,
    payload: dict[str, Any],
) -> list[DispatchCommand]:
    sid = str(uuid.uuid4())
    return [
        dispatch(nemesis_type, item, action, payload, scenario_id=sid)
        for item in hosts_or_targets
    ]


__all__ = [
    "DispatchCommand",
    "dispatch",
    "fanout",
]
