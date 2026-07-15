"""Shared dispatch types and helpers (orchestrator → agent RPC shape)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DispatchCommand:
    execution_id: str
    scenario_id: str
    nemesis_type: str
    host: str
    action: str
    payload: dict[str, Any]


def dispatch(
    nemesis_type: str,
    host: str,
    action: str,
    payload: dict[str, Any],
    *,
    scenario_id: str | None = None,
) -> DispatchCommand:
    return DispatchCommand(
        execution_id=str(uuid.uuid4()),
        scenario_id=scenario_id or str(uuid.uuid4()),
        nemesis_type=nemesis_type,
        host=host,
        action=action,
        payload=payload,
    )


def fanout(
    nemesis_type: str,
    hosts: list[str],
    action: str,
    payload: dict[str, Any],
) -> list[DispatchCommand]:
    sid = str(uuid.uuid4())
    return [dispatch(nemesis_type, h, action, payload, scenario_id=sid) for h in hosts]
