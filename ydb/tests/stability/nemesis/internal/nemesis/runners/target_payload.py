"""Helpers to read ChaosTarget from agent payload."""

from __future__ import annotations

from typing import Any

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget


def target_from_payload(payload: dict[str, Any] | None) -> ChaosTarget | None:
    if not isinstance(payload, dict):
        return None
    return ChaosTarget.from_dict(payload.get("chaos_target"))
