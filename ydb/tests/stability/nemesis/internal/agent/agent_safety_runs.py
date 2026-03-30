"""
Agent safety runs: same execution path as orchestrator cluster safety (see safety_warden_execution).
"""

from __future__ import annotations

from ydb.tests.stability.nemesis.internal.safety_warden_execution import (
    SafetyWardenRun,
    build_safety_runs_from_pairs,
    safety_build_error_result,
    safety_warden_to_result,
)

AgentSafetyRun = SafetyWardenRun
build_agent_safety_runs_from_pairs = build_safety_runs_from_pairs

__all__ = [
    "AgentSafetyRun",
    "build_agent_safety_runs_from_pairs",
    "safety_build_error_result",
    "safety_warden_to_result",
]
