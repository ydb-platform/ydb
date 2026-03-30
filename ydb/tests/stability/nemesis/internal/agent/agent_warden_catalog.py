"""Agent-side warden catalog: safety checks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

from ydb.tests.library.nemesis.safety_warden import (
    GrepDMesgForPatternsSafetyWarden,
    GrepGzippedLogFilesForMarkersSafetyWarden,
    GrepLogFileForMarkers,
    UnifiedAgentVerifyFailedSafetyWarden,
)

_KIKIMR_START_MARKERS: List[str] = [
    "VERIFY",
    "FAIL ",
    "signal 11",
    "signal 6",
    "signal 15",
    "uncaught exception",
    "ERROR: AddressSanitizer",
    "SIG",
]

_DMESG_MARKERS: List[str] = ["Out of memory: Kill process"]


@dataclass(frozen=True)
class AgentSafetyContext:
    """Context passed to every agent safety check build."""

    log_directory: str
    hostname: str

    @property
    def log_prefix(self) -> str:
        return f"[{self.hostname}] "

    @property
    def local_hosts(self) -> List[str]:
        return [self.hostname]


@dataclass(frozen=True)
class AgentSafetyCheck:
    """One agent safety check; build(ctx) returns a warden with list_of_safety_violations."""

    name: str
    description: str
    build: Callable[[AgentSafetyContext], Any]


AGENT_SAFETY_CHECKS: Tuple[AgentSafetyCheck, ...] = (
    AgentSafetyCheck(
        "GrepLogFileForMarkersSafetyWarden",
        "Check kikimr.start logs for error markers",
        lambda ctx: GrepLogFileForMarkers(
            ctx.local_hosts,
            log_file_name=os.path.join(ctx.log_directory, "kikimr.start"),
            list_of_markers=_KIKIMR_START_MARKERS,
            username=None,
            lines_after=5,
            cut=True,
        ),
    ),
    AgentSafetyCheck(
        "GrepGzippedLogFilesForMarkersSafetyWarden",
        "Check gzipped kikimr.start logs for error markers",
        lambda ctx: GrepGzippedLogFilesForMarkersSafetyWarden(
            ctx.local_hosts,
            log_file_pattern=os.path.join(ctx.log_directory, "kikimr.start.*gz"),
            list_of_markers=_KIKIMR_START_MARKERS,
            modification_days=1,
            username=None,
            lines_after=5,
            cut=True,
        ),
    ),
    AgentSafetyCheck(
        "GrepDMesgForPatternsSafetyWarden",
        "Check dmesg for OOM and other critical patterns",
        lambda ctx: GrepDMesgForPatternsSafetyWarden(
            ctx.local_hosts,
            list_of_markers=_DMESG_MARKERS,
            username=None,
            lines_after=5,
        ),
    ),
    AgentSafetyCheck(
        "UnifiedAgentVerifyFailedSafetyWarden",
        "Check unified_agent logs for VERIFY failed errors",
        lambda _ctx: UnifiedAgentVerifyFailedSafetyWarden(hours_back=24),
    ),
)
