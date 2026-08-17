"""Parsers for the per-TU cost counters collected during a build.

Three independent sources, joined on the ``<digest>`` key that
``compdb.record_cc.tu_digest`` assigns to every compile:

``<digest>.pstat``
    clang's own ``-fproc-stat-report`` CSV: one line per subprocess the
    driver spawned, ``"tool","output",wall_us,user_us,peak_rss_kb``.

``<digest>.perf``
    ``perf stat -x,`` output: hardware counters, chiefly instructions
    retired, which is the only cost metric that stays put when the
    machine is loaded.

``<digest>.json``
    clang ``-ftime-trace``, reused from the existing timing pipeline for
    the frontend/backend split.
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

from ..common import repo_relative


log = logging.getLogger("buildbench.parse")

DEFAULT_PERF_EVENTS = "instructions:u,task-clock:u"
# Per-compile counters, deliberately minimal: proc-stat already supplies
# time and memory, so all perf has to add is the instruction count.
PER_TU_EVENTS_DEFAULT = "instructions:u"
# Values perf prints instead of a number when a counter could not run.
_PERF_NON_VALUES = ("<not counted>", "<not supported>", "<not-counted>")


@dataclass
class ProcStat:
    """Resource usage of one compile, as measured by the clang driver."""

    wall_us: int = 0
    user_us: int = 0
    peak_rss_kb: int = 0
    subprocesses: int = 0


def parse_procstat(path: Path) -> Optional[ProcStat]:
    """Parse a ``-fproc-stat-report`` file, or None if unusable.

    The driver may report several subprocesses for one compile. They run
    sequentially, so times add up while peak memory is a maximum.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    out = ProcStat()
    for row in csv.reader(text.splitlines()):
        if len(row) < 5:
            continue
        try:
            wall_us = int(float(row[2]))
            user_us = int(float(row[3]))
            peak_rss_kb = int(float(row[4]))
        except (TypeError, ValueError):
            continue
        out.wall_us += wall_us
        out.user_us += user_us
        out.peak_rss_kb = max(out.peak_rss_kb, peak_rss_kb)
        out.subprocesses += 1
    return out if out.subprocesses else None


def parse_perf(path: Path) -> Dict[str, float]:
    """Parse ``perf stat -x,`` output into ``{event: value}``.

    Skips the ``# started on ...`` banner, blank lines and events perf
    could not count. Never raises: a missing counter degrades to a
    missing key, not to a failed measurement run.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}

    counters: Dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) < 3:
            continue
        raw, event = fields[0].strip(), fields[2].strip()
        if not event or raw in _PERF_NON_VALUES:
            continue
        try:
            counters[event] = float(raw)
        except ValueError:
            continue
    return counters


def perf_value(counters: Dict[str, float], name: str) -> Optional[float]:
    """Look up an event tolerating the ``:u`` / ``:k`` modifier suffix."""
    if name in counters:
        return counters[name]
    for event, value in counters.items():
        if event.split(":", 1)[0] == name:
            return value
    return None


def _true_binary() -> str:
    for cand in ("/bin/true", "/usr/bin/true"):
        if os.path.exists(cand):
            return cand
    return shutil.which("true") or "/bin/true"


def probe_perf(perf_bin: Optional[str] = None,
               events: str = DEFAULT_PERF_EVENTS) -> Optional[str]:
    """Return a usable ``perf`` path, or None if counting is not possible.

    Checked in increasing order of cost: the binary exists, the kernel
    allows unprivileged user-space counting (``perf_event_paranoid <= 2``),
    and a throwaway run actually produces a non-zero instruction count.
    Containers and VMs often satisfy the first two and fail the third, so
    the functional check is the one that matters.
    """
    binary = perf_bin or shutil.which("perf")
    if not binary or not os.path.exists(binary):
        log.debug("perf: binary not found")
        return None

    try:
        paranoid = int(Path("/proc/sys/kernel/perf_event_paranoid")
                       .read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        paranoid = None
    if paranoid is not None and paranoid > 2:
        log.warning("perf: perf_event_paranoid=%d forbids unprivileged "
                    "counting; falling back to CPU time", paranoid)
        return None

    tmp = Path(tempfile.mkdtemp(prefix="ydb-perfprobe-"))
    try:
        report = tmp / "probe.perf"
        cmd = [binary, "stat", "-x,", "-e", events, "-o", str(report), "--",
               _true_binary()]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError) as exc:
            log.warning("perf: probe failed to run (%s); falling back to CPU time", exc)
            return None
        if proc.returncode != 0:
            log.warning("perf: probe exited %d (%s); falling back to CPU time",
                        proc.returncode, (proc.stderr or "").strip()[:200])
            return None
        counters = parse_perf(report)
        if not perf_value(counters, "instructions"):
            log.warning("perf: probe produced no instruction count; "
                        "falling back to CPU time")
            return None
        return binary
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@dataclass
class TuCost:
    """Measured cost of compiling one source file during a build."""

    tu: str
    compiles: int = 0
    user_us: int = 0
    wall_us: int = 0
    peak_rss_kb: int = 0
    instructions: float = 0.0
    execute_us: int = 0
    frontend_us: int = 0
    backend_us: int = 0

    def as_dict(self) -> dict:
        return {
            "compiles": self.compiles,
            "user_us": self.user_us,
            "wall_us": self.wall_us,
            "peak_rss_kb": self.peak_rss_kb,
            "instructions": self.instructions,
            "execute_us": self.execute_us,
            "frontend_us": self.frontend_us,
            "backend_us": self.backend_us,
        }


def _iter_digests(dirs: Iterable[Optional[Path]]) -> Dict[str, str]:
    """Collect ``{digest: abs_source}`` from ``.src`` sidecars in ``dirs``."""
    found: Dict[str, str] = {}
    for d in dirs:
        if not d or not d.is_dir():
            continue
        for sidecar in d.glob("*.src"):
            try:
                source = sidecar.read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if source:
                found.setdefault(sidecar.stem, source)
    return found


def collect_tu_costs(ps_dir: Optional[Path] = None,
                     perf_dir: Optional[Path] = None,
                     tt_dir: Optional[Path] = None) -> Dict[str, TuCost]:
    """Join per-TU artifacts into ``{repo-relative source: TuCost}``.

    A source compiled more than once in a build (different targets, PIC
    and non-PIC) genuinely costs that many compiles, so the entries are
    summed rather than deduplicated, and ``compiles`` records how many.
    """
    costs: Dict[str, TuCost] = {}
    for digest, source_abs in _iter_digests((ps_dir, perf_dir, tt_dir)).items():
        rel = repo_relative(source_abs)
        cost = costs.get(rel)
        if cost is None:
            cost = costs[rel] = TuCost(tu=rel)
        cost.compiles += 1

        if ps_dir:
            stat = parse_procstat(ps_dir / f"{digest}.pstat")
            if stat:
                cost.user_us += stat.user_us
                cost.wall_us += stat.wall_us
                cost.peak_rss_kb = max(cost.peak_rss_kb, stat.peak_rss_kb)

        if perf_dir:
            counters = parse_perf(perf_dir / f"{digest}.perf")
            instructions = perf_value(counters, "instructions")
            if instructions:
                cost.instructions += instructions

        if tt_dir:
            trace = tt_dir / f"{digest}.json"
            if trace.exists():
                from ..timing.parse import parse_trace
                timing = parse_trace(trace, tu_name=rel)
                if timing:
                    cost.execute_us += timing.execute_us or 0
                    cost.frontend_us += timing.frontend_us or 0
                    cost.backend_us += timing.backend_us or 0

    return costs


def clear_artifacts(dirs: Iterable[Optional[Path]]) -> None:
    """Drop artifacts from a previous run so totals cannot double-count."""
    for d in dirs:
        if not d or not d.is_dir():
            continue
        for pattern in ("*.src", "*.pstat", "*.perf", "*.json"):
            for p in d.glob(pattern):
                try:
                    p.unlink()
                except OSError:
                    pass


def human_count(value: float) -> str:
    """Render a large counter value compactly (1.23e12 -> ``1.23 T``)."""
    for scale, suffix in ((1e12, "T"), (1e9, "G"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= scale:
            return f"{value / scale:.2f} {suffix}"
    return f"{value:.0f}"
