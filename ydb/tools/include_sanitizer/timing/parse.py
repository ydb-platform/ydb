"""Parse a single clang ``-ftime-trace`` JSON into compact metrics.

clang emits a Chrome Trace Event file: ``{"traceEvents": [...], ...}``.
Duration events use either ``ph == "X"`` with ``dur`` in microseconds,
or paired ``ph == "b"`` / ``ph == "e"`` with timestamps (Clang 20 Source
events). Relevant event names:

- ``ExecuteCompiler``       — whole-TU wall time (our TU total).
- ``Total Frontend``        — parsing + sema (where includes/templates cost).
- ``Total Backend``         — codegen + optimizer.
- ``Source`` (args.detail)  — time to parse one included file (inclusive
                              of files it includes); summed per file this
                              is "how much this header costs to include".
- ``InstantiateClass`` /
  ``InstantiateFunction``   — per-template instantiation cost (args.detail).
- ``Total <X>``             — clang's own per-category rollups.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional


# clang time-trace "detail" location forms seen in practice:
#   "/path/file.h:12:3"                      (ParseDeclarationOrFunctionDefinition)
#   "</path/file.h:25:48, col:49>"           (EvaluateAsRValue / EvaluateForOverflow)
#   "</path/file.h:25:48, /other:9:1>"       (ranges spanning files)
_LOCATION_RE = re.compile(r"^(.*?):(\d+):(\d+)$")

_FILE_EXTS = (".h", ".hh", ".hpp", ".hxx", ".h++",
              ".cpp", ".cc", ".cxx", ".c++", ".c",
              ".ipp", ".inc", ".inl", ".def")


def _file_from_location(detail: str) -> Optional[str]:
    """Extract the file path from a clang time-trace location detail.

    Handles the bare ``path:line:col`` form and the angle-wrapped range
    form ``<path:line:col, col:NN>`` / ``<path:line:col, other:line:col>``.
    Returns None for non-location details (e.g. ``std::map<int,int>``).
    """
    s = detail.strip()
    if s.startswith("<"):
        s = s[1:]
    # A location is a single token "path:line:col". Cut at the first
    # space or comma to drop trailing junk like " <Spelling=...>" (macro
    # expansion locations) or ", col:NN" (ranges).
    s = re.split(r"[,\s]", s, maxsplit=1)[0]
    if s.endswith(">"):
        s = s[:-1]
    m = _LOCATION_RE.match(s)
    if not m:
        return None
    f = m.group(1)
    # A location must look like a path (contains '/' or a source suffix);
    # this rejects type/template names that happen to contain digits.
    if "/" in f or f.endswith(_FILE_EXTS):
        return f
    return None


@dataclass
class TuTiming:
    tu: str
    execute_us: int = 0
    frontend_us: int = 0
    backend_us: int = 0
    # detail -> microseconds, within this TU
    source_us: Dict[str, int] = field(default_factory=dict)
    template_us: Dict[str, int] = field(default_factory=dict)
    # Fallback per-file activity derived from location-tagged events
    # (Parse*/Instantiate* whose detail is "path:line:col"). Used to
    # attribute per-header cost when clang emitted no "Source" events.
    file_us: Dict[str, int] = field(default_factory=dict)
    totals: Dict[str, int] = field(default_factory=dict)


def duration_events(events: Iterable[dict]) -> Iterator[dict]:
    """Yield complete durations, including LLVM's async begin/end pairs.

    LLVM writes each async pair together, in completion order, reusing
    id=0. Preserve that order: sorting by timestamp before pairing can
    incorrectly match nested includes. Incomplete pairs are ignored.
    """
    pending = {}
    for event in events:
        phase = event.get("ph")
        if phase == "X":
            yield event
        elif phase in ("b", "e"):
            key = tuple(event.get(k) for k in ("pid", "tid", "cat", "id"))
            if phase == "b":
                pending[key] = event
            else:
                begin = pending.pop(key, None)
                if begin is None:
                    continue
                start, end = begin.get("ts"), event.get("ts")
                if start is not None and end is not None and end >= start:
                    yield dict(begin, ph="X", dur=end - start)


def parse_trace(path: Path, tu_name: Optional[str] = None) -> Optional[TuTiming]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None

    events = data.get("traceEvents") or []
    t = TuTiming(tu=tu_name or path.stem)

    # Fallbacks if "Total Frontend"/"Total Backend" are absent: sum the
    # per-instance "Frontend"/"Backend" events.
    frontend_sum = 0
    backend_sum = 0

    for e in duration_events(events):
        name = e.get("name") or ""
        dur = e.get("dur") or 0
        if name == "ExecuteCompiler":
            t.execute_us = max(t.execute_us, dur)
        elif name == "Frontend":
            frontend_sum += dur
        elif name == "Backend":
            backend_sum += dur
        elif name == "Source":
            detail = (e.get("args") or {}).get("detail")
            if detail:
                t.source_us[detail] = t.source_us.get(detail, 0) + dur
        elif name in ("InstantiateClass", "InstantiateFunction"):
            detail = (e.get("args") or {}).get("detail")
            if detail:
                t.template_us[detail] = t.template_us.get(detail, 0) + dur

        # Fallback per-file attribution from any location-tagged event.
        detail = (e.get("args") or {}).get("detail")
        if detail and name != "Source":
            f = _file_from_location(detail)
            if f:
                t.file_us[f] = t.file_us.get(f, 0) + dur

        if name.startswith("Total "):
            t.totals[name] = dur

    t.frontend_us = t.totals.get("Total Frontend", frontend_sum)
    t.backend_us = t.totals.get("Total Backend", backend_sum)
    if not t.execute_us:
        t.execute_us = t.totals.get("Total ExecuteCompiler", 0) or (
            t.frontend_us + t.backend_us
        )
    return t
