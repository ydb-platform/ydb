"""Aggregate per-TU time-trace JSONs into build-wide statistics."""

from __future__ import annotations

import concurrent.futures
import csv
import json
import logging
import multiprocessing
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .parse import TuTiming, parse_trace


log = logging.getLogger("timing.aggregate")

_AGG_CACHE_VERSION = 3


def _us(v: int) -> float:
    """microseconds -> seconds."""
    return v / 1_000_000.0


def template_module(detail: str) -> str:
    """Classify a template instantiation by its originating library.

    Lets us roll up "where is the instantiation cost" without needing
    per-header attribution: abseil / protobuf / std / ydb / yt / other.
    """
    d = detail
    if "y_absl::" in d or "absl::" in d:
        return "abseil"
    if "google::protobuf" in d or "NKikimrProto" in d or "::protobuf::" in d:
        return "protobuf"
    if d.startswith("std::") or d.startswith("__") or "std::__" in d:
        return "std"
    if d.startswith("NKikimr") or "::NKikimr" in d or "TRcBuf" in d:
        return "ydb"
    if d.startswith("NYT") or d.startswith("NYql") or "::NYT" in d:
        return "yt/yql"
    if d.startswith("arrow::") or "::arrow::" in d:
        return "arrow"
    return "other"


class Aggregate:
    def __init__(self) -> None:
        self.tus: List[TuTiming] = []
        self.header_us: Dict[str, int] = defaultdict(int)
        self.header_tus: Dict[str, int] = defaultdict(int)
        self.template_us: Dict[str, int] = defaultdict(int)
        self.template_count: Dict[str, int] = defaultdict(int)
        # Fallback per-file activity (from location-tagged events) used
        # when no per-file "Source" events were recorded.
        self.file_us: Dict[str, int] = defaultdict(int)
        self.file_tus: Dict[str, int] = defaultdict(int)
        self.total_execute = 0
        self.total_frontend = 0
        self.total_backend = 0

    def add(self, t: TuTiming) -> None:
        self.tus.append(t)
        self.total_execute += t.execute_us
        self.total_frontend += t.frontend_us
        self.total_backend += t.backend_us
        for detail, dur in t.source_us.items():
            self.header_us[detail] += dur
            self.header_tus[detail] += 1
        for detail, dur in t.template_us.items():
            self.template_us[detail] += dur
            self.template_count[detail] += 1
        for detail, dur in t.file_us.items():
            self.file_us[detail] += dur
            self.file_tus[detail] += 1

    def merge_partial(self, part: dict) -> None:
        """Fold a worker's chunk result (compact dict) into this aggregate."""
        for tu, ex, fe, be in part["tus"]:
            self.tus.append(TuTiming(tu=tu, execute_us=ex, frontend_us=fe, backend_us=be))
        self.total_execute += part["te"]
        self.total_frontend += part["tf"]
        self.total_backend += part["tb"]
        for d, u in part["header_us"].items():
            self.header_us[d] += u
        for d, c in part["header_tus"].items():
            self.header_tus[d] += c
        for d, u in part["template_us"].items():
            self.template_us[d] += u
        for d, c in part["template_count"].items():
            self.template_count[d] += c
        for d, u in part.get("file_us", {}).items():
            self.file_us[d] += u
        for d, c in part.get("file_tus", {}).items():
            self.file_tus[d] += c

    def to_dict(self) -> dict:
        return {
            "version": _AGG_CACHE_VERSION,
            "tus": [(t.tu, t.execute_us, t.frontend_us, t.backend_us) for t in self.tus],
            "header_us": dict(self.header_us),
            "header_tus": dict(self.header_tus),
            "template_us": dict(self.template_us),
            "template_count": dict(self.template_count),
            "file_us": dict(self.file_us),
            "file_tus": dict(self.file_tus),
            "te": self.total_execute, "tf": self.total_frontend, "tb": self.total_backend,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Aggregate":
        agg = cls()
        agg.tus = [TuTiming(tu=t[0], execute_us=t[1], frontend_us=t[2], backend_us=t[3])
                   for t in d.get("tus", [])]
        agg.header_us = defaultdict(int, d.get("header_us", {}))
        agg.header_tus = defaultdict(int, d.get("header_tus", {}))
        agg.template_us = defaultdict(int, d.get("template_us", {}))
        agg.template_count = defaultdict(int, d.get("template_count", {}))
        agg.file_us = defaultdict(int, d.get("file_us", {}))
        agg.file_tus = defaultdict(int, d.get("file_tus", {}))
        agg.total_execute = d.get("te", 0)
        agg.total_frontend = d.get("tf", 0)
        agg.total_backend = d.get("tb", 0)
        return agg


def event_histogram(trace_dir: Path, sample: int = 300) -> Dict[str, Tuple[int, int]]:
    """Count event names (and summed dur) across up to ``sample`` traces.

    Diagnostic: reveals what duration events clang actually emitted —
    e.g. whether per-file ``Source`` events are present, and what the
    file-parse events are named. Use this when hot_headers.csv is empty.
    """
    import json as _json
    hist: Dict[str, List[int]] = {}
    n = 0
    for p in sorted(trace_dir.glob("*.json")):
        if n >= sample:
            break
        n += 1
        try:
            with p.open("r", encoding="utf-8") as fh:
                data = _json.load(fh)
        except (OSError, ValueError):
            continue
        for e in data.get("traceEvents") or []:
            if e.get("ph") != "X":
                continue
            name = e.get("name") or "?"
            slot = hist.setdefault(name, [0, 0])
            slot[0] += 1
            slot[1] += e.get("dur") or 0
    return {k: (v[0], v[1]) for k, v in hist.items()}


def _read_src_sidecar(trace_path: Path) -> Optional[str]:
    side = trace_path.with_suffix(".src")
    try:
        return side.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _reduce_chunk(args) -> dict:
    """Worker: parse a chunk of trace files and return a merged compact dict.

    Chunking keeps IPC small (one merged result per chunk instead of per
    file) and bounds memory (per-TU event lists are discarded after
    folding into the chunk's dicts).
    """
    paths, repo_root_str = args
    repo_root = Path(repo_root_str) if repo_root_str else None

    tus: List[Tuple[str, int, int, int]] = []
    header_us: Dict[str, int] = {}
    header_tus: Dict[str, int] = {}
    template_us: Dict[str, int] = {}
    template_count: Dict[str, int] = {}
    file_us: Dict[str, int] = {}
    file_tus: Dict[str, int] = {}
    te = tf = tb = 0

    for ps in paths:
        p = Path(ps)
        tu_name = _read_src_sidecar(p)
        if tu_name and repo_root is not None:
            try:
                tu_name = str(Path(tu_name).resolve().relative_to(repo_root))
            except ValueError:
                pass
        t = parse_trace(p, tu_name=tu_name)
        if t is None:
            continue
        tus.append((t.tu, t.execute_us, t.frontend_us, t.backend_us))
        te += t.execute_us
        tf += t.frontend_us
        tb += t.backend_us
        for d, u in t.source_us.items():
            header_us[d] = header_us.get(d, 0) + u
            header_tus[d] = header_tus.get(d, 0) + 1
        for d, u in t.template_us.items():
            template_us[d] = template_us.get(d, 0) + u
            template_count[d] = template_count.get(d, 0) + 1
        for d, u in t.file_us.items():
            file_us[d] = file_us.get(d, 0) + u
            file_tus[d] = file_tus.get(d, 0) + 1

    return {
        "tus": tus, "header_us": header_us, "header_tus": header_tus,
        "template_us": template_us, "template_count": template_count,
        "file_us": file_us, "file_tus": file_tus,
        "te": te, "tf": tf, "tb": tb,
    }


def _chunks(lst: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        n = 1
    size = max(1, (len(lst) + n - 1) // n)
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def _dir_signature(files: List[Path]) -> str:
    import hashlib
    h = hashlib.sha1()
    h.update(str(len(files)).encode())
    total = 0
    latest = 0.0
    for p in files:
        try:
            st = p.stat()
        except OSError:
            continue
        total += st.st_size
        latest = max(latest, st.st_mtime)
    h.update(f"|{total}|{latest}|{_AGG_CACHE_VERSION}".encode())
    return h.hexdigest()


def collect_traces(
    trace_dir: Path,
    repo_root: Optional[Path] = None,
    jobs: int = 0,
    use_cache: bool = True,
    cache_path: Optional[Path] = None,
) -> Aggregate:
    """Parse all traces in ``trace_dir`` into an Aggregate, in parallel.

    A merged-result cache keyed by a cheap directory signature (file
    count + total size + newest mtime) makes re-runs instant. The first
    run parses in parallel with progress logging.
    """
    files = sorted(trace_dir.glob("*.json"))
    if not files:
        log.warning("no trace files in %s", trace_dir)
        return Aggregate()

    sig = _dir_signature(files)
    if cache_path is None:
        cache_path = trace_dir.parent / "timing_aggregate.json"
    if use_cache and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("signature") == sig and cached.get("version") == _AGG_CACHE_VERSION:
                log.info("loaded cached aggregate (%d TUs) from %s",
                         len(cached.get("agg", {}).get("tus", [])), cache_path)
                return Aggregate.from_dict(cached["agg"])
        except (OSError, ValueError):
            pass

    if jobs <= 0:
        jobs = max(1, multiprocessing.cpu_count() - 1)
    n_chunks = max(jobs, min(len(files), jobs * 4))
    chunks = _chunks([str(p) for p in files], n_chunks)
    repo_root_str = str(repo_root) if repo_root else None

    log.info("parsing %d trace files in %d chunks across %d workers",
             len(files), len(chunks), jobs)
    agg = Aggregate()
    t0 = time.time()
    done_files = 0
    done_chunks = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as ex:
        futures = [ex.submit(_reduce_chunk, (c, repo_root_str)) for c in chunks]
        for fut in concurrent.futures.as_completed(futures):
            part = fut.result()
            agg.merge_partial(part)
            done_chunks += 1
            done_files += len(part["tus"])
            rate = done_files / max(0.001, time.time() - t0)
            log.info("merged chunk %d/%d (%d TUs, %.0f TU/s)",
                     done_chunks, len(chunks), done_files, rate)

    if use_cache:
        try:
            tmp = cache_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({"version": _AGG_CACHE_VERSION,
                                       "signature": sig, "agg": agg.to_dict()}),
                           encoding="utf-8")
            os.replace(tmp, cache_path)
            log.info("cached aggregate to %s (re-runs will be instant)", cache_path)
        except OSError as e:
            log.warning("could not write aggregate cache: %s", e)

    return agg


def write_reports(agg: Aggregate, out_dir: Path, top: int = 100,
                  trace_dir: Optional[Path] = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Per-TU CSV, sorted by total descending.
    per_tu = sorted(agg.tus, key=lambda t: -t.execute_us)
    with (out_dir / "per_tu.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["tu", "total_s", "frontend_s", "backend_s", "frontend_pct"])
        for t in per_tu:
            pct = (100.0 * t.frontend_us / t.execute_us) if t.execute_us else 0.0
            w.writerow([t.tu, f"{_us(t.execute_us):.3f}", f"{_us(t.frontend_us):.3f}",
                        f"{_us(t.backend_us):.3f}", f"{pct:.1f}"])

    # Hot headers CSV: cumulative inclusive parse time across the build.
    # Prefer "Source" events; fall back to location-derived file activity
    # when clang emitted no Source events (e.g. coarse granularity).
    using_fallback = not agg.header_us and bool(agg.file_us)
    src_us = dict(agg.file_us) if using_fallback else dict(agg.header_us)
    src_tus = dict(agg.file_tus) if using_fallback else dict(agg.header_tus)
    hot_headers = sorted(src_us.items(), key=lambda kv: -kv[1])
    header_col = "file" if using_fallback else "header"
    with (out_dir / "hot_headers.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([header_col, "total_parse_s", "num_tus", "avg_per_tu_ms",
                    "source" if using_fallback else "kind"])
        kind = "location-fallback" if using_fallback else "Source"
        for detail, dur in hot_headers:
            n = src_tus.get(detail, 1)
            w.writerow([detail, f"{_us(dur):.3f}", n, f"{(dur / n) / 1000.0:.2f}", kind])
    if using_fallback:
        log.warning("no 'Source' events in traces; hot_headers.csv derived "
                    "from location-tagged parse events (re-collect with finer "
                    "--granularity for true per-header Source timing)")

    # Hot templates CSV.
    hot_templates = sorted(agg.template_us.items(), key=lambda kv: -kv[1])
    with (out_dir / "hot_templates.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["template", "total_s", "instantiations"])
        for detail, dur in hot_templates:
            w.writerow([detail, f"{_us(dur):.3f}", agg.template_count.get(detail, 0)])

    # Template cost rolled up by originating library/module.
    module_us: Dict[str, int] = defaultdict(int)
    module_cnt: Dict[str, int] = defaultdict(int)
    for detail, dur in agg.template_us.items():
        m = template_module(detail)
        module_us[m] += dur
        module_cnt[m] += agg.template_count.get(detail, 0)
    by_module = sorted(module_us.items(), key=lambda kv: -kv[1])
    with (out_dir / "template_modules.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["module", "total_s", "instantiations"])
        for m, dur in by_module:
            w.writerow([m, f"{_us(dur):.3f}", module_cnt.get(m, 0)])

    # Always emit an event-name histogram so the report itself reveals
    # what the traces contain (and why header data may be missing).
    if trace_dir is not None:
        try:
            hist = event_histogram(trace_dir)
            with (out_dir / "event_histogram.csv").open("w", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["event_name", "count", "total_s"])
                for name, (cnt, dur) in sorted(hist.items(), key=lambda kv: -kv[1][1]):
                    w.writerow([name, cnt, f"{_us(dur):.3f}"])
        except Exception as e:  # diagnostics must never fail the report
            log.warning("could not write event_histogram.csv: %s", e)

    _write_summary(agg, out_dir / "summary.md", per_tu, hot_headers,
                   hot_templates, by_module, module_cnt, src_tus,
                   using_fallback, top)


def _write_summary(
    agg: Aggregate,
    path: Path,
    per_tu: List[TuTiming],
    hot_headers: List[Tuple[str, int]],
    hot_templates: List[Tuple[str, int]],
    by_module: List[Tuple[str, int]],
    module_cnt: Dict[str, int],
    src_tus: Dict[str, int],
    using_fallback: bool,
    top: int,
) -> None:
    lines: List[str] = []
    lines.append("# Build timing report (clang -ftime-trace)")
    lines.append("")
    lines.append(f"- translation units analyzed: **{len(agg.tus)}**")
    lines.append(f"- summed compile time (ExecuteCompiler): **{_us(agg.total_execute):.1f} s**")
    fe, be = agg.total_frontend, agg.total_backend
    denom = (fe + be) or 1
    lines.append(f"- frontend (parse+sema): **{_us(fe):.1f} s** "
                 f"({100.0 * fe / denom:.0f}%)")
    lines.append(f"- backend (codegen+opt): **{_us(be):.1f} s** "
                 f"({100.0 * be / denom:.0f}%)")
    lines.append("")
    lines.append("> If backend dominates, include cleanup will NOT speed up "
                 "the build much — the cost is in codegen/optimization, not "
                 "parsing. If frontend dominates and a few headers top the "
                 "list below, slimming/moving those headers is high-leverage.")
    lines.append("")

    lines.append(f"## Top {min(top, len(per_tu))} slowest TUs")
    lines.append("")
    lines.append("| TU | total s | frontend s | backend s | front% |")
    lines.append("|---|---:|---:|---:|---:|")
    for t in per_tu[:top]:
        pct = (100.0 * t.frontend_us / t.execute_us) if t.execute_us else 0.0
        lines.append(f"| `{t.tu}` | {_us(t.execute_us):.2f} | "
                     f"{_us(t.frontend_us):.2f} | {_us(t.backend_us):.2f} | {pct:.0f} |")
    lines.append("")

    total_template_us = sum(d for _, d in by_module)
    if by_module:
        lines.append("## Template instantiation cost by module")
        lines.append("")
        lines.append("Relative distribution of template-instantiation cost. "
                     "NOTE: these durations are *inclusive* (a template's "
                     "instantiation includes nested instantiations it "
                     "triggers), so they overlap and the absolute sum can "
                     "exceed total frontend time — read the **shares**, not "
                     "the absolute seconds. This is per-TU work redone in "
                     "every TU that instantiates the template; the lever is to "
                     "stop instantiating it widely (extern template / explicit "
                     "instantiation in one .cpp, or stop including the header "
                     "that triggers it).")
        lines.append("")
        lines.append("| module | inclusive s | share | instantiations |")
        lines.append("|---|---:|---:|---:|")
        for m, dur in by_module:
            share = 100.0 * dur / (total_template_us or 1)
            lines.append(f"| {m} | {_us(dur):.1f} | {share:.0f}% | {module_cnt.get(m, 0)} |")
        lines.append("")

    if not hot_headers:
        lines.append("## Headers by parse time — NO DATA")
        lines.append("")
        lines.append("No per-file `Source` events and no location-tagged parse "
                     "events were found in the traces, so header parse time "
                     "could not be attributed. See `event_histogram.csv` for "
                     "what the traces actually contain, then re-collect with "
                     "finer granularity:")
        lines.append("")
        lines.append("```")
        lines.append("sanitize_includes timetrace --granularity 100 -- ./ya make <target> --rebuild")
        lines.append("sanitize_includes timing")
        lines.append("```")
        lines.append("")
    else:
        if using_fallback:
            lines.append(f"## Top {min(top, len(hot_headers))} files by frontend cost "
                         "(location-attributed)")
            lines.append("")
            lines.append("This toolchain's clang does not emit per-file `Source` "
                         "events, so per-file cost is attributed from "
                         "location-tagged events (`ParseDeclarationOrFunction"
                         "Definition`, `Evaluate*`, etc. with a "
                         "`file:line:col` detail) — i.e. time spent parsing "
                         "declarations and evaluating constexpr in each file. "
                         "This is a strong per-header frontend-cost signal "
                         "(headers with heavy templates/constexpr rise to the "
                         "top) and is the right ranking to target for "
                         "slimming / moving code out of widely-included "
                         "headers. See `event_histogram.csv` for the event mix.")
        else:
            lines.append(f"## Top {min(top, len(hot_headers))} headers by cumulative parse time")
            lines.append("")
            lines.append("Inclusive parse time summed across every TU that "
                         "includes the header — the highest-leverage targets "
                         "for slimming / forward-declaring / moving out of "
                         "widely-included headers.")
        lines.append("")
        lines.append("| file | total s | #TUs | avg ms/TU |")
        lines.append("|---|---:|---:|---:|")
        for detail, dur in hot_headers[:top]:
            n = src_tus.get(detail, 1)
            lines.append(f"| `{detail}` | {_us(dur):.2f} | {n} | {(dur / n) / 1000.0:.1f} |")
        lines.append("")

    if hot_templates:
        lines.append(f"## Top {min(top, len(hot_templates))} template instantiations")
        lines.append("")
        lines.append("| template | total s | instantiations |")
        lines.append("|---|---:|---:|")
        for detail, dur in hot_templates[:top]:
            lines.append(f"| `{detail}` | {_us(dur):.2f} | "
                         f"{agg.template_count.get(detail, 0)} |")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
