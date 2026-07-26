"""Duty investigation action tree — persist + render under <details> for analysis.md."""

from __future__ import annotations

import json
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .run_dir import read_json, write_json

TRACE_FILE = "action_tree.json"
TRACE_MARK_START = "<!-- duty-action-tree:start -->"
TRACE_MARK_END = "<!-- duty-action-tree:end -->"
ARTIFACTS_NODE_ID = "artifacts_rollup"

# Human-facing titles for CLI stages (report is Russian; keep agent notes as written).
TITLE_RU: dict[str, str] = {
    "prepare": "Подготовка",
    "detect_type": "тип разбора",
    "focus / Allure": "Allure разбираемого прогона",
    "crash dig hints": "подсказки crash (coredump/journal)",
    "priors / history": "прошлые Allure",
    "metrics_delta": "метрики suite",
    "dig-runs": "История прогонов (mart)",
    "mart summarize": "сводка mart",
    "baseline_candidate": "кандидат baseline",
    "baseline_focus / plan_compare": "планы baseline",
    "dig-prs": "PR в окне кода",
    "bisect": "Проверка пути в коде",
    "Сводка по артефактам": "Сводка по артефактам",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id(prefix: str = "n") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _human_title(title: str | None) -> str:
    t = str(title or "?")
    return TITLE_RU.get(t, t)


def _fmt_val(v: Any) -> str:
    """Compact values for humans (no Python list/dict repr)."""
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "да" if v else "нет"
    if isinstance(v, (list, tuple)):
        if not v:
            return "—"
        return ", ".join(_fmt_val(x) for x in v[:12])
    if isinstance(v, dict):
        return ",".join(f"{k}={_fmt_val(val)}" for k, val in list(v.items())[:6])
    s = str(v).strip()
    return s if s else "—"


def _short_sha(v: Any) -> str:
    s = str(v or "").strip()
    for prefix in ("main.", "origin/main.", "trunk."):
        if s.startswith(prefix):
            s = s[len(prefix) :]
            break
    return s[:7] if s else ""


def _short_path(path: str | None) -> str:
    if not path:
        return "—"
    p = str(path)
    return p.rsplit("/", 1)[-1] if "/" in p else p


def empty_tree(*, run_dir: str | None = None) -> dict[str, Any]:
    return {
        "version": 1,
        "run_dir": run_dir,
        "started_at": _now_iso(),
        "updated_at": _now_iso(),
        "nodes": [],
    }


def load_tree(out_dir: Path) -> dict[str, Any]:
    path = out_dir / TRACE_FILE
    if path.is_file():
        try:
            data = read_json(path)
            if isinstance(data, dict) and isinstance(data.get("nodes"), list):
                return data
        except (OSError, json.JSONDecodeError):
            pass
    return empty_tree(run_dir=str(out_dir))


def save_tree(out_dir: Path, tree: dict[str, Any]) -> Path:
    tree["updated_at"] = _now_iso()
    tree["run_dir"] = str(out_dir)
    path = out_dir / TRACE_FILE
    write_json(path, tree)
    return path


def _find_node(nodes: list[dict[str, Any]], node_id: str) -> dict[str, Any] | None:
    for n in nodes:
        if n.get("id") == node_id:
            return n
        found = _find_node(list(n.get("children") or []), node_id)
        if found:
            return found
    return None


def add_node(
    tree: dict[str, Any],
    *,
    title: str,
    parent_id: str | None = None,
    node_id: str | None = None,
    status: str = "ok",
    detail: str | None = None,
    kind: str = "step",
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    node = {
        "id": node_id or _new_id(kind[:4] if kind else "n"),
        "title": title,
        "kind": kind,
        "status": status,
        "detail": (detail or "")[:800] or None,
        "meta": meta or {},
        "ts": _now_iso(),
        "children": [],
    }
    if parent_id:
        parent = _find_node(list(tree.get("nodes") or []), parent_id)
        if parent is not None:
            parent.setdefault("children", []).append(node)
            return node
    tree.setdefault("nodes", []).append(node)
    return node


def record(
    out_dir: Path,
    title: str,
    *,
    parent_id: str | None = None,
    status: str = "ok",
    detail: str | None = None,
    kind: str = "step",
    meta: dict[str, Any] | None = None,
    node_id: str | None = None,
) -> dict[str, Any]:
    """Append one node to action_tree.json and return the node."""
    tree = load_tree(out_dir)
    node = add_node(
        tree,
        title=title,
        parent_id=parent_id,
        node_id=node_id,
        status=status,
        detail=detail,
        kind=kind,
        meta=meta,
    )
    save_tree(out_dir, tree)
    return node


@contextmanager
def span(
    out_dir: Path,
    title: str,
    *,
    parent_id: str | None = None,
    kind: str = "stage",
    detail: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Context manager that records a stage and updates status on exit."""
    tree = load_tree(out_dir)
    node = add_node(
        tree,
        title=title,
        parent_id=parent_id,
        kind=kind,
        status="running",
        detail=detail,
    )
    save_tree(out_dir, tree)
    t0 = time.time()
    try:
        yield node
        tree = load_tree(out_dir)
        cur = _find_node(list(tree.get("nodes") or []), str(node["id"]))
        if cur is not None:
            cur["status"] = "ok"
            cur["meta"] = {**(cur.get("meta") or {}), "elapsed_sec": round(time.time() - t0, 2)}
        save_tree(out_dir, tree)
    except Exception as e:  # noqa: BLE001
        tree = load_tree(out_dir)
        cur = _find_node(list(tree.get("nodes") or []), str(node["id"]))
        if cur is not None:
            cur["status"] = "error"
            cur["detail"] = (str(e))[:400]
            cur["meta"] = {**(cur.get("meta") or {}), "elapsed_sec": round(time.time() - t0, 2)}
        save_tree(out_dir, tree)
        raise


# Root CLI stages — on re-run keep only the latest node of each title.
DEDUPE_ROOT_TITLES = frozenset(
    {
        "prepare",
        "dig-runs",
        "dig-prs",
        "bisect",
    }
)


def _strip_artifact_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop auto rollups (including historical duplicates with same/different ids)."""
    keep: list[dict[str, Any]] = []
    for n in nodes:
        if n.get("id") == ARTIFACTS_NODE_ID or n.get("kind") == "artifacts":
            continue
        keep.append(n)
    return keep


def _dedupe_root_stages(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep a single latest prepare/dig-runs/dig-prs/bisect (re-runs append otherwise)."""
    last_i: dict[str, int] = {}
    for i, n in enumerate(nodes):
        t = str(n.get("title") or "")
        if t in DEDUPE_ROOT_TITLES:
            last_i[t] = i
    if not last_i:
        return nodes
    out: list[dict[str, Any]] = []
    for i, n in enumerate(nodes):
        t = str(n.get("title") or "")
        if t in DEDUPE_ROOT_TITLES and last_i.get(t) != i:
            continue
        out.append(n)
    return out


def _detail_is_placeholder(detail: Any) -> bool:
    s = str(detail or "").strip()
    if not s:
        return True
    return bool(
        re.search(
            r"path\s*=\s*None|path\s*=\s*—|window=\.\.|окно\s*—\s*$|introduced_in_window=None",
            s,
            re.I,
        )
    )


def _sync_live_stages_from_files(tree: dict[str, Any], out_dir: Path) -> None:
    """Refresh stage details from JSON (fixes stale path=None / Python-repr / empty windows)."""
    files: dict[str, Any] = {}
    for name in (
        "dig_prs.json",
        "code_bisect.json",
        "dig_runs.json",
        "detect_type.json",
        "focus.json",
        "metrics_delta.json",
        "priors.json",
    ):
        p = out_dir / name
        if p.is_file():
            try:
                files[name] = read_json(p)
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                pass

    dig_prs = files.get("dig_prs.json")
    bis = files.get("code_bisect.json")
    dig_runs = files.get("dig_runs.json")
    detect = files.get("detect_type.json")
    focus = files.get("focus.json")
    metrics = files.get("metrics_delta.json")
    priors = files.get("priors.json")

    nodes = list(tree.get("nodes") or [])
    # Collapse consecutive placeholder bisect stages into one synced from file.
    if bis and isinstance(bis, dict):
        paths = list(bis.get("paths") or [])
        path = bis.get("path") or (paths[0] if paths else None)
        w = bis.get("window") or {}
        changed = bis.get("introduced_in_window")
        if changed is True:
            ch = "менялся в окне"
        elif changed is False:
            ch = "не менялся в окне"
        else:
            ch = "окно не проверено"
        wb, wh = _short_sha(w.get("base")), _short_sha(w.get("head"))
        win = f"{wb}…{wh}" if wb or wh else "—"
        new_detail = f"{_short_path(path)} — {ch}; окно {win}"
        bisect_idxs = [i for i, n in enumerate(nodes) if n.get("title") == "bisect"]
        if bisect_idxs:
            keep_i = bisect_idxs[-1]
            nodes[keep_i]["detail"] = new_detail
            drop = {i for i in bisect_idxs[:-1] if _detail_is_placeholder(nodes[i].get("detail"))}
            if drop:
                nodes = [n for i, n in enumerate(nodes) if i not in drop]

    for n in nodes:
        title = n.get("title")
        if title == "prepare":
            for c in n.get("children") or []:
                ct = c.get("title")
                if ct == "detect_type" and isinstance(detect, dict):
                    c["detail"] = (
                        f"типы: {_fmt_val(detect.get('analysis_types'))}; "
                        f"rollup={_fmt_val(detect.get('rollup'))}"
                    )
                elif ct == "focus / Allure" and isinstance(focus, dict):
                    fatal = focus.get("fatal") or {}
                    c["detail"] = (
                        f"скачан={_fmt_val(focus.get('fetched'))}; "
                        f"сигналы: {_fmt_val(fatal.get('signals'))}; "
                        f"slow: {_fmt_val(focus.get('slow_query_names'))}"
                    )
                elif ct == "crash dig hints" and isinstance(focus, dict):
                    fatal = focus.get("fatal") or {}
                    c["detail"] = (
                        f"coredump-ссылок={len(fatal.get('coredump_urls') or [])}; "
                        f"journal-рецептов={len(fatal.get('journal_cmds') or [])}"
                    )
                elif ct == "priors / history" and isinstance(priors, dict):
                    c["detail"] = (
                        f"сканов={len(priors.get('prior_scans') or [])}; "
                        f"same_class={_fmt_val(priors.get('same_class_before'))}"
                    )
                elif ct == "metrics_delta" and isinstance(metrics, dict):
                    c["detail"] = f"флаги: {_fmt_val(metrics.get('flags'))}"
        if title == "dig-prs" and isinstance(dig_prs, dict):
            hot = dig_prs.get("hot_prs") or dig_prs.get("prs") or []
            b = _short_sha(dig_prs.get("base") or dig_prs.get("base_sha"))
            h = _short_sha(dig_prs.get("head") or dig_prs.get("head_sha"))
            n_prod = len(dig_prs.get("product_prs") or [])
            win = f"{b}…{h}" if b or h else "—"
            src = dig_prs.get("window_source")
            src_bit = f"; source={src}" if src else ""
            n["detail"] = (
                f"окно {win}; product PR={n_prod}; горячих={len(hot)}{src_bit}"
            )
        if title == "dig-runs" and isinstance(dig_runs, dict):
            s = dig_runs.get("summary") or {}
            fail = s.get("largest_fail_step") or {}
            ydb = s.get("largest_ydb_step") or {}
            bc = s.get("baseline_candidate") or {}
            bits = [f"срезов={_fmt_val(s.get('slice_count'))}"]
            if fail.get("to_version") or fail.get("delta"):
                bits.append(
                    f"fail↑ {_short_sha(fail.get('from_version'))}→{_short_sha(fail.get('to_version'))}"
                )
            if ydb.get("to_version") or ydb.get("delta"):
                bits.append(
                    f"ydb↑ {_short_sha(ydb.get('from_version'))}→{_short_sha(ydb.get('to_version'))}"
                )
            n["detail"] = "; ".join(bits)
            for c in n.get("children") or []:
                if c.get("title") == "mart summarize":
                    c["detail"] = (
                        f"строк={_fmt_val(s.get('row_count'))}; "
                        f"срезов={_fmt_val(s.get('slice_count'))}"
                        + (f"; {'; '.join(bits[1:])}" if len(bits) > 1 else "")
                    )
                elif c.get("title") == "baseline_candidate" and bc:
                    c["detail"] = (
                        f"{bc.get('reason')}; Version={bc.get('Version')}; "
                        f"metric={bc.get('metric_value')}; "
                        f"report={'да' if bc.get('Report') else 'нет'}"
                    )
    tree["nodes"] = nodes


def rebuild_from_artifacts(out_dir: Path) -> dict[str, Any]:
    """Keep live dig nodes; replace a single artifacts rollup (never append duplicates)."""
    tree = load_tree(out_dir)
    tree["nodes"] = _dedupe_root_stages(
        _strip_artifact_nodes(list(tree.get("nodes") or []))
    )
    _sync_live_stages_from_files(tree, out_dir)
    art = add_node(
        tree,
        title="Сводка по артефактам",
        kind="artifacts",
        status="ok",
        detail="итог файлов run dir (одна сводка)",
        node_id=ARTIFACTS_NODE_ID,
    )
    art["children"] = []

    def child(title: str, detail: str | None = None, status: str = "ok") -> None:
        art["children"].append(
            {
                "id": _new_id("art"),
                "title": title,
                "kind": "artifact",
                "status": status,
                "detail": detail,
                "meta": {},
                "ts": _now_iso(),
                "children": [],
            }
        )

    def _fmt_detect(d: dict[str, Any]) -> str:
        return f"типы: {_fmt_val(d.get('analysis_types'))}; rollup={_fmt_val(d.get('rollup'))}"

    def _fmt_focus(d: dict[str, Any]) -> str:
        fatal = d.get("fatal") or {}
        return (
            f"скачан={_fmt_val(d.get('fetched'))}; "
            f"сигналы: {_fmt_val(fatal.get('signals'))}; "
            f"slow: {_fmt_val(d.get('slow_query_names'))}"
        )

    def _fmt_metrics(d: dict[str, Any]) -> str:
        return f"флаги: {_fmt_val(d.get('flags'))}"

    def _fmt_dig_runs(d: dict[str, Any]) -> str:
        s = d.get("summary") or {}
        bc = s.get("baseline_candidate") or {}
        fail = s.get("largest_fail_step") or {}
        ydb = s.get("largest_ydb_step") or {}
        pw = s.get("pr_window") or {}
        parts = [f"срезов={_fmt_val(s.get('slice_count'))}"]
        if pw.get("base") or pw.get("head"):
            parts.append(
                f"pr_window {_short_sha(pw.get('base'))}→{_short_sha(pw.get('head'))}"
                f" ({pw.get('source') or '?'})"
            )
        if fail.get("to_version") or fail.get("delta"):
            parts.append(
                f"fail↑ {_short_sha(fail.get('from_version'))}→{_short_sha(fail.get('to_version'))}"
            )
        if ydb.get("to_version") or ydb.get("delta"):
            parts.append(
                f"ydb↑ {_short_sha(ydb.get('from_version'))}→{_short_sha(ydb.get('to_version'))}"
            )
        if bc.get("reason"):
            parts.append(f"baseline={bc.get('reason')}")
        return "; ".join(parts)

    def _fmt_baseline(d: dict[str, Any]) -> str:
        comps = (d.get("plan_compare") or {}).get("comparisons") or []
        return f"скачан={_fmt_val(d.get('fetched'))}; сравнений планов={len(comps)}"

    def _fmt_prs(d: dict[str, Any]) -> str:
        hot = d.get("hot_prs") or d.get("prs") or []
        b = _short_sha(d.get("base") or d.get("base_sha"))
        h = _short_sha(d.get("head") or d.get("head_sha"))
        n_prod = len(d.get("product_prs") or [])
        win = f"{b}…{h}" if b or h else "—"
        src = d.get("window_source")
        src_bit = f"; source={src}" if src else ""
        return f"окно {win}; product PR={n_prod}; горячих={len(hot)}{src_bit}"

    def _fmt_bisect(d: dict[str, Any]) -> str:
        paths = list(d.get("paths") or [])
        path = d.get("path") or (paths[0] if paths else None)
        w = d.get("window") or {}
        b = _short_sha(w.get("base"))
        h = _short_sha(w.get("head"))
        changed = d.get("introduced_in_window")
        if changed is True:
            ch = "менялся в окне"
        elif changed is False:
            ch = "не менялся в окне"
        else:
            ch = "окно не проверено"
        extra = ""
        if len(paths) > 1:
            extra = f"; ещё путей: {len(paths) - 1}"
        win = f"{b}…{h}" if b or h else "—"
        return f"{_short_path(path)} — {ch}; окно {win}{extra}"

    def _fmt_priors(d: dict[str, Any]) -> str:
        n = len(d.get("prior_scans") or [])
        return f"сканов={n}; same_class={_fmt_val(d.get('same_class_before'))}"

    def _fmt_problems(d: Any) -> str:
        items = d if isinstance(d, list) else (d.get("items") or [])
        analyzed = sum(
            1
            for it in items
            if isinstance(it, dict) and str(it.get("status") or "") == "analyzed"
        )
        return f"проблем={len(items)}; разобрано={analyzed}"

    def _fmt_validate(d: dict[str, Any]) -> str:
        errs = d.get("errors") or []
        warns = d.get("warnings") or []
        return f"ok={_fmt_val(d.get('ok'))}; ошибок={len(errs)}; предупреждений={len(warns)}"

    def _fmt_result(d: dict[str, Any]) -> str:
        return (
            f"status={_fmt_val(d.get('status'))}; "
            f"решение={_fmt_val(d.get('resolution'))}"
        )

    mapping: list[tuple[str, str, Any]] = [
        ("detect_type.json", "тип разбора", _fmt_detect),
        ("focus.json", "Allure разбираемого прогона", _fmt_focus),
        ("metrics_delta.json", "метрики suite", _fmt_metrics),
        ("dig_runs.json", "история mart", _fmt_dig_runs),
        ("baseline_focus.json", "Allure baseline", _fmt_baseline),
        ("dig_prs.json", "PR в окне кода", _fmt_prs),
        ("code_bisect.json", "проверка пути в коде", _fmt_bisect),
        ("priors.json", "прошлые Allure", _fmt_priors),
        ("problems.json", "problems.json", _fmt_problems),
        ("analysis.md", "analysis.md", lambda _d: "есть"),
        ("validate.json", "validate", _fmt_validate),
        ("result.json", "result", _fmt_result),
    ]
    for fname, title, fmt in mapping:
        p = out_dir / fname
        if not p.is_file():
            # Optional for pure fail without baseline plans / metrics.
            if fname in ("baseline_focus.json", "metrics_delta.json"):
                child(f"{title} — нет (не нужен для этого типа)", status="skip")
            else:
                child(f"{title} — нет", status="missing")
            continue
        if fname.endswith(".md"):
            child(title, f"{p.stat().st_size} bytes")
            continue
        try:
            data = read_json(p)
            if isinstance(data, dict):
                child(title, fmt(data))
            elif isinstance(data, list):
                child(title, fmt(data))
            else:
                child(title, type(data).__name__)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as e:
            child(title, f"ошибка чтения: {e}", status="error")

    # plan_compare verdicts as subtree
    bf = out_dir / "baseline_focus.json"
    if bf.is_file():
        try:
            blob = read_json(bf)
            comps = (blob.get("plan_compare") or {}).get("comparisons") or []
            if comps:
                pc = {
                    "id": _new_id("pc"),
                    "title": "сравнение планов",
                    "kind": "compare",
                    "status": "ok",
                    "detail": f"{len(comps)} query",
                    "meta": {},
                    "ts": _now_iso(),
                    "children": [],
                }
                for c in comps[:12]:
                    pc["children"].append(
                        {
                            "id": _new_id("q"),
                            "title": f"{c.get('query')}: {c.get('verdict')}",
                            "kind": "compare",
                            "status": "ok",
                            "detail": (
                                f"разбираемый={_fmt_val(c.get('focus_hints'))}; "
                                f"baseline={_fmt_val(c.get('baseline_hints'))}"
                            )[:400],
                            "meta": {},
                            "ts": _now_iso(),
                            "children": [],
                        }
                    )
                art["children"].append(pc)
        except (OSError, json.JSONDecodeError):
            pass

    save_tree(out_dir, tree)
    return tree


def render_ascii_tree(tree: dict[str, Any]) -> str:
    lines: list[str] = []

    def walk(nodes: list[dict[str, Any]], is_last_list: list[bool] | None = None) -> None:
        is_last_list = is_last_list or []
        for i, n in enumerate(nodes):
            last = i == len(nodes) - 1
            branch = "└─ " if last else "├─ "
            pad = "".join("   " if is_last else "│  " for is_last in is_last_list)
            st = n.get("status") or ""
            st_s = f" [{st}]" if st and st not in ("ok", "running", "skip") else ""
            title = _human_title(n.get("title"))
            detail = n.get("detail")
            line = f"{pad}{branch}{title}{st_s}"
            if detail:
                line += f" — {detail}"
            lines.append(line)
            kids = list(n.get("children") or [])
            if kids:
                walk(kids, is_last_list + [last])

    roots = list(tree.get("nodes") or [])
    if not roots:
        return "(пусто)"
    for i, n in enumerate(roots):
        st = n.get("status") or ""
        st_s = f" [{st}]" if st and st not in ("ok", "skip") else ""
        head = f"{_human_title(n.get('title'))}{st_s}"
        if n.get("detail"):
            head += f" — {n['detail']}"
        lines.append(head)
        kids = list(n.get("children") or [])
        if kids:
            walk(kids, [])
        if i != len(roots) - 1:
            lines.append("")
    return "\n".join(lines)


def render_details_markdown(tree: dict[str, Any], *, summary: str = "Дерево разбора (от начала до конца)") -> str:
    """GitHub-friendly collapsible block for analysis.md."""
    ascii_tree = render_ascii_tree(tree)
    started = tree.get("started_at") or ""
    updated = tree.get("updated_at") or ""
    meta = []
    if started:
        meta.append(f"start `{started}`")
    if updated:
        meta.append(f"updated `{updated}`")
    meta_line = (" · ".join(meta) + "\n\n") if meta else "\n"
    body = (
        f"{TRACE_MARK_START}\n"
        f"<details>\n"
        f"<summary>{summary}</summary>\n\n"
        f"{meta_line}"
        f"```\n{ascii_tree}\n```\n\n"
        f"</details>\n"
        f"{TRACE_MARK_END}\n"
    )
    return body


def inject_into_analysis(
    analysis_md: str,
    tree: dict[str, Any],
    *,
    summary: str = "Дерево разбора (от начала до конца)",
) -> str:
    """Replace or append the details block in analysis.md."""
    block = render_details_markdown(tree, summary=summary)
    text = analysis_md or ""
    if TRACE_MARK_START in text and TRACE_MARK_END in text:
        pre = text.split(TRACE_MARK_START, 1)[0]
        post = text.split(TRACE_MARK_END, 1)[1]
        return pre.rstrip() + "\n\n" + block + post.lstrip("\n")
    # Prefer after Заключение / before Материалы — append at end with heading
    section = "\n## Ход разбора\n\n" + block
    if "## Материалы для issue" in text:
        return text.replace("## Материалы для issue", section + "## Материалы для issue", 1)
    if "## Что дальше" in text:
        # after Что дальше block is hard; append before it
        return text.replace("## Что дальше", section + "## Что дальше", 1)
    return text.rstrip() + "\n" + section


def ensure_trace_in_analysis(out_dir: Path, *, rebuild: bool = True) -> dict[str, Any]:
    """Rebuild artifacts rollup, inject into analysis.md if present."""
    if rebuild:
        tree = rebuild_from_artifacts(out_dir)
    else:
        tree = load_tree(out_dir)
    md_path = out_dir / "analysis.md"
    out: dict[str, Any] = {"tree_path": str(out_dir / TRACE_FILE), "injected": False}
    if md_path.is_file():
        text = md_path.read_text(encoding="utf-8")
        new_text = inject_into_analysis(text, tree)
        if new_text != text:
            md_path.write_text(new_text, encoding="utf-8")
            out["injected"] = True
        else:
            # still rewrite block content
            md_path.write_text(inject_into_analysis(text, tree), encoding="utf-8")
            out["injected"] = True
    out["ascii"] = render_ascii_tree(tree)
    return out
