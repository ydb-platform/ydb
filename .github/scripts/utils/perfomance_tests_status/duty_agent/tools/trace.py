"""Duty investigation action tree — persist + render under <details> for analysis.md."""

from __future__ import annotations

import json
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id(prefix: str = "n") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


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


def rebuild_from_artifacts(out_dir: Path) -> dict[str, Any]:
    """Synthesize a tree from known duty artifacts (even if live trace was sparse)."""
    tree = load_tree(out_dir)
    # Keep live nodes; append an "Артефакты" branch summarizing files.
    art = add_node(
        tree,
        title="Сводка по артефактам",
        kind="artifacts",
        status="ok",
        detail="автосборка из файлов run dir",
        node_id="artifacts_rollup",
    )
    # wipe previous auto children if re-run
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
        return f"types={d.get('analysis_types')}"

    def _fmt_focus(d: dict[str, Any]) -> str:
        fatal = d.get("fatal") or {}
        return (
            f"fetched={d.get('fetched')} signals={fatal.get('signals')} "
            f"slow={d.get('slow_query_names')}"
        )

    def _fmt_metrics(d: dict[str, Any]) -> str:
        return f"flags={d.get('flags')}"

    def _fmt_dig_runs(d: dict[str, Any]) -> str:
        s = d.get("summary") or {}
        bc = s.get("baseline_candidate") or {}
        return f"slice={s.get('slice_count')} baseline={bc.get('reason')}"

    def _fmt_baseline(d: dict[str, Any]) -> str:
        comps = (d.get("plan_compare") or {}).get("comparisons") or []
        return f"fetched={d.get('fetched')} compare={len(comps)}"

    def _fmt_prs(d: dict[str, Any]) -> str:
        hot = d.get("hot_prs") or d.get("prs") or []
        b = str(d.get("base_sha") or "")[:7]
        h = str(d.get("head_sha") or "")[:7]
        return f"window={b}..{h} hot={len(hot)}"

    def _fmt_bisect(d: dict[str, Any]) -> str:
        return (
            f"introduced_in_window={d.get('introduced_in_window')} "
            f"path={d.get('path')}"
        )

    def _fmt_priors(d: dict[str, Any]) -> str:
        return f"same_class={d.get('same_class_before')}"

    def _fmt_problems(d: Any) -> str:
        items = d if isinstance(d, list) else (d.get("items") or [])
        return f"items={len(items)}"

    def _fmt_validate(d: dict[str, Any]) -> str:
        errs = d.get("errors") or []
        return f"ok={d.get('ok')} errors={len(errs)}"

    def _fmt_result(d: dict[str, Any]) -> str:
        return f"status={d.get('status')} resolution={d.get('resolution')}"

    mapping: list[tuple[str, str, Any]] = [
        ("detect_type.json", "тип разбора", _fmt_detect),
        ("focus.json", "Allure разбираемого прогона", _fmt_focus),
        ("metrics_delta.json", "метрики suite", _fmt_metrics),
        ("dig_runs.json", "история mart", _fmt_dig_runs),
        ("baseline_focus.json", "Allure baseline", _fmt_baseline),
        ("dig_prs.json", "PR в окне jump", _fmt_prs),
        ("code_bisect.json", "окно кода", _fmt_bisect),
        ("priors.json", "прошлые Allure", _fmt_priors),
        ("problems.json", "problems.json", _fmt_problems),
        ("analysis.md", "analysis.md", lambda _d: "written"),
        ("validate.json", "validate", _fmt_validate),
        ("result.json", "result", _fmt_result),
    ]
    for fname, title, fmt in mapping:
        p = out_dir / fname
        if not p.is_file():
            child(f"{title} — нет", status="missing")
            continue
        if fname.endswith(".md"):
            child(title, f"{p.stat().st_size} bytes")
            continue
        try:
            data = read_json(p)
            child(title, fmt(data) if isinstance(data, dict) else str(type(data)))
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as e:
            child(title, f"read error: {e}", status="error")

    # plan_compare verdicts as subtree
    bf = out_dir / "baseline_focus.json"
    if bf.is_file():
        try:
            blob = read_json(bf)
            comps = (blob.get("plan_compare") or {}).get("comparisons") or []
            if comps:
                pc = {
                    "id": _new_id("pc"),
                    "title": "plan_compare",
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
                                f"focus={c.get('focus_hints')} baseline={c.get('baseline_hints')}"
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

    def walk(nodes: list[dict[str, Any]], prefix: str = "", is_last_list: list[bool] | None = None) -> None:
        is_last_list = is_last_list or []
        for i, n in enumerate(nodes):
            last = i == len(nodes) - 1
            branch = "└─ " if last else "├─ "
            pad = ""
            for is_last in is_last_list:
                pad += "   " if is_last else "│  "
            st = n.get("status") or ""
            st_s = f" [{st}]" if st and st not in ("ok", "running") else ""
            title = str(n.get("title") or "?")
            detail = n.get("detail")
            line = f"{pad}{branch}{title}{st_s}"
            if detail:
                line += f" — {detail}"
            lines.append(line)
            kids = list(n.get("children") or [])
            if kids:
                walk(kids, prefix, is_last_list + [last])

    roots = list(tree.get("nodes") or [])
    if not roots:
        return "(пусто)"
    for i, n in enumerate(roots):
        st = n.get("status") or ""
        st_s = f" [{st}]" if st and st not in ("ok",) else ""
        head = f"{n.get('title')}{st_s}"
        if n.get("detail"):
            head += f" — {n['detail']}"
        lines.append(head)
        kids = list(n.get("children") or [])
        if kids:
            walk(kids, "", [])
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
