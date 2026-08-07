"""Load and validate perf-duty-context/v1 packs (olap + tpcc).

Supports plain ``*.json`` or Save-context zip bundles
(``context.json`` + optional ``sandbox/focus/index.html``).
"""

from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA = "perf-duty-context/v1"
KINDS = frozenset({"olap", "tpcc"})
CONTEXT_JSON_NAMES = ("context.json",)


class ContextError(ValueError):
    """Invalid or unsupported duty context pack."""


@dataclass
class LoadedContext:
    ctx: dict[str, Any]
    """Directory used to resolve ``report_local`` (json parent or zip extract root)."""
    base_dir: Path
    source_path: Path
    cleanup_dir: Path | None = None  # temp extract dir to remove later

    def close(self) -> None:
        if self.cleanup_dir is not None:
            shutil.rmtree(self.cleanup_dir, ignore_errors=True)
            self.cleanup_dir = None


def load_context(path: Path | str) -> dict[str, Any]:
    """Backward-compatible: return context dict only (no zip extract cleanup)."""
    loaded = load_context_pack(path)
    # Keep extract alive for process lifetime if zip — caller should use load_context_pack.
    return loaded.ctx


def load_context_pack(path: Path | str) -> LoadedContext:
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        raise ContextError(f"context file not found: {p}")
    if p.suffix.lower() == ".zip":
        return _load_zip(p)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ContextError(f"invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ContextError("context root must be an object")
    validate_context(data)
    return LoadedContext(ctx=data, base_dir=p.parent, source_path=p, cleanup_dir=None)


def _load_zip(path: Path) -> LoadedContext:
    tmp = Path(tempfile.mkdtemp(prefix="perf-duty-"))
    try:
        with zipfile.ZipFile(path, "r") as zf:
            zf.extractall(tmp)
        ctx_path = _find_context_json(tmp)
        if ctx_path is None:
            raise ContextError("zip missing context.json")
        data = json.loads(ctx_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ContextError("context root must be an object")
        validate_context(data)
        # Prefer directory that contains context.json (handles nested zip layout).
        return LoadedContext(
            ctx=data,
            base_dir=ctx_path.parent,
            source_path=path,
            cleanup_dir=tmp,
        )
    except ContextError:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    except zipfile.BadZipFile as e:
        shutil.rmtree(tmp, ignore_errors=True)
        raise ContextError(f"invalid zip: {e}") from e
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise


def _find_context_json(root: Path) -> Path | None:
    for name in CONTEXT_JSON_NAMES:
        direct = root / name
        if direct.is_file():
            return direct
    for p in sorted(root.rglob("context.json")):
        if p.is_file():
            return p
    return None


def validate_context(data: dict[str, Any]) -> None:
    schema = data.get("schema")
    if schema != SCHEMA:
        raise ContextError(f"unsupported schema {schema!r}; want {SCHEMA!r}")
    report = data.get("report") or {}
    if not isinstance(report, dict):
        raise ContextError("report must be an object")
    kind = report.get("kind")
    if kind not in KINDS:
        raise ContextError(f"report.kind must be one of {sorted(KINDS)}; got {kind!r}")
    sel = data.get("selection") or {}
    if not isinstance(sel, dict):
        raise ContextError("selection must be an object")
    for key in ("branch", "db", "suite"):
        if not sel.get(key):
            raise ContextError(f"selection.{key} is required")


def kind_of(ctx: dict[str, Any]) -> str:
    return str((ctx.get("report") or {}).get("kind") or "")


def focus_report_url(ctx: dict[str, Any]) -> str | None:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    url = fr.get("report") if isinstance(fr, dict) else None
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


def focus_report_local(ctx: dict[str, Any], base_dir: Path | None) -> Path | None:
    """Resolve ``selection.focus_run.report_local`` under base_dir (zip root / json dir)."""
    if base_dir is None:
        return None
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    if not isinstance(fr, dict):
        return None
    rel = fr.get("report_local")
    if not isinstance(rel, str) or not rel.strip():
        # fallback: conventional Save-context layout
        cand = base_dir / "sandbox" / "focus" / "index.html"
        if cand.is_file():
            return cand.resolve()
        return None
    rel = rel.strip().lstrip("/")
    if ".." in Path(rel).parts:
        raise ContextError(f"unsafe report_local path: {rel!r}")
    path = (base_dir / rel).resolve()
    try:
        path.relative_to(base_dir.resolve())
    except ValueError as e:
        raise ContextError(f"report_local escapes base dir: {rel!r}") from e
    if path.is_file():
        return path
    return None


def selection_summary(ctx: dict[str, Any]) -> str:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    bits = [
        kind_of(ctx),
        str(sel.get("branch") or ""),
        str(sel.get("db") or ""),
        str(sel.get("suite") or ""),
    ]
    if isinstance(fr, dict):
        label = fr.get("label") or fr.get("day") or fr.get("sha")
        if label:
            bits.append(str(label))
    return " · ".join(b for b in bits if b)
