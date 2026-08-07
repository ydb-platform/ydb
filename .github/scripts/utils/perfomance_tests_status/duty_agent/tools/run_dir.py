"""Run-directory helpers for dutyctl artifacts."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def make_run_id(ctx: dict[str, Any]) -> str:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    suite = str(sel.get("suite") or "suite")
    sha = str(fr.get("sha") or "nosha")[:12]
    label = str(fr.get("label") or fr.get("day") or sha)
    raw = f"{label}_{suite}"
    safe = re.sub(r"[^\w.\-@]+", "_", raw).strip("_")
    return safe[:120] or "duty-run"


def ensure_run_dir(out_dir: Path | str | None, ctx: dict[str, Any] | None = None) -> Path:
    if out_dir:
        d = Path(out_dir).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parents[1]
        rid = make_run_id(ctx or {})
        d = (root / rid).resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def context_stub(ctx: dict[str, Any]) -> dict[str, Any]:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    return {
        "kind": (ctx.get("report") or {}).get("kind"),
        "branch": sel.get("branch"),
        "db": sel.get("db"),
        "suite": sel.get("suite"),
        "focus_sha": fr.get("sha") if isinstance(fr, dict) else None,
        "focus_label": (
            (fr.get("label") or fr.get("day") or fr.get("sha"))
            if isinstance(fr, dict)
            else None
        ),
    }
