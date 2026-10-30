"""JSON/HTML helpers that must stay outside f-string expressions (Python < 3.12)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def js_script_json(value: Any) -> str:
    """JSON safe to embed in a <script> tag (any case of </script>)."""
    text = json.dumps(value, ensure_ascii=False)
    text = text.replace("<", "\\u003c")
    text = text.replace(">", "\\u003e")
    text = text.replace("&", "\\u0026")
    text = text.replace("\u2028", "\\u2028")
    text = text.replace("\u2029", "\\u2029")
    return text


def strip_suggestion_hover_text(suggestions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop *_tooltip / *_explain strings from suggestion rows for the dashboard payload.

    Full text stays in the sibling *_suggestions.json; the HTML table falls back to
    short defaults when these keys are absent.
    """
    out: list[dict[str, Any]] = []
    for row in suggestions:
        out.append(
            {
                key: value
                for key, value in row.items()
                if not (key.endswith("_tooltip") or key.endswith("_explain"))
            }
        )
    return out


def write_dashboard_data_json(path: Path, payload: Any) -> None:
    """Write compact JSON next to dashboard.html (loaded via fetch)."""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
