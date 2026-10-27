"""JSON/HTML helpers that must stay outside f-string expressions (Python < 3.12)."""

from __future__ import annotations

import json
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
