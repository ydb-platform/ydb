"""
Filesystem utility helpers for telnetlib3.

Provides atomic write operations and JSON encoding used by the fingerprinting subsystem.
"""

from __future__ import annotations

# std imports
import os
import json
from typing import Any


class _BytesSafeEncoder(json.JSONEncoder):
    """JSON encoder that converts bytes to str (UTF-8) or hex."""

    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode("utf-8")
            except UnicodeDecodeError:
                return o.hex()
        return super().default(o)


def _atomic_json_write(filepath: str, data: dict[str, Any]) -> None:
    """Atomically write JSON data to file via write-to-new + rename."""
    tmp_path = os.path.splitext(filepath)[0] + ".json.new"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, cls=_BytesSafeEncoder)
    os.replace(tmp_path, filepath)
