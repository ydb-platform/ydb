"""Stable on-disk JSON schemas for per-file analysis output.

Keep these classes minimal and serializable. They are persisted under
``.cache/per_tu/`` and consumed by the aggregator. Any change here is
schema-breaking; bump ``SCHEMA_VERSION`` and add a migration if needed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


SCHEMA_VERSION = 1


@dataclass
class IncludeLine:
    """A single ``#include`` directive on a specific line of a file."""

    line: int
    spelled: str
    angled: bool
    resolved: Optional[str] = None
    has_iwyu_keep: bool = False
    has_iwyu_export: bool = False


@dataclass
class FileAnalysis:
    """Per-file analysis result, one of these per .cpp TU and per .h probe."""

    schema_version: int = SCHEMA_VERSION
    file: str = ""
    kind: str = "tu"
    compile_flags_hash: str = ""
    input_hash: str = ""

    includes: List[IncludeLine] = field(default_factory=list)

    unused_includes: List[str] = field(default_factory=list)
    suggested_inserts: List[str] = field(default_factory=list)

    include_tree: Dict[str, List[str]] = field(default_factory=dict)

    notes: List[str] = field(default_factory=list)
    error: Optional[str] = None
    stderr_tail: Optional[str] = None

    def to_json(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict) -> "FileAnalysis":
        includes = [IncludeLine(**i) for i in data.get("includes", [])]
        d = dict(data)
        d["includes"] = includes
        d.setdefault("stderr_tail", None)
        return cls(**d)
