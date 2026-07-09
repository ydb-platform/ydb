"""Synthesize probe TUs to analyze headers as if they were translation units.

clang-include-cleaner works only on TUs. To get unused-include diagnostics
for a header ``H``, we manufacture a tiny probe ``probes/<hash>.cpp``:

    // Probe TU for ``ydb/.../H.h``
    #include "ydb/.../H.h"

and invoke clang-include-cleaner on it, inheriting compile flags from one
of the TUs that already include ``H``.

The "unused includes" reported for the probe map almost 1-to-1 to includes
of ``H`` itself: clang-include-cleaner attributes diagnostics to the file
containing the ``#include`` directive, which for transitive includes is
``H``, not the probe.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence


log = logging.getLogger("header_probe")


@dataclass
class HeaderProbe:
    """A synthesized TU whose single purpose is to ``#include`` one header."""

    header: str
    probe_path: Path
    template_tu: str

    def cleanup(self) -> None:
        try:
            self.probe_path.unlink()
        except OSError:
            pass


def pick_template_tu(header: str, include_trees: Dict[str, Dict[str, List[str]]]) -> Optional[str]:
    """Pick a TU whose include tree contains ``header``.

    We prefer the TU with the smallest include tree to minimize overhead.
    """
    candidates: List[str] = []
    for tu, edges in include_trees.items():
        seen = False
        for kids in edges.values():
            if header in kids:
                seen = True
                break
        if seen or header in edges:
            candidates.append(tu)
    if not candidates:
        return None
    candidates.sort(key=lambda t: sum(len(v) for v in include_trees.get(t, {}).values()))
    return candidates[0]


def synthesize_probe(
    header_rel: str,
    repo_root: Path,
    probes_dir: Path,
    preamble_includes: Optional[List[str]] = None,
) -> HeaderProbe:
    """Create (or reuse) a probe ``.cpp`` that includes ``header_rel``.

    ``preamble_includes`` is an optional list of repo-relative header
    paths to include BEFORE ``header_rel`` in the probe. Use this when
    the target header is not self-contained (e.g. it references a
    template defined in a sibling header that callers are expected to
    include first). Without a preamble, clang fails to parse the
    isolated header and clang-include-cleaner reports nothing useful.

    Race-safe: parallel ya compile workers may synthesize the same probe
    concurrently (same content → same digest). Each writer materializes
    the file under a unique temp name and atomically renames into
    place.
    """
    import os
    import secrets

    probes_dir.mkdir(parents=True, exist_ok=True)

    pre_lines = "".join(
        f'#include "{p}"\n' for p in (preamble_includes or [])
    )
    digest_key = header_rel + "|" + pre_lines
    digest = hashlib.sha1(digest_key.encode("utf-8")).hexdigest()[:16]
    probe_path = probes_dir / f"probe_{digest}.cpp"

    content = (
        f"// Auto-generated probe TU for {header_rel}\n"
        f"{pre_lines}"
        f'#include "{header_rel}"\n'
    )

    if not probe_path.exists() or probe_path.read_text(encoding="utf-8") != content:
        tmp = probe_path.with_suffix(f".cpp.tmp.{os.getpid()}.{secrets.token_hex(4)}")
        try:
            tmp.write_text(content, encoding="utf-8")
            os.replace(tmp, probe_path)
        except OSError:
            try:
                tmp.unlink()
            except OSError:
                pass
            # If another worker already wrote the same file, that is OK
            # (content is identical). Re-raise only if the probe still
            # is not on disk.
            if not probe_path.exists():
                raise

    return HeaderProbe(header=header_rel, probe_path=probe_path, template_tu="")


def derive_probe_compile_args(
    template_arguments: Sequence[str],
    template_source: str,
    probe_path: Path,
) -> List[str]:
    """Copy the compile command of a template TU, swap the source path.

    Drops dependency-tracking, output, and ``-c`` flags. Keeps include
    paths, defines, language standard, target, etc. The result is a
    ``[clang_path, ...flags..., probe_source]`` list suitable for both
    ``clang -H`` and clang-include-cleaner (via stripping argv[0]).
    """
    from .clang_runner import strip_compile_flags

    cleaned = strip_compile_flags(template_arguments, drop_sources=True)
    # If argv[0] of the original compile was the clang binary, preserve
    # it as our probe's clang too — strip_compile_flags treats it as a
    # regular argument and leaves it in place.
    cleaned.append(str(probe_path))
    return cleaned
