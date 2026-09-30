#!/usr/bin/env python3
"""taxonomy.py, emitters, and the README tables must list the same names."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_SCRIPTS / "analytics"))

from github_actions import taxonomy

_ANALYTICS = _SCRIPTS / "analytics"
_GITHUB = _SCRIPTS.parent.parent
_ACTION_YML = _GITHUB / "actions" / "test_ya" / "action.yml"
_GH_README = _ANALYTICS / "github_actions" / "README.md"
_EVLOG = _ANALYTICS / "github_actions" / "ya_evlog_phases.py"
_EXPORT = _ANALYTICS / "github_actions" / "export_github_job_metrics.py"


def _table_rows(header_starts_with: str) -> list:
    """Rows of the README table whose header starts with this text."""
    rows = []
    inside = False
    for line in _GH_README.read_text(encoding="utf-8").splitlines():
        if line.startswith(header_starts_with):
            inside = True
            continue
        if not inside:
            continue
        if not line.startswith("|"):
            break
        if set(line.replace("|", "").replace(" ", "")) <= {"-", ":"}:
            continue
        rows.append([cell.strip() for cell in line.split("|")[1:-1]])
    return rows


def _ticked(cell: str) -> set:
    return set(re.findall(r"`([^`]+)`", cell))


def _documented_sources() -> set:
    sources = set()
    for row in _table_rows("| `source` | `name` |"):
        sources |= _ticked(row[0])
    return sources


def _documented_export_names() -> set:
    names = set()
    for row in _table_rows("| `source` | `name` |"):
        names |= _ticked(row[1])
    return names


def _documented_phase_names() -> set:
    names = set()
    for row in _table_rows("| `name` | Что измеряет |"):
        names |= _ticked(row[0])
    return names


def _span_names_from_action() -> set:
    text = _ACTION_YML.read_text(encoding="utf-8")
    names = set()
    # CLI calls: ci_metrics.py, `analytics start/track`, or `analytics_run NAME`.
    for match in re.finditer(r"analytics_run\s+\"?([A-Za-z_][\w-]*)", text):
        names.add(match.group(1))
    for match in re.finditer(
        r"(?:ci_metrics\.py|\banalytics)\s+(?:start|track)\s+"
        r"(?:--name\s+)?\"?([A-Za-z_][\w-]*)",
        text,
    ):
        names.add(match.group(1))
    # ya_make_try_${RETRY} is expanded by the action, not a literal name.
    resolved = set()
    for name in names:
        if "$" in name and "RETRY" in name:
            resolved.add("ya_make_try_N")
        elif "$" in name:
            continue
        else:
            resolved.add(taxonomy.canonical_name(name))
    return resolved


class TaxonomyMatchesCodeTest(unittest.TestCase):
    def test_every_span_in_test_ya_is_in_the_taxonomy(self):
        known = set(taxonomy.YA_PHASE_NAMES)
        emitted = _span_names_from_action()
        self.assertTrue(
            emitted <= known,
            f"names emitted by test_ya but missing from taxonomy.py: {sorted(emitted - known)}",
        )

    def test_taxonomy_has_no_phase_that_nothing_emits(self):
        emitted = _span_names_from_action() | set(taxonomy.YA_EVLOG_NAMES)
        stale = set(taxonomy.YA_PHASE_NAMES) - emitted
        self.assertEqual(
            stale, set(), f"taxonomy.py lists phases no code emits any more: {sorted(stale)}"
        )

    def test_evlog_phase_names_match_the_script(self):
        text = _EVLOG.read_text(encoding="utf-8")
        emitted = set(re.findall(r'\(\s*"(ya_[a-z_]+)"\s*,\s*start', text))
        self.assertEqual(emitted, set(taxonomy.YA_EVLOG_NAMES))

    def test_export_sources_match_the_exporter(self):
        text = _EXPORT.read_text(encoding="utf-8")
        used = set(re.findall(r'"source":\s*"([a-z_]+)"', text))
        self.assertEqual(used, set(taxonomy.EXPORT_SOURCES))

    def test_exporter_writes_no_state_rows_into_the_metrics_table(self):
        text = _EXPORT.read_text(encoding="utf-8")
        self.assertNotIn("export_state", text)
        self.assertNotIn('"run_id": 0', text)


class TaxonomyMatchesDocsTest(unittest.TestCase):
    def test_readme_documents_every_source(self):
        self.assertEqual(_documented_sources(), set(taxonomy.SOURCES))

    def test_readme_documents_every_exported_name(self):
        documented = _documented_export_names()
        for name in taxonomy.GITHUB_JOB_NAMES:
            self.assertIn(name, documented, f"{name} is not in the README source/name table")

    def test_readme_phase_table_matches_the_taxonomy_exactly(self):
        documented = _documented_phase_names()
        known = set(taxonomy.YA_PHASE_NAMES)
        self.assertEqual(
            documented - known,
            set(),
            f"README documents phases the taxonomy does not know: {sorted(documented - known)}",
        )
        self.assertEqual(
            known - documented,
            set(),
            f"phases missing from the README table: {sorted(known - documented)}",
        )


if __name__ == "__main__":
    unittest.main()
