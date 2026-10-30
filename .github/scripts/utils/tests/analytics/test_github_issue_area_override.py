#!/usr/bin/env python3
"""area_override must follow current TESTOWNERS, not a stale Owner: line in the issue body."""

import os
import sys
import unittest

_SCRIPTS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_ANALYTICS = os.path.join(_SCRIPTS, "analytics")
_MUTE = os.path.join(_SCRIPTS, "tests", "mute")
for p in (_SCRIPTS, _ANALYTICS, _MUTE):
    if p not in sys.path:
        sys.path.insert(0, p)

from github_issue_mapping import resolve_area_override  # noqa: E402


_AREA_TO_OWNER = {
    "area/engineering": "engineering",
    "area/cs": "cs",
    "area/duty": "engineering",
}


class ResolveAreaOverrideBaselineTest(unittest.TestCase):
    def test_stale_owner_body_current_testowners_cs_area_engineering(self):
        """#57390-style: Owner: engineering in body, TESTOWNERS already cs, area/engineering."""
        body = "Owner: TEAM:@ydb-platform/engineering\n"
        info = {"area": "area/engineering"}
        ao = resolve_area_override(
            body,
            info,
            _AREA_TO_OWNER,
            issue_state="OPEN",
            baseline_owner_team="TEAM:@ydb-platform/cs",
        )
        self.assertEqual(ao, "area/engineering")

    def test_same_as_current_testowners_no_override(self):
        body = "Owner: TEAM:@ydb-platform/engineering\n"
        info = {"area": "area/cs"}
        ao = resolve_area_override(
            body,
            info,
            _AREA_TO_OWNER,
            issue_state="OPEN",
            baseline_owner_team="TEAM:@ydb-platform/cs",
        )
        self.assertIsNone(ao)

    def test_fallback_to_body_owner_when_no_baseline(self):
        body = "Owner: TEAM:@ydb-platform/engineering\n"
        info = {"area": "area/engineering"}
        ao = resolve_area_override(
            body,
            info,
            _AREA_TO_OWNER,
            issue_state="OPEN",
            baseline_owner_team=None,
        )
        self.assertIsNone(ao)

    def test_fallback_body_differs_sets_override(self):
        body = "Owner: TEAM:@ydb-platform/cs\n"
        info = {"area": "area/engineering"}
        ao = resolve_area_override(
            body,
            info,
            _AREA_TO_OWNER,
            issue_state="OPEN",
            baseline_owner_team=None,
        )
        self.assertEqual(ao, "area/engineering")


if __name__ == "__main__":
    unittest.main()
