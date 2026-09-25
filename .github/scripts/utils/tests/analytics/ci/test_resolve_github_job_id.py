#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _paths import add_product_paths

add_product_paths(Path(__file__).resolve().parents[4] / "analytics")

import unittest

from resolve_github_job_id import job_name_matches_preset, list_run_jobs, pick_job, resolve_github_job


class JobNameMatchTest(unittest.TestCase):
    def test_preset_at_end_and_middle(self):
        self.assertTrue(job_name_matches_preset("Build and test relwithdebinfo", "relwithdebinfo"))
        self.assertTrue(job_name_matches_preset("linux relwithdebinfo extra", "relwithdebinfo"))
        self.assertFalse(job_name_matches_preset("check-running-allowed", "relwithdebinfo"))


class PaginationTest(unittest.TestCase):
    def test_lists_two_pages_and_picks_preset(self):
        pages = {
            1: {"jobs": [{"id": 1, "name": "other"}, {"id": 2, "name": "setup"}]},
            2: {"jobs": [{"id": 9, "name": "Build and test relwithdebinfo"}]},
        }

        def get_json(url, token, params=None):
            return pages[params["page"]]

        jobs = list_run_jobs("ydb-platform/ydb", "1", "tok", get_json=get_json, per_page=2)
        self.assertEqual(len(jobs), 3)
        picked = pick_job(jobs, "relwithdebinfo")
        self.assertEqual(picked["id"], 9)

    def test_resolve_uses_injected_fetch(self):
        def get_json(url, token, params=None):
            return {"jobs": [{"id": 77, "name": "Postcommit · Build and test release-asan"}]}

        job = resolve_github_job(
            "release-asan",
            repository="ydb-platform/ydb",
            run_id="5",
            token="t",
            get_json=get_json,
        )
        self.assertEqual(job["id"], 77)


if __name__ == "__main__":
    unittest.main()
