#!/usr/bin/env python3
import json
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).parents[1]


class ReleasePublicationSkillContractTest(unittest.TestCase):
    def test_skill_has_release_gates_and_publication_contract(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")

        normalized_skill = skill.lower()
        for required in (
            "release-notes PR gate",
            "docs_release",
            "Release notes:",
            "default-branch.txt",
            "Club YDB",
        ):
            self.assertIn(required.lower(), normalized_skill)

    def test_evals_cover_blocked_ready_and_github_release_cases(self) -> None:
        payload = json.loads((SKILL_ROOT / "evals" / "evals.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["skill_name"], "ydb-release-publication")
        self.assertEqual([case["id"] for case in payload["evals"]], [1, 2, 3])
        for case in payload["evals"]:
            self.assertTrue(case["expectations"])
            for fixture in case["files"]:
                self.assertTrue((SKILL_ROOT / "evals" / fixture).is_file())


if __name__ == "__main__":
    unittest.main()
