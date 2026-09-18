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
            "SELECT Version();",
            "approved wording",
        ):
            self.assertIn(required.lower(), normalized_skill)

        self.assertIn(
            "Before writing or editing the draft, ask the owner for approved wording about\n"
            "internal availability or installation.",
            skill,
        )
        self.assertIn(
            "When the owner has already supplied the wording,\nuse it without asking again.",
            skill,
        )
        self.assertIn("Do not claim the release is installed", skill)
        self.assertIn(
            "Версию базы данных можно узнать, выполнив запрос:",
            skill,
        )
        self.assertIn("SELECT Version();", skill)
        self.assertIn("without asking the owner whether to add\nit", skill)

    def test_evals_cover_blocked_ready_and_github_release_cases(self) -> None:
        payload = json.loads((SKILL_ROOT / "evals" / "evals.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["skill_name"], "ydb-release-publication")
        self.assertEqual([case["id"] for case in payload["evals"]], [1, 2, 3, 4, 5])
        for case in payload["evals"]:
            self.assertTrue(case["expectations"])
            for fixture in case["files"]:
                self.assertTrue((SKILL_ROOT / "evals" / fixture).is_file())


if __name__ == "__main__":
    unittest.main()
