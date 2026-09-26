#!/usr/bin/env python3
import json
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).parents[1]


class ReleaseCandidatePublicationSkillContractTest(unittest.TestCase):
    def test_skill_separates_rc_from_final_release(self) -> None:
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")

        for required in (
            "--prerelease",
            "--latest=false",
            "--verify-tag",
            "#x-y-rc",
            "isPrerelease=true",
            "ydb-release-publication",
            "GET /repos/ydb-platform/ydb/releases/latest",
            "tag_name` is not\nthe RC tag",
            "focused remediation PR",
        ):
            self.assertIn(required, skill)

        for forbidden_action in (
            "Do not run or dispatch\n`docs_release`",
            "change `ydb/docs/default-branch.txt`",
            "create a Club YDB\ndraft or post",
        ):
            self.assertIn(forbidden_action, skill)

        self.assertIn('--title "$version RC"', skill)
        self.assertNotIn('--title "$version" \\', skill)

    def test_evals_cover_ready_blocked_and_final_routing(self) -> None:
        payload = json.loads((SKILL_ROOT / "evals" / "evals.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["skill_name"], "ydb-release-candidate-publication")
        self.assertEqual([case["id"] for case in payload["evals"]], [1, 2, 3])
        for case in payload["evals"]:
            self.assertTrue(case["expectations"])
            for fixture in case["files"]:
                self.assertTrue((SKILL_ROOT / "evals" / fixture).is_file())

        ready = json.loads(
            (SKILL_ROOT / "evals" / "fixtures" / "ready-release-candidate.json").read_text(encoding="utf-8")
        )
        blocked = json.loads(
            (SKILL_ROOT / "evals" / "fixtures" / "blocked-release-candidate.json").read_text(encoding="utf-8")
        )
        self.assertEqual(ready["stable_backport_pr"]["required_checks"], "failed")
        self.assertEqual(ready["release_title"], "26.3.1.16 RC")
        self.assertEqual(ready["remediation_pr"]["required_checks"], "green")
        self.assertTrue(ready["remediation_pr"]["published"])
        self.assertEqual(blocked["published_docs"], {"en_date": "TBD", "ru_date": "уточняется"})


if __name__ == "__main__":
    unittest.main()
