import copy
import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "evaluate_release_coverage.py"
SPEC = importlib.util.spec_from_file_location("coverage_eval", SCRIPT)
coverage_eval = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(coverage_eval)


def valid_manifest():
    return {
        "release_input": "26.3.1",
        "release_line": "26.3",
        "notes": [
            {
                "ticket_key": "YDBFEATURES-10",
                "en": "[Feature A](./a.md?version=v26.3) is available.",
                "ru": "Доступна [функция A](./a.md?version=v26.3).",
            },
            {
                "ticket_key": "YDBFEATURES-11",
                "en": "[Feature B](https://github.com/ydb-platform/ydb/pull/11) is available.",
                "ru": "Доступна [функция B](https://github.com/ydb-platform/ydb/pull/11).",
            },
        ],
    }


def valid_tracker_export():
    return {
        "queue": "YDBFEATURES",
        "issue_type": "feature",
        "value": "stable-26-3-1",
        "fields": {
            "actualCodeReadyBranch": {
                "field_id": "queue-id--actualCodeReadyBranch",
                "pages": [
                    {
                        "page": 1,
                        "pages_count": 2,
                        "total_issues_count": 2,
                        "issues": [
                            {
                                "key": "YDBFEATURES-10",
                                "type": "feature",
                                "actualCodeReadyBranch": "stable-26-3-1",
                            }
                        ],
                    },
                    {
                        "page": 2,
                        "pages_count": 2,
                        "total_issues_count": 2,
                        "issues": [
                            {
                                "key": "YDBFEATURES-11",
                                "type": "feature",
                                "actualCodeReadyBranch": "stable-26-3-1",
                            }
                        ],
                    },
                ],
            },
            "actualDefaultInclusionBranch": {
                "field_id": "queue-id--actualDefaultInclusionBranch",
                "pages": [
                    {
                        "page": 1,
                        "pages_count": 1,
                        "total_issues_count": 1,
                        "issues": [
                            {
                                "key": "YDBFEATURES-11",
                                "type": "feature",
                                "actualDefaultInclusionBranch": "stable-26-3-1",
                            }
                        ],
                    }
                ],
            },
        },
    }


def union_tracker_export():
    return {
        "queue": "YDBFEATURES",
        "issue_type": "feature",
        "value": "stable-26-3-1",
        "fields": {
            "actualCodeReadyBranch": {
                "field_id": "queue-id--actualCodeReadyBranch",
                "pages": [
                    {
                        "page": 1,
                        "pages_count": 1,
                        "total_issues_count": 2,
                        "issues": [
                            {
                                "key": "YDBFEATURES-10",
                                "type": "feature",
                                "actualCodeReadyBranch": "stable-26-3-1",
                            },
                            {
                                "key": "YDBFEATURES-11",
                                "type": "feature",
                                "actualCodeReadyBranch": "stable-26-3-1",
                            },
                        ],
                    }
                ],
            },
            "actualDefaultInclusionBranch": {
                "field_id": "queue-id--actualDefaultInclusionBranch",
                "pages": [
                    {
                        "page": 1,
                        "pages_count": 1,
                        "total_issues_count": 2,
                        "issues": [
                            {
                                "key": "YDBFEATURES-11",
                                "type": "feature",
                                "actualDefaultInclusionBranch": "stable-26-3-1",
                            },
                            {
                                "key": "YDBFEATURES-12",
                                "type": "feature",
                                "actualDefaultInclusionBranch": "stable-26-3-1",
                            },
                        ],
                    }
                ],
            },
        },
    }


def valid_en():
    return """# Changelog

## Version 26.3 {#26-3}

Release date: TBD.

### Functionality

* [Feature A](./a.md?version=v26.3) is available.
* [Feature B](https://github.com/ydb-platform/ydb/pull/11) is available.

## Version 26.2 {#26-2}
"""


def valid_ru():
    return """# Список изменений

## Версия 26.3 {#26-3}

Дата выхода: уточняется.

### Функциональность

* Доступна [функция A](./a.md?version=v26.3).
* Доступна [функция B](https://github.com/ydb-platform/ydb/pull/11).

## Версия 26.2 {#26-2}
"""


class CoverageEvalTest(unittest.TestCase):
    def test_accepts_union_coverage_with_a_reasoned_exclusion(self):
        manifest = valid_manifest()
        manifest["exclusions"] = [
            {
                "ticket_key": "YDBFEATURES-12",
                "reason_code": "not-in-target",
                "reason": "The complete implementation is not in the target branch.",
            }
        ]

        errors = coverage_eval.evaluate(
            manifest, union_tracker_export(), valid_en(), valid_ru()
        )

        self.assertEqual([], errors)

    def test_rejects_export_without_default_inclusion_filter(self):
        tracker_export = valid_tracker_export()
        del tracker_export["fields"]["actualDefaultInclusionBranch"]

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(
            any("Tracker export is missing actualDefaultInclusionBranch" in e for e in errors)
        )

    def test_rejects_legacy_single_filter_export(self):
        tracker_export = {
            "queue": "YDBFEATURES",
            "issue_type": "feature",
            "field_key": "actualCodeReadyBranch",
            "field_id": "queue-id--actualCodeReadyBranch",
            "value": "stable-26-3-1",
            "pages": valid_tracker_export()["fields"]["actualCodeReadyBranch"]["pages"],
        }

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(any("Tracker export must contain both release fields" in e for e in errors))

    def test_cli_reports_candidate_note_and_exclusion_counts(self):
        manifest = valid_manifest()
        manifest["exclusions"] = [
            {
                "ticket_key": "YDBFEATURES-12",
                "reason_code": "not-in-target",
                "reason": "The complete implementation is not in the target branch.",
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "manifest": root / "manifest.json",
                "tracker": root / "tracker.json",
                "en": root / "en.md",
                "ru": root / "ru.md",
            }
            paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")
            paths["tracker"].write_text(
                json.dumps(union_tracker_export()), encoding="utf-8"
            )
            paths["en"].write_text(valid_en(), encoding="utf-8")
            paths["ru"].write_text(valid_ru(), encoding="utf-8")

            completed = subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--manifest",
                    str(paths["manifest"]),
                    "--tracker-export",
                    str(paths["tracker"]),
                    "--en",
                    str(paths["en"]),
                    "--ru",
                    str(paths["ru"]),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(0, completed.returncode)
        self.assertEqual(
            {
                "status": "pass",
                "tracker_count": 3,
                "note_count": 2,
                "exclusion_count": 1,
            },
            json.loads(completed.stdout),
        )

    def test_accepts_exact_bilingual_bijection_and_rendered_files(self):
        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), valid_en(), valid_ru()
        )
        self.assertEqual([], errors)

    def test_rejects_equal_counts_with_different_ticket_sets(self):
        manifest = valid_manifest()
        manifest["notes"][1]["ticket_key"] = "YDBFEATURES-12"

        errors = coverage_eval.evaluate(
            manifest, valid_tracker_export(), valid_en(), valid_ru()
        )

        self.assertTrue(any("unaccounted ticket keys: YDBFEATURES-11" in e for e in errors))
        self.assertTrue(any("extra ticket keys: YDBFEATURES-12" in e for e in errors))

    def test_rejects_manifest_that_does_not_match_markdown(self):
        en = valid_en().replace(
            "* [Feature B](https://github.com/ydb-platform/ydb/pull/11) is available.\n",
            "",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN bullets do not exactly match manifest order/content" in e for e in errors))

    def test_rejects_incomplete_pagination_and_language_order_drift(self):
        tracker_export = copy.deepcopy(valid_tracker_export())
        tracker_export["fields"]["actualCodeReadyBranch"]["pages"] = tracker_export[
            "fields"
        ]["actualCodeReadyBranch"]["pages"][:1]
        ru = valid_ru().replace(
            "* Доступна [функция A](./a.md?version=v26.3).\n"
            "* Доступна [функция B](https://github.com/ydb-platform/ydb/pull/11).",
            "* Доступна [функция B](https://github.com/ydb-platform/ydb/pull/11).\n"
            "* Доступна [функция A](./a.md?version=v26.3).",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), ru
        )

        self.assertTrue(any("actualCodeReadyBranch pagination is incomplete" in e for e in errors))
        self.assertTrue(any("RU bullets do not exactly match manifest order/content" in e for e in errors))

    def test_rejects_wrong_tracker_filter_and_unversioned_docs_link(self):
        manifest = valid_manifest()
        tracker_export = valid_tracker_export()
        tracker_export["value"] = "stable-26-3"
        manifest["notes"][0]["en"] = "[Feature A](./a.md) is available."

        errors = coverage_eval.evaluate(
            manifest, tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(any("Tracker value must be stable-26-3-1" in e for e in errors))
        self.assertTrue(any("unversioned documentation link" in e for e in errors))

    def test_rejects_tracker_export_without_feature_filter(self):
        tracker_export = valid_tracker_export()
        tracker_export["issue_type"] = "bug"

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(any("Tracker issue_type must be feature" in e for e in errors))

    def test_rejects_patch_and_bugfix_sections_even_when_feature_bullets_match(self):
        en = valid_en().replace(
            "### Functionality",
            "### Version 26.3.1 {#26-3-1}\n\n### Functionality",
        )
        ru = valid_ru().replace(
            "## Версия 26.2",
            "### Исправления ошибок\n\n* Исправлена ошибка.\n\n## Версия 26.2",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, ru
        )

        self.assertTrue(any("EN release section contains a patch-version subsection" in e for e in errors))
        self.assertTrue(any("RU release section contains a bug-fix subsection" in e for e in errors))

    def test_rejects_extra_bullets_under_another_subsection(self):
        en = valid_en().replace(
            "## Version 26.2",
            "### Performance\n\n* Hidden extra feature.\n\n## Version 26.2",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN release section has unexpected subsections" in e for e in errors))
        self.assertTrue(any("EN bullets do not exactly match manifest order/content" in e for e in errors))

    def test_rejects_non_feature_issues_in_feature_filtered_export(self):
        tracker_export = copy.deepcopy(valid_tracker_export())
        pages = tracker_export["fields"]["actualCodeReadyBranch"]["pages"]
        for page in pages:
            page["total_issues_count"] = 3
        pages[1]["issues"].append(
            {
                "key": "YDB-999",
                "type": "bug",
                "actualCodeReadyBranch": "stable-26-3-1",
            }
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(any("YDB-999: Tracker type is not feature" in e for e in errors))

    def test_rejects_issue_without_type_in_feature_filtered_export(self):
        tracker_export = copy.deepcopy(valid_tracker_export())
        del tracker_export["fields"]["actualCodeReadyBranch"]["pages"][0]["issues"][0][
            "type"
        ]

        errors = coverage_eval.evaluate(
            valid_manifest(), tracker_export, valid_en(), valid_ru()
        )

        self.assertTrue(any("YDBFEATURES-10: Tracker type is not feature" in e for e in errors))

    def test_rejects_dash_bullet(self):
        en = valid_en().replace(
            "* [Feature A]",
            "- Hidden extra feature.\n* [Feature A]",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN release section uses a non-asterisk bullet" in e for e in errors))
        self.assertTrue(any("EN bullets do not exactly match manifest order/content" in e for e in errors))

    def test_rejects_duplicate_release_section(self):
        en = valid_en() + "\n## Version 26.3 {#26-3}\n\n### Functionality\n"

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN release heading appears 2 times" in e for e in errors))

    def test_rejects_bullets_outside_functionality(self):
        en = valid_en().replace(
            "### Functionality\n\n"
            "* [Feature A](./a.md?version=v26.3) is available.\n"
            "* [Feature B](https://github.com/ydb-platform/ydb/pull/11) is available.",
            "* [Feature A](./a.md?version=v26.3) is available.\n"
            "* [Feature B](https://github.com/ydb-platform/ydb/pull/11) is available.\n\n"
            "### Functionality",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN release section has bullets before Functionality" in e for e in errors))
        self.assertTrue(any("EN bullets do not exactly match manifest order/content" in e for e in errors))

    def test_rejects_h4_subsection(self):
        en = valid_en().replace(
            "* [Feature A]",
            "#### Hidden group\n\n* [Feature A]",
        )

        errors = coverage_eval.evaluate(
            valid_manifest(), valid_tracker_export(), en, valid_ru()
        )

        self.assertTrue(any("EN release section has unexpected subsections" in e for e in errors))


if __name__ == "__main__":
    unittest.main()
