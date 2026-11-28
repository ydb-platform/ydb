#!/usr/bin/env python3
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import resolve_log_conflicts  # noqa: E402


def conflict(ours, theirs):
    return "<<<<<<< HEAD\n%s=======\n%s>>>>>>> abc\n" % (ours, theirs)


class ResolveTest(unittest.TestCase):
    def resolve(self, text):
        return resolve_log_conflicts.resolve_text(text)

    def test_keeps_cherry_picked_side(self):
        ours = "            AFL_VERIFY(TargetTierName);\n            RWAddress = Old();\n"
        theirs = "            YDB_LOG_VERIFY(TargetTierName);\n            RWAddress = New();\n"
        new, hunks = self.resolve(conflict(ours, theirs))
        self.assertEqual(hunks, [(1, "resolved")])
        self.assertEqual(new, theirs)

    def test_keeps_code_that_is_not_a_log(self):
        ours = "    const bool isCriticalScheme = indexInfo.GetSchemeNeedActualization();\n"
        theirs = "    const auto thresholdBefore = vIndex.GetSchemeActualizationThreshold();\n"
        new, hunks = self.resolve(conflict(ours, theirs))
        self.assertEqual(hunks, [(1, "resolved")])
        self.assertEqual(new, theirs)

    def test_empty_cherry_picked_side(self):
        new, hunks = self.resolve(conflict("        AFL_VERIFY(PortionsInfo.empty());\n", ""))
        self.assertEqual(hunks, [(1, "resolved")])
        self.assertEqual(new, "")

    def test_two_hunks(self):
        text = conflict("    return 1;\n", "    return 2;\n") + conflict("    foo();\n", "    bar();\n")
        new, hunks = self.resolve(text)
        self.assertEqual(hunks, [(1, "resolved"), (2, "resolved")])
        self.assertEqual(new, "    return 2;\n    bar();\n")
        self.assertNotIn("<<<<<<<", new)

    def test_diff3_base_is_skipped(self):
        text = "<<<<<<< HEAD\n    old();\n||||||| base\n    base();\n=======\n    new();\n>>>>>>> abc\n"
        new, hunks = self.resolve(text)
        self.assertEqual(hunks, [(1, "resolved")])
        self.assertEqual(new, "    new();\n")

    def test_clean_and_malformed(self):
        new, hunks = self.resolve("YDB_LOG_VERIFY(ok);\n")
        self.assertEqual((new, hunks), ("YDB_LOG_VERIFY(ok);\n", []))
        with self.assertRaises(resolve_log_conflicts.MalformedConflict):
            self.resolve("<<<<<<< HEAD\nYDB_LOG_VERIFY(ok);\n")
        with self.assertRaises(resolve_log_conflicts.MalformedConflict):
            self.resolve("<<<<<<< HEAD\n<<<<<<< HEAD\n=======\n>>>>>>>\n")

    def test_file_roundtrip(self):
        text = "before\n" + conflict("    old();\n", "    new();\n")
        tmp = tempfile.NamedTemporaryFile(prefix="log-conflict-", suffix=".cpp", delete=False)
        try:
            tmp.write(text.encode("utf-8"))
            tmp.close()
            hunks = resolve_log_conflicts.resolve_file(tmp.name)
            self.assertEqual(hunks, [(1, "resolved")])
            with open(tmp.name, "r", encoding="utf-8") as handle:
                written = handle.read()
            self.assertEqual(written, "before\n    new();\n")
        finally:
            os.remove(tmp.name)


if __name__ == "__main__":
    unittest.main()
