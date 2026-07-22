#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path

from check_ya_configure_integrity import find_hits, main


class TestCheckYaConfigureIntegrity(unittest.TestCase):
    def test_baddep_is_fatal(self):
        log = (
            "Configuring dependencies for platform tools\n"
            "Error[-WBadDep]: in $B/ydb/core/persqueue/pqrb/ut/ydb-core-persqueue-pqrb-ut: "
            "depends on two modules which PROVIDES same feature 'test_framework':\n"
            "Configure error (use -k to proceed)\n"
        )
        hits = find_hits(log)
        self.assertEqual(len(hits), 1)
        self.assertIn("BadDep", hits[0])

    def test_noise_is_ignored(self):
        log = (
            "Warn: Path is not buildable target: /tmp/docker-compose.yml\n"
            "Configuration done. Preparing for execution\n"
        )
        self.assertEqual(find_hits(log), [])

    def test_main_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok = Path(tmp) / "ok.txt"
            bad = Path(tmp) / "bad.txt"
            ok.write_text("Configuration done.\n", encoding="utf-8")
            bad.write_text("Error[-WBadDep]: conflict\n", encoding="utf-8")
            self.assertEqual(main([str(ok)]), 0)
            self.assertEqual(main([str(bad)]), 1)


if __name__ == "__main__":
    unittest.main()
