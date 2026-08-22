import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ydb.tools.mnc.agent import config


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        self.old_managed_partlabels = config.managed_partlabels
        self.old_managed_partlabel_regexps = config.managed_partlabel_regexps

    def tearDown(self):
        config.mnc_home = self.old_mnc_home
        config.managed_partlabels = self.old_managed_partlabels
        config.managed_partlabel_regexps = self.old_managed_partlabel_regexps

    def write_config(self, text: str):
        tmp = tempfile.NamedTemporaryFile("w", delete=False)
        tmp.write(text)
        tmp.close()
        return tmp.name

    def test_empty_managed_rules_deny_all_partlabels(self):
        config.managed_partlabels = set()
        config.managed_partlabel_regexps = []

        self.assertFalse(config.has_managed_partlabel_rules())
        self.assertFalse(config.is_managed_partlabel("ydb_disk_1"))

    def test_exact_managed_partlabel_matches(self):
        config.managed_partlabels = {"ydb_disk_1"}
        config.managed_partlabel_regexps = []

        self.assertTrue(config.has_managed_partlabel_rules())
        self.assertTrue(config.is_managed_partlabel("ydb_disk_1"))
        self.assertFalse(config.is_managed_partlabel("ydb_disk_2"))

    def test_regex_uses_full_match(self):
        config.managed_partlabels = set()
        config.managed_partlabel_regexps = [re.compile(r"ydb_disk_\d+")]

        self.assertTrue(config.is_managed_partlabel("ydb_disk_12"))
        self.assertFalse(config.is_managed_partlabel("prefix_ydb_disk_12"))
        self.assertFalse(config.is_managed_partlabel("ydb_disk_12_suffix"))

    def test_load_config_supports_top_level_and_disks_keys(self):
        path = self.write_config(
            "\n".join(
                [
                    "mnc_home: /tmp/mnc-home",
                    "disks:",
                    "  managed_partlabels:",
                    "    - disk_from_disks",
                    "  managed_partlabel_regexps:",
                    "    - 'disk_[0-9]+'",
                ]
            )
        )

        config.load_config(path)

        self.assertEqual(config.mnc_home, "/tmp/mnc-home")
        self.assertEqual(config.managed_partlabels, {"disk_from_disks"})
        self.assertTrue(config.is_managed_partlabel("disk_42"))

    def test_top_level_managed_partlabels_prefer_over_disks_key(self):
        path = self.write_config(
            "\n".join(
                [
                    "managed_partlabels:",
                    "  - top_level",
                    "disks:",
                    "  managed_partlabels:",
                    "    - nested",
                ]
            )
        )

        config.load_config(path)

        self.assertEqual(config.managed_partlabels, {"top_level"})
        self.assertTrue(config.is_managed_partlabel("top_level"))
        self.assertFalse(config.is_managed_partlabel("nested"))

    def test_empty_yaml_is_accepted_and_disables_disk_access(self):
        path = self.write_config("")

        config.load_config(path)

        self.assertFalse(config.has_managed_partlabel_rules())
        self.assertFalse(config.is_managed_partlabel("disk"))

    def test_ensure_mnc_home_creates_home_and_run_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config.mnc_home = str(Path(tmpdir) / "mnc")

            self.assertEqual(config.ensure_mnc_home(), config.mnc_home)

            self.assertTrue(Path(config.mnc_home).is_dir())
            self.assertTrue((Path(config.mnc_home) / "run").is_dir())

    def test_invalid_regexp_propagates_error(self):
        path = self.write_config("managed_partlabel_regexps:\n  - '['\n")

        with self.assertRaises(re.error):
            config.load_config(path)

    def test_ensure_mnc_home_calls_mkdir_for_run_directory(self):
        config.mnc_home = "/tmp/mnc"

        with mock.patch.object(Path, "mkdir") as mkdir:
            config.ensure_mnc_home()

        self.assertEqual(mkdir.call_count, 2)
