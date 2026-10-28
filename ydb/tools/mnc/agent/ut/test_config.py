import re
import tempfile
import unittest
import yaml
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from ydb.tools.mnc.agent import config, main


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

    def test_multinode_disks_for_use_grant_only_selected_host_partitions(self):
        path = self.write_config(yaml.safe_dump({
            "hosts": ["host1", "host2"],
            "disks": [
                {
                    "hosts": ["host1"],
                    "disks_for_use": [{"partlabel": f"kikimr_nvme_00_{part}"} for part in range(4)],
                },
                {"hosts": ["host2"], "disks_for_use": [{"partlabel": "other_host_disk"}]},
            ],
        }))

        config.load_config(path, host="host1")

        self.assertEqual(config.managed_partlabels, {f"kikimr_nvme_00_{part}" for part in range(4)})
        self.assertTrue(config.has_managed_partlabel_rules())
        self.assertEqual(config.managed_partlabel_regexps, [])
        self.assertFalse(config.is_managed_partlabel("kikimr_nvme_00_4"))
        self.assertFalse(config.is_managed_partlabel("kikimr_nvme_00"))
        self.assertFalse(config.is_managed_partlabel("other_host_disk"))

    def test_agent_startup_selects_its_host_from_cluster_config(self):
        path = self.write_config(yaml.safe_dump({
            "disks": [
                {"hosts": ["host1"], "disks_for_use": [{"partlabel": "local_disk"}]},
                {"hosts": ["host2"], "disks_for_use": [{"partlabel": "other_host_disk"}]},
            ],
        }))
        args = SimpleNamespace(config=path, host="host1", mnc_home=None, port=8999, debug=False)
        with mock.patch.object(main, "parse_args", return_value=args), \
                mock.patch.object(config, "ensure_mnc_home"), \
                mock.patch.object(main, "setup_logging"), \
                mock.patch.object(main, "setup_routes"), \
                mock.patch.multiple(main, app=mock.MagicMock(), host="localhost", port=8999, debug=False), \
                mock.patch.object(main.web, "run_app"):
            main.main()

        self.assertEqual(config.managed_partlabels, {"local_disk"})
        self.assertFalse(config.is_managed_partlabel("other_host_disk"))

    def test_multinode_disks_for_split_grant_disk_and_generated_partitions(self):
        path = self.write_config(yaml.safe_dump({
            "disks": [{
                "hosts": ["host1"],
                "disks_for_split": [{"partlabel": "disk.1", "device": "/dev/sdb"}],
            }],
        }))

        config.load_config(path, host="host1")

        self.assertEqual(config.managed_partlabels, {"disk.1"})
        self.assertTrue(config.is_managed_partlabel("disk.1"))
        self.assertTrue(config.is_managed_partlabel("disk.1_0"))
        self.assertTrue(config.is_managed_partlabel("disk.1_12"))
        self.assertFalse(config.is_managed_partlabel("diskx1_0"))
        self.assertFalse(config.is_managed_partlabel("disk.1_suffix"))
        self.assertFalse(config.is_managed_partlabel("prefix_disk.1_0"))

    def test_multinode_shared_groups_are_combined_for_host(self):
        path = self.write_config(yaml.safe_dump({
            "disks": [
                {"hosts": ["host1", "host2"], "disks_for_use": [{"partlabel": "shared"}]},
                {"hosts": ["host1"], "disks_for_use": [{"partlabel": "local"}, {"partlabel": "shared"}]},
            ],
        }))

        config.load_config(path, host="host1")
        self.assertEqual(config.managed_partlabels, {"shared", "local"})

        config.load_config(path, host="host2")
        self.assertEqual(config.managed_partlabels, {"shared"})
        self.assertFalse(config.is_managed_partlabel("local"))

    def test_multinode_unknown_or_missing_host_disables_disk_access(self):
        path = self.write_config(yaml.safe_dump({
            "disks": [{"hosts": ["host1"], "disks_for_use": [{"partlabel": "disk"}]}],
        }))

        for host in ["other_host", None]:
            with self.subTest(host=host):
                config.managed_partlabels = {"stale_disk"}
                config.managed_partlabel_regexps = [re.compile(r"stale_.*")]

                config.load_config(path, host=host)

                self.assertFalse(config.has_managed_partlabel_rules())
                self.assertFalse(config.is_managed_partlabel("disk"))
                self.assertFalse(config.is_managed_partlabel("stale_disk"))

    def test_multinode_explicit_rules_keep_precedence(self):
        path = self.write_config(yaml.safe_dump({
            "managed_partlabels": ["explicit"],
            "managed_partlabel_regexps": ["explicit_[0-9]+"],
            "disks": [{
                "hosts": ["host1"],
                "disks_for_split": [{"partlabel": "derived", "device": "/dev/sdb"}],
            }],
        }))

        config.load_config(path, host="host1")

        self.assertEqual(config.managed_partlabels, {"explicit"})
        self.assertTrue(config.is_managed_partlabel("explicit_0"))
        self.assertFalse(config.is_managed_partlabel("derived"))
        self.assertFalse(config.is_managed_partlabel("derived_0"))

    def test_multinode_empty_disk_groups_disable_disk_access(self):
        for disks in [None, [], [{"hosts": ["host1"], "disks_for_use": [], "disks_for_split": []}]]:
            with self.subTest(disks=disks):
                path = self.write_config(yaml.safe_dump({"disks": disks}))
                config.managed_partlabels = {"stale_disk"}
                config.managed_partlabel_regexps = [re.compile(r"stale_.*")]

                config.load_config(path, host="host1")

                self.assertFalse(config.has_managed_partlabel_rules())

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
