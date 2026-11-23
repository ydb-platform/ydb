import copy
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from ydb.tools.dc_admin import placement


class TestPlacement(unittest.TestCase):
    def setUp(self):
        self.locations = {i: f"dc{(i - 1) // 3 + 1}" for i in range(1, 10)}
        old = {"nto_select": 9, "ring": [{"node": [n]} for n in self.locations]}
        self.configs = {key: {"ring": old} for key in placement.SERVICES.values()}
        self.base = {
            "metadata": {"kind": "MainConfig", "version": 5},
            "config": {"hosts": [], "domains_config": {"domain": [{"name": "Root"}]}},
        }
        self.cancel = Mock()
        self.cancel.wait.return_value = self.cancel.is_set.return_value = False
        self.steps = []
        self.log = []

    def command(self, argv, progress):
        if "fetch" in argv:
            doc = copy.deepcopy(self.base)
            if "--v2-explicit-sections" in argv:
                doc["config"]["domains_config"].update(self.configs)
                doc["config"]["blob_storage_config"] = {"managed": True}
            return yaml.safe_dump(doc)
        self.assertEqual(argv[1:-1], ["admin", "storage", "replace", "-f"])
        self.assertIn("+++ " + argv[-1], self.log[-1])
        self.assertIn("@@", self.log[-1])
        doc = yaml.safe_load(Path(argv[-1]).read_text())
        self.assertEqual(doc["metadata"], self.base["metadata"])
        self.assertNotIn("blob_storage_config", doc["config"])
        self.configs = {key: doc["config"]["domains_config"][key] for key in placement.SERVICES.values()}
        self.base = doc
        self.base["metadata"]["version"] += 1
        self.steps.append(
            [
                [(placement.nodes(g), g.get("write_only", False)) for g in placement.rings(cfg)]
                for cfg in self.configs.values()
            ]
        )
        return ""

    def migrate(self):
        placement.migrate(lambda: ["ydb"], self.locations, ["dc2", "dc3"], self.cancel, self.log.append)

    def test_four_versioned_replaces_and_noop(self):
        with patch.object(placement, "run", self.command):
            self.migrate()
            self.migrate()
        old, new = list(self.locations), list(range(4, 10))
        expected = [
            [(old, False), (new, True)],
            [(old, False), (new, False)],
            [(new, False), (old, True)],
            [(new, False)],
        ]
        self.assertEqual(self.steps, [[step] * 3 for step in expected])
        self.assertEqual(self.cancel.wait.call_count, 3)
        self.assertEqual(sum(line.startswith("$ diff ") for line in self.log), 4)

    def test_resume_after_first_replace(self):
        with patch.object(placement, "run", self.command):
            self.cancel.wait.return_value = True
            with self.assertRaisesRegex(ValueError, "остановлена"):
                self.migrate()
            self.assertEqual(len(self.steps), 1)
            self.cancel.wait.return_value = False
            self.migrate()
        self.assertEqual(len(self.steps), 4)

    def test_concurrent_yaml_change(self):
        def race(argv, progress):
            if "fetch" in argv and "--v2-explicit-sections" not in argv:
                self.base["metadata"]["version"] += 1
            return self.command(argv, progress)

        with patch.object(placement, "run", race), self.assertRaisesRegex(ValueError, "Версия YAML"):
            self.migrate()
        self.assertFalse(self.steps)
