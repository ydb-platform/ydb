import os
import tempfile
import unittest

from ydb.tools.mnc.viewer.widgets import _validate_multinode_config


class ClusterConfigValidationTest(unittest.TestCase):
    def test_accepts_config_matching_multinode_scheme(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\nerasure: none\n")

            validation = _validate_multinode_config(path)

        self.assertTrue(validation.ok)
        self.assertEqual(validation.errors, [])

    def test_reports_config_scheme_errors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")

            validation = _validate_multinode_config(path)

        self.assertFalse(validation.ok)
        self.assertTrue(any("erasure" in error for error in validation.errors))
