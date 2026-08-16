import asyncio
import os
import tempfile
import unittest
import yaml

import ydb.tools.mnc.scheme as scheme
from ydb.tools.mnc.lib import configs, deploy_ctx
from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.scheme import multinode


class ConfigsTest(unittest.TestCase):
    def _apply_multinode_scheme(self, config):
        result, errors = scheme.apply_scheme(config, multinode.scheme)
        self.assertEqual(errors, [])
        self.assertIsNotNone(result)
        return result

    def _base_config(self):
        return self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "none",
            "domain": {
                "name": "Root",
                "databases": [
                    {
                        "name": "NBS",
                        "storage_group_count": 1,
                        "compute_unit_count": 1,
                    },
                ],
            },
        })

    def test_nbs_database_is_counted_as_dynamic_node(self):
        config = {
            "nodes_per_host": 1,
            "domain": {
                "databases": [
                    {"name": "NBS", "compute_unit_count": 2},
                    {"name": "oltp", "compute_unit_count": 1},
                ],
            },
        }

        node_counts = configs.NodeCountByKind(config, ["host1", "host2"])

        self.assertEqual(node_counts.static_node_count, 2)
        self.assertEqual(node_counts.dynamic_node_count, 3)
        self.assertEqual(dict(node_counts.dynamic_node_count_by_host()), {
            "host1": [1, 3],
            "host2": [2],
        })

    def test_nbs_schema_defaults(self):
        config = self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "none",
            "domain": {
                "name": "Root",
                "databases": [
                    {
                        "name": "NBS",
                        "storage_group_count": 1,
                        "compute_unit_count": 1,
                    },
                ],
            },
            "nbs": {
                "enabled": True,
            },
        })

        self.assertEqual(config["nbs"], {
            "enabled": True,
            "database": "NBS",
            "folder_id": "testFolder",
            "storage_pool_kind": "ssd",
            "pipe_client_retry_count": 3,
            "pipe_client_min_retry_time": 1,
            "pipe_client_max_retry_time": 10,
        })

    def test_nbs_schema_has_no_legacy_with_nbs_field(self):
        self.assertNotIn("with_nbs", multinode.scheme)

    def test_validate_nbs_requires_domain(self):
        config = self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "none",
            "nbs": {
                "enabled": True,
            },
        })

        with self.assertRaisesRegex(CliError, "requires domain"):
            configs.validate_nbs_config(config)

    def test_validate_nbs_requires_database(self):
        config = self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "none",
            "domain": {
                "name": "Root",
                "databases": [
                    {
                        "name": "oltp",
                        "storage_group_count": 1,
                        "compute_unit_count": 1,
                    },
                ],
            },
            "nbs": {
                "enabled": True,
            },
        })

        with self.assertRaisesRegex(CliError, "domain.databases"):
            configs.validate_nbs_config(config)

    def test_validate_nbs_rejects_nbs_config_override(self):
        config = self._base_config()
        config["nbs"] = {"enabled": True, "database": "NBS"}
        config["overridden_configs"] = {"nbs_config": {"enabled": True}}

        with self.assertRaisesRegex(CliError, "overridden_configs.nbs_config"):
            configs.validate_nbs_config(config)

    def test_validate_nbs_rejects_grpc_config_override(self):
        config = self._base_config()
        config["nbs"] = {"enabled": True, "database": "NBS"}
        config["overridden_configs"] = {"grpc_config": {"services_enabled": []}}

        with self.assertRaisesRegex(CliError, "overridden_configs.grpc_config"):
            configs.validate_nbs_config(config)

    def test_gen_yaml_adds_nbs_config(self):
        config = self._base_config()
        config["nbs"] = {
            "enabled": True,
            "database": "NBS",
            "folder_id": "folder-1",
            "storage_pool_kind": "ssd",
            "pipe_client_retry_count": 5,
            "pipe_client_min_retry_time": 2,
            "pipe_client_max_retry_time": 20,
        }

        old_work_directory = deploy_ctx.work_directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            deploy_ctx.work_directory = tmp_dir
            try:
                result = asyncio.run(configs.gen_yaml_one_dc("config.yaml", {"host1": []}, config))
                self.assertTrue(result)

                with open(os.path.join(tmp_dir, "config.yaml")) as config_file:
                    generated = yaml.safe_load(config_file)
            finally:
                deploy_ctx.work_directory = old_work_directory

        generated_config = generated["config"]
        self.assertEqual(generated_config["grpc_config"]["services_enabled"], ["legacy"])
        self.assertEqual(generated_config["nbs_config"]["enabled"], True)
        self.assertEqual(
            generated_config["nbs_config"]["nbs_storage_config"],
            {
                "scheme_shard_dir": "/Root/NBS",
                "folder_id": "folder-1",
                "ssd_system_channel_pool_kind": "ssd",
                "ssd_log_channel_pool_kind": "ssd",
                "ssd_index_channel_pool_kind": "ssd",
                "pipe_client_retry_count": 5,
                "pipe_client_min_retry_time": 2,
                "pipe_client_max_retry_time": 20,
            },
        )

    def test_gen_yaml_skips_disabled_nbs_config(self):
        config = self._base_config()
        config["nbs"] = {
            "enabled": False,
        }

        old_work_directory = deploy_ctx.work_directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            deploy_ctx.work_directory = tmp_dir
            try:
                result = asyncio.run(configs.gen_yaml_one_dc("config.yaml", {"host1": []}, config))
                self.assertTrue(result)

                with open(os.path.join(tmp_dir, "config.yaml")) as config_file:
                    generated = yaml.safe_load(config_file)
            finally:
                deploy_ctx.work_directory = old_work_directory

        self.assertNotIn("grpc_config", generated["config"])
        self.assertNotIn("nbs_config", generated["config"])
