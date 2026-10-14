import asyncio
import os
import tempfile
import unittest
from unittest import mock
import yaml

import ydb.tools.mnc.scheme as scheme
from ydb.tools.mnc.lib import common, configs, deploy_ctx
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

    def test_node_layout_accepts_datacenter_count(self):
        config = self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "mirror-3-dc",
            "node_layout": {
                "num_datacenters": 4,
                "nodes_per_rack": 1,
                "shuffle_locations": False,
            },
        })
        self.assertEqual(config["node_layout"]["num_datacenters"], 4)
        self.assertIsNone(config["node_layout"]["piles"])

    def test_node_layout_accepts_pile_count(self):
        config = self._apply_multinode_scheme({
            "hosts": ["host1"],
            "erasure": "mirror-3-dc",
            "node_layout": {"piles": 2, "datacenters_per_pile": 3},
        })
        self.assertEqual(config["node_layout"]["piles"], 2)
        self.assertEqual(config["node_layout"]["datacenters_per_pile"], 3)
        self.assertIsNone(config["node_layout"]["num_datacenters"])

    def test_node_layout_requires_exactly_one_topology(self):
        for layout, expected_error in [
            ({}, "There aren't any of keys"),
            ({"piles": 2, "num_datacenters": 4}, "There are more than 1 key"),
        ]:
            with self.subTest(layout=layout):
                config, errors = scheme.apply_scheme({
                    "hosts": ["host1"],
                    "erasure": "mirror-3-dc",
                    "node_layout": layout,
                }, multinode.scheme)
                self.assertIsNone(config)
                self.assertTrue(any(expected_error in error for error in errors), errors)

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


class ConfigDiskPartsTest(unittest.IsolatedAsyncioTestCase):
    def _labels(self):
        return [f"kikimr_nvme_{disk:02}_{part}" for part in range(4) for disk in range(4)]

    def _disk_reply(self, device):
        disk = int(device.partlabel.split("_")[2])
        return {
            "partlabel": device.partlabel,
            "device": device.path,
            "path": f"/dev/nvme{disk}n1",
            "parts": [{"id": "1", "label": "primary"}] + [
                {"id": str(part + 2), "label": f"kikimr_nvme_{disk:02}_{part}"} for part in range(5)
            ],
        }

    async def test_ready_partitions_preserve_selection_and_order(self):
        devices = [common.Device(label, None) for label in self._labels()]
        response = {"disks": [self._disk_reply(device) for device in devices]}
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value=response) as post:
            paths = await configs.get_parts("host1", devices, 4, "never", 100, "NONE")

        self.assertEqual(paths, [f"/dev/disk/by-partlabel/{label}" for label in self._labels()])
        self.assertEqual(len(set(paths)), 16)
        post.assert_awaited_once_with("host1", "/disks/info", {
            "devices": [{"partlabel": device.partlabel, "device": None} for device in devices],
        })

    async def test_whole_disk_keeps_all_named_data_partitions(self):
        device = common.Device("kikimr_nvme_00", "/dev/nvme0n1")
        disk = self._disk_reply(device)
        disk["parts"].append({"id": "7", "label": ""})
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value={"disks": [disk]}):
            paths = await configs.get_parts("host1", [device], 1, "never", 100, "NONE")
        self.assertEqual(paths, [f"/dev/disk/by-partlabel/kikimr_nvme_00_{part}" for part in range(5)])

    async def test_missing_partition_fails_without_using_other_partitions(self):
        device = common.Device("kikimr_nvme_00_9", None)
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value={"disks": [self._disk_reply(device)]}), \
                self.assertRaisesRegex(CliError, "Partition kikimr_nvme_00_9 not found"):
            await configs.get_parts("host1", [device], 4, "if_needed", 100, "NONE")

    async def test_incomplete_disk_info_fails(self):
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value={"disks": []}), \
                self.assertRaisesRegex(CliError, "Incomplete disk info"):
            await configs.get_parts("host1", [common.Device("kikimr_nvme_00_0", None)], 4, "never", 100, "NONE")

    async def test_agent_failure_is_reported(self):
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value=None), \
                self.assertRaisesRegex(CliError, "Failed to receive disk info"):
            await configs.get_parts("host1", [common.Device("kikimr_nvme_00_0", None)], 4, "never", 100, "NONE")

    async def test_disk_error_is_reported(self):
        response = {"disks": [{"partlabel": "kikimr_nvme_00_0", "error": "parted failed"}]}
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value=response), \
                self.assertRaisesRegex(CliError, "parted failed"):
            await configs.get_parts("host1", [common.Device("kikimr_nvme_00_0", None)], 4, "never", 100, "NONE")

    async def test_sector_maps_do_not_probe_disks(self):
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock) as post:
            paths = await configs.get_parts("host1", [], 4, "always", 100, "NONE")
        post.assert_not_awaited()
        self.assertEqual(paths, [f"SectorMap:map_{part}:100:NONE" for part in range(4)])

    async def test_sector_maps_fill_only_missing_devices(self):
        device = common.Device("kikimr_nvme_00_0", None)
        with mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value={"disks": [self._disk_reply(device)]}):
            paths = await configs.get_parts("host1", [device], 4, "if_needed", 100, "NONE")
        self.assertEqual(paths, ["/dev/disk/by-partlabel/kikimr_nvme_00_0"] + [f"SectorMap:map_{part}:100:NONE" for part in range(3)])

    async def test_generated_nodes_have_four_distinct_pdisks(self):
        labels = self._labels()
        config, errors = scheme.apply_scheme({
            "hosts": ["host1"],
            "nodes_per_host": 4,
            "erasure": "mirror-3-dc",
            "sector_map": {"use": "never"},
            "disks": [{"hosts": ["host1"], "disks_for_use": [{"partlabel": label} for label in labels]}],
        }, multinode.scheme)
        self.assertEqual(errors, [])
        hosts = common.get_host_disks(config)
        response = {"disks": [self._disk_reply(device) for device in hosts["host1"]]}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(deploy_ctx, "work_directory", directory), \
                mock.patch.object(configs.agent_client, "post_json", new_callable=mock.AsyncMock, return_value=response):
            self.assertTrue(await configs.gen_yaml("config.yaml", hosts, config, 1, 4, 1, False))
            with open(os.path.join(directory, "config.yaml")) as config_file:
                generated = yaml.safe_load(config_file)["config"]

        self.assertEqual(len(generated["hosts"]), 4)
        self.assertEqual(len(generated["host_configs"]), 4)
        for node, host_config in enumerate(generated["host_configs"]):
            self.assertEqual(list(host_config["ssd"]), [f"/dev/disk/by-partlabel/{label}" for label in labels[node * 4:node * 4 + 4]])
