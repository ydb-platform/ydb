import argparse
import io
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import parser_factory
from ydb.tools.mnc.cli.commands import nbs
from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.lib.exceptions import CliError


class NbsCommandTest(unittest.IsolatedAsyncioTestCase):
    def config(self):
        return {
            "hosts": ["host1"],
            "nodes_per_host": 1,
            "ports": None,
            "nbs": {
                "enabled": True,
                "database": "NBS",
                "folder_id": "testFolder",
                "storage_pool_kind": "ssd",
                "pipe_client_retry_count": 3,
                "pipe_client_min_retry_time": 1,
                "pipe_client_max_retry_time": 10,
            },
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
        }

    def test_parser_accepts_create_disk_args(self):
        parser = argparse.ArgumentParser()
        nbs.add_arguments(parser)

        args = parser.parse_args([
            "create-disk",
            "--config",
            "cfg",
            "--disk-id",
            "disk1",
            "--blocks-count",
            "1048576",
        ])

        self.assertEqual(args.cmd, "create-disk")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.disk_id, "disk1")
        self.assertEqual(args.blocks_count, 1048576)
        self.assertEqual(args.block_size, 4096)
        self.assertEqual(args.pool, "ddp1")
        self.assertEqual(args.disk_type, "ssd")
        self.assertEqual(args.sync_requests_batch_size, 100)

    def test_parser_requires_disk_id(self):
        parser = argparse.ArgumentParser()
        nbs.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["create-disk", "--blocks-count", "1"])

    def test_parser_requires_blocks_count(self):
        parser = argparse.ArgumentParser()
        nbs.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["create-disk", "--disk-id", "disk1"])

    def test_top_level_parser_contains_nbs_command(self):
        parser, actions, expected_config, _ = parser_factory.build_parser()

        args = parser.parse_args([
            "nbs",
            "get-load-actor-adapter-id",
            "--config",
            "cfg",
            "--disk-id",
            "disk1",
        ])

        self.assertEqual(args.verb, "nbs")
        self.assertEqual(args.cmd, "get-load-actor-adapter-id")
        self.assertIn("nbs", actions)
        self.assertIsNotNone(expected_config["nbs"])

    def test_build_create_partition_request(self):
        request = nbs.build_create_partition_request(
            disk_id="disk1",
            blocks_count=1048576,
            block_size=4096,
            pool="ddp1",
            disk_type="ssd",
            sync_requests_batch_size=100,
        )

        self.assertEqual(request.DiskId, "disk1")
        self.assertEqual(request.BlockSize, 4096)
        self.assertEqual(request.BlocksCount, 1048576)
        self.assertEqual(request.StoragePoolName, "ddp1")
        self.assertEqual(request.StorageMedia, nbs.dstool_nbs_partition_create.nbs.StorageMediaKind.STORAGE_MEDIA_DEFAULT)
        self.assertEqual(request.SyncRequestsBatchSize, 100)

    def test_build_get_actor_request(self):
        request = nbs.build_get_load_actor_adapter_actor_id_request("disk1")

        self.assertEqual(request.DiskId, "disk1")

    def test_build_io_requests(self):
        write_request = nbs.build_write_blocks_request("disk1", 7, "payload")
        read_request = nbs.build_read_blocks_request("disk1", 8, 3)

        self.assertEqual(write_request.DiskId, "disk1")
        self.assertEqual(write_request.StartIndex, 7)
        self.assertEqual(write_request.Blocks.Buffers, [b"payload"])
        self.assertEqual(read_request.DiskId, "disk1")
        self.assertEqual(read_request.StartIndex, 8)
        self.assertEqual(read_request.BlocksCount, 3)

    def test_parser_accepts_get_actor_args(self):
        parser = argparse.ArgumentParser()
        nbs.add_arguments(parser)

        args = parser.parse_args([
            "get-load-actor-adapter-id",
            "--config",
            "cfg",
            "--disk-id",
            "disk1",
        ])

        self.assertEqual(args.cmd, "get-load-actor-adapter-id")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.disk_id, "disk1")

    def test_parser_accepts_io_args(self):
        parser = argparse.ArgumentParser()
        nbs.add_arguments(parser)

        args = parser.parse_args([
            "io",
            "--config",
            "cfg",
            "--id",
            "disk1",
            "--start_index",
            "5",
            "--blocks_count",
            "2",
            "--type",
            "read",
        ])

        self.assertEqual(args.cmd, "io")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.disk_id, "disk1")
        self.assertEqual(args.start_index, 5)
        self.assertEqual(args.blocks_count, 2)
        self.assertEqual(args.operation_type, "read")

    async def test_create_disk_invokes_dstool_library_with_default_endpoint(self):
        calls = []

        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            calls.append((endpoint, request_type, request, verbose, quiet))
            return object()

        args = types.SimpleNamespace(
            config=self.config(),
            endpoint=None,
            disk_id="disk1",
            blocks_count=1048576,
            block_size=4096,
            pool="ddp1",
            disk_type="ssd",
            sync_requests_batch_size=100,
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=True), \
                mock.patch.object(nbs, "_format_create_partition_result", return_value="created"):
            result = await nbs.do_create_disk(args)

        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertEqual(calls[0][0], "grpc://host1:2135")
        self.assertEqual(calls[0][1], "CreatePartition")
        self.assertEqual(calls[0][2].DiskId, "disk1")
        self.assertEqual(calls[0][2].BlocksCount, 1048576)
        self.assertEqual(calls[0][3], False)
        self.assertEqual(calls[0][4], False)

    async def test_create_disk_uses_custom_endpoint(self):
        calls = []

        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            calls.append(endpoint)
            return object()

        args = types.SimpleNamespace(
            config=self.config(),
            endpoint="grpc://custom:9001",
            disk_id="disk1",
            blocks_count=1,
            block_size=4096,
            pool="pool1",
            disk_type="ssd",
            sync_requests_batch_size=100,
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=True), \
                mock.patch.object(nbs, "_format_create_partition_result", return_value=""):
            result = await nbs.do_create_disk(args)

        self.assertTrue(result)
        self.assertEqual(calls, ["grpc://custom:9001"])

    async def test_create_disk_requires_enabled_nbs(self):
        config = self.config()
        config["nbs"]["enabled"] = False
        args = types.SimpleNamespace(config=config)

        with self.assertRaisesRegex(CliError, "nbs.enabled"):
            await nbs.do_create_disk(args)

    async def test_create_disk_query_failure_returns_task_result_error(self):
        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            raise nbs.dstool_common.QueryError("bad response")

        args = types.SimpleNamespace(
            config=self.config(),
            endpoint="grpc://host1:2135",
            disk_id="disk1",
            blocks_count=1,
            block_size=4096,
            pool="ddp1",
            disk_type="ssd",
            sync_requests_batch_size=100,
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request):
            result = await nbs.do_create_disk(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("bad response", result.message)

    async def test_create_disk_failed_response_returns_task_result_error(self):
        response = object()

        args = types.SimpleNamespace(
            config=self.config(),
            endpoint="grpc://host1:2135",
            disk_id="disk1",
            blocks_count=1,
            block_size=4096,
            pool="ddp1",
            disk_type="ssd",
            sync_requests_batch_size=100,
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", return_value=response), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=False):
            result = await nbs.do_create_disk(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)

    async def test_get_actor_invokes_dstool_library(self):
        calls = []

        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            calls.append((endpoint, request_type, request))
            return object()

        args = types.SimpleNamespace(
            cmd="get-load-actor-adapter-id",
            config=self.config(),
            endpoint="grpc://host1:2135",
            disk_id="disk1",
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=True), \
                mock.patch.object(nbs, "_format_get_actor_result", return_value="[1:2:3]"):
            result = await nbs.do_get_load_actor_adapter_id(args)

        self.assertTrue(result)
        self.assertIn("[1:2:3]", result.message)
        self.assertEqual(calls[0][1], "GetLoadActorAdapterActorId")
        self.assertEqual(calls[0][2].DiskId, "disk1")

    async def test_io_write_invokes_dstool_library(self):
        calls = []

        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            calls.append((endpoint, request_type, request))
            return object()

        args = types.SimpleNamespace(
            cmd="io",
            config=self.config(),
            endpoint="grpc://host1:2135",
            disk_id="disk1",
            start_index=4,
            blocks_count=1,
            data="payload",
            operation_type="write",
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=True):
            result = await nbs.do_io(args)

        self.assertTrue(result)
        self.assertEqual(calls[0][1], "WriteBlocks")
        self.assertEqual(calls[0][2].DiskId, "disk1")
        self.assertEqual(calls[0][2].StartIndex, 4)
        self.assertEqual(calls[0][2].Blocks.Buffers, [b"payload"])

    async def test_io_read_invokes_dstool_library(self):
        calls = []

        def invoke_nbs_request(endpoint, request_type, request, verbose=False, quiet=False):
            calls.append((endpoint, request_type, request))
            return object()

        args = types.SimpleNamespace(
            cmd="io",
            config=self.config(),
            endpoint="grpc://host1:2135",
            disk_id="disk1",
            start_index=4,
            blocks_count=2,
            data="payload",
            operation_type="read",
            verbose=False,
            quiet=False,
        )

        with mock.patch.object(nbs, "invoke_nbs_request", invoke_nbs_request), \
                mock.patch.object(nbs.dstool_common, "get_status", return_value=True), \
                mock.patch.object(nbs, "_format_read_blocks_result", return_value="payload"):
            result = await nbs.do_io(args)

        self.assertTrue(result)
        self.assertIn("payload", result.message)
        self.assertEqual(calls[0][1], "ReadBlocks")
        self.assertEqual(calls[0][2].DiskId, "disk1")
        self.assertEqual(calls[0][2].StartIndex, 4)
        self.assertEqual(calls[0][2].BlocksCount, 2)
