import argparse
import os
import tempfile
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import parser_factory
from ydb.tools.mnc.cli.commands import qemu
from ydb.tools.mnc.lib import deploy_ctx, progress, term


class QemuCommandTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._old_work_directory = deploy_ctx.work_directory
        self._old_deploy_path = deploy_ctx.deploy_path
        self._old_source_root = deploy_ctx.source_root
        deploy_ctx.deploy_path = "/mnc"
        deploy_ctx.source_root = "/repo"

    def tearDown(self):
        deploy_ctx.work_directory = self._old_work_directory
        deploy_ctx.deploy_path = self._old_deploy_path
        deploy_ctx.source_root = self._old_source_root

    def config(self):
        return {
            "hosts": ["host1", "host2"],
            "build_args": ["--build", "relwithdebinfo"],
        }

    def args(self, **kwargs):
        values = {
            "config": self.config(),
            "host": "host1",
            "remote_dir": None,
        }
        values.update(kwargs)
        return types.SimpleNamespace(**values)

    def test_parser_accepts_prepare_args(self):
        parser = argparse.ArgumentParser()
        qemu.add_arguments(parser)

        args = parser.parse_args([
            "prepare",
            "--config",
            "cfg",
            "--host",
            "host1",
            "--rootfs-img",
            "rootfs.img",
            "--qemu-bin-tar",
            "qemu-bin.tar.gz",
            "--blockstore-client-bin",
            "blockstore-client",
        ])

        self.assertEqual(args.cmd, "prepare")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.host, "host1")
        self.assertEqual(args.blockstore_client_bin, "blockstore-client")

    def test_top_level_parser_contains_qemu_command(self):
        parser, actions, expected_config, _ = parser_factory.build_parser()

        args = parser.parse_args([
            "qemu",
            "start-endpoint",
            "--config",
            "cfg",
            "--host",
            "host1",
            "--disk-id",
            "disk1",
        ])

        self.assertEqual(args.verb, "qemu")
        self.assertEqual(args.cmd, "start-endpoint")
        self.assertIn("qemu", actions)
        self.assertIsNotNone(expected_config["qemu"])

    def test_generated_script_contains_vhost_device(self):
        script = qemu.generate_run_qemu_script()

        self.assertIn("vhost-user-blk-pci", script)
        self.assertIn("qemu-system-x86_64", script)
        self.assertIn("qemu-bin.tar.gz", script)
        self.assertNotIn("\n    -s\n", script)
        self.assertNotIn("-gdb", script)

    def test_endpoint_commands(self):
        self.assertEqual(qemu.default_socket_path("disk1"), "/tmp/disk1.sock")
        self.assertEqual(
            qemu.build_stop_endpoint_command("/remote", "/tmp/disk1.sock"),
            "/remote/blockstore-client stopendpoint --socket /tmp/disk1.sock",
        )
        start_cmd = qemu.build_start_endpoint_command(
            "/remote",
            "disk1",
            "/tmp/disk1.sock",
            "client-1",
            "localhost",
        )

        self.assertIn("startendpoint", start_cmd)
        self.assertIn("--ipc-type vhost", start_cmd)
        self.assertIn("--disk-id disk1", start_cmd)
        self.assertIn("--persistent", start_cmd)

    def test_qemu_run_command_contains_expected_args(self):
        cmd = qemu.build_qemu_run_command(
            "/remote",
            "disk1",
            "/tmp/disk1.sock",
            2222,
            3333,
            "8G",
            "2",
        )

        self.assertIn("/remote/run_qemu.sh", cmd)
        self.assertIn("--disk-id disk1", cmd)
        self.assertIn("--socket /tmp/disk1.sock", cmd)
        self.assertIn("--ssh-port 2222", cmd)
        self.assertIn("--qmp-port 3333", cmd)
        self.assertIn("> /remote/qemu-disk1.log 2>&1", cmd)
        self.assertIn("echo $! > /mnc/run/qemu-disk1.pid", cmd)

    def test_parser_accepts_ssh_args(self):
        parser = argparse.ArgumentParser()
        qemu.add_arguments(parser)

        args = parser.parse_args([
            "ssh",
            "--config",
            "cfg",
            "--host",
            "host1",
            "--ssh-port",
            "2222",
            "--vm-user",
            "ubuntu",
            "--identity-file",
            "/remote/key",
        ])

        self.assertEqual(args.cmd, "ssh")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.host, "host1")
        self.assertEqual(args.ssh_port, 2222)
        self.assertEqual(args.vm_user, "ubuntu")
        self.assertEqual(args.identity_file, "/remote/key")

    def test_qemu_ssh_command_defaults_to_root_on_forwarded_port(self):
        cmd = qemu.build_qemu_ssh_command("host1", "root", 8679)

        self.assertIn("ssh -A -t host1", cmd)
        self.assertIn("ssh -t", cmd)
        self.assertIn("-p 8679", cmd)
        self.assertIn("root@127.0.0.1", cmd)

    def test_qemu_ssh_command_honors_identity_file(self):
        cmd = qemu.build_qemu_ssh_command("host1", "ubuntu", 2222, "/remote/key")

        self.assertIn("-p 2222", cmd)
        self.assertIn("-i /remote/key", cmd)
        self.assertIn("ubuntu@127.0.0.1", cmd)

    async def test_prepare_copies_assets_and_script(self):
        shell_calls = []
        ssh_calls = []

        async def shell(cmd, **kwargs):
            shell_calls.append((cmd, kwargs))
            return term.Result(0, "", "")

        async def ssh_run(host, cmd, **kwargs):
            ssh_calls.append((host, cmd, kwargs))
            return term.Result(0, "", "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            deploy_ctx.work_directory = tmp_dir
            rootfs = os.path.join(tmp_dir, "rootfs.img")
            qemu_tar = os.path.join(tmp_dir, "qemu-bin.tar.gz")
            blockstore = os.path.join(tmp_dir, "blockstore-client")
            for path in (rootfs, qemu_tar, blockstore):
                with open(path, "w") as file:
                    file.write("data")

            args = self.args(
                cmd="prepare",
                rootfs_img=rootfs,
                qemu_bin_tar=qemu_tar,
                blockstore_client_bin=blockstore,
            )

            with mock.patch.object(qemu.term, "shell", shell), mock.patch.object(qemu.term, "ssh_run", ssh_run):
                result = await qemu.act_prepare(args)

        self.assertTrue(result)
        self.assertEqual(ssh_calls[0][1], "mkdir -p /mnc/qemu")
        shell_commands = [call[0] for call in shell_calls]
        self.assertTrue(any("rootfs.img" in cmd and "host1:/mnc/qemu/rootfs.img" in cmd for cmd in shell_commands))
        self.assertTrue(any("qemu-bin.tar.gz" in cmd and "host1:/mnc/qemu/qemu-bin.tar.gz" in cmd for cmd in shell_commands))
        self.assertTrue(any("blockstore-client" in cmd and "host1:/mnc/qemu/blockstore-client" in cmd for cmd in shell_commands))
        self.assertTrue(any("run_qemu.sh" in cmd and "host1:/mnc/qemu/run_qemu.sh" in cmd for cmd in shell_commands))
        self.assertIn("chmod +x /mnc/qemu/blockstore-client /mnc/qemu/run_qemu.sh", ssh_calls[-1][1])

    async def test_prepare_missing_asset_fails_before_ssh(self):
        args = self.args(
            cmd="prepare",
            rootfs_img="/missing/rootfs.img",
            qemu_bin_tar="/missing/qemu-bin.tar.gz",
            blockstore_client_bin="/missing/blockstore-client",
        )

        result = await qemu.act_prepare(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("does not exist", result.message)

    async def test_resolve_blockstore_client_requires_existing_explicit_binary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_path = os.path.join(tmp_dir, "blockstore-client")
            with open(bin_path, "w") as file:
                file.write("bin")
            args = self.args(
                blockstore_client_bin=bin_path,
            )

            result = await qemu.resolve_blockstore_client_bin(args)

        self.assertEqual(result, bin_path)

    async def test_start_endpoint_runs_remote_blockstore_client(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd, kwargs))
            return term.Result(0, "", "")

        args = self.args(
            cmd="start-endpoint",
            disk_id="disk1",
            socket=None,
            client_id="client-1",
            instance_id="localhost",
            encryption_key_path=None,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_start_endpoint(args)

        self.assertTrue(result)
        self.assertEqual(calls[0][0], "host1")
        self.assertIn("stopendpoint --socket /tmp/disk1.sock || true", calls[0][1])
        self.assertIn("startendpoint --ipc-type vhost", calls[0][1])
        self.assertIn("test -S /tmp/disk1.sock", calls[0][1])

    async def test_stop_endpoint_runs_remote_blockstore_client(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd, kwargs))
            return term.Result(0, "", "")

        args = self.args(cmd="stop-endpoint", disk_id="disk1", socket=None)

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_stop_endpoint(args)

        self.assertTrue(result)
        self.assertEqual(calls[0][1], "/mnc/qemu/blockstore-client stopendpoint --socket /tmp/disk1.sock")

    async def test_run_starts_endpoint_and_qemu(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd, kwargs))
            return term.Result(0, "", "")

        args = self.args(
            cmd="run",
            disk_id="disk1",
            socket=None,
            client_id="client-1",
            instance_id="localhost",
            encryption_key_path=None,
            no_start_endpoint=False,
            ssh_port=2222,
            qmp_port=3333,
            memory="8G",
            smp="2",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_run(args)

        self.assertTrue(result)
        self.assertIn("startendpoint", calls[0][1])
        self.assertIn("nohup /mnc/qemu/run_qemu.sh", calls[1][1])
        self.assertIn("--ssh-port 2222", calls[1][1])

    async def test_run_can_skip_endpoint_start(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd, kwargs))
            return term.Result(0, "", "")

        args = self.args(
            cmd="run",
            disk_id="disk1",
            socket=None,
            client_id="client-1",
            instance_id="localhost",
            encryption_key_path=None,
            no_start_endpoint=True,
            ssh_port=2222,
            qmp_port=3333,
            memory="8G",
            smp="2",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_run(args)

        self.assertTrue(result)
        self.assertEqual(len(calls), 1)
        self.assertIn("nohup /mnc/qemu/run_qemu.sh", calls[0][1])

    async def test_ssh_runs_interactive_command(self):
        calls = []

        async def run_interactive_shell(cmd):
            calls.append(cmd)
            return 0

        args = self.args(
            cmd="ssh",
            ssh_port=2222,
            vm_user="ubuntu",
            identity_file="/remote/key",
        )

        with mock.patch.object(qemu, "run_interactive_shell", run_interactive_shell):
            result = await qemu.act_ssh(args)

        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertIn("ssh -A -t host1", calls[0])
        self.assertIn("-p 2222", calls[0])
        self.assertIn("ubuntu@127.0.0.1", calls[0])

    async def test_ssh_rejects_unknown_host_before_launch(self):
        calls = []

        async def run_interactive_shell(cmd):
            calls.append(cmd)
            return 0

        args = self.args(
            cmd="ssh",
            host="unknown",
            ssh_port=8679,
            vm_user="root",
            identity_file=None,
        )

        with mock.patch.object(qemu, "run_interactive_shell", run_interactive_shell):
            with self.assertRaisesRegex(Exception, "not listed"):
                await qemu.act_ssh(args)

        self.assertEqual(calls, [])
