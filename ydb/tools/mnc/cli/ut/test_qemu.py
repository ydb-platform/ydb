import argparse
import os
import shutil
import subprocess
import tempfile
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import parser_factory
from ydb.tools.mnc.cli.commands import qemu
from ydb.tools.mnc.lib import deploy_ctx, progress, term
from ydb.tools.mnc.lib.exceptions import CliError


BASH = shutil.which("bash")


def _bash_syntax(script_text):
    return subprocess.run(
        [BASH, "-n"], input=script_text, capture_output=True, text=True, timeout=10,
    )


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
            "ssh_verbose": False,
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
        self.assertIn("</dev/null", cmd)
        self.assertIn("echo $! > /mnc/run/qemu-disk1.pid", cmd)
        # the command emits a log tail and ps info on stdout so the caller
        # can show what QEMU printed during startup
        self.assertIn("QEMU log tail", cmd)
        self.assertIn("tail -n", cmd)
        self.assertIn("/remote/qemu-disk1.log", cmd)
        self.assertIn("ps -fp", cmd)

    def test_parser_accepts_status_args(self):
        parser = argparse.ArgumentParser()
        qemu.add_arguments(parser)

        args = parser.parse_args([
            "status",
            "--config",
            "cfg",
            "--host",
            "host1",
            "--disk-id",
            "disk1",
            "--ssh-port",
            "2222",
            "--qmp-port",
            "3333",
        ])

        self.assertEqual(args.cmd, "status")
        self.assertEqual(args.config_name, "cfg")
        self.assertEqual(args.host, "host1")
        self.assertEqual(args.disk_id, "disk1")
        self.assertEqual(args.ssh_port, 2222)
        self.assertEqual(args.qmp_port, 3333)

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
        self.assertFalse(args.ssh_verbose)

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

    def test_qemu_status_command_contains_main_checks(self):
        cmd = qemu.build_qemu_status_command("/remote", "disk1", "/tmp/disk1.sock", 2222, 3333)

        self.assertIn("disk_id: %s", cmd)
        self.assertIn("/mnc/run/qemu-disk1.pid", cmd)
        self.assertIn("/remote/qemu-disk1.log", cmd)
        self.assertIn("/tmp/disk1.sock", cmd)
        self.assertIn("ps -fp", cmd)
        self.assertIn("ss -lntp", cmd)
        self.assertIn("query-status", cmd)
        self.assertIn("nc -vz 127.0.0.1 $ssh_port", cmd)
        self.assertIn("tail -100", cmd)

    def test_qemu_ssh_command_can_enable_verbose_mode(self):
        cmd = qemu.build_qemu_ssh_command("host1", "ubuntu", 2222, "/remote/key", ssh_verbose=True)

        self.assertIn("ssh -A -t -vvv host1", cmd)
        self.assertIn("ssh -t -vvv", cmd)
        self.assertIn("-p 2222", cmd)
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

    async def test_status_collects_remote_qemu_info(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd, kwargs))
            return term.Result(0, "status-output", "")

        args = self.args(
            cmd="status",
            disk_id="disk1",
            socket=None,
            ssh_port=2222,
            qmp_port=3333,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_status(args)

        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertEqual(result.message, "status-output")
        self.assertEqual(calls[0][0], "host1")
        self.assertIn("/mnc/run/qemu-disk1.pid", calls[0][1])
        self.assertIn("ssh_port=2222", calls[0][1])
        self.assertEqual(calls[0][2]["step_title"], "QEMU status")

    async def test_do_status_prints_status_result_without_progress_wrapper(self):
        printed = []

        class Console:
            def print(self, value):
                printed.append(value)

        async def act_status(args):
            return progress.TaskResult(
                level=progress.TaskResultLevel.OK,
                step_title="QEMU status",
                message="status-output",
            )

        async def run_steps(*args, **kwargs):
            raise AssertionError("status should not be wrapped into run_steps")

        args = self.args(cmd="status", verbose=False)

        with mock.patch.object(qemu, "act_status", act_status), \
                mock.patch.object(qemu.progress, "run_steps", run_steps), \
                mock.patch.object(qemu.output, "get_console", lambda: Console()):
            result = await qemu.do(args)

        self.assertTrue(result)
        self.assertEqual(result.message, "status-output")
        self.assertEqual(len(printed), 1)

    async def test_status_failure_preserves_remote_details(self):
        async def ssh_run(host, cmd, **kwargs):
            return term.Result(1, "stdout", "stderr", log_path="/tmp/status.log")

        args = self.args(
            cmd="status",
            disk_id="disk1",
            socket=None,
            ssh_port=2222,
            qmp_port=3333,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_status(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("Failed to collect QEMU status", result.message)
        self.assertIn("stderr", result.message)
        self.assertIn("stdout", result.message)
        self.assertIn("/tmp/status.log", result.message)

    async def test_ssh_failure_returns_command_and_verbose_hint(self):
        async def run_interactive_shell(cmd):
            return 255

        args = self.args(
            cmd="ssh",
            ssh_port=2222,
            vm_user="ubuntu",
            identity_file="/remote/key",
        )

        with mock.patch.object(qemu, "run_interactive_shell", run_interactive_shell):
            result = await qemu.act_ssh(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("return code 255", result.message)
        self.assertIn("Command: ssh -A -t host1", result.message)
        self.assertIn("--ssh-verbose", result.message)

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


@unittest.skipUnless(BASH, "bash not available in test environment")
class QemuBashScriptVerificationTest(unittest.TestCase):
    """Verify generated shell scripts and remote command strings are syntactically valid."""

    def test_generated_run_qemu_script_has_valid_bash_syntax(self):
        result = _bash_syntax(qemu.generate_run_qemu_script())
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_qemu_run_command_has_valid_bash_syntax(self):
        cmd = qemu.build_qemu_run_command(
            "/remote", "disk1", "/tmp/disk1.sock", 2222, 3333, "8G", "2",
        )
        result = _bash_syntax(cmd)
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_qemu_status_command_has_valid_bash_syntax(self):
        cmd = qemu.build_qemu_status_command("/remote", "disk1", "/tmp/disk1.sock", 2222, 3333)
        result = _bash_syntax(cmd)
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_start_endpoint_command_has_valid_bash_syntax(self):
        cmd = qemu.build_start_endpoint_command(
            "/remote", "disk1", "/tmp/disk1.sock", "client-1", "localhost",
        )
        result = _bash_syntax(cmd)
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_stop_endpoint_command_has_valid_bash_syntax(self):
        cmd = qemu.build_stop_endpoint_command("/remote", "/tmp/disk1.sock")
        result = _bash_syntax(cmd)
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_qemu_ssh_command_has_valid_bash_syntax(self):
        cmd = qemu.build_qemu_ssh_command("host1", "ubuntu", 2222, "/remote/key")
        result = _bash_syntax(cmd)
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_run_qemu_script_requires_disk_id(self):
        """The generated run_qemu.sh must exit with code 2 if --disk-id is missing."""
        with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
            f.write(qemu.generate_run_qemu_script())
            path = f.name
        try:
            os.chmod(path, 0o755)
            result = subprocess.run(
                [BASH, path, "--ssh-port", "2222"],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--disk-id is required", result.stderr)
        finally:
            os.unlink(path)

    def test_run_qemu_script_rejects_unknown_option(self):
        with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
            f.write(qemu.generate_run_qemu_script())
            path = f.name
        try:
            os.chmod(path, 0o755)
            result = subprocess.run(
                [BASH, path, "--bogus", "x"],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("Unknown option", result.stderr)
        finally:
            os.unlink(path)

    def test_run_qemu_script_passes_all_cli_args_to_qemu(self):
        """Replace the qemu binary with a stub and confirm CLI options are propagated."""
        script = qemu.generate_run_qemu_script()
        # Stub tar extraction and replace exec with echo to capture the invocation.
        script = script.replace(
            'tar -xzf "$QEMU_TAR" -C "$SCRIPT_DIR"',
            'echo "stub tar"',
        )
        script = script.replace('exec "$QEMU"', 'echo QEMU_CMD: "$QEMU"')

        with tempfile.TemporaryDirectory() as work:
            script_path = os.path.join(work, "run_qemu.sh")
            with open(script_path, "w") as f:
                f.write(script)
            os.chmod(script_path, 0o755)
            # Pre-create the stub qemu binary so the `-x` check passes.
            qemu_dir = os.path.join(work, "usr", "bin")
            os.makedirs(qemu_dir, exist_ok=True)
            qemu_bin = os.path.join(qemu_dir, "qemu-system-x86_64")
            with open(qemu_bin, "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(qemu_bin, 0o755)

            result = subprocess.run(
                [
                    BASH, script_path,
                    "--disk-id", "diskA",
                    "--socket", "/tmp/sockA",
                    "--ssh-port", "2200",
                    "--qmp-port", "3300",
                    "--memory", "4G",
                    "--smp", "2,sockets=1,cores=2,threads=1",
                ],
                capture_output=True, text=True, timeout=10,
            )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        out = result.stdout
        self.assertIn("hostfwd=tcp::2200-:22", out)
        self.assertIn("tcp:127.0.0.1:3300", out)
        self.assertIn("-m 4G", out)
        self.assertIn("size=4G", out)
        self.assertIn("-smp 2,sockets=1,cores=2,threads=1", out)
        self.assertIn("path=/tmp/sockA", out)
        self.assertIn("vhost-user-blk-pci", out)
        self.assertIn("virtio-blk-pci", out)
        self.assertIn("-enable-kvm", out)
        self.assertIn("-snapshot", out)

    def test_run_qemu_script_defaults(self):
        """If only --disk-id is provided, defaults should be used."""
        script = qemu.generate_run_qemu_script()
        script = script.replace(
            'tar -xzf "$QEMU_TAR" -C "$SCRIPT_DIR"',
            'echo "stub tar"',
        )
        script = script.replace('exec "$QEMU"', 'echo QEMU_CMD: "$QEMU"')
        with tempfile.TemporaryDirectory() as work:
            script_path = os.path.join(work, "run_qemu.sh")
            with open(script_path, "w") as f:
                f.write(script)
            os.chmod(script_path, 0o755)
            qemu_dir = os.path.join(work, "usr", "bin")
            os.makedirs(qemu_dir, exist_ok=True)
            qemu_bin = os.path.join(qemu_dir, "qemu-system-x86_64")
            with open(qemu_bin, "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(qemu_bin, 0o755)

            result = subprocess.run(
                [BASH, script_path, "--disk-id", "diskB"],
                capture_output=True, text=True, timeout=10,
            )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        out = result.stdout
        self.assertIn("hostfwd=tcp::8679-:22", out)
        self.assertIn("tcp:127.0.0.1:8678", out)
        self.assertIn("-m 16G", out)
        self.assertIn("path=/tmp/diskB.sock", out)


class QemuExtraTest(unittest.IsolatedAsyncioTestCase):
    """Cover encryption flow, custom paths, and dispatch branches."""

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
        return {"hosts": ["host1"], "build_args": []}

    def args(self, **kwargs):
        values = {
            "config": self.config(),
            "host": "host1",
            "remote_dir": None,
            "ssh_verbose": False,
        }
        values.update(kwargs)
        return types.SimpleNamespace(**values)

    def test_start_endpoint_command_includes_encryption_args(self):
        encryption_args = ["--encryption-mode", "aes-xts", "--encryption-key-path", "/secret/key"]
        cmd = qemu.build_start_endpoint_command(
            "/remote", "disk1", "/tmp/disk1.sock", "client-1", "localhost", encryption_args,
        )
        self.assertIn("--encryption-mode aes-xts", cmd)
        self.assertIn("--encryption-key-path /secret/key", cmd)
        # encryption args must come after the persistent flag in our current layout
        self.assertLess(cmd.index("--persistent"), cmd.index("--encryption-mode"))

    async def test_act_start_endpoint_passes_encryption_args(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append((host, cmd))
            return term.Result(0, "", "")

        args = self.args(
            cmd="start-endpoint",
            disk_id="disk1",
            socket="/var/run/disk1.sock",
            client_id="c1",
            instance_id="inst1",
            encryption_key_path="/secret/key",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_start_endpoint(args)

        self.assertTrue(result)
        self.assertEqual(len(calls), 1)
        remote_cmd = calls[0][1]
        self.assertIn("--encryption-mode aes-xts", remote_cmd)
        self.assertIn("--encryption-key-path /secret/key", remote_cmd)
        # custom socket must be used everywhere
        self.assertIn("--socket /var/run/disk1.sock", remote_cmd)
        self.assertIn("test -S /var/run/disk1.sock", remote_cmd)

    async def test_act_start_endpoint_uses_custom_remote_dir(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append(cmd)
            return term.Result(0, "", "")

        args = self.args(
            cmd="start-endpoint",
            remote_dir="/srv/qemu",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            await qemu.act_start_endpoint(args)

        self.assertIn("/srv/qemu/blockstore-client startendpoint", calls[0])

    async def test_act_stop_endpoint_uses_custom_socket(self):
        calls = []

        async def ssh_run(host, cmd, **kwargs):
            calls.append(cmd)
            return term.Result(0, "", "")

        args = self.args(
            cmd="stop-endpoint",
            disk_id="disk1",
            socket="/var/run/disk1.sock",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            await qemu.act_stop_endpoint(args)

        self.assertEqual(
            calls[0],
            "/mnc/qemu/blockstore-client stopendpoint --socket /var/run/disk1.sock",
        )

    async def test_act_start_endpoint_rejects_unknown_host(self):
        async def ssh_run(host, cmd, **kwargs):
            raise AssertionError("ssh_run should not be called for unknown hosts")

        args = self.args(
            cmd="start-endpoint",
            host="bogus",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            with self.assertRaisesRegex(CliError, "not listed"):
                await qemu.act_start_endpoint(args)

    async def test_act_run_skips_endpoint_when_requested(self):
        ssh_calls = []

        async def ssh_run(host, cmd, **kwargs):
            ssh_calls.append(cmd)
            return term.Result(0, "", "")

        args = self.args(
            cmd="run",
            disk_id="disk1",
            socket="/var/run/d1.sock",
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
            no_start_endpoint=True,
            ssh_port=2200,
            qmp_port=3300,
            memory="4G",
            smp="2",
            remote_dir="/srv/qemu",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_run(args)

        self.assertTrue(result)
        self.assertEqual(len(ssh_calls), 1)
        cmd = ssh_calls[0]
        self.assertIn("/srv/qemu/run_qemu.sh", cmd)
        self.assertIn("--socket /var/run/d1.sock", cmd)
        self.assertIn("--ssh-port 2200", cmd)
        self.assertIn("--qmp-port 3300", cmd)
        self.assertIn("--memory 4G", cmd)
        self.assertIn("</dev/null", cmd)

    async def test_act_prepare_stops_on_first_failure(self):
        # The first ssh call (mkdir) fails — no subsequent rsync should be made.
        shell_calls = []
        ssh_calls = []

        async def shell(cmd, **kwargs):
            shell_calls.append(cmd)
            return term.Result(0, "", "")

        async def ssh_run(host, cmd, **kwargs):
            ssh_calls.append(cmd)
            return term.Result(1, "", "mkdir failed", log_path="/var/log/mkdir.log")

        with tempfile.TemporaryDirectory() as tmp_dir:
            deploy_ctx.work_directory = tmp_dir
            rootfs = os.path.join(tmp_dir, "rootfs.img")
            qemu_tar = os.path.join(tmp_dir, "qemu-bin.tar.gz")
            blockstore = os.path.join(tmp_dir, "blockstore-client")
            for path in (rootfs, qemu_tar, blockstore):
                with open(path, "w") as file:
                    file.write("x")

            args = self.args(
                cmd="prepare",
                rootfs_img=rootfs,
                qemu_bin_tar=qemu_tar,
                blockstore_client_bin=blockstore,
            )

            with mock.patch.object(qemu.term, "shell", shell), \
                    mock.patch.object(qemu.term, "ssh_run", ssh_run):
                result = await qemu.act_prepare(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("mkdir failed", result.message)
        self.assertEqual(len(ssh_calls), 1)
        # No rsync attempts on local shell.
        self.assertEqual(shell_calls, [])

    def test_make_command_step_unknown_cmd_raises(self):
        args = self.args(cmd="bogus", disk_id="d1")
        with self.assertRaisesRegex(CliError, "Unknown QEMU command"):
            qemu.make_command_step(args)

    def test_make_command_step_returns_step_for_each_known_cmd(self):
        common = dict(
            disk_id="d1",
            socket=None,
            client_id="c1",
            instance_id="inst",
            encryption_key_path=None,
            no_start_endpoint=False,
            ssh_port=2222,
            qmp_port=3333,
            memory="8G",
            smp="2",
            rootfs_img="r",
            qemu_bin_tar="q",
            blockstore_client_bin="b",
        )
        for cmd in ("prepare", "run", "status", "start-endpoint", "stop-endpoint"):
            with self.subTest(cmd=cmd):
                step = qemu.make_command_step(self.args(cmd=cmd, **common))
                self.assertIsNotNone(step)

    def test_run_qemu_script_chmod_is_executable(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "sub", "run_qemu.sh")
            qemu.write_local_run_script(path)
            self.assertTrue(os.path.exists(path))
            self.assertTrue(os.access(path, os.X_OK))


class QemuLogSurfaceTest(unittest.IsolatedAsyncioTestCase):
    """Verify that act_* commands surface remote stdout/stderr in their success message."""

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

    def args(self, **kwargs):
        values = {
            "config": {"hosts": ["host1"], "build_args": []},
            "host": "host1",
            "remote_dir": None,
            "ssh_verbose": False,
        }
        values.update(kwargs)
        return types.SimpleNamespace(**values)

    async def test_act_run_surfaces_qemu_stdout_in_success_message(self):
        async def ssh_run(host, cmd, **kwargs):
            return term.Result(
                0,
                "== QEMU log tail (50 lines) ==\nQEMU starting up\n== QEMU process ==\nps line",
                "",
            )

        args = self.args(
            cmd="run",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
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
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertIn("QEMU started on host1", result.message)
        self.assertIn("QEMU log tail", result.message)
        self.assertIn("QEMU starting up", result.message)
        self.assertIn("ps line", result.message)

    async def test_act_run_includes_remote_log_path_in_failure_message(self):
        async def ssh_run(host, cmd, **kwargs):
            return term.Result(
                1,
                "stub-qemu output before crash",
                "qemu-system-x86_64: failed",
                log_path="/tmp/ssh.log",
            )

        args = self.args(
            cmd="run",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
            no_start_endpoint=True,
            ssh_port=2222,
            qmp_port=3333,
            memory="8G",
            smp="2",
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_run(args)

        self.assertFalse(result)
        self.assertEqual(result.level, progress.TaskResultLevel.ERROR)
        self.assertIn("Failed to run QEMU", result.message)
        self.assertIn("qemu-system-x86_64: failed", result.message)
        self.assertIn("stub-qemu output before crash", result.message)
        self.assertIn("/tmp/ssh.log", result.message)

    async def test_act_start_endpoint_surfaces_remote_stdout_on_success(self):
        async def ssh_run(host, cmd, **kwargs):
            return term.Result(0, "blockstore-client OK\nsocket: srw...", "")

        args = self.args(
            cmd="start-endpoint",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_start_endpoint(args)

        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertIn("NBS endpoint started on host1", result.message)
        self.assertIn("blockstore-client OK", result.message)
        # The remote command should run a socket-check section so the user can
        # see the socket file listing.
        # Verify that the issued remote command includes those probes.

    async def test_act_start_endpoint_command_runs_socket_check_section(self):
        captured = []

        async def ssh_run(host, cmd, **kwargs):
            captured.append(cmd)
            return term.Result(0, "", "")

        args = self.args(
            cmd="start-endpoint",
            disk_id="disk1",
            socket=None,
            client_id="c1",
            instance_id="inst1",
            encryption_key_path=None,
        )

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            await qemu.act_start_endpoint(args)

        cmd = captured[0]
        self.assertIn("blockstore-client startendpoint", cmd)
        self.assertIn("== blockstore-client startendpoint ==", cmd)
        self.assertIn("== socket check ==", cmd)
        # The socket file is listed for visibility
        self.assertIn("ls -l /tmp/disk1.sock", cmd)

    async def test_act_stop_endpoint_surfaces_remote_stdout_on_success(self):
        async def ssh_run(host, cmd, **kwargs):
            return term.Result(0, "endpoint stopped: disk1", "")

        args = self.args(cmd="stop-endpoint", disk_id="disk1", socket=None)

        with mock.patch.object(qemu.term, "ssh_run", ssh_run):
            result = await qemu.act_stop_endpoint(args)

        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.OK)
        self.assertIn("NBS endpoint stopped on host1", result.message)
        self.assertIn("endpoint stopped: disk1", result.message)

    def test_format_command_output_includes_stdout_stderr_and_log(self):
        result = term.Result(0, "out", "warn", log_path="/tmp/x.log")
        formatted = qemu.format_command_output(result, header="header line")
        self.assertIn("header line", formatted)
        self.assertIn("stdout:\nout", formatted)
        self.assertIn("stderr:\nwarn", formatted)
        self.assertIn("Full log: /tmp/x.log", formatted)

    def test_format_command_output_empty_when_no_streams(self):
        result = term.Result(0, "", "")
        self.assertEqual(qemu.format_command_output(result), "")

    def test_qemu_run_command_emits_log_tail_section(self):
        cmd = qemu.build_qemu_run_command(
            "/r", "d1", "/tmp/d1.sock", 2200, 3300, "1G", "1",
        )
        # The success path of the command exits 0 only if the process is alive;
        # otherwise it must emit a clear message and exit 1.
        self.assertIn("QEMU process $pid is not running", cmd)
        self.assertIn("exit 0", cmd)
        self.assertIn("exit 1", cmd)
