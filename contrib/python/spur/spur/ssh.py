from __future__ import unicode_literals
from __future__ import absolute_import

import subprocess
import os
import os.path
import shutil
import contextlib
import uuid
import socket
import traceback
import sys
import io

import paramiko

from .tempdir import create_temporary_dir
from .files import FileOperations
from . import results
from .io import IoHandler, Channel
from .errors import NoSuchCommandError, CommandInitializationError, CouldNotChangeDirectoryError


_ONE_MINUTE = 60


class ConnectionError(Exception):
    pass


class UnsupportedArgumentError(Exception):
    pass


class AcceptParamikoPolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return


class MissingHostKey(object):
    raise_error = paramiko.RejectPolicy()
    warn = paramiko.WarningPolicy()
    auto_add = paramiko.AutoAddPolicy()
    accept = AcceptParamikoPolicy()


class MinimalShellType(object):
    supports_which = False

    def generate_run_command(self, command_args, store_pid,
            cwd=None, update_env={}, new_process_group=False):

        if store_pid:
            raise self._unsupported_argument_error("store_pid")

        if cwd is not None:
            raise self._unsupported_argument_error("cwd")

        if update_env:
            raise self._unsupported_argument_error("update_env")

        if new_process_group:
            raise self._unsupported_argument_error("new_process_group")

        return " ".join(map(escape_sh, command_args))


    def _unsupported_argument_error(self, name):
        return UnsupportedArgumentError("'{0}' is not supported when using a minimal shell".format(name))


class ShShellType(object):
    supports_which = True

    def generate_run_command(self, command_args, store_pid,
            cwd=None, update_env={}, new_process_group=False):
        commands = []

        if store_pid:
            commands.append("echo $$")

        if cwd is not None:
            commands.append("cd {0} 2>&1 || {{ echo '\n'spur-cd: $?; exit 1; }}".format(escape_sh(cwd)))
            commands.append("echo '\n'spur-cd: 0")

        update_env_commands = [
            "export {0}={1}".format(key, escape_sh(value))
            for key, value in _iteritems(update_env)
        ]
        commands += update_env_commands
        which_commands = " || ".join(self._generate_which_commands(command_args[0]))
        which_commands = "{ { " + which_commands + "; } && echo 0; } || { echo $?; exit 1; }"
        commands.append(which_commands)

        command = " ".join(map(escape_sh, command_args))
        command = "exec {0}".format(command)
        if new_process_group:
            command = "setsid {0}".format(command)
        commands.append(command)
        return "; ".join(commands)

    def _generate_which_commands(self, command):
        which_commands = ["command -v {0}", "which {0}"]
        return (
            self._generate_which_command(which, command)
            for which in which_commands
        )

    def _generate_which_command(self, which, command):
        return which.format(escape_sh(command)) + " > /dev/null 2>&1"


class ShellTypes(object):
    minimal = MinimalShellType()
    sh = ShShellType()


class SshShell(object):
    def __init__(self,
            hostname,
            username=None,
            password=None,
            port=None,
            private_key_file=None,
            connect_timeout=None,
            missing_host_key=None,
            shell_type=None,
            look_for_private_keys=True,
            load_system_host_keys=True,
            sock=None):

        if connect_timeout is None:
            connect_timeout = _ONE_MINUTE

        if port is None:
            port = 22

        if shell_type is None:
            shell_type = ShellTypes.sh

        self._hostname = hostname
        self._port = port
        self._username = username
        self._password = password
        self._private_key_file = private_key_file
        self._client = None
        self._connect_timeout = connect_timeout
        self._look_for_private_keys = look_for_private_keys
        self._load_system_host_keys = load_system_host_keys
        self._closed = False
        self._sock = sock

        if missing_host_key is None:
            self._missing_host_key = MissingHostKey.raise_error
        else:
            self._missing_host_key = missing_host_key

        self._shell_type = shell_type

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._closed = True
        if self._client is not None:
            self._client.close()

    def run(self, *args, **kwargs):
        return self.spawn(*args, **kwargs).wait_for_result()

    def spawn(self, command, *args, **kwargs):
        stdout = kwargs.pop("stdout", None)
        stderr = kwargs.pop("stderr", None)
        allow_error = kwargs.pop("allow_error", False)
        store_pid = kwargs.pop("store_pid", False)
        use_pty = kwargs.pop("use_pty", False)
        encoding = kwargs.pop("encoding", None)
        cwd = kwargs.get('cwd')
        command_in_cwd = self._shell_type.generate_run_command(command, *args, store_pid=store_pid, **kwargs)
        try:
            channel = self._get_ssh_transport().open_session()
        except EOFError as error:
            raise self._connection_error(error)
        if use_pty:
            channel.get_pty()
        channel.exec_command(command_in_cwd)

        process_stdout = channel.makefile('rb')

        if store_pid:
            pid = _read_int_initialization_line(process_stdout)

        if cwd is not None:
            cd_output = []
            while True:
                line = process_stdout.readline()
                if line.startswith(b"spur-cd: "):
                    if line.strip() == b"spur-cd: 0":
                        break
                    else:
                        raise CouldNotChangeDirectoryError(cwd, b"".join(cd_output))
                else:
                    cd_output.append(line)

        if self._shell_type.supports_which:
            which_return_code = _read_int_initialization_line(process_stdout)

            if which_return_code != 0:
                raise NoSuchCommandError(command[0])

        process = SshProcess(
            channel,
            allow_error=allow_error,
            process_stdout=process_stdout,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
            shell=self,
        )
        if store_pid:
            process.pid = pid

        return process

    @contextlib.contextmanager
    def temporary_dir(self):
        result = self.run(["mktemp", "--directory"], encoding="ascii")
        temp_dir = result.output.strip()
        try:
            yield temp_dir
        finally:
            self.run(["rm", "-rf", temp_dir])

    def upload_dir(self, local_dir, remote_dir, ignore):
        with create_temporary_dir() as temp_dir:
            content_tarball_path = os.path.join(temp_dir, "content.tar.gz")
            content_path = os.path.join(temp_dir, "content")
            shutil.copytree(local_dir, content_path, ignore=shutil.ignore_patterns(*ignore))
            subprocess.check_call(
                ["tar", "czf", content_tarball_path, "content"],
                cwd=temp_dir
            )
            with self._connect_sftp() as sftp:
                remote_tarball_path = "/tmp/{0}.tar.gz".format(uuid.uuid4())
                sftp.put(content_tarball_path, remote_tarball_path)
                self.run(["mkdir", "-p", remote_dir])
                self.run([
                    "tar", "xzf", remote_tarball_path,
                    "--strip-components", "1", "--directory", remote_dir
                ])

                sftp.remove(remote_tarball_path)

    def open(self, name, mode="r"):
        sftp = self._open_sftp_client()
        sftp_file = SftpFile(sftp, sftp.open(name, mode), mode)

        if "b" not in mode:
            sftp_file = io.TextIOWrapper(sftp_file)

        return sftp_file

    @property
    def files(self):
        return FileOperations(self)

    def _get_ssh_transport(self):
        try:
            return self._connect_ssh().get_transport()
        except (socket.error, paramiko.SSHException, EOFError) as error:
            raise self._connection_error(error)

    def _connect_ssh(self):
        if self._client is None:
            if self._closed:
                raise RuntimeError("Shell is closed")
            client = paramiko.SSHClient()
            if self._load_system_host_keys:
                client.load_system_host_keys()
            client.set_missing_host_key_policy(self._missing_host_key)
            client.connect(
                hostname=self._hostname,
                port=self._port,
                username=self._username,
                password=self._password,
                key_filename=self._private_key_file,
                look_for_keys=self._look_for_private_keys,
                timeout=self._connect_timeout,
                sock=self._sock
            )
            self._client = client
        return self._client

    @contextlib.contextmanager
    def _connect_sftp(self):
        sftp = self._open_sftp_client()
        try:
            yield sftp
        finally:
            sftp.close()

    def _open_sftp_client(self):
        return self._get_ssh_transport().open_sftp_client()

    def _connection_error(self, error):
        connection_error = ConnectionError(
            "Error creating SSH connection\n" +
            "Original error: {0}".format(error)
        )
        connection_error.original_error = error
        connection_error.original_traceback = traceback.format_exc()
        return connection_error


def _read_int_initialization_line(output_file):
    while True:
        line = output_file.readline().strip()
        if line:
            try:
                return int(line)
            except ValueError:
                raise CommandInitializationError(line)


class SftpFile(object):
    def __init__(self, sftp, file, mode):
        self._sftp = sftp
        self._file = file
        self._mode = mode

    def __getattr__(self, key):
        return getattr(self._file, key)

    def close(self):
        try:
            self._file.close()
        finally:
            self._sftp.close()

    def readable(self):
        return "r" in self._mode or "+" in self._mode

    def writable(self):
        return "w" in self._mode or "+" in self._mode or "a" in self._mode

    def seekable(self):
        return True


    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def escape_sh(value):
    return "'" + value.replace("'", "'\\''") + "'"


class SshProcess(object):
    def __init__(self, channel, allow_error, process_stdout, stdout, stderr, encoding, shell):
        self._channel = channel
        self._allow_error = allow_error
        self._stdin = channel.makefile('wb')
        self._stdout = process_stdout
        self._stderr = channel.makefile_stderr('rb')
        self._shell = shell
        self._result = None

        self._io = IoHandler([
            Channel(self._stdout, stdout),
            Channel(self._stderr, stderr),
        ], encoding=encoding)

    def is_running(self):
        return not self._channel.exit_status_ready()

    def stdin_write(self, value):
        self._channel.sendall(value)

    def send_signal(self, signal):
        self._shell.run(["kill", "-{0}".format(signal), str(self.pid)])

    def wait_for_result(self):
        if self._result is None:
            self._result = self._generate_result()

        return self._result

    def _generate_result(self):
        output, stderr_output = self._io.wait()
        return_code = self._channel.recv_exit_status()

        return results.result(
            return_code,
            self._allow_error,
            output,
            stderr_output
        )


if sys.version_info[0] < 3:
    _iteritems = lambda d: d.iteritems()
else:
    _iteritems = lambda d: d.items()
