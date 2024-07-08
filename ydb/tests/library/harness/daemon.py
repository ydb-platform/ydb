# -*- coding: utf-8 -*-
import abc
import logging
import os
import signal
import sys
import subprocess

from yatest.common import process
import six

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.common import yatest_common
from . import param_constants


logger = logging.getLogger(__name__)


def extract_stderr_details(stderr_file, max_lines=0):
    if max_lines == 0:
        return []

    if stderr_file is None:
        return []

    result = ["Stderr content:", ""]
    with open(stderr_file, 'r') as r:
        for line in r.readlines():
            if len(result) >= max_lines:
                break
            result.append(line.strip())
    return result


class DaemonError(RuntimeError):
    def __init__(self, message, stdout, stderr, exit_code, max_stderr_lines=0):

        super(DaemonError, self).__init__(
            '\n'.join(
                [
                    "Daemon failed with message: {message}.".format(message=message),
                    "Process exit_code = {exit_code}.".format(exit_code=exit_code),
                    "Stdout file name: \n{}".format(stdout if stdout is not None else "is not present."),
                    "Stderr file name: \n{}".format(stderr if stderr is not None else "is not present.")
                ] + extract_stderr_details(stderr, max_stderr_lines)
            )
        )


class SeveralDaemonErrors(RuntimeError):
    def __init__(self, exceptions):
        super(SeveralDaemonErrors, self).__init__(
            "\n".join(
                str(x) for x in exceptions
            )
        )


class Daemon(object):
    def __init__(
        self,
        command,
        cwd,
        timeout,
        stdin_file=yatest_common.work_path('stdin'),
        stdout_file=yatest_common.work_path('stdout'),
        stderr_file=yatest_common.work_path('stderr'),
        stderr_on_error_lines=0,
        core_pattern=None
    ):
        self.__cwd = cwd
        self.__timeout = timeout
        self.__command = tuple(command)
        self.__stderr_on_error_lines = stderr_on_error_lines
        self.__daemon = None
        self.killed = False
        self.__core_pattern = core_pattern
        self.logger = logger.getChild(self.__class__.__name__)
        self.__stdout_file = open(stdout_file, mode='w+b')
        self.__stdin_file = open(stdin_file, mode='w+b')
        self.__stderr_file = open(stderr_file, mode='w+b')

    @property
    def daemon(self):
        return self.__daemon

    @property
    def stdin_file_name(self):
        if self.__stdin_file is not sys.stdin:
            return os.path.abspath(self.__stdin_file.name)
        else:
            return None

    @property
    def stdout_file_name(self):
        if self.__stdout_file is not sys.stdout:
            return os.path.abspath(self.__stdout_file.name)
        else:
            return None

    @property
    def stderr_file_name(self):
        if self.__stderr_file is not sys.stderr:
            return os.path.abspath(self.__stderr_file.name)
        else:
            return None

    def is_alive(self):
        return self.__daemon is not None and self.__daemon.running

    def start(self):
        if self.is_alive():
            return
        stderr_stream = self.__stderr_file
        if param_constants.kikimr_stderr_to_console():
            stderr_stream = sys.stderr
        self.__daemon = process.execute(
            self.__command,
            check_exit_code=False,
            cwd=self.__cwd,
            stdin=self.__stdin_file,
            stdout=self.__stdout_file,
            stderr=stderr_stream,
            wait=False,
            core_pattern=self.__core_pattern
        )
        wait_for(self.is_alive, self.__timeout)

        if not self.is_alive():
            self.__check_before_fail()
            raise DaemonError(
                "Unexpectedly finished on start",
                exit_code=self.__daemon.exit_code,
                stdout=self.stdout_file_name,
                stderr=self.stderr_file_name,
                max_stderr_lines=self.__stderr_on_error_lines,
            )

        self.killed = False

        return self

    def __check_before_fail(self):
        self.__daemon.verify_no_coredumps()
        self.__daemon.verify_sanitize_errors()

    @property
    def _acceptable_exit_codes(self):
        return 0, -signal.SIGTERM

    def __check_can_launch_stop(self, stop_type):
        if self.__daemon is None or self.killed:
            return False

        if self.__daemon is not None and self.__daemon.exit_code == 0:
            return False

        if not self.is_alive():
            self.__check_before_fail()
            raise DaemonError(
                "Unexpectedly finished before %s" % stop_type,
                exit_code=self.__daemon.exit_code,
                stdout=self.stdout_file_name,
                stderr=self.stderr_file_name,
                max_stderr_lines=self.__stderr_on_error_lines,
            )

        return True

    def __check_before_end_stop(self, stop_type):
        if self.is_alive():
            msg = "Cannot {stop_type} daemon cmd = {cmd}".format(cmd=' '.join(self.__command), stop_type=stop_type)
            self.logger.error(msg)
            raise DaemonError(
                msg,
                exit_code=self.__daemon.exit_code,
                stdout=self.stdout_file_name,
                stderr=self.stderr_file_name,
                max_stderr_lines=self.__stderr_on_error_lines,
            )

    def stop(self):
        if not self.__check_can_launch_stop("stop"):
            return

        self.__daemon.process.terminate()
        wait_for(lambda: not self.is_alive(), self.__timeout)

        is_killed = False
        if self.is_alive():
            self.__daemon.process.send_signal(signal.SIGKILL)
            wait_for(lambda: not self.is_alive(), self.__timeout)
            is_killed = True
        self.__check_before_end_stop("stop")

        if not is_killed:
            exit_code = self.__daemon.exit_code
            self.__check_before_fail()

            if exit_code not in self._acceptable_exit_codes:
                raise DaemonError(
                    "Bad exit_code.",
                    exit_code=exit_code,
                    stdout=self.stdout_file_name,
                    stderr=self.stderr_file_name,
                    max_stderr_lines=self.__stderr_on_error_lines,
                )
        else:
            self.logger.warning("Exit code is not checked, cos binary was stopped by sigkill")

    def kill(self):
        if not self.__check_can_launch_stop("kill"):
            return

        self.__daemon.process.send_signal(signal.SIGKILL)
        wait_for(lambda: not self.is_alive(), self.__timeout)
        self.killed = True

        self.__check_before_end_stop("kill")


@six.add_metaclass(abc.ABCMeta)
class ExternalNodeDaemon(object):
    def __init__(self, host):
        self._host = host
        self._ssh_username = param_constants.ssh_username
        self.logger = logger.getChild(self.__class__.__name__)

    @property
    def _username_at_host(self):
        return self._host if self._ssh_username is None else self._ssh_username + '@' + self._host

    @property
    def _artifacts_path(self):
        return "/home/%s/kikimr-dir" % self._ssh_username

    def _path_at_host(self, path):
        return ":".join([self._username_at_host, path])

    def ssh_command(self, command_and_params, raise_on_error=False, executable='ssh'):
        if not isinstance(command_and_params, list):
            command_and_params = [command_and_params]

        args = [
            executable,
            "-A", "-o", "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", self._username_at_host]
        args += command_and_params
        return self._run_in_subprocess(args, raise_on_error)

    def copy_file_or_dir(self, file_or_dir, target_path):
        self.ssh_command(['mkdir -p %s' % self._artifacts_path])

        transit_path = os.path.join(self._artifacts_path, os.path.basename(file_or_dir))

        self.ssh_command(['sudo', 'rm', '-fr', target_path], raise_on_error=True)

        self._run_in_subprocess(
            ["scp", '-r', file_or_dir, self._path_at_host(transit_path)],
            raise_on_error=True
        )

        self.ssh_command(["sudo", "mv", transit_path, target_path], raise_on_error=True)

    @abc.abstractproperty
    def logs_directory(self):
        pass

    def cleanup_logs(self):
        self.ssh_command("sudo dmesg --clear", raise_on_error=True)
        self.ssh_command(
            'sudo rm -rf {}/* && sudo service rsyslog restart'.format(self.logs_directory),
            raise_on_error=True)

    def sky_get_and_move(self, rb_torrent, item_to_move, target_path):
        self.ssh_command(['sky get -d %s %s' % (self._artifacts_path, rb_torrent)], raise_on_error=True)
        self.ssh_command(
            ['sudo mv %s %s' % (os.path.join(self._artifacts_path, item_to_move), target_path)],
            raise_on_error=True
        )

    def send_signal(self, signal):
        self.ssh_command(
            "ps aux | grep %d | grep -v daemon | grep -v grep | awk '{ print $2 }' | xargs sudo kill -%d" % (
                int(self.ic_port),
                int(signal),
            )
        )

    def kill_process_and_daemon(self):
        self.ssh_command(
            "ps aux | grep daemon | grep %d | grep -v grep | awk '{ print $2 }' | xargs sudo kill -%d" % (
                int(self.ic_port),
                int(signal.SIGKILL),
            )
        )
        self.ssh_command(
            "ps aux | grep %d | grep -v grep | awk '{ print $2 }' | xargs sudo kill -%d" % (
                int(self.ic_port),
                int(signal.SIGKILL),
            )
        )

    def kill(self):
        self.send_signal(9)

    def _run_in_subprocess(self, command, raise_on_error=False):
        self.logger.info("Executing command = " + str(command))
        try:
            ret_str = subprocess.check_output(command, stderr=subprocess.STDOUT)
            self.logger.info("Command returned stdout + stderr = " + str(ret_str))
            return ret_str
        except subprocess.CalledProcessError as e:
            self.logger.exception("Ssh command failed with output = " + str(e.output))
            if raise_on_error:
                raise
