#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import signal
import tempfile
import shutil

import six

try:
    from . import gdb
except ValueError:
    import gdb

from yatest.common import process, output_path, TimeoutError, cores

MAX_IO_LEN = 1024 * 10
GYGABYTES = 1 << 30

logger = logging.getLogger(__name__)


def run_daemon(command, check_exit_code=True, shell=False, timeout=5, cwd=None,
               env=None, stdin=None, stdout=None, stderr=None, creationflags=0):
    daemon = Daemon(command, check_exit_code, shell, timeout, cwd, env, stdin, stdout, stderr, creationflags)
    daemon.run()
    return daemon


def get_free_space(path):
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize


class DaemonError(RuntimeError):
    def __init__(self, message, stdout=None, stderr=None, exit_code=None):
        lst = [
            "Daemon failed with message: {message}.".format(message=message),
        ]
        if exit_code is not None:
            lst.append(
                "Process exit_code = {exit_code}.".format(exit_code=exit_code)
            )
        if stdout is not None:
            lst.append(
                "Stdout: {stdout}".format(stdout=stdout)
            )
        if stderr is not None:
            lst.append(
                "Stderr: {stderr}".format(stderr=stderr)
            )

        super(DaemonError, self).__init__('\n'.join(lst))


class Daemon(object):
    def __init__(self, command, check_exit_code=True, shell=False, timeout=5, cwd=None,
                 env=None, stdin=None, stdout=None, stderr=None, creationflags=0):
        if cwd is None:
            cwd = tempfile.mkdtemp()
        self.cwd = cwd

        self.stdoutf = stdout or tempfile.NamedTemporaryFile(dir=self.cwd, prefix="stdout_", delete=False)
        self.stderrf = stderr or tempfile.NamedTemporaryFile(dir=self.cwd, prefix="stderr_", delete=False)
        self.stdinf = stdin or tempfile.NamedTemporaryFile(dir=self.cwd, prefix="stdin_", delete=False)

        self.cmd = command
        if isinstance(command, six.string_types):
            self.cmd = [arg for arg in command.split() if arg]
        self.daemon = None
        self.name = os.path.basename(self.cmd[0])

        self._shell = shell
        self._env = env
        self._creationflags = creationflags

        self._check_exit_code = check_exit_code
        self._timeout = timeout

    def before_start(self):
        pass

    def after_start(self):
        pass

    def before_stop(self):
        pass

    def after_stop(self):
        pass

    def is_alive(self):
        return self.daemon and self.daemon.running

    def required_args(self):
        return []

    def check_run(self):
        """This function checks that daemon is running. By default it
        checks only the process status. But you can override it to
        check your binary specific marks like 'port is busy' and
        others."""
        return self.is_alive()

    def run(self):
        if self.check_run():
            logger.error("Can't run %s.\nProcess already started" % self.cmd)
            raise DaemonError("daemon already started.")

        try:
            self.before_start()
        except Exception:
            logger.exception("Exception in user hook before_start")
        self.daemon = process.execute(self.cmd[:1] + self.required_args() + self.cmd[1:],
                                      False,
                                      shell=self._shell,
                                      cwd=self.cwd,
                                      env=self._env,
                                      stdin=self.stdinf,
                                      stdout=self.stdoutf,
                                      stderr=self.stderrf,
                                      creationflags=self._creationflags,
                                      wait=False)
        stdout, stderr = self.__communicate()
        timeout_reason_msg = "Failed to execute '{cmd}'.\n\tstdout: {out}\n\tstderr: {err}".format(
            cmd=" ".join(self.cmd),
            out=stdout,
            err=stderr)
        try:
            process.wait_for(self.check_run, self._timeout, timeout_reason_msg, sleep_time=0.1)
        except process.TimeoutError:
            self.raise_on_death(timeout_reason_msg)

        if not self.is_alive():
            self.raise_on_death("WHY? %s %s" % (self.daemon, self.daemon.running))

        try:
            self.after_start()
        except Exception as e:
            msg = "Exception in user hook after_start. Exception: %s" % str(e)
            logger.exception(msg)

        return self

    def raise_on_death(self, additional_text=""):
        stdout = "[NO STDOUT]"
        stderr = "[NO STDERR]"

        if self.stdoutf and self.stdinf:
            stdout, stderr = self.__communicate()
        if self.daemon and getattr(self.daemon, "process"):
            self.check_coredump()

        raise DaemonError(
            Daemon.__log_failed(
                "process {} unexpectedly finished. \n\n {}".format(self.cmd, additional_text),
                stdout,
                stderr
            )
        )

    def check_coredump(self):
        try:
            core_file = cores.recover_core_dump_file(self.cmd[0], self.cwd, self.daemon.process.pid)
            if core_file:
                logger.debug(core_file + " found, maybe this is our coredump file")
                self.save_coredump(core_file)
            else:
                logger.debug("Core dump file was not found")
        except Exception as e:
            logger.warn("While checking coredump: " + str(e))

    def save_coredump(self, core_file):
        output_core_dir = output_path("cores")
        shared_core_file = os.path.join(output_core_dir, os.path.basename(core_file))
        if not os.path.isdir(output_core_dir):
            os.mkdir(output_core_dir)

        short_bt, _ = gdb.dump_traceback(executable=self.cmd[0], core_file=core_file,
                                         output_file=shared_core_file + ".trace.txt")
        if short_bt:
            logger.error("Short backtrace = \n" + "=" * 80 + "\n" + short_bt + "\n" + "=" * 80)

        space_left = float(get_free_space(output_core_dir))
        if space_left > 5 * GYGABYTES:
            shutil.copy2(
                core_file,
                shared_core_file
            )
            os.chmod(shared_core_file, 0o755)
            logger.debug("Saved to " + output_core_dir)

        else:
            logger.error("Not enough space left on device (%s GB). Won't save %s file" % (float(space_left / GYGABYTES), core_file))

    def stop(self, kill=False):
        if not self.is_alive() and self.daemon.exit_code == 0:
            return

        if not self.is_alive():
            stdout, stderr = self.__communicate()
            self.check_coredump()
            try:
                self.after_stop()
            except Exception:
                logger.exception("Exception in user hook after_stop.")

            raise DaemonError(
                Daemon.__log_failed(
                    "process {} unexpectedly finished with exit code {}.".format(self.cmd, self.daemon.exit_code),
                    stdout,
                    stderr
                ),
                exit_code=self.daemon.exit_code
            )

        try:
            self.before_stop()
        except Exception:
            logger.exception("Exception in user hook before_stop.")

        stderr, stdout = self.__communicate()
        timeout_reason_msg = "Cannot stop {cmd}.\n\tstdout: {out}\n\tstderr: {err}".format(
            cmd=" ".join(self.cmd),
            out=stdout,
            err=stderr)
        if not kill:
            self.daemon.process.send_signal(signal.SIGINT)
            try:  # soft wait for. trying to kill with sigint
                process.wait_for(lambda: not self.is_alive(), self._timeout, timeout_reason_msg, sleep_time=0.1)
            except TimeoutError:
                pass

        is_killed = False
        if self.is_alive():
            self.daemon.process.send_signal(signal.SIGKILL)
            is_killed = True

        process.wait_for(lambda: not self.is_alive(), self._timeout, timeout_reason_msg, sleep_time=0.1)

        try:
            self.after_stop()
        except Exception:
            logger.exception("Exception in user hook after_stop")

        if self.daemon.running:
            stdout, stderr = self.__communicate()
            msg = "cannot stop daemon {cmd}\n\tstdout: {out}\n\tstderr: {err}".format(
                cmd=' '.join(self.cmd),
                out=stdout,
                err=stderr
            )
            logger.error(msg)
            raise DaemonError(msg, stdout=stdout, stderr=stderr, exit_code=self.daemon.exit_code)

        stdout, stderr = self.__communicate()
        logger.debug(
            "Process stopped: {cmd}.\n\tstdout:\n{out}\n\tstderr:\n{err}".format(
                cmd=" ".join(self.cmd),
                out=stdout,
                err=stderr
            )
        )
        if not is_killed:
            self.check_coredump()
            if self._check_exit_code and self.daemon.exit_code != 0:
                stdout, stderr = self.__communicate()
                raise DaemonError("Bad exit_code.", stdout=stdout, stderr=stderr, exit_code=self.daemon.exit_code)
        else:
            logger.warning("Exit code is not checked, cos binary was stopped by sigkill")

    def _read_io(self, file_obj):
        file_obj.flush()

        cur_pos = file_obj.tell()
        seek_pos_from_end = max(-cur_pos, -MAX_IO_LEN)
        file_obj.seek(seek_pos_from_end, os.SEEK_END)
        return file_obj.read()

    def __communicate(self):
        stderr = self._read_io(self.stderrf)
        stdout = self._read_io(self.stdoutf)
        return stdout, stderr

    @staticmethod
    def __log_failed(msg, stderr, stdout):
        final_msg = '{msg}\nstdout: {out}\nstderr: {err}'.format(
            msg=msg,
            out=stdout,
            err=stderr)
        logger.error(msg)
        return final_msg
