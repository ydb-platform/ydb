""" Temporary directory fixtures
"""
from __future__ import absolute_import
import os
import tempfile
import shutil
import logging
import subprocess

from pathlib import Path
import pytest

from . import cmdline

log = logging.getLogger(__name__)


@pytest.yield_fixture()
def workspace():
    """ Function-scoped temporary workspace that cleans up on exit.

    Attributes
    ----------
    workspace (`path.path`):  Path to the workspace directory.
    debug (bool):             If set to True, will log more debug when running commands.
    delete (bool):            If True, will always delete the workspace on teardown;
    ..                        If None, delete the workspace unless teardown occurs via an exception;
    ..                        If False, never delete the workspace on teardown.

    """
    ws = Workspace()
    yield ws
    ws.teardown()


class Workspace(object):
    """
    Creates a temp workspace, cleans up on teardown. Can also be used as a context manager.
    Has a 'run' method to execute commands relative to this directory.
    """
    debug = False
    delete = True

    def __init__(self, workspace=None, delete=None):
        self.delete = delete

        log.debug("")
        log.debug("=======================================================")
        if workspace is None:
            self.workspace = Path(tempfile.mkdtemp(dir=self.get_base_tempdir()))
            log.debug("pytest_shutil created workspace %s" % self.workspace)

        else:
            self.workspace = Path(workspace)
            log.debug("pytest_shutil using workspace %s" % self.workspace)
        if 'DEBUG' in os.environ:
            self.debug = True
        if self.delete is not False:
            log.debug("This workspace will delete itself on teardown")
        log.debug("=======================================================")
        log.debug("")

    def __enter__(self):
        return self

    def __exit__(self, errtype, value, traceback):  # @UnusedVariable
        if self.delete is None:
            self.delete = (errtype is None)
        self.teardown()

    def __del__(self):
        self.teardown()

    @staticmethod
    def get_base_tempdir():
        """ Returns an appropriate dir to pass into
            tempfile.mkdtemp(dir=xxx) or similar.
        """
        # Prefer CI server workspaces. TODO: look for env vars for other CI servers
        return os.getenv('WORKSPACE')

    def run(self, cmd, capture=False, check_rc=True, cd=None, shell=False, **kwargs):
        """
        Run a command relative to a given directory, defaulting to the workspace root

        Parameters
        ----------
        cmd : `str` or `list`
            Command string or list. Commands given as a string will be run in a subshell.
        capture : `bool`
            Capture and return output
        check_rc : `bool`
            Assert return code is zero
        cd : `str`
            Path to chdir to, defaults to workspace root
        """
        if isinstance(cmd, str):
            shell = True
        else:
            # Some of the command components might be path objects or numbers
            cmd = [str(i) for i in cmd]

        if not cd:
            cd = self.workspace

        with cmdline.chdir(cd):
            log.debug("run: {0}".format(cmd))
            if capture:
                p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
            else:
                p = subprocess.Popen(cmd, shell=shell, **kwargs)
            (out, _) = p.communicate()

            if out is not None and not isinstance(out, str):
                out = out.decode('utf-8')

            if self.debug and capture:
                log.debug("Stdout/stderr:")
                log.debug(out)

            if check_rc and p.returncode != 0:
                err = subprocess.CalledProcessError(p.returncode, cmd)
                err.output = out
                if capture and not self.debug:
                    log.error("Stdout/stderr:")
                    log.error(out)
                raise err

        return out

    def teardown(self):
        if self.delete is not None and not self.delete:
            return
        if hasattr(self, 'workspace') and self.workspace.is_dir():
            log.debug("")
            log.debug("=======================================================")
            log.debug("pytest_shutil deleting workspace %s" % self.workspace)
            log.debug("=======================================================")
            log.debug("")
            shutil.rmtree(self.workspace, ignore_errors=True)
