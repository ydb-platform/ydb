#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import ctypes
import contextlib
import io
import os
from os import path

import logging
import signal
import subprocess

from commoncode.fileutils import get_temp_dir
from commoncode.system import on_posix
from commoncode.system import on_windows
from commoncode import text

"""
Wrapper for executing external commands in sub-processes which works
consistently the same way on POSIX and Windows OSes and can cope with the
capture of large command standard outputs without exhausting memory.
"""

logger = logging.getLogger(__name__)

TRACE = False

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

# current directory is the root dir of this library
curr_dir = path.dirname(path.dirname(path.abspath(__file__)))

PATH_ENV_VAR = 'PATH'
LD_LIBRARY_PATH = 'LD_LIBRARY_PATH'
DYLD_LIBRARY_PATH = 'DYLD_LIBRARY_PATH'


def execute(cmd_loc, args, cwd=None, env=None, to_files=False, log=TRACE):
    """
    Run a `cmd_loc` command with the `args` arguments list and return the return
    code, the stdout and stderr.

    To avoid RAM exhaustion, always write stdout and stderr streams to files.

    If `to_files` is False, return the content of stderr and stdout as ASCII
    strings. Otherwise, return the locations to the stderr and stdout temporary
    files.

    Run the command using the `cwd` current working directory with an `env` dict
    of environment variables.
    """
    assert cmd_loc
    full_cmd = [cmd_loc] + (args or [])

    # any shared object should be either in the PATH, the rpath or
    # side-by-side with the exceutable
    cmd_dir = os.path.dirname(cmd_loc)
    env = get_env(env, lib_dir=cmd_dir) or None
    cwd = cwd or curr_dir

    # temp files for stderr and stdout
    tmp_dir = get_temp_dir(prefix='cmd-')

    sop = path.join(tmp_dir, 'stdout')
    sep = path.join(tmp_dir, 'stderr')

    # shell==True is DANGEROUS but we are not running arbitrary commands
    # though we can execute commands that just happen to be in the path
    # See why we need it on Windows https://bugs.python.org/issue8557
    shell = True if on_windows else False

    if log:
        printer = logger.debug if TRACE else lambda x: print(x)
        printer(
            'Executing command %(cmd_loc)r as:\n%(full_cmd)r\nwith: env=%(env)r\n'
            'shell=%(shell)r\ncwd=%(cwd)r\nstdout=%(sop)r\nstderr=%(sep)r'
            % locals())

    proc = None
    rc = 100

    try:
        with io.open(sop, 'wb') as stdout, io.open(sep, 'wb') as stderr, pushd(cmd_dir):
            proc = subprocess.Popen(
                full_cmd,
                cwd=cwd,
                env=env,
                stdout=stdout,
                stderr=stderr,
                shell=shell,
                # -1 defaults bufsize to system bufsize
                bufsize=-1,
                universal_newlines=True,
            )
            stdout, stderr = proc.communicate()
            rc = proc.returncode if proc else 0
    finally:
        close(proc)

    if not to_files:
        # return output as ASCII string loaded from the output files
        with open(sop, 'rb') as so:
            sor = so.read()
            sop = text.toascii(sor).strip()

        with open(sep, 'rb') as se:
            ser = se.read()
            sep = text.toascii(ser).strip()

    return rc, sop, sep


def execute2(
    cmd_loc,
    args,
    lib_dir=None,
    cwd=None,
    env=None,
    to_files=False,
    log=TRACE,
):
    """
    DEPRECATED: DO NOT USE. Use execute() instead
    Run a `cmd_loc` command with the `args` arguments list and return the return
    code, the stdout and stderr.

    To avoid RAM exhaustion, always write stdout and stderr streams to files.

    If `to_files` is False, return the content of stderr and stdout as ASCII
    strings. Otherwise, return the locations to the stderr and stdout temporary
    files.

    Run the command using the `cwd` current working directory with an `env` dict
    of environment variables.
    """
    import warnings

    warnings.warn(
        "commoncode.command.execute2 is deprecated. Use execute() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    return execute(cmd_loc, args, cwd, env, to_files, log)


def get_env(base_vars=None, lib_dir=None):
    """
    Return a dictionary of environment variables for command execution with
    appropriate DY/LD_LIBRARY_PATH path variables. Use the optional `base_vars`
    environment variables dictionary as a base if provided. Note: if `base_vars`
    contains DY/LD_LIBRARY_PATH variables these will be overwritten. On POSIX,
    add `lib_dir` as DY/LD_LIBRARY_PATH-like path if provided.
    """
    env_vars = {}
    if base_vars:
        env_vars.update(base_vars)

    # Create and add LD environment variables
    if lib_dir and on_posix:
        new_path = f'{lib_dir}'
        # on Linux/posix
        ld_lib_path = os.environ.get(LD_LIBRARY_PATH)
        env_vars.update({LD_LIBRARY_PATH: update_path_var(ld_lib_path, new_path)})
        # on Mac, though LD_LIBRARY_PATH should work too
        dyld_lib_path = os.environ.get(DYLD_LIBRARY_PATH)
        env_vars.update({DYLD_LIBRARY_PATH: update_path_var(dyld_lib_path, new_path)})

    env_vars = {text.as_unicode(k): text.as_unicode(v) for k, v in env_vars.items()}

    return env_vars


def close(proc):
    """
    Close a `proc` process opened pipes and kill the process.
    """
    if not proc:
        return

    def close_pipe(p):
        if not p:
            return
        try:
            p.close()
        except IOError:
            pass

    close_pipe(getattr(proc, 'stdin', None))
    close_pipe(getattr(proc, 'stdout', None))
    close_pipe(getattr(proc, 'stderr', None))

    try:
        # Ensure process death otherwise proc.wait may hang in some cases
        # NB: this will run only on POSIX OSes supporting signals
        os.kill(proc.pid, signal.SIGKILL)  # NOQA
    except:
        pass

    # This may slow things down a tad on non-POSIX Oses but is safe:
    # this calls os.waitpid() to make sure the process is dead
    proc.wait()


def load_shared_library(dll_loc, *args):
    """
    Return the loaded shared library object from the ``dll_loc`` location.
    """
    if not dll_loc or not path.exists(dll_loc):
        raise ImportError(f'Shared library does not exists: dll_loc: {dll_loc}')

    if not isinstance(dll_loc, str):
        dll_loc = os.fsdecode(dll_loc)

    lib = None

    dll_dir = os.path.dirname(dll_loc)
    try:
        with pushd(dll_dir):
            lib = ctypes.CDLL(dll_loc)
    except OSError as e:
        from pprint import pformat
        import traceback
        msgs = tuple([
            f'ctypes.CDLL("{dll_loc}")',
            'os.environ:\n{}'.format(pformat(dict(os.environ))),
            traceback.format_exc(),
        ])
        raise Exception(msgs) from e

    if lib and lib._name:
        return lib

    raise Exception(f'Failed to load shared library with ctypes: {dll_loc}')


@contextlib.contextmanager
def pushd(path):
    """
    Context manager to change the current working directory to `path`.
    """
    original_cwd = os.getcwd()
    if not path:
        path = original_cwd
    try:
        os.chdir(path)
        yield os.getcwd()
    finally:
        os.chdir(original_cwd)


def update_path_var(existing_path_var, new_path):
    """
    Return an updated value for the `existing_path_var` PATH-like environment
    variable value  by adding `new_path` to the front of that variable if
    `new_path` is not already part of this PATH-like variable.
    """
    if not new_path:
        return existing_path_var

    existing_path_var = existing_path_var or ''

    existing_path_var = os.fsdecode(existing_path_var)
    new_path = os.fsdecode(new_path)

    path_elements = existing_path_var.split(os.pathsep)

    if not path_elements:
        updated_path_var = new_path

    elif new_path not in path_elements:
        # add new path to the front of the PATH env var
        path_elements.insert(0, new_path)
        updated_path_var = os.pathsep.join(path_elements)

    else:
        # new path is already in PATH, change nothing
        updated_path_var = existing_path_var

    if not isinstance(updated_path_var, str):
        updated_path_var = os.fsdecode(updated_path_var)

    return updated_path_var


PATH_VARS = DYLD_LIBRARY_PATH, LD_LIBRARY_PATH, 'PATH',


def searchable_paths(env_vars=PATH_VARS):
    """
    Return a list of directories where to search "in the PATH" in the provided
    ``env_vars`` list of PATH-like environment variables.
    """
    dirs = []
    for env_var in env_vars:
        value = os.environ.get(env_var, '') or ''
        dirs.extend(value.split(os.pathsep))
    dirs = [os.path.realpath(d.strip()) for d in dirs if d.strip()]
    return tuple(d for d in dirs if os.path.isdir(d))


def find_in_path(filename, searchable_paths=searchable_paths()):
    """
    Return the location of a ``filename`` found in the ``searchable_paths`` list
    of directory or None.
    """
    for path in searchable_paths:
        location = os.path.join(path, filename)
        if os.path.exists(location):
            return location
