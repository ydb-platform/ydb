# -*- coding: utf-8 -*-
import logging
import os
import platform
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from tempfile import mkdtemp

from plumbum.commands import CommandNotFound, ConcreteCommand
from plumbum.commands.daemons import posix_daemonize, win32_daemonize
from plumbum.commands.processes import iter_lines
from plumbum.lib import IS_WIN32, ProcInfo, StaticProperty, six
from plumbum.machines.base import BaseMachine, PopenAddons
from plumbum.machines.env import BaseEnv
from plumbum.machines.session import ShellSession
from plumbum.path.local import LocalPath, LocalWorkdir
from plumbum.path.remote import RemotePath

if sys.version_info[0] >= 3:
    # python 3 has the new-and-improved subprocess module
    from subprocess import PIPE, Popen

    has_new_subprocess = True
else:
    # otherwise, see if we have subprocess32
    try:
        from subprocess32 import PIPE, Popen

        has_new_subprocess = True
    except ImportError:
        from subprocess import PIPE, Popen

        has_new_subprocess = False


class PlumbumLocalPopen(PopenAddons):
    iter_lines = iter_lines

    def __init__(self, *args, **kwargs):
        self._proc = Popen(*args, **kwargs)

    def __iter__(self):
        return self.iter_lines()

    def __enter__(self):
        return self._proc.__enter__()

    def __exit__(self, *args, **kwargs):
        return self._proc.__exit__(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._proc, name)


if IS_WIN32:
    from plumbum.machines._windows import IMAGE_SUBSYSTEM_WINDOWS_CUI, get_pe_subsystem

logger = logging.getLogger("plumbum.local")


# ===================================================================================================
# Environment
# ===================================================================================================
class LocalEnv(BaseEnv):
    """The local machine's environment; exposes a dict-like interface"""

    __slots__ = ()
    CASE_SENSITIVE = not IS_WIN32

    def __init__(self):
        # os.environ already takes care of upper'ing on windows
        self._curr = os.environ.copy()
        BaseEnv.__init__(self, LocalPath, os.path.pathsep)
        if IS_WIN32 and "HOME" not in self and self.home is not None:
            self["HOME"] = self.home

    def expand(self, expr):
        """Expands any environment variables and home shortcuts found in ``expr``
        (like ``os.path.expanduser`` combined with ``os.path.expandvars``)

        :param expr: An expression containing environment variables (as ``$FOO``) or
                     home shortcuts (as ``~/.bashrc``)

        :returns: The expanded string"""
        prev = os.environ
        os.environ = self.getdict()
        try:
            output = os.path.expanduser(os.path.expandvars(expr))
        finally:
            os.environ = prev
        return output

    def expanduser(self, expr):
        """Expand home shortcuts (e.g., ``~/foo/bar`` or ``~john/foo/bar``)

        :param expr: An expression containing home shortcuts

        :returns: The expanded string"""
        prev = os.environ
        os.environ = self.getdict()
        try:
            output = os.path.expanduser(expr)
        finally:
            os.environ = prev
        return output


# ===================================================================================================
# Local Commands
# ===================================================================================================
class LocalCommand(ConcreteCommand):
    __slots__ = ()
    QUOTE_LEVEL = 2

    def __init__(self, executable, encoding="auto"):
        ConcreteCommand.__init__(
            self, executable, local.custom_encoding if encoding == "auto" else encoding
        )

    @property
    def machine(self):
        return local

    def popen(self, args=(), cwd=None, env=None, **kwargs):
        if isinstance(args, six.string_types):
            args = (args,)
        return self.machine._popen(
            self.executable,
            self.formulate(0, args),
            cwd=self.cwd if cwd is None else cwd,
            env=self.env if env is None else env,
            **kwargs
        )


# ===================================================================================================
# Local Machine
# ===================================================================================================


class LocalMachine(BaseMachine):
    """The *local machine* (a singleton object). It serves as an entry point to everything
    related to the local machine, such as working directory and environment manipulation,
    command creation, etc.

    Attributes:

    * ``cwd`` - the local working directory
    * ``env`` - the local environment
    * ``custom_encoding`` - the local machine's default encoding (``sys.getfilesystemencoding()``)
    """

    cwd = StaticProperty(LocalWorkdir)
    env = LocalEnv()

    custom_encoding = sys.getfilesystemencoding()
    uname = platform.uname()[0]

    def __init__(self):
        self._as_user_stack = []

    if IS_WIN32:
        _EXTENSIONS = [""] + env.get("PATHEXT", ":.exe:.bat").lower().split(
            os.path.pathsep
        )

        @classmethod
        def _which(cls, progname):
            progname = progname.lower()
            for p in cls.env.path:
                for ext in cls._EXTENSIONS:
                    fn = p / (progname + ext)
                    if fn.access("x") and not fn.is_dir():
                        return fn
            return None

    else:

        @classmethod
        def _which(cls, progname):
            for p in cls.env.path:
                fn = p / progname
                if fn.access("x") and not fn.is_dir():
                    return fn
            return None

    @classmethod
    def which(cls, progname):
        """Looks up a program in the ``PATH``. If the program is not found, raises
        :class:`CommandNotFound <plumbum.commands.CommandNotFound>`

        :param progname: The program's name. Note that if underscores (``_``) are present
                         in the name, and the exact name is not found, they will be replaced
                         in turn by hyphens (``-``) then periods (``.``), and the name will
                         be looked up again for each alternative

        :returns: A :class:`LocalPath <plumbum.machines.local.LocalPath>`
        """
        alternatives = [progname]
        if "_" in progname:
            alternatives.append(progname.replace("_", "-"))
            alternatives.append(progname.replace("_", "."))
        for pn in alternatives:
            path = cls._which(pn)
            if path:
                return path
        raise CommandNotFound(progname, list(cls.env.path))

    def path(self, *parts):
        """A factory for :class:`LocalPaths <plumbum.path.local.LocalPath>`.
        Usage: ``p = local.path("/usr", "lib", "python2.7")``
        """
        parts2 = [str(self.cwd)]
        for p in parts:
            if isinstance(p, RemotePath):
                raise TypeError("Cannot construct LocalPath from {!r}".format(p))
            parts2.append(self.env.expanduser(str(p)))
        return LocalPath(os.path.join(*parts2))

    def __contains__(self, cmd):
        try:
            self[cmd]
        except CommandNotFound:
            return False
        else:
            return True

    def __getitem__(self, cmd):
        """Returns a `Command` object representing the given program. ``cmd`` can be a string or
        a :class:`LocalPath <plumbum.path.local.LocalPath>`; if it is a path, a command
        representing this path will be returned; otherwise, the program name will be looked up
        in the system's ``PATH`` (using ``which``). Usage::

            ls = local["ls"]
        """

        if isinstance(cmd, LocalPath):
            return LocalCommand(cmd)
        elif not isinstance(cmd, RemotePath):
            if "/" in cmd or "\\" in cmd:
                # assume path
                return LocalCommand(local.path(cmd))
            else:
                # search for command
                return LocalCommand(self.which(cmd))
        else:
            raise TypeError("cmd must not be a RemotePath: {!r}".format(cmd))

    def _popen(
        self,
        executable,
        argv,
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        cwd=None,
        env=None,
        new_session=False,
        **kwargs
    ):
        if new_session:
            if has_new_subprocess:
                kwargs["start_new_session"] = True
            elif IS_WIN32:
                kwargs["creationflags"] = (
                    kwargs.get("creationflags", 0) | subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:

                def preexec_fn(prev_fn=kwargs.get("preexec_fn", lambda: None)):
                    os.setsid()
                    prev_fn()

                kwargs["preexec_fn"] = preexec_fn

        if IS_WIN32 and "startupinfo" not in kwargs and stdin not in (sys.stdin, None):
            subsystem = get_pe_subsystem(str(executable))

            if subsystem == IMAGE_SUBSYSTEM_WINDOWS_CUI:
                # don't open a new console
                sui = subprocess.STARTUPINFO()
                kwargs["startupinfo"] = sui
                if hasattr(subprocess, "_subprocess"):
                    sui.dwFlags |= (
                        subprocess._subprocess.STARTF_USESHOWWINDOW
                    )  # @UndefinedVariable
                    sui.wShowWindow = (
                        subprocess._subprocess.SW_HIDE
                    )  # @UndefinedVariable
                else:
                    sui.dwFlags |= subprocess.STARTF_USESHOWWINDOW  # @UndefinedVariable
                    sui.wShowWindow = subprocess.SW_HIDE  # @UndefinedVariable

        if not has_new_subprocess and "close_fds" not in kwargs:
            if IS_WIN32 and (
                stdin is not None or stdout is not None or stderr is not None
            ):
                # we can't close fds if we're on windows and we want to redirect any std handle
                kwargs["close_fds"] = False
            else:
                kwargs["close_fds"] = True

        if cwd is None:
            cwd = self.cwd

        envs = [self.env, env]
        env = {}
        for _env in envs:
            if not _env:
                continue
            if isinstance(_env, BaseEnv):
                _env = _env.getdict()
            env.update(_env)

        if self._as_user_stack:
            argv, executable = self._as_user_stack[-1](argv)

        logger.debug("Running %r", argv)
        proc = PlumbumLocalPopen(
            argv,
            executable=str(executable),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            cwd=str(cwd),
            env=env,
            **kwargs
        )  # bufsize = 4096
        proc._start_time = time.time()
        proc.custom_encoding = self.custom_encoding
        proc.argv = argv
        return proc

    def daemonic_popen(self, command, cwd="/", stdout=None, stderr=None, append=True):
        """
        On POSIX systems:

        Run ``command`` as a UNIX daemon: fork a child process to setpid, redirect std handles to /dev/null,
        umask, close all fds, chdir to ``cwd``, then fork and exec ``command``. Returns a ``Popen`` process that
        can be used to poll/wait for the executed command (but keep in mind that you cannot access std handles)

        On Windows:

        Run ``command`` as a "Windows daemon": detach from controlling console and create a new process group.
        This means that the command will not receive console events and would survive its parent's termination.
        Returns a ``Popen`` object.

        .. note:: this does not run ``command`` as a system service, only detaches it from its parent.

        .. versionadded:: 1.3
        """
        if IS_WIN32:
            return win32_daemonize(command, cwd, stdout, stderr, append)
        else:
            return posix_daemonize(command, cwd, stdout, stderr, append)

    if IS_WIN32:

        def list_processes(self):
            """
            Returns information about all running processes (on Windows: using ``tasklist``)

            .. versionadded:: 1.3
            """
            import csv

            tasklist = local["tasklist"]
            output = tasklist("/V", "/FO", "CSV")
            if not six.PY3:
                # The Py2 csv reader does not support non-ascii values
                output = output.encode("ascii", "ignore")
            lines = output.splitlines()
            rows = csv.reader(lines)
            header = next(rows)
            imgidx = header.index("Image Name")
            pididx = header.index("PID")
            statidx = header.index("Status")
            useridx = header.index("User Name")
            for row in rows:
                yield ProcInfo(
                    int(row[pididx]), row[useridx], row[statidx], row[imgidx]
                )

    else:

        def list_processes(self):
            """
            Returns information about all running processes (on POSIX systems: using ``ps``)

            .. versionadded:: 1.3
            """
            ps = self["ps"]
            lines = ps("-e", "-o", "pid,uid,stat,args").splitlines()
            lines.pop(0)  # header
            for line in lines:
                parts = line.strip().split()
                yield ProcInfo(
                    int(parts[0]), int(parts[1]), parts[2], " ".join(parts[3:])
                )

    def pgrep(self, pattern):
        """
        Process grep: return information about all processes whose command-line args match the given regex pattern
        """
        pat = re.compile(pattern)
        for procinfo in self.list_processes():
            if pat.search(procinfo.args):
                yield procinfo

    def session(self, new_session=False):
        """Creates a new :class:`ShellSession <plumbum.session.ShellSession>` object; this
        invokes ``/bin/sh`` and executes commands on it over stdin/stdout/stderr"""
        return ShellSession(self["sh"].popen(new_session=new_session))

    @contextmanager
    def tempdir(self):
        """A context manager that creates a temporary directory, which is removed when the context
        exits"""
        dir = self.path(mkdtemp())  # @ReservedAssignment
        try:
            yield dir
        finally:
            dir.delete()

    @contextmanager
    def as_user(self, username=None):
        """Run nested commands as the given user. For example::

            head = local["head"]
            head("-n1", "/dev/sda1")    # this will fail...
            with local.as_user():
                head("-n1", "/dev/sda1")

        :param username: The user to run commands as. If not given, root (or Administrator) is assumed
        """
        if IS_WIN32:
            if username is None:
                username = "Administrator"
            self._as_user_stack.append(
                lambda argv: (
                    [
                        "runas",
                        "/savecred",
                        "/user:{}".format(username),
                        '"' + " ".join(str(a) for a in argv) + '"',
                    ],
                    self.which("runas"),
                )
            )
        else:
            if username is None:
                self._as_user_stack.append(
                    lambda argv: (["sudo"] + list(argv), self.which("sudo"))
                )
            else:
                self._as_user_stack.append(
                    lambda argv: (
                        ["sudo", "-u", username] + list(argv),
                        self.which("sudo"),
                    )
                )
        try:
            yield
        finally:
            self._as_user_stack.pop(-1)

    def as_root(self):
        """A shorthand for :func:`as_user("root") <plumbum.machines.local.LocalMachine.as_user>`"""
        return self.as_user()

    python = LocalCommand(sys.executable, custom_encoding)
    """A command that represents the current python interpreter (``sys.executable``)"""


local = LocalMachine()
"""The *local machine* (a singleton object). It serves as an entry point to everything
related to the local machine, such as working directory and environment manipulation,
command creation, etc.

Attributes:

* ``cwd`` - the local working directory
* ``env`` - the local environment
* ``custom_encoding`` - the local machine's default encoding (``sys.getfilesystemencoding()``)
"""
