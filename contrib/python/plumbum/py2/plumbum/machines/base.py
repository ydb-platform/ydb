# -*- coding: utf-8 -*-
from plumbum.commands.processes import (
    CommandNotFound,
    ProcessExecutionError,
    ProcessTimedOut,
)


class PopenAddons(object):
    """This adds a verify to popen objects to that the correct command is attributed when
    an error is thrown."""

    def verify(self, retcode, timeout, stdout, stderr):
        """This verifies that the correct command is attributed."""
        if getattr(self, "_timed_out", False):
            raise ProcessTimedOut(
                "Process did not terminate within {} seconds".format(timeout),
                getattr(self, "argv", None),
            )

        if retcode is not None:
            if hasattr(retcode, "__contains__"):
                if self.returncode not in retcode:
                    raise ProcessExecutionError(
                        getattr(self, "argv", None), self.returncode, stdout, stderr
                    )
            elif self.returncode != retcode:
                raise ProcessExecutionError(
                    getattr(self, "argv", None), self.returncode, stdout, stderr
                )


class BaseMachine(object):
    """This is a base class for other machines. It contains common code to
    all machines in Plumbum."""

    def get(self, cmd, *othercommands):
        """This works a little like the ``.get`` method with dict's, only
        it supports an unlimited number of arguments, since later arguments
        are tried as commands and could also fail. It
        will try to call the first command, and if that is not found,
        it will call the next, etc. Will raise if no file named for the
        executable if a path is given, unlike ``[]`` access.

        Usage::

            best_zip = local.get('pigz','gzip')
        """
        try:
            command = self[cmd]
            if not command.executable.exists():
                raise CommandNotFound(cmd, command.executable)
            else:
                return command
        except CommandNotFound:
            if othercommands:
                return self.get(othercommands[0], *othercommands[1:])
            else:
                raise

    def __contains__(self, cmd):
        """Tests for the existance of the command, e.g., ``"ls" in plumbum.local``.
        ``cmd`` can be anything acceptable by ``__getitem__``.
        """
        try:
            self[cmd]
        except CommandNotFound:
            return False
        else:
            return True

    @property
    def encoding(self):
        "This is a wrapper for custom_encoding"
        return self.custom_encoding

    @encoding.setter
    def encoding(self, value):
        self.custom_encoding = value

    def daemonic_popen(self, command, cwd="/", stdout=None, stderr=None, append=True):
        raise NotImplementedError("This is not implemented on this machine!")

    class Cmd(object):
        def __init__(self, machine):
            self._machine = machine

        def __getattr__(self, name):
            try:
                return self._machine[name]
            except CommandNotFound:
                raise AttributeError(name)

    @property
    def cmd(self):
        return self.Cmd(self)
