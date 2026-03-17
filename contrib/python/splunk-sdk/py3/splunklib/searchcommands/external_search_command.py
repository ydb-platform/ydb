# coding=utf-8
#
# Copyright © 2011-2024 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from logging import getLogger
import os
import sys
import traceback
from . import splunklib_logger as logger


if sys.platform == "win32":
    from signal import signal, CTRL_BREAK_EVENT, SIGBREAK, SIGINT, SIGTERM
    from subprocess import Popen
    import atexit


# P1 [ ] TODO: Add ExternalSearchCommand class documentation


class ExternalSearchCommand:
    def __init__(self, path, argv=None, environ=None):
        if not isinstance(path, (bytes, str)):
            raise ValueError(f"Expected a string value for path, not {repr(path)}")

        self._logger = getLogger(self.__class__.__name__)
        self._path = str(path)
        self._argv = None
        self._environ = None

        self.argv = argv
        self.environ = environ

    # region Properties

    @property
    def argv(self):
        return getattr(self, "_argv")

    @argv.setter
    def argv(self, value):
        if not (value is None or isinstance(value, (list, tuple))):
            raise ValueError(
                f"Expected a list, tuple or value of None for argv, not {repr(value)}"
            )
        self._argv = value

    @property
    def environ(self):
        return getattr(self, "_environ")

    @environ.setter
    def environ(self, value):
        if not (value is None or isinstance(value, dict)):
            raise ValueError(
                f"Expected a dictionary value for environ, not {repr(value)}"
            )
        self._environ = value

    @property
    def logger(self):
        return self._logger

    @property
    def path(self):
        return self._path

    # endregion

    # region Methods

    def execute(self):
        # noinspection PyBroadException
        try:
            if self._argv is None:
                self._argv = os.path.splitext(os.path.basename(self._path))[0]
            self._execute(self._path, self._argv, self._environ)
        except:
            error_type, error, tb = sys.exc_info()
            message = f"Command execution failed: {str(error)}"
            self._logger.error(
                message + "\nTraceback:\n" + "".join(traceback.format_tb(tb))
            )
            sys.exit(1)

    if sys.platform == "win32":

        @staticmethod
        def _execute(path, argv=None, environ=None):
            """Executes an external search command.

            :param path: Path to the external search command.
            :type path: unicode

            :param argv: Argument list.
            :type argv: list or tuple
                The arguments to the child process should start with the name of the command being run, but this is not
                enforced. A value of :const:`None` specifies that the base name of path name :param:`path` should be used.

            :param environ: A mapping which is used to define the environment variables for the new process.
            :type environ: dict or None.
                This mapping is used instead of the current process’s environment. A value of :const:`None` specifies that
                the :data:`os.environ` mapping should be used.

            :return: None

            """
            search_path = os.getenv("PATH") if environ is None else environ.get("PATH")
            found = ExternalSearchCommand._search_path(path, search_path)

            if found is None:
                raise ValueError(f"Cannot find command on path: {path}")

            path = found
            logger.debug(f'starting command="{path}", arguments={argv}')

            def terminate(signal_number):
                sys.exit(
                    f"External search command is terminating on receipt of signal={signal_number}."
                )

            def terminate_child():
                if p.pid is not None and p.returncode is None:
                    logger.debug(
                        'terminating command="%s", arguments=%d, pid=%d',
                        path,
                        argv,
                        p.pid,
                    )
                    os.kill(p.pid, CTRL_BREAK_EVENT)

            p = Popen(
                argv,
                executable=path,
                env=environ,
                stdin=sys.stdin,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            atexit.register(terminate_child)
            signal(SIGBREAK, terminate)
            signal(SIGINT, terminate)
            signal(SIGTERM, terminate)

            logger.debug(
                'started command="%s", arguments=%s, pid=%d', path, argv, p.pid
            )
            p.wait()

            logger.debug(
                'finished command="%s", arguments=%s, pid=%d, returncode=%d',
                path,
                argv,
                p.pid,
                p.returncode,
            )

            if p.returncode != 0:
                sys.exit(p.returncode)

        @staticmethod
        def _search_path(executable, paths):
            """Locates an executable program file.

            :param executable: The name of the executable program to locate.
            :type executable: unicode

            :param paths: A list of one or more directory paths where executable programs are located.
            :type paths: unicode

            :return:
            :rtype: Path to the executable program located or :const:`None`.

            """
            directory, filename = os.path.split(executable)
            extension = os.path.splitext(filename)[1].upper()
            executable_extensions = ExternalSearchCommand._executable_extensions

            if directory:
                if len(extension) and extension in executable_extensions:
                    return None
                for extension in executable_extensions:
                    path = executable + extension
                    if os.path.isfile(path):
                        return path
                return None

            if not paths:
                return None

            directories = [
                directory for directory in paths.split(";") if len(directory)
            ]

            if len(directories) == 0:
                return None

            if len(extension) and extension in executable_extensions:
                for directory in directories:
                    path = os.path.join(directory, executable)
                    if os.path.isfile(path):
                        return path
                return None

            for directory in directories:
                path_without_extension = os.path.join(directory, executable)
                for extension in executable_extensions:
                    path = path_without_extension + extension
                    if os.path.isfile(path):
                        return path

            return None

        _executable_extensions = (".COM", ".EXE")
    else:

        @staticmethod
        def _execute(path, argv, environ):
            if environ is None:
                os.execvp(path, argv)
            else:
                os.execvpe(path, argv, environ)

    # endregion


def execute(path, argv=None, environ=None, command_class=ExternalSearchCommand):
    """
    :param path:
    :type path: basestring
    :param argv:
    :type: argv: list, tuple, or None
    :param environ:
    :type environ: dict
    :param command_class: External search command class to instantiate and execute.
    :type command_class: type
    :return:
    :rtype: None
    """
    assert issubclass(command_class, ExternalSearchCommand)
    command_class(path, argv, environ).execute()
