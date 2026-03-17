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


from logging import getLogger, root, StreamHandler
from logging.config import fileConfig
from os import chdir, environ, path, getcwd
import sys


def configure_logging(logger_name, filename=None):
    """Configure logging and return the named logger and the location of the logging configuration file loaded.

    This function expects a Splunk app directory structure::

        <app-root>
            bin
                ...
            default
                ...
            local
                ...

    This function looks for a logging configuration file at each of these locations, loading the first, if any,
    logging configuration file that it finds::

        local/{name}.logging.conf
        default/{name}.logging.conf
        local/logging.conf
        default/logging.conf

    The current working directory is set to *<app-root>* before the logging configuration file is loaded. Hence, paths
    in the logging configuration file are relative to *<app-root>*. The current directory is reset before return.

    You may short circuit the search for a logging configuration file by providing an alternative file location in
    `path`. Logging configuration files must be in `ConfigParser format`_.

    #Arguments:

    :param logger_name: Logger name
    :type logger_name: bytes, unicode

    :param filename: Location of an alternative logging configuration file or `None`.
    :type filename: bytes, unicode or NoneType

    :returns: The named logger and the location of the logging configuration file loaded.
    :rtype: tuple

    .. _ConfigParser format: https://docs.python.org/2/library/logging.config.html#configuration-file-format

    """
    if filename is None:
        if logger_name is None:
            probing_paths = [
                path.join("local", "logging.conf"),
                path.join("default", "logging.conf"),
            ]
        else:
            probing_paths = [
                path.join("local", logger_name + ".logging.conf"),
                path.join("default", logger_name + ".logging.conf"),
                path.join("local", "logging.conf"),
                path.join("default", "logging.conf"),
            ]
        for relative_path in probing_paths:
            configuration_file = path.join(app_root, relative_path)
            if path.exists(configuration_file):
                filename = configuration_file
                break
    elif not path.isabs(filename):
        found = False
        for conf in "local", "default":
            configuration_file = path.join(app_root, conf, filename)
            if path.exists(configuration_file):
                filename = configuration_file
                found = True
                break
        if not found:
            raise ValueError(
                f'Logging configuration file "{filename}" not found in local or default directory'
            )
    elif not path.exists(filename):
        raise ValueError(f'Logging configuration file "{filename}" not found')

    if filename is not None:
        global _current_logging_configuration_file
        filename = path.realpath(filename)

        if filename != _current_logging_configuration_file:
            working_directory = getcwd()
            chdir(app_root)
            try:
                fileConfig(filename, {"SPLUNK_HOME": splunk_home})
            finally:
                chdir(working_directory)
            _current_logging_configuration_file = filename

    if len(root.handlers) == 0:
        root.addHandler(StreamHandler())

    return None if logger_name is None else getLogger(logger_name), filename


_current_logging_configuration_file = None

splunk_home = path.abspath(path.join(getcwd(), environ.get("SPLUNK_HOME", "")))
app_file = getattr(sys.modules["__main__"], "__file__", sys.executable)
app_root = path.dirname(path.abspath(path.dirname(app_file)))

splunklib_logger, logging_configuration = configure_logging("splunklib")


__all__ = [
    "app_file",
    "app_root",
    "logging_configuration",
    "splunk_home",
    "splunklib_logger",
]
