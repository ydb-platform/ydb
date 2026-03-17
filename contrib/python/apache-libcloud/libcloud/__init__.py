# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
libcloud provides a unified interface to the cloud computing resources.

:var __version__: Current version of libcloud
"""

import os
import atexit
import codecs

from libcloud.base import DriverType  # NOQA
from libcloud.base import DriverTypeFactoryMap  # NOQA
from libcloud.base import get_driver  # NOQA

try:
    # TODO: This import is slow and adds overhead in situations when no
    # requests are made but it's necessary for detecting bad version of
    # requests
    import requests  # NOQA

    have_requests = True
except ImportError:
    have_requests = False

__all__ = ["__version__", "enable_debug"]

__version__ = "3.9.0"


def enable_debug(fo):
    """
    Enable library wide debugging to a file-like object.

    :param fo: Where to append debugging information
    :type fo: File like object, only write operations are used.
    """
    from libcloud.common.base import Connection
    from libcloud.utils.loggingconnection import LoggingConnection

    LoggingConnection.log = fo
    Connection.conn_class = LoggingConnection

    # Ensure the file handle is closed on exit
    def close_file(fd):
        try:
            fd.close()
        except Exception:
            pass

    atexit.register(close_file, fo)


def reset_debug():
    """
    Reset debugging functionality (if set).

    NOTE: This function is only meant to be used in the tests.
    """
    from libcloud.common.base import Connection, LibcloudConnection
    from libcloud.utils.loggingconnection import LoggingConnection

    LoggingConnection.log = None
    Connection.conn_class = LibcloudConnection


def _init_once():
    """
    Utility function that is ran once on Library import.

    This checks for the LIBCLOUD_DEBUG environment variable, which if it exists
    is where we will log debug information about the provider transports.

    This also checks for known environment/dependency incompatibilities.
    """
    path = os.getenv("LIBCLOUD_DEBUG")

    if path:
        mode = "a"

        # Special case for /dev/stderr and /dev/stdout on Python 3.
        from libcloud.utils.py3 import PY3

        # Opening those files in append mode will throw "illegal seek"
        # exception there.
        # Late import to avoid setup.py related side affects
        if path in ["/dev/stderr", "/dev/stdout"] and PY3:
            mode = "w"

        fo = codecs.open(path, mode, encoding="utf8")
        enable_debug(fo)

        # NOTE: We use lazy import to avoid unnecessary import time overhead
        try:
            import paramiko  # NOQA

            have_paramiko = True
        except ImportError:
            have_paramiko = False

        if have_paramiko and hasattr(paramiko.util, "log_to_file"):
            import logging

            # paramiko always tries to open file path in append mode which
            # won't work with /dev/{stdout, stderr} so we just ignore those
            # errors
            try:
                paramiko.util.log_to_file(filename=path, level=logging.DEBUG)
            except OSError as e:
                if "illegal seek" not in str(e).lower():
                    raise e

    # check for broken `yum install python-requests`
    if have_requests and requests.__version__ == "2.6.0":
        chardet_version = requests.packages.chardet.__version__
        required_chardet_version = "2.3.0"
        assert chardet_version == required_chardet_version, (
            "Known bad version of requests detected! This can happen when "
            "requests was installed from a source other than PyPI, e.g. via "
            "a package manager such as yum. Please either install requests "
            "from PyPI or run `pip install chardet==%s` to resolve this "
            "issue." % required_chardet_version
        )


_init_once()
