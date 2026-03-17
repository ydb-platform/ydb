#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
# Copyright (c) 2018 Peter Odding
# Author: Peter Odding <peter@peterodding.com>
# URL: https://github.com/xolox/python-deb-pkg-tools

import os


def find_debian_architecture():
    """
    Find the Debian architecture of the current environment.

    Uses :func:`os.uname()` to determine the current machine architecture
    (the fifth value returned by :func:`os.uname()`) and translates it into
    one of the `machine architecture labels`_ used in the Debian packaging
    system:

    ====================  ===================
    Machine architecture  Debian architecture
    ====================  ===================
    ``i686``              ``i386``
    ``x86_64``            ``amd64``
    ``armv6l``            ``armhf``
    ====================  ===================

    When the machine architecture is not listed above, this function falls back
    to the external command ``dpkg-architecture -qDEB_BUILD_ARCH`` (provided by
    the ``dpkg-dev`` package). This command is not used by default because:

    1. deb-pkg-tools doesn't have a strict dependency on ``dpkg-dev``.
    2. The ``dpkg-architecture`` program enables callers to set the current
       architecture and the exact semantics of this are unclear to me at the
       time of writing (it can't automagically provide a cross compilation
       environment, so what exactly does it do?).

    :returns: The Debian architecture (a string like ``i386``, ``amd64``,
              ``armhf``, etc).
    :raises: :exc:`~executor.ExternalCommandFailed` when the
             ``dpkg-architecture`` program is not available or reports an
             error.

    .. _machine architecture labels: https://www.debian.org/doc/debian-policy/ch-controlfields.html#architecture
    .. _more architectures: https://www.debian.org/ports/index.en.html#portlist-released
    """
    _sysname, _nodename, _release, _version, machine = os.uname()
    if machine == "i686":
        return "i386"
    elif machine == "x86_64":
        return "amd64"
    elif machine == "armv6l":
        return "armhf"
    else:
        raise Exception("unknown machine")
