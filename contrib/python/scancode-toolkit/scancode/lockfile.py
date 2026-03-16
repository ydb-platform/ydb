#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from contextlib import contextmanager

import fasteners

"""
An interprocess lockfile with a timeout.
"""


class LockTimeout(Exception):
    pass


class FileLock(fasteners.InterProcessLock):

    @contextmanager
    def locked(self, timeout):
        acquired = self.acquire(timeout=timeout)
        if not acquired:
            raise LockTimeout(timeout)
        try:
            yield
        finally:
            self.release()
