# Copyright 2009 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A fake shutil module implementation that uses fake_filesystem for
unit tests.
Note that only `shutildisk_usage()` is faked, the rest of the functions shall
work fine with the fake file system if `os`/`os.path` are patched.

:Includes:
  FakeShutil: Uses a FakeFilesystem to provide a fake replacement for the
    shutil module.

:Usage:
  The fake implementation is automatically involved if using
  `fake_filesystem_unittest.TestCase`, pytest fs fixture,
  or directly `Patcher`.
"""

import os
import shutil
import sys


class FakeShutilModule:
    """Uses a FakeFilesystem to provide a fake replacement
    for shutil module.
    """

    @staticmethod
    def dir():
        """Return the list of patched function names. Used for patching
        functions imported from the module.
        """
        return ("disk_usage",)

    def __init__(self, filesystem):
        """Construct fake shutil module using the fake filesystem.

        Args:
          filesystem:  FakeFilesystem used to provide file system information
        """
        self.filesystem = filesystem
        self._shutil_module = shutil

    def disk_usage(self, path):
        """Return the total, used and free disk space in bytes as named tuple
        or placeholder holder values simulating unlimited space if not set.

        Args:
          path: defines the filesystem device which is queried
        """
        return self.filesystem.get_disk_usage(path)

    if sys.version_info >= (3, 12) and sys.platform == "win32":

        def copy2(self, src, dst, *, follow_symlinks=True):
            """Since Python 3.12, there is an optimization fow Windows,
            using the Windows API. We just remove this and fall back to the previous
            implementation.
            """
            if self.filesystem.isdir(dst):
                dst = self.filesystem.joinpaths(dst, os.path.basename(src))

            self.copyfile(src, dst, follow_symlinks=follow_symlinks)
            self.copystat(src, dst, follow_symlinks=follow_symlinks)
            return dst

        def copytree(
            self,
            src,
            dst,
            symlinks=False,
            ignore=None,
            copy_function=shutil.copy2,
            ignore_dangling_symlinks=False,
            dirs_exist_ok=False,
        ):
            """Make sure the default argument is patched."""
            if copy_function == shutil.copy2:
                copy_function = self.copy2
            return self._shutil_module.copytree(
                src,
                dst,
                symlinks,
                ignore,
                copy_function,
                ignore_dangling_symlinks,
                dirs_exist_ok,
            )

        def move(self, src, dst, copy_function=shutil.copy2):
            """Make sure the default argument is patched."""
            if copy_function == shutil.copy2:
                copy_function = self.copy2
            return self._shutil_module.move(src, dst, copy_function)

    def __getattr__(self, name):
        """Forwards any non-faked calls to the standard shutil module."""
        return getattr(self._shutil_module, name)
