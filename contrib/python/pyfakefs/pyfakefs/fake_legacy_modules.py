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

import warnings


from pyfakefs.fake_pathlib import FakePathlibModule
from pyfakefs.fake_scandir import scandir, walk


def legacy_warning(module_name):
    msg = (
        f"You are using the legacy package '{module_name}' instead of the "
        f"built-in module."
        "Patching this package will no longer be supported in pyfakefs >= 6"
    )
    warnings.warn(msg, category=DeprecationWarning)


class FakePathlib2Module(FakePathlibModule):
    """Uses FakeFilesystem to provide a fake pathlib module replacement.
    for the `pathlib2` package available on PyPi.
    The usage of `pathlib2` is deprecated and will no longer be supported
    in future pyfakefs versions.
    """

    has_warned = False

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if hasattr(attr, "__call__") and not FakePathlib2Module.has_warned:
            FakePathlib2Module.has_warned = True
            legacy_warning("pathlib2")
        return attr


class FakeScanDirModule:
    """Uses FakeFilesystem to provide a fake module replacement
    for the `scandir` package available on PyPi.

    The usage of the `scandir` package is deprecated and will no longer be supported
    in future pyfakefs versions.

    You need a fake_filesystem to use this:
    `filesystem = fake_filesystem.FakeFilesystem()`
    `fake_scandir_module = fake_filesystem.FakeScanDirModule(filesystem)`
    """

    @staticmethod
    def dir():
        """Return the list of patched function names. Used for patching
        functions imported from the module.
        """
        return "scandir", "walk"

    def __init__(self, filesystem):
        self.filesystem = filesystem

    has_warned = False

    def scandir(self, path="."):
        """Return an iterator of DirEntry objects corresponding to the entries
        in the directory given by path.

        Args:
            path: Path to the target directory within the fake filesystem.

        Returns:
            an iterator to an unsorted list of os.DirEntry objects for
            each entry in path.

        Raises:
            OSError: if the target is not a directory.
        """
        if not self.has_warned:
            self.__class__.has_warned = True
            legacy_warning("scandir")
        return scandir(self.filesystem, path)

    def walk(self, top, topdown=True, onerror=None, followlinks=False):
        """Perform a walk operation over the fake filesystem.

        Args:
            top: The root directory from which to begin walk.
            topdown: Determines whether to return the tuples with the root as
                the first entry (`True`) or as the last, after all the child
                directory tuples (`False`).
            onerror: If not `None`, function which will be called to handle the
                `os.error` instance provided when `os.listdir()` fails.
            followlinks: If `True`, symbolic links are followed.

        Yields:
            (path, directories, nondirectories) for top and each of its
            subdirectories.  See the documentation for the builtin os module
            for further details.
        """
        if not self.has_warned:
            self.__class__.has_warned = True
            legacy_warning("scandir")

        return walk(self.filesystem, top, topdown, onerror, followlinks)
