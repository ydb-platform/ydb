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

"""A fake implementation for the `scandir` function working with
FakeFilesystem.
Works with both the function integrated into the `os` module since Python 3.5
and the standalone function available in the standalone `scandir` python
package.
"""

import os
import sys

from pyfakefs.helpers import to_string, make_string_path


class DirEntry(os.PathLike):
    """Emulates os.DirEntry. Note that we did not enforce keyword only
    arguments."""

    def __init__(self, filesystem):
        """Initialize the dir entry with unset values.

        Args:
            filesystem: the fake filesystem used for implementation.
        """
        self._filesystem = filesystem
        self.name = ""
        self.path = ""
        self._abspath = ""
        self._inode = None
        self._islink = False
        self._isdir = False
        self._statresult = None
        self._statresult_symlink = None

    def inode(self):
        """Return the inode number of the entry."""
        if self._inode is None:
            self.stat(follow_symlinks=False)
        return self._inode

    def is_dir(self, follow_symlinks=True):
        """Return True if this entry is a directory entry.

        Args:
            follow_symlinks: If True, also return True if this entry is a
                symlink pointing to a directory.

        Returns:
            True if this entry is an existing directory entry, or if
                follow_symlinks is set, and this entry points to an existing
                directory entry.
        """
        return self._isdir and (follow_symlinks or not self._islink)

    def is_file(self, follow_symlinks=True):
        """Return True if this entry is a regular file entry.

        Args:
            follow_symlinks: If True, also return True if this entry is a
                symlink pointing to a regular file.

        Returns:
            True if this entry is an existing file entry, or if
                follow_symlinks is set, and this entry points to an existing
                file entry.
        """
        return not self._isdir and (follow_symlinks or not self._islink)

    def is_symlink(self):
        """Return True if this entry is a symbolic link (even if broken)."""
        return self._islink

    def stat(self, follow_symlinks=True):
        """Return a stat_result object for this entry.

        Args:
            follow_symlinks: If False and the entry is a symlink, return the
                result for the symlink, otherwise for the object it points to.
        """
        if follow_symlinks:
            if self._statresult_symlink is None:
                file_object = self._filesystem.resolve(self._abspath)
                self._statresult_symlink = file_object.stat_result.copy()
                if self._filesystem.is_windows_fs:
                    self._statresult_symlink.st_nlink = 0
            return self._statresult_symlink

        if self._statresult is None:
            file_object = self._filesystem.lresolve(self._abspath)
            self._inode = file_object.st_ino
            self._statresult = file_object.stat_result.copy()
            if self._filesystem.is_windows_fs:
                self._statresult.st_nlink = 0
        return self._statresult

    def __fspath__(self):
        return self.path

    if sys.version_info >= (3, 12):

        def is_junction(self) -> bool:
            """Return True if this entry is a junction.
            Junctions are not a part of posix semantic."""
            if not self._filesystem.is_windows_fs:
                return False
            file_object = self._filesystem.resolve(self._abspath)
            return file_object.is_junction


class ScanDirIter:
    """Iterator for DirEntry objects returned from `scandir()`
    function."""

    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        if isinstance(path, int):
            if self.filesystem.is_windows_fs:
                raise NotImplementedError(
                    "scandir does not support file descriptor path argument"
                )
            self.abspath = self.filesystem.absnormpath(
                self.filesystem.get_open_file(path).get_object().path
            )
            self.path = ""
            self.entry_iter = iter(tuple())
        else:
            if path is None:
                path = "."
            path = make_string_path(path)
            self.abspath = self.filesystem.absnormpath(path)
            self.path = to_string(path)
        entries = self.filesystem.confirmdir(self.abspath, check_exe_perm=False).entries
        self.entry_iter = iter(tuple(entries))

    def __iter__(self):
        return self

    def __next__(self):
        entry = self.entry_iter.__next__()
        dir_entry = DirEntry(self.filesystem)
        dir_entry.name = entry
        dir_entry.path = self.filesystem.joinpaths(self.path, dir_entry.name)
        dir_entry._abspath = self.filesystem.joinpaths(self.abspath, dir_entry.name)
        dir_entry._isdir = self.filesystem.isdir(dir_entry._abspath)
        dir_entry._islink = self.filesystem.islink(dir_entry._abspath)
        return dir_entry

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        pass


def scandir(filesystem, path=""):
    """Return an iterator of DirEntry objects corresponding to the entries
    in the directory given by path.

    Args:
        filesystem: The fake filesystem used for implementation
        path: Path to the target directory within the fake filesystem.

    Returns:
        an iterator to an unsorted list of os.DirEntry objects for
        each entry in path.

    Raises:
        OSError: if the target is not a directory.
    """
    return ScanDirIter(filesystem, path)


def _classify_directory_contents(filesystem, root):
    """Classify contents of a directory as files/directories.

    Args:
        filesystem: The fake filesystem used for implementation
        root: (str) Directory to examine.

    Returns:
        (tuple) A tuple consisting of three values: the directory examined,
        a list containing all of the directory entries, and a list
        containing all of the non-directory entries.
        (This is the same format as returned by the `os.walk` generator.)

    Raises:
        Nothing on its own, but be ready to catch exceptions generated by
        underlying mechanisms like `os.listdir`.
    """
    dirs = []
    files = []
    for entry in filesystem.listdir(root):
        if filesystem.isdir(filesystem.joinpaths(root, entry)):
            dirs.append(entry)
        else:
            files.append(entry)
    return root, dirs, files


def walk(filesystem, top, topdown=True, onerror=None, followlinks=False):
    """Perform an os.walk operation over the fake filesystem.

    Args:
        filesystem: The fake filesystem used for implementation
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

    def do_walk(top_dir, top_most=False):
        if not top_most and not followlinks and filesystem.islink(top_dir):
            return
        try:
            top_contents = _classify_directory_contents(filesystem, top_dir)
        except OSError as exc:
            top_contents = None
            if onerror is not None:
                onerror(exc)

        if top_contents is not None:
            if topdown:
                yield top_contents

            for directory in top_contents[1]:
                path = filesystem.joinpaths(top_dir, directory)
                if not followlinks and filesystem.islink(path):
                    continue
                yield from do_walk(path)
            if not topdown:
                yield top_contents

    return do_walk(make_string_path(to_string(top)), top_most=True)
