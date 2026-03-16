# -*- coding: utf-8 -*-
import errno
import glob
import logging
import os
import shutil
import sys
from contextlib import contextmanager

from plumbum.lib import IS_WIN32, _setdoc, glob_escape, six
from plumbum.path.base import FSUser, Path
from plumbum.path.remote import RemotePath

try:
    from grp import getgrgid, getgrnam
    from pwd import getpwnam, getpwuid
except ImportError:

    def getpwuid(x):  # type: ignore
        return (None,)

    def getgrgid(x):  # type: ignore
        return (None,)

    def getpwnam(x):  # type: ignore
        raise OSError("`getpwnam` not supported")

    def getgrnam(x):  # type: ignore
        raise OSError("`getgrnam` not supported")


try:  # Py3
    import urllib.parse as urlparse
    import urllib.request as urllib
except ImportError:
    import urllib  # type: ignore

    import urlparse  # type: ignore

logger = logging.getLogger("plumbum.local")


# ===================================================================================================
# Local Paths
# ===================================================================================================
class LocalPath(Path):
    """The class implementing local-machine paths"""

    CASE_SENSITIVE = not IS_WIN32

    def __new__(cls, *parts):
        if (
            len(parts) == 1
            and isinstance(parts[0], cls)
            and not isinstance(parts[0], LocalWorkdir)
        ):
            return parts[0]
        if not parts:
            raise TypeError("At least one path part is required (none given)")
        if any(isinstance(path, RemotePath) for path in parts):
            raise TypeError("LocalPath cannot be constructed from {!r}".format(parts))
        self = super(LocalPath, cls).__new__(
            cls, os.path.normpath(os.path.join(*(str(p) for p in parts)))
        )
        return self

    @property
    def _path(self):
        return str(self)

    def _get_info(self):
        return self._path

    def _form(self, *parts):
        return LocalPath(*parts)

    @property  # type: ignore
    @_setdoc(Path)
    def name(self):
        return os.path.basename(str(self))

    @property  # type: ignore
    @_setdoc(Path)
    def dirname(self):
        return LocalPath(os.path.dirname(str(self)))

    @property  # type: ignore
    @_setdoc(Path)
    def suffix(self):
        return os.path.splitext(str(self))[1]

    @property
    def suffixes(self):
        exts = []
        base = str(self)
        while True:
            base, ext = os.path.splitext(base)
            if ext:
                exts.append(ext)
            else:
                return list(reversed(exts))

    @property  # type: ignore
    @_setdoc(Path)
    def uid(self):
        uid = self.stat().st_uid
        name = getpwuid(uid)[0]
        return FSUser(uid, name)

    @property  # type: ignore
    @_setdoc(Path)
    def gid(self):
        gid = self.stat().st_gid
        name = getgrgid(gid)[0]
        return FSUser(gid, name)

    @_setdoc(Path)
    def join(self, *others):
        return LocalPath(self, *others)

    @_setdoc(Path)
    def list(self):
        return [self / fn for fn in os.listdir(str(self))]

    @_setdoc(Path)
    def iterdir(self):
        try:
            return (self / fn.name for fn in os.scandir(str(self)))
        except AttributeError:
            return (self / fn for fn in os.listdir(str(self)))

    @_setdoc(Path)
    def is_dir(self):
        return os.path.isdir(str(self))

    @_setdoc(Path)
    def is_file(self):
        return os.path.isfile(str(self))

    @_setdoc(Path)
    def is_symlink(self):
        return os.path.islink(str(self))

    @_setdoc(Path)
    def exists(self):
        return os.path.exists(str(self))

    @_setdoc(Path)
    def stat(self):
        return os.stat(str(self))

    @_setdoc(Path)
    def with_name(self, name):
        return LocalPath(self.dirname) / name

    @property  # type: ignore
    @_setdoc(Path)
    def stem(self):
        return self.name.rsplit(os.path.extsep)[0]

    @_setdoc(Path)
    def with_suffix(self, suffix, depth=1):
        if suffix and not suffix.startswith(os.path.extsep) or suffix == os.path.extsep:
            raise ValueError("Invalid suffix %r" % (suffix))
        name = self.name
        depth = len(self.suffixes) if depth is None else min(depth, len(self.suffixes))
        for _ in range(depth):
            name, _ = os.path.splitext(name)
        return LocalPath(self.dirname) / (name + suffix)

    @_setdoc(Path)
    def glob(self, pattern):
        fn = lambda pat: [
            LocalPath(m) for m in glob.glob(os.path.join(glob_escape(str(self)), pat))
        ]
        return self._glob(pattern, fn)

    @_setdoc(Path)
    def delete(self):
        if not self.exists():
            return
        if self.is_dir():
            shutil.rmtree(str(self))
        else:
            try:
                os.remove(str(self))
            except OSError:  # pragma: no cover
                # file might already been removed (a race with other threads/processes)
                _, ex, _ = sys.exc_info()
                if ex.errno != errno.ENOENT:
                    raise

    @_setdoc(Path)
    def move(self, dst):
        if isinstance(dst, RemotePath):
            raise TypeError("Cannot move local path {} to {!r}".format(self, dst))
        shutil.move(str(self), str(dst))
        return LocalPath(dst)

    @_setdoc(Path)
    def copy(self, dst, override=None):
        if isinstance(dst, RemotePath):
            raise TypeError("Cannot copy local path {} to {!r}".format(self, dst))
        dst = LocalPath(dst)
        if override is False and dst.exists():
            raise TypeError("File exists and override was not specified")
        if override:
            dst.delete()
        if self.is_dir():
            shutil.copytree(str(self), str(dst))
        else:
            dst_dir = LocalPath(dst).dirname
            if not dst_dir.exists():
                dst_dir.mkdir()
            shutil.copy2(str(self), str(dst))
        return dst

    @_setdoc(Path)
    def mkdir(self, mode=0o777, parents=True, exist_ok=True):
        if not self.exists() or not exist_ok:
            try:
                if parents:
                    os.makedirs(str(self), mode)
                else:
                    os.mkdir(str(self), mode)
            except OSError:  # pragma: no cover
                # directory might already exist (a race with other threads/processes)
                _, ex, _ = sys.exc_info()
                if ex.errno != errno.EEXIST or not exist_ok:
                    raise

    @_setdoc(Path)
    def open(self, mode="r"):
        return open(str(self), mode)

    @_setdoc(Path)
    def read(self, encoding=None, mode="r"):
        if encoding and "b" not in mode:
            mode = mode + "b"
        with self.open(mode) as f:
            data = f.read()
            if encoding:
                data = data.decode(encoding)
            return data

    @_setdoc(Path)
    def write(self, data, encoding=None, mode=None):
        if encoding:
            data = data.encode(encoding)
        if mode is None:
            if isinstance(data, six.unicode_type):
                mode = "w"
            else:
                mode = "wb"
        with self.open(mode) as f:
            f.write(data)

    @_setdoc(Path)
    def touch(self):
        with open(str(self), "a"):
            os.utime(str(self), None)

    @_setdoc(Path)
    def chown(self, owner=None, group=None, recursive=None):
        if not hasattr(os, "chown"):
            raise OSError("os.chown() not supported")
        uid = (
            self.uid
            if owner is None
            else (owner if isinstance(owner, int) else getpwnam(owner)[2])
        )
        gid = (
            self.gid
            if group is None
            else (group if isinstance(group, int) else getgrnam(group)[2])
        )
        os.chown(str(self), uid, gid)
        if recursive or (recursive is None and self.is_dir()):
            for subpath in self.walk():
                os.chown(str(subpath), uid, gid)

    @_setdoc(Path)
    def chmod(self, mode):
        if not hasattr(os, "chmod"):
            raise OSError("os.chmod() not supported")
        os.chmod(str(self), mode)

    @_setdoc(Path)
    def access(self, mode=0):
        return os.access(str(self), self._access_mode_to_flags(mode))

    @_setdoc(Path)
    def link(self, dst):
        if isinstance(dst, RemotePath):
            raise TypeError(
                "Cannot create a hardlink from local path {} to {!r}".format(self, dst)
            )
        if hasattr(os, "link"):
            os.link(str(self), str(dst))
        else:
            from plumbum.machines.local import local

            # windows: use mklink
            if self.is_dir():
                local["cmd"]("/C", "mklink", "/D", "/H", str(dst), str(self))
            else:
                local["cmd"]("/C", "mklink", "/H", str(dst), str(self))

    @_setdoc(Path)
    def symlink(self, dst):
        if isinstance(dst, RemotePath):
            raise TypeError(
                "Cannot create a symlink from local path {} to {!r}".format(self, dst)
            )
        if hasattr(os, "symlink"):
            os.symlink(str(self), str(dst))
        else:
            from plumbum.machines.local import local

            # windows: use mklink
            if self.is_dir():
                local["cmd"]("/C", "mklink", "/D", str(dst), str(self))
            else:
                local["cmd"]("/C", "mklink", str(dst), str(self))

    @_setdoc(Path)
    def unlink(self):
        try:
            if hasattr(os, "symlink") or not self.is_dir():
                os.unlink(str(self))
            else:
                # windows: use rmdir for directories and directory symlinks
                os.rmdir(str(self))
        except OSError:  # pragma: no cover
            # file might already been removed (a race with other threads/processes)
            _, ex, _ = sys.exc_info()
            if ex.errno != errno.ENOENT:
                raise

    @_setdoc(Path)
    def as_uri(self, scheme="file"):
        return urlparse.urljoin(str(scheme) + ":", urllib.pathname2url(str(self)))

    @property  # type: ignore
    @_setdoc(Path)
    def drive(self):
        return os.path.splitdrive(str(self))[0]

    @property  # type: ignore
    @_setdoc(Path)
    def root(self):
        return os.path.sep


class LocalWorkdir(LocalPath):
    """Working directory manipulator"""

    def __hash__(self):
        raise TypeError("unhashable type")

    def __new__(cls):
        return super(LocalWorkdir, cls).__new__(cls, os.getcwd())

    def chdir(self, newdir):
        """Changes the current working directory to the given one

        :param newdir: The destination director (a string or a ``LocalPath``)
        """
        if isinstance(newdir, RemotePath):
            raise TypeError("newdir cannot be {!r}".format(newdir))
        logger.debug("Chdir to %s", newdir)
        os.chdir(str(newdir))
        return self.__class__()

    def getpath(self):
        """Returns the current working directory as a ``LocalPath`` object"""
        return LocalPath(self._path)

    @contextmanager
    def __call__(self, newdir):
        """A context manager used to ``chdir`` into a directory and then ``chdir`` back to
        the previous location; much like ``pushd``/``popd``.

        :param newdir: The destination directory (a string or a ``LocalPath``)
        """
        prev = self._path
        newdir = self.chdir(newdir)
        try:
            yield newdir
        finally:
            self.chdir(prev)
