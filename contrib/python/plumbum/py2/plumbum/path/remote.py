# -*- coding: utf-8 -*-
import errno
import os
import sys
from contextlib import contextmanager

from plumbum.commands import ProcessExecutionError, shquote
from plumbum.lib import _setdoc, six
from plumbum.path.base import FSUser, Path

try:  # Py3
    import urllib.request as urllib
except ImportError:
    import urllib  # type: ignore


class StatRes(object):
    """POSIX-like stat result"""

    def __init__(self, tup):
        self._tup = tuple(tup)

    def __getitem__(self, index):
        return self._tup[index]

    st_mode = mode = property(lambda self: self[0])
    st_ino = ino = property(lambda self: self[1])
    st_dev = dev = property(lambda self: self[2])
    st_nlink = nlink = property(lambda self: self[3])
    st_uid = uid = property(lambda self: self[4])
    st_gid = gid = property(lambda self: self[5])
    st_size = size = property(lambda self: self[6])
    st_atime = atime = property(lambda self: self[7])
    st_mtime = mtime = property(lambda self: self[8])
    st_ctime = ctime = property(lambda self: self[9])


class RemotePath(Path):
    """The class implementing remote-machine paths"""

    def __new__(cls, remote, *parts):
        if not parts:
            raise TypeError("At least one path part is required (none given)")
        windows = remote.uname.lower() == "windows"
        normed = []

        parts = tuple(
            map(str, parts)
        )  # force the paths into string, so subscription works properly
        # Simple skip if path is absolute
        if parts[0] and parts[0][0] not in ("/", "\\"):
            cwd = (
                remote._cwd
                if hasattr(remote, "_cwd")
                else remote._session.run("pwd")[1].strip()
            )
            parts = (cwd,) + parts

        for p in parts:
            if windows:
                plist = str(p).replace("\\", "/").split("/")
            else:
                plist = str(p).split("/")
            if not plist[0]:
                plist.pop(0)
                del normed[:]
            for item in plist:
                if item == "" or item == ".":
                    continue
                if item == "..":
                    if normed:
                        normed.pop(-1)
                else:
                    normed.append(item)
        if windows:
            self = super(RemotePath, cls).__new__(cls, "\\".join(normed))
            self.CASE_SENSITIVE = False  # On this object only
        else:
            self = super(RemotePath, cls).__new__(cls, "/" + "/".join(normed))
            self.CASE_SENSITIVE = True

        self.remote = remote
        return self

    def _form(self, *parts):
        return RemotePath(self.remote, *parts)

    @property
    def _path(self):
        return str(self)

    @property  # type: ignore
    @_setdoc(Path)
    def name(self):
        if not "/" in str(self):
            return str(self)
        return str(self).rsplit("/", 1)[1]

    @property  # type: ignore
    @_setdoc(Path)
    def dirname(self):
        if not "/" in str(self):
            return str(self)
        return self.__class__(self.remote, str(self).rsplit("/", 1)[0])

    @property  # type: ignore
    @_setdoc(Path)
    def suffix(self):
        return "." + self.name.rsplit(".", 1)[1]

    @property  # type: ignore
    @_setdoc(Path)
    def suffixes(self):
        name = self.name
        exts = []
        while "." in name:
            name, ext = name.rsplit(".", 1)
            exts.append("." + ext)
        return list(reversed(exts))

    @property  # type: ignore
    @_setdoc(Path)
    def uid(self):
        uid, name = self.remote._path_getuid(self)
        return FSUser(int(uid), name)

    @property  # type: ignore
    @_setdoc(Path)
    def gid(self):
        gid, name = self.remote._path_getgid(self)
        return FSUser(int(gid), name)

    def _get_info(self):
        return (self.remote, self._path)

    @_setdoc(Path)
    def join(self, *parts):
        return RemotePath(self.remote, self, *parts)

    @_setdoc(Path)
    def list(self):
        if not self.is_dir():
            return []
        return [self.join(fn) for fn in self.remote._path_listdir(self)]

    @_setdoc(Path)
    def iterdir(self):
        if not self.is_dir():
            return ()
        return (self.join(fn) for fn in self.remote._path_listdir(self))

    @_setdoc(Path)
    def is_dir(self):
        res = self.remote._path_stat(self)
        if not res:
            return False
        return res.text_mode == "directory"

    @_setdoc(Path)
    def is_file(self):
        res = self.remote._path_stat(self)
        if not res:
            return False
        return res.text_mode in ("regular file", "regular empty file")

    @_setdoc(Path)
    def is_symlink(self):
        res = self.remote._path_stat(self)
        if not res:
            return False
        return res.text_mode == "symbolic link"

    @_setdoc(Path)
    def exists(self):
        return self.remote._path_stat(self) is not None

    @_setdoc(Path)
    def stat(self):
        res = self.remote._path_stat(self)
        if res is None:
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), "")
        return res

    @_setdoc(Path)
    def with_name(self, name):
        return self.__class__(self.remote, self.dirname) / name

    @_setdoc(Path)
    def with_suffix(self, suffix, depth=1):
        if suffix and not suffix.startswith(".") or suffix == ".":
            raise ValueError("Invalid suffix %r" % (suffix))
        name = self.name
        depth = len(self.suffixes) if depth is None else min(depth, len(self.suffixes))
        for i in range(depth):
            name, ext = name.rsplit(".", 1)
        return self.__class__(self.remote, self.dirname) / (name + suffix)

    @_setdoc(Path)
    def glob(self, pattern):
        fn = lambda pat: [
            RemotePath(self.remote, m) for m in self.remote._path_glob(self, pat)
        ]
        return self._glob(pattern, fn)

    @_setdoc(Path)
    def delete(self):
        if not self.exists():
            return
        self.remote._path_delete(self)

    unlink = delete

    @_setdoc(Path)
    def move(self, dst):
        if isinstance(dst, RemotePath):
            if dst.remote is not self.remote:
                raise TypeError("dst points to a different remote machine")
        elif not isinstance(dst, six.string_types):
            raise TypeError(
                "dst must be a string or a RemotePath (to the same remote machine), "
                "got %r" % (dst,)
            )
        self.remote._path_move(self, dst)

    @_setdoc(Path)
    def copy(self, dst, override=False):
        if isinstance(dst, RemotePath):
            if dst.remote is not self.remote:
                raise TypeError("dst points to a different remote machine")
        elif not isinstance(dst, six.string_types):
            raise TypeError(
                "dst must be a string or a RemotePath (to the same remote machine), "
                "got %r" % (dst,)
            )
        if override:
            if isinstance(dst, six.string_types):
                dst = RemotePath(self.remote, dst)
            dst.delete()
        else:
            if isinstance(dst, six.string_types):
                dst = RemotePath(self.remote, dst)
            if dst.exists():
                raise TypeError("Override not specified and dst exists")

        self.remote._path_copy(self, dst)

    @_setdoc(Path)
    def mkdir(self, mode=None, parents=True, exist_ok=True):
        if parents and exist_ok:
            self.remote._path_mkdir(self, mode=mode, minus_p=True)
        else:
            if parents and len(self.parts) > 1:
                self.remote._path_mkdir(self.parent, mode=mode, minus_p=True)
            try:
                self.remote._path_mkdir(self, mode=mode, minus_p=False)
            except ProcessExecutionError:
                _, ex, _ = sys.exc_info()
                if "File exists" in ex.stderr:
                    if not exist_ok:
                        raise OSError(
                            errno.EEXIST, "File exists (on remote end)", str(self)
                        )
                else:
                    raise

    @_setdoc(Path)
    def read(self, encoding=None):
        data = self.remote._path_read(self)
        if encoding:
            data = data.decode(encoding)
        return data

    @_setdoc(Path)
    def write(self, data, encoding=None):
        if encoding:
            data = data.encode(encoding)
        self.remote._path_write(self, data)

    @_setdoc(Path)
    def touch(self):
        self.remote._path_touch(str(self))

    @_setdoc(Path)
    def chown(self, owner=None, group=None, recursive=None):
        self.remote._path_chown(
            self, owner, group, self.is_dir() if recursive is None else recursive
        )

    @_setdoc(Path)
    def chmod(self, mode):
        self.remote._path_chmod(mode, self)

    @_setdoc(Path)
    def access(self, mode=0):
        mode = self._access_mode_to_flags(mode)
        res = self.remote._path_stat(self)
        if res is None:
            return False
        mask = res.st_mode & 0x1FF
        return ((mask >> 6) & mode) or ((mask >> 3) & mode)

    @_setdoc(Path)
    def link(self, dst):
        if isinstance(dst, RemotePath):
            if dst.remote is not self.remote:
                raise TypeError("dst points to a different remote machine")
        elif not isinstance(dst, six.string_types):
            raise TypeError(
                "dst must be a string or a RemotePath (to the same remote machine), "
                "got %r" % (dst,)
            )
        self.remote._path_link(self, dst, False)

    @_setdoc(Path)
    def symlink(self, dst):
        if isinstance(dst, RemotePath):
            if dst.remote is not self.remote:
                raise TypeError("dst points to a different remote machine")
        elif not isinstance(dst, six.string_types):
            raise TypeError(
                "dst must be a string or a RemotePath (to the same remote machine), "
                "got %r" % (dst,)
            )
        self.remote._path_link(self, dst, True)

    def open(self, mode="r", bufsize=-1):
        """
        Opens this path as a file.

        Only works for ParamikoMachine-associated paths for now.
        """
        if hasattr(self.remote, "sftp") and hasattr(self.remote.sftp, "open"):
            return self.remote.sftp.open(self, mode, bufsize)
        else:
            raise NotImplementedError(
                "RemotePath.open only works for ParamikoMachine-associated "
                "paths for now"
            )

    @_setdoc(Path)
    def as_uri(self, scheme="ssh"):
        return "{}://{}{}".format(
            scheme, self.remote._fqhost, urllib.pathname2url(str(self))
        )

    @property  # type: ignore
    @_setdoc(Path)
    def stem(self):
        return self.name.rsplit(".")[0]

    @property  # type: ignore
    @_setdoc(Path)
    def root(self):
        return "/"

    @property  # type: ignore
    @_setdoc(Path)
    def drive(self):
        return ""


class RemoteWorkdir(RemotePath):
    """Remote working directory manipulator"""

    def __new__(cls, remote):
        self = super(RemoteWorkdir, cls).__new__(
            cls, remote, remote._session.run("pwd")[1].strip()
        )
        return self

    def __hash__(self):
        raise TypeError("unhashable type")

    def chdir(self, newdir):
        """Changes the current working directory to the given one"""
        self.remote._session.run("cd {}".format(shquote(newdir)))
        if hasattr(self.remote, "_cwd"):
            del self.remote._cwd
        return self.__class__(self.remote)

    def getpath(self):
        """Returns the current working directory as a
        `remote path <plumbum.path.remote.RemotePath>` object"""
        return RemotePath(self.remote, self)

    @contextmanager
    def __call__(self, newdir):
        """A context manager used to ``chdir`` into a directory and then ``chdir`` back to
        the previous location; much like ``pushd``/``popd``.

        :param newdir: The destination director (a string or a
                       :class:`RemotePath <plumbum.path.remote.RemotePath>`)
        """
        prev = self._path
        changed_dir = self.chdir(newdir)
        try:
            yield changed_dir
        finally:
            self.chdir(prev)
