# vfs.py - Mercurial 'vfs' classes
#
#  Copyright Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import abc
import contextlib
import os
import shutil
import stat
import threading
import typing

from typing import (
    Any,
    BinaryIO,
    Callable,
    Iterable,
    Iterator,
    MutableMapping,
    Optional,
    TypeVar,
)

from .i18n import _
from . import (
    encoding,
    error,
    pathutil,
    pycompat,
    util,
)

if typing.TYPE_CHECKING:
    from . import (
        ui as uimod,
    )

    _Tbackgroundfilecloser = TypeVar(
        '_Tbackgroundfilecloser', bound='backgroundfilecloser'
    )
    _Tclosewrapbase = TypeVar('_Tclosewrapbase', bound='closewrapbase')
    _OnErrorFn = Callable[[Exception], Optional[object]]


def _avoidambig(path: bytes, oldstat: util.filestat) -> None:
    """Avoid file stat ambiguity forcibly

    This function causes copying ``path`` file, if it is owned by
    another (see issue5418 and issue5584 for detail).
    """

    def checkandavoid():
        newstat = util.filestat.frompath(path)
        # return whether file stat ambiguity is (already) avoided
        return not newstat.isambig(oldstat) or newstat.avoidambig(path, oldstat)

    if not checkandavoid():
        # simply copy to change owner of path to get privilege to
        # advance mtime (see issue5418)
        util.rename(util.mktempcopy(path), path)
        checkandavoid()


class abstractvfs(abc.ABC):
    """Abstract base class; cannot be instantiated"""

    # default directory separator for vfs
    #
    # Other vfs code always use `/` and this works fine because python file API
    # abstract the use of `/` and make it work transparently. For consistency
    # vfs will always use `/` when joining. This avoids some confusion in
    # encoded vfs (see issue6546)
    _dir_sep: bytes = b'/'

    # Used to disable the Rust `InnerRevlog` in case the VFS is not supported
    # by the Rust code
    rust_compatible = True

    # createmode is always available on subclasses
    createmode: int

    _chmod: bool

    # TODO: type return, which is util.posixfile wrapped by a proxy
    @abc.abstractmethod
    def __call__(self, path: bytes, mode: bytes = b'rb', **kwargs) -> Any:
        ...

    @abc.abstractmethod
    def _auditpath(self, path: bytes, mode: bytes) -> None:
        ...

    @abc.abstractmethod
    def join(self, path: bytes | None, *insidef: bytes) -> bytes:
        ...

    def tryread(self, path: bytes) -> bytes:
        '''gracefully return an empty string for missing files'''
        try:
            return self.read(path)
        except FileNotFoundError:
            pass
        return b""

    def tryreadlines(self, path: bytes, mode: bytes = b'rb') -> list[bytes]:
        '''gracefully return an empty array for missing files'''
        try:
            return self.readlines(path, mode=mode)
        except FileNotFoundError:
            pass
        return []

    @util.propertycache
    def open(self):
        """Open ``path`` file, which is relative to vfs root.

        Newly created directories are marked as "not to be indexed by
        the content indexing service", if ``notindexed`` is specified
        for "write" mode access.
        """
        return self.__call__

    def read(self, path: bytes) -> bytes:
        with self(path, b'rb') as fp:
            return fp.read()

    def readlines(self, path: bytes, mode: bytes = b'rb') -> list[bytes]:
        with self(path, mode=mode) as fp:
            return fp.readlines()

    def write(
        self, path: bytes, data: bytes, backgroundclose: bool = False, **kwargs
    ) -> int:
        with self(path, b'wb', backgroundclose=backgroundclose, **kwargs) as fp:
            return fp.write(data)

    def writelines(
        self,
        path: bytes,
        data: Iterable[bytes],
        mode: bytes = b'wb',
        notindexed: bool = False,
    ) -> None:
        with self(path, mode=mode, notindexed=notindexed) as fp:
            return fp.writelines(data)

    def append(self, path: bytes, data: bytes) -> int:
        with self(path, b'ab') as fp:
            return fp.write(data)

    def basename(self, path: bytes) -> bytes:
        """return base element of a path (as os.path.basename would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.basename(path)

    def chmod(self, path: bytes, mode: int) -> None:
        return os.chmod(self.join(path), mode)

    def dirname(self, path: bytes) -> bytes:
        """return dirname element of a path (as os.path.dirname would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.dirname(path)

    def exists(self, path: bytes | None = None) -> bool:
        return os.path.exists(self.join(path))

    def fstat(self, fp: BinaryIO) -> os.stat_result:
        return util.fstat(fp)

    def isdir(self, path: bytes | None = None) -> bool:
        return os.path.isdir(self.join(path))

    def isfile(self, path: bytes | None = None) -> bool:
        return os.path.isfile(self.join(path))

    def islink(self, path: bytes | None = None) -> bool:
        return os.path.islink(self.join(path))

    def isfileorlink(self, path: bytes | None = None) -> bool:
        """return whether path is a regular file or a symlink

        Unlike isfile, this doesn't follow symlinks."""
        try:
            st = self.lstat(path)
        except OSError:
            return False
        mode = st.st_mode
        return stat.S_ISREG(mode) or stat.S_ISLNK(mode)

    def _join(self, *paths: bytes) -> bytes:
        root_idx = 0
        for idx, p in enumerate(paths):
            if os.path.isabs(p) or p.startswith(self._dir_sep):
                root_idx = idx
        if root_idx != 0:
            paths = paths[root_idx:]
        paths = [p for p in paths if p]
        return self._dir_sep.join(paths)

    def reljoin(self, *paths: bytes) -> bytes:
        """join various elements of a path together (as os.path.join would do)

        The vfs base is not injected so that path stay relative. This exists
        to allow handling of strange encoding if needed."""
        return self._join(*paths)

    def split(self, path: bytes) -> tuple[bytes, bytes]:
        """split top-most element of a path (as os.path.split would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.split(path)

    def lexists(self, path: bytes | None = None) -> bool:
        return os.path.lexists(self.join(path))

    def lstat(self, path: bytes | None = None) -> os.stat_result:
        return os.lstat(self.join(path))

    def is_mmap_safe(self, path: bytes | None = None) -> bool:
        """return True if it is safe to read a file content as mmap

        This focus on the file system aspect of such safety, the application
        logic around that file is not taken into account, so caller need to
        make sure the file won't be truncated in a way that will create SIGBUS
        on access.


        The initial motivation for this logic is that if mmap is used on NFS
        and somebody deletes the mapped file (e.g. by renaming on top of it),
        then you get SIGBUS, which can be pretty disruptive: we get core dump
        reports, and the process terminates without writing to the blackbox.

        Instead, in this situation we prefer to read the file normally.
        The risk of ESTALE in the middle of the read remains, but it's
        smaller because we read sooner and the error should be reported
        just as any other error.

        Note that python standard library does not offer the necessary function
        to detect the file stem bits. So this detection rely on compiled bits
        and is not available in pure python.
        """
        # XXX Since we already assume a vfs to address a consistent file system
        # in other location, we could determine the fstype once for the root
        # and cache that value.
        fstype = util.getfstype(self.join(path))
        return fstype is not None and fstype != b'nfs'

    def listdir(self, path: bytes | None = None) -> list[bytes]:
        return os.listdir(self.join(path))

    def makedir(self, path: bytes | None = None, notindexed=True) -> None:
        return util.makedir(self.join(path), notindexed)

    def makedirs(
        self, path: bytes | None = None, mode: int | None = None
    ) -> None:
        return util.makedirs(self.join(path), mode)

    def makelock(self, info: bytes, path: bytes) -> None:
        return util.makelock(info, self.join(path))

    def mkdir(self, path: bytes | None = None) -> None:
        return os.mkdir(self.join(path))

    def mkstemp(
        self,
        suffix: bytes = b'',
        prefix: bytes = b'tmp',
        dir: bytes | None = None,
    ) -> tuple[int, bytes]:
        fd, name = pycompat.mkstemp(
            suffix=suffix, prefix=prefix, dir=self.join(dir)
        )
        dname, fname = util.split(name)
        if dir:
            return fd, os.path.join(dir, fname)
        else:
            return fd, fname

    # TODO: This doesn't match osutil.listdir().  stat=False in pure;
    #  non-optional bool in cext.  'skip' is bool if we trust cext, or bytes
    #  going by how pure uses it.  Also, cext returns a custom stat structure.
    #  from cext.osutil.pyi:
    #
    #     path: bytes, st: bool, skip: Optional[bool]
    def readdir(self, path: bytes | None = None, stat=None, skip=None) -> Any:
        return util.listdir(self.join(path), stat, skip)

    def readlock(self, path: bytes) -> bytes:
        return util.readlock(self.join(path))

    def rename(self, src: bytes, dst: bytes, checkambig: bool = False) -> None:
        """Rename from src to dst

        checkambig argument is used with util.filestat, and is useful
        only if destination file is guarded by any lock
        (e.g. repo.lock or repo.wlock).

        To avoid file stat ambiguity forcibly, checkambig=True involves
        copying ``src`` file, if it is owned by another. Therefore, use
        checkambig=True only in limited cases (see also issue5418 and
        issue5584 for detail).
        """
        self._auditpath(dst, b'w')
        srcpath = self.join(src)
        dstpath = self.join(dst)
        oldstat = util.filestat.frompath(dstpath) if checkambig else None

        util.rename(srcpath, dstpath)

        if oldstat and oldstat.stat:
            _avoidambig(dstpath, oldstat)

    def readlink(self, path: bytes) -> bytes:
        return util.readlink(self.join(path))

    def removedirs(self, path: bytes | None = None) -> None:
        """Remove a leaf directory and all empty intermediate ones"""
        return util.removedirs(self.join(path))

    def rmdir(self, path: bytes | None = None) -> None:
        """Remove an empty directory."""
        return os.rmdir(self.join(path))

    def rmtree(
        self,
        path: bytes | None = None,
        ignore_errors: bool = False,
        forcibly: bool = False,
    ) -> None:
        """Remove a directory tree recursively

        If ``forcibly``, this tries to remove READ-ONLY files, too.
        """
        if forcibly:

            def onexc(function: Callable, path: str, excinfo: Exception):
                # Note: str is passed here even if bytes are passed to rmtree
                # on platforms where `shutil._use_fd_functions == True`.  It is
                # bytes otherwise.  Fortunately, the methods used here accept
                # both.
                if function is not os.remove:
                    raise
                # read-only files cannot be unlinked under Windows
                s = os.stat(path)
                if (s.st_mode & stat.S_IWRITE) != 0:
                    raise
                os.chmod(path, stat.S_IMODE(s.st_mode) | stat.S_IWRITE)
                os.remove(path)

        else:

            def onexc(*args):
                pass

        try:
            # pytype: disable=wrong-keyword-args
            return shutil.rmtree(
                self.join(path), ignore_errors=ignore_errors, onexc=onexc
            )
            # pytype: enable=wrong-keyword-args
        except TypeError:  # onexc was introduced in Python 3.12
            return shutil.rmtree(
                self.join(path), ignore_errors=ignore_errors, onerror=onexc
            )

    def setflags(self, path: bytes, l: bool, x: bool) -> None:
        return util.setflags(self.join(path), l, x)

    def stat(self, path: bytes | None = None) -> os.stat_result:
        return os.stat(self.join(path))

    def unlink(self, path: bytes | None = None) -> None:
        return util.unlink(self.join(path))

    def tryunlink(self, path: bytes | None = None) -> bool:
        """Attempt to remove a file, ignoring missing file errors."""
        return util.tryunlink(self.join(path))

    def unlinkpath(
        self,
        path: bytes | None = None,
        ignoremissing: bool = False,
        rmdir: bool = True,
    ) -> None:
        return util.unlinkpath(
            self.join(path), ignoremissing=ignoremissing, rmdir=rmdir
        )

    # TODO: could be Tuple[float, float] too.
    def utime(
        self, path: bytes | None = None, t: tuple[int, int] | None = None
    ) -> None:
        return os.utime(self.join(path), t)

    def walk(
        self, path: bytes | None = None, onerror: _OnErrorFn | None = None
    ) -> Iterator[tuple[bytes, list[bytes], list[bytes]]]:
        """Yield (dirpath, dirs, files) tuple for each directory under path

        ``dirpath`` is relative one from the root of this vfs. This
        uses ``os.sep`` as path separator, even you specify POSIX
        style ``path``.

        "The root of this vfs" is represented as empty ``dirpath``.
        """
        root = os.path.normpath(self.join(None))
        # when dirpath == root, dirpath[prefixlen:] becomes empty
        # because len(dirpath) < prefixlen.
        prefixlen = len(pathutil.normasprefix(root))
        for dirpath, dirs, files in os.walk(self.join(path), onerror=onerror):
            yield (dirpath[prefixlen:], dirs, files)

    @contextlib.contextmanager
    def backgroundclosing(
        self, ui: uimod.ui, expectedcount: int = -1
    ) -> Iterator[backgroundfilecloser | None]:
        """Allow files to be closed asynchronously.

        When this context manager is active, ``backgroundclose`` can be passed
        to ``__call__``/``open`` to result in the file possibly being closed
        asynchronously, on a background thread.
        """
        # Sharing backgroundfilecloser between threads is complex and using
        # multiple instances puts us at risk of running out of file descriptors
        # only allow to use backgroundfilecloser when in main thread.
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        vfs = getattr(self, 'vfs', self)
        if getattr(vfs, '_backgroundfilecloser', None):
            raise error.Abort(
                _(b'can only have 1 active background file closer')
            )

        with backgroundfilecloser(ui, expectedcount=expectedcount) as bfc:
            try:
                vfs._backgroundfilecloser = (
                    bfc  # pytype: disable=attribute-error
                )
                yield bfc
            finally:
                vfs._backgroundfilecloser = (
                    None  # pytype: disable=attribute-error
                )

    def register_file(self, path: bytes) -> None:
        """generic hook point to lets fncache steer its stew"""

    def prepare_streamed_file(
        self, path: bytes, known_directories: set[bytes]
    ) -> tuple[bytes, int | None]:
        """make sure we are ready to write a file from a stream clone

        The "known_directories" variable is here to avoid trying to create the
        same directories over and over during a stream clone. It will be
        updated by this function.

        return (path, mode)::

            <path> is the real file system path content should be written to,
            <mode> is the file mode that need to be set if any.
        """
        real_path = self.join(path)
        self._auditpath(real_path, b'wb')
        self.register_file(path)
        dirname, basename = util.split(real_path)
        if dirname not in known_directories:
            util.makedirs(dirname, self.createmode, True)
            known_directories.add(dirname)
        mode = None
        if self.createmode is not None:
            mode = self.createmode & 0o666
        return real_path, mode


class vfs(abstractvfs):
    """Operate files relative to a base directory

    This class is used to hide the details of COW semantics and
    remote file access from higher level code.

    'cacheaudited' should be enabled only if (a) vfs object is short-lived, or
    (b) the base directory is managed by hg and considered sort-of append-only.
    See pathutil.pathauditor() for details.
    """

    audit: pathutil.pathauditor | Callable[[bytes, bytes | None], Any]
    base: bytes
    createmode: int | None
    options: dict[bytes, Any]
    _audit: bool
    _trustnlink: bool | None

    def __init__(
        self,
        base: bytes,
        audit: bool = True,
        cacheaudited: bool = False,
        expandpath: bool = False,
        realpath: bool = False,
    ) -> None:
        if expandpath:
            base = util.expandpath(base)
        if realpath:
            base = os.path.realpath(base)
        self.base = base
        self._audit = audit
        if audit:
            self.audit = pathutil.pathauditor(self.base, cached=cacheaudited)
        else:
            self.audit = lambda path, mode=None: True
        self.createmode = None
        self._trustnlink = None
        self.options = {}

    @util.propertycache
    def _cansymlink(self) -> bool:
        return util.checklink(self.base)

    @util.propertycache
    def _chmod(self) -> bool:
        return util.checkexec(self.base)

    def _fixfilemode(self, name: bytes) -> None:
        if self.createmode is None or not self._chmod:
            return
        os.chmod(name, self.createmode & 0o666)

    def _auditpath(self, path: bytes, mode: bytes) -> None:
        if self._audit:
            if os.path.isabs(path) and path.startswith(self.base):
                path = os.path.relpath(path, self.base)
            r = util.checkosfilename(path)
            if r:
                raise error.Abort(b"%s: %r" % (r, path))
            self.audit(path, mode=mode)

    def isfileorlink_checkdir(
        self,
        dircache: MutableMapping[bytes, bool],
        path: bytes,
    ) -> bool:
        """return True if the path is a regular file or a symlink and
        the directories along the path are "normal", that is
        not symlinks or nested hg repositories.

        Ignores the `_audit` setting, and checks the directories regardless.
        `dircache` is used to cache the directory checks.
        """
        try:
            for prefix in pathutil.finddirs_rev_noroot(util.localpath(path)):
                if prefix in dircache:
                    res = dircache[prefix]
                else:
                    res = pathutil.pathauditor._checkfs_exists(
                        self.base, prefix, path
                    )
                    dircache[prefix] = res
                if not res:
                    return False
        except (OSError, error.Abort):
            return False
        return self.isfileorlink(path)

    def __call__(
        self,
        path: bytes,
        mode: bytes = b"rb",
        atomictemp: bool = False,
        notindexed: bool = False,
        backgroundclose: bool = False,
        checkambig: bool = False,
        auditpath: bool = True,
        makeparentdirs: bool = True,
        buffering: int = -1,
    ) -> Any:  # TODO: should be BinaryIO if util.atomictempfile can be coersed
        """Open ``path`` file, which is relative to vfs root.

        By default, parent directories are created as needed. Newly created
        directories are marked as "not to be indexed by the content indexing
        service", if ``notindexed`` is specified for "write" mode access.
        Set ``makeparentdirs=False`` to not create directories implicitly.

        If ``backgroundclose`` is passed, the file may be closed asynchronously.
        It can only be used if the ``self.backgroundclosing()`` context manager
        is active. This should only be specified if the following criteria hold:

        1. There is a potential for writing thousands of files. Unless you
           are writing thousands of files, the performance benefits of
           asynchronously closing files is not realized.
        2. Files are opened exactly once for the ``backgroundclosing``
           active duration and are therefore free of race conditions between
           closing a file on a background thread and reopening it. (If the
           file were opened multiple times, there could be unflushed data
           because the original file handle hasn't been flushed/closed yet.)

        ``checkambig`` argument is passed to atomictempfile (valid
        only for writing), and is useful only if target file is
        guarded by any lock (e.g. repo.lock or repo.wlock).

        To avoid file stat ambiguity forcibly, checkambig=True involves
        copying ``path`` file opened in "append" mode (e.g. for
        truncation), if it is owned by another. Therefore, use
        combination of append mode and checkambig=True only in limited
        cases (see also issue5418 and issue5584 for detail).
        """
        if auditpath:
            self._auditpath(path, mode)
        f = self.join(path)

        if b"b" not in mode:
            mode += b"b"  # for that other OS

        nlink = -1
        if mode not in (b'r', b'rb'):
            dirname, basename = util.split(f)
            # If basename is empty, then the path is malformed because it points
            # to a directory. Let the posixfile() call below raise IOError.
            if basename:
                if atomictemp:
                    if makeparentdirs:
                        util.makedirs(dirname, self.createmode, notindexed)
                    return util.atomictempfile(
                        f, mode, self.createmode, checkambig=checkambig
                    )
                try:
                    if b'w' in mode:
                        util.unlink(f)
                        nlink = 0
                    else:
                        # nlinks() may behave differently for files on Windows
                        # shares if the file is open.
                        with util.posixfile(f):
                            nlink = util.nlinks(f)
                            if nlink < 1:
                                nlink = 2  # force mktempcopy (issue1922)
                except FileNotFoundError:
                    nlink = 0
                    if makeparentdirs:
                        util.makedirs(dirname, self.createmode, notindexed)
                if nlink > 0:
                    if self._trustnlink is None:
                        self._trustnlink = nlink > 1 or util.checknlink(f)
                    if nlink > 1 or not self._trustnlink:
                        util.rename(util.mktempcopy(f), f)
        fp = util.posixfile(f, mode, buffering=buffering)
        if nlink == 0:
            self._fixfilemode(f)

        if checkambig:
            if mode in (b'r', b'rb'):
                raise error.Abort(
                    _(
                        b'implementation error: mode %s is not'
                        b' valid for checkambig=True'
                    )
                    % mode
                )
            fp = checkambigatclosing(fp)

        if (
            backgroundclose
            and threading.current_thread() is threading.main_thread()
        ):
            if (
                not self._backgroundfilecloser  # pytype: disable=attribute-error
            ):
                raise error.Abort(
                    _(
                        b'backgroundclose can only be used when a '
                        b'backgroundclosing context manager is active'
                    )
                )

            fp = delayclosedfile(
                fp,
                self._backgroundfilecloser,  # pytype: disable=attribute-error
            )

        return fp

    def symlink(self, src: bytes, dst: bytes) -> None:
        self.audit(dst)
        linkname = self.join(dst)
        util.tryunlink(linkname)

        util.makedirs(os.path.dirname(linkname), self.createmode)

        if self._cansymlink:
            try:
                os.symlink(src, linkname)
            except OSError as err:
                raise OSError(
                    err.errno,
                    _(b'could not symlink to %r: %s')
                    % (src, encoding.strtolocal(err.strerror)),
                    linkname,
                )
        else:
            self.write(dst, src)

    def join(self, path: bytes | None, *insidef: bytes) -> bytes:
        if path:
            parts = [self.base, path]
            parts.extend(insidef)
            return self._join(*parts)
        else:
            return self.base


opener: type[vfs] = vfs


class proxyvfs(abstractvfs, abc.ABC):
    def __init__(self, vfs: vfs) -> None:
        self.vfs = vfs

    @property
    def createmode(self) -> int | None:
        return self.vfs.createmode

    def _auditpath(self, path: bytes, mode: bytes) -> None:
        return self.vfs._auditpath(path, mode)

    @property
    def options(self) -> dict[bytes, Any]:
        return self.vfs.options

    @options.setter
    def options(self, value: dict[bytes, Any]) -> None:
        self.vfs.options = value

    @property
    def audit(self):
        return self.vfs.audit


class filtervfs(proxyvfs, abstractvfs):
    '''Wrapper vfs for filtering filenames with a function.'''

    def __init__(self, vfs: vfs, filter: Callable[[bytes], bytes]) -> None:
        proxyvfs.__init__(self, vfs)
        self._filter = filter

    # TODO: The return type should be BinaryIO
    def __call__(self, path: bytes, *args, **kwargs) -> Any:
        return self.vfs(self._filter(path), *args, **kwargs)

    def join(self, path: bytes | None, *insidef: bytes) -> bytes:
        if path:
            return self.vfs.join(self._filter(self.vfs.reljoin(path, *insidef)))
        else:
            return self.vfs.join(path)


filteropener: type[filtervfs] = filtervfs


class readonlyvfs(proxyvfs):
    '''Wrapper vfs preventing any writing.'''

    def __init__(self, vfs: vfs) -> None:
        proxyvfs.__init__(self, vfs)

    # TODO: The return type should be BinaryIO
    def __call__(self, path: bytes, mode: bytes = b'rb', *args, **kw) -> Any:
        if mode not in (b'r', b'rb'):
            raise error.Abort(_(b'this vfs is read only'))
        return self.vfs(path, mode, *args, **kw)

    def join(self, path: bytes | None, *insidef: bytes) -> bytes:
        return self.vfs.join(path, *insidef)


class closewrapbase(abc.ABC):
    """Base class of wrapper, which hooks closing

    Do not instantiate outside the vfs layer.
    """

    def __init__(self, fh) -> None:
        object.__setattr__(self, '_origfh', fh)

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._origfh, attr)

    def __setattr__(self, attr: str, value: Any) -> None:
        return setattr(self._origfh, attr, value)

    def __delattr__(self, attr: str) -> None:
        return delattr(self._origfh, attr)

    def __enter__(self: _Tclosewrapbase) -> _Tclosewrapbase:
        self._origfh.__enter__()
        return self

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...


class delayclosedfile(closewrapbase):
    """Proxy for a file object whose close is delayed.

    Do not instantiate outside the vfs layer.
    """

    def __init__(self, fh, closer) -> None:
        super().__init__(fh)
        object.__setattr__(self, '_closer', closer)

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self._closer.close(self._origfh)

    def close(self) -> None:
        self._closer.close(self._origfh)


class backgroundfilecloser:
    """Coordinates background closing of file handles on multiple threads."""

    def __init__(self, ui: uimod.ui, expectedcount: int = -1) -> None:
        self._running = False
        self._entered = False
        self._threads = []
        self._threadexception = None

        # Only Windows/NTFS has slow file closing. So only enable by default
        # on that platform. But allow to be enabled elsewhere for testing.
        defaultenabled = pycompat.iswindows
        enabled = ui.configbool(b'worker', b'backgroundclose', defaultenabled)

        if not enabled:
            return

        # There is overhead to starting and stopping the background threads.
        # Don't do background processing unless the file count is large enough
        # to justify it.
        minfilecount = ui.configint(b'worker', b'backgroundcloseminfilecount')
        # FUTURE dynamically start background threads after minfilecount closes.
        # (We don't currently have any callers that don't know their file count)
        if expectedcount > 0 and expectedcount < minfilecount:
            return

        maxqueue = ui.configint(b'worker', b'backgroundclosemaxqueue')
        threadcount = ui.configint(b'worker', b'backgroundclosethreadcount')

        ui.debug(
            b'starting %d threads for background file closing\n' % threadcount
        )

        self._queue = pycompat.queue.Queue(maxsize=maxqueue)
        self._running = True

        for i in range(threadcount):
            t = threading.Thread(target=self._worker, name='backgroundcloser')
            self._threads.append(t)
            t.start()

    def __enter__(self: _Tbackgroundfilecloser) -> _Tbackgroundfilecloser:
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self._running = False

        # Wait for threads to finish closing so open files don't linger for
        # longer than lifetime of context manager.
        for t in self._threads:
            t.join()

    def _worker(self) -> None:
        """Main routine for worker thread."""
        while True:
            try:
                fh = self._queue.get(block=True, timeout=0.100)
                # Need to catch or the thread will terminate and
                # we could orphan file descriptors.
                try:
                    fh.close()
                except Exception as e:
                    # Stash so can re-raise from main thread later.
                    self._threadexception = e
            except pycompat.queue.Empty:
                if not self._running:
                    break

    def close(self, fh) -> None:
        """Schedule a file for closing."""
        if not self._entered:
            raise error.Abort(
                _(b'can only call close() when context manager active')
            )

        # If a background thread encountered an exception, raise now so we fail
        # fast. Otherwise, we may potentially go on for minutes until the error
        # is acted on.
        if self._threadexception:
            e = self._threadexception
            self._threadexception = None
            raise e

        # If we're not actively running, close synchronously.
        if not self._running:
            fh.close()
            return

        self._queue.put(fh, block=True, timeout=None)


class checkambigatclosing(closewrapbase):
    """Proxy for a file object, to avoid ambiguity of file stat

    See also util.filestat for detail about "ambiguity of file stat".

    This proxy is useful only if the target file is guarded by any
    lock (e.g. repo.lock or repo.wlock)

    Do not instantiate outside the vfs layer.
    """

    def __init__(self, fh) -> None:
        super().__init__(fh)
        object.__setattr__(self, '_oldstat', util.filestat.frompath(fh.name))

    def _checkambig(self) -> None:
        oldstat = self._oldstat
        if oldstat.stat:
            _avoidambig(self._origfh.name, oldstat)

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self._origfh.__exit__(exc_type, exc_value, exc_tb)
        self._checkambig()

    def close(self) -> None:
        self._origfh.close()
        self._checkambig()
