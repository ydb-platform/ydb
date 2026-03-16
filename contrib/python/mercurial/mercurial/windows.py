# windows.py - Windows utility function implementations for Mercurial
#
#  Copyright 2005-2009 Olivia Mackall <olivia@selenic.com> and others
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import errno
import getpass
import msvcrt  # pytype: disable=import-error
import os
import re
import stat
import string
import sys
import typing
import winreg  # pytype: disable=import-error

from typing import (
    Any,
    AnyStr,
    BinaryIO,
    Iterable,
    Iterator,
    Mapping,
    NoReturn,
    Pattern,
    Sequence,
)

from .i18n import _
from .interfaces import misc as int_misc

from . import (
    encoding,
    error,
    policy,
    pycompat,
    typelib,
    win32,
)


osutil = policy.importmod('osutil')

getfsmountpoint = win32.getvolumename
getfstype = win32.getfstype
getuser = win32.getuser
hidewindow = win32.hidewindow
makedir = win32.makedir
nlinks = win32.nlinks
oslink = win32.oslink
samedevice = win32.samedevice
samefile = win32.samefile
setsignalhandler = win32.setsignalhandler
spawndetached = win32.spawndetached
split = os.path.split
testpid = win32.testpid
unlink = win32.unlink

if typing.TYPE_CHECKING:

    def split(p: bytes) -> tuple[bytes, bytes]:
        raise NotImplementedError


umask: int = 0o022


class mixedfilemodewrapper:
    """Wraps a file handle when it is opened in read/write mode.

    fopen() and fdopen() on Windows have a specific-to-Windows requirement
    that files opened with mode r+, w+, or a+ make a call to a file positioning
    function when switching between reads and writes. Without this extra call,
    Python will raise a not very intuitive "IOError: [Errno 0] Error."

    This class wraps posixfile instances when the file is opened in read/write
    mode and automatically adds checks or inserts appropriate file positioning
    calls when necessary.
    """

    OPNONE = 0
    OPREAD = 1
    OPWRITE = 2

    def __init__(self, fp):
        object.__setattr__(self, '_fp', fp)
        object.__setattr__(self, '_lastop', 0)

    def __enter__(self):
        self._fp.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fp.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._fp, name)

    def __setattr__(self, name, value):
        return self._fp.__setattr__(name, value)

    def _noopseek(self):
        self._fp.seek(0, os.SEEK_CUR)

    def seek(self, *args, **kwargs):
        object.__setattr__(self, '_lastop', self.OPNONE)
        return self._fp.seek(*args, **kwargs)

    def write(self, d):
        if self._lastop == self.OPREAD:
            self._noopseek()

        object.__setattr__(self, '_lastop', self.OPWRITE)
        return self._fp.write(d)

    def writelines(self, *args, **kwargs):
        if self._lastop == self.OPREAD:
            self._noopeseek()

        object.__setattr__(self, '_lastop', self.OPWRITE)
        return self._fp.writelines(*args, **kwargs)

    def read(self, *args, **kwargs):
        if self._lastop == self.OPWRITE:
            self._noopseek()

        object.__setattr__(self, '_lastop', self.OPREAD)
        return self._fp.read(*args, **kwargs)

    def readline(self, *args, **kwargs):
        if self._lastop == self.OPWRITE:
            self._noopseek()

        object.__setattr__(self, '_lastop', self.OPREAD)
        return self._fp.readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):
        if self._lastop == self.OPWRITE:
            self._noopseek()

        object.__setattr__(self, '_lastop', self.OPREAD)
        return self._fp.readlines(*args, **kwargs)


class fdproxy:
    """Wraps osutil.posixfile() to override the name attribute to reflect the
    underlying file name.
    """

    def __init__(self, name, fp):
        self.name = name
        self._fp = fp

    def __enter__(self):
        self._fp.__enter__()
        # Return this wrapper for the context manager so that the name is
        # still available.
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._fp.__exit__(exc_type, exc_value, traceback)

    def __iter__(self):
        return iter(self._fp)

    def __getattr__(self, name):
        return getattr(self._fp, name)


def posixfile(name, mode=b'r', buffering=-1):
    '''Open a file with even more POSIX-like semantics'''
    try:
        fp = osutil.posixfile(name, mode, buffering)  # may raise WindowsError

        # PyFile_FromFd() ignores the name, and seems to report fp.name as the
        # underlying file descriptor.
        fp = fdproxy(name, fp)

        # The position when opening in append mode is implementation defined, so
        # make it consistent with other platforms, which position at EOF.
        if b'a' in mode:
            fp.seek(0, os.SEEK_END)

        if b'+' in mode:
            return mixedfilemodewrapper(fp)

        return fp
    except OSError as err:
        # convert to a friendlier exception
        raise OSError(
            err.errno, '%s: %s' % (encoding.strfromlocal(name), err.strerror)
        )


# may be wrapped by win32mbcs extension
listdir = osutil.listdir


def get_password() -> bytes:
    """Prompt for password with echo off, using Windows getch().

    This shouldn't be called directly- use ``ui.getpass()`` instead, which
    checks if the session is interactive first.
    """
    pw = ""
    while True:
        c = msvcrt.getwch()  # pytype: disable=module-attr
        if c == '\r' or c == '\n':
            break
        if c == '\003':
            raise KeyboardInterrupt
        if c == '\b':
            pw = pw[:-1]
        else:
            pw = pw + c
    msvcrt.putwch('\r')  # pytype: disable=module-attr
    msvcrt.putwch('\n')  # pytype: disable=module-attr
    return encoding.unitolocal(pw)


class winstdout(typelib.BinaryIO_Proxy):
    """Some files on Windows misbehave.

    When writing to a broken pipe, EINVAL instead of EPIPE may be raised.

    When writing too many bytes to a console at the same, a "Not enough space"
    error may happen. Python 3 already works around that.
    """

    def __init__(self, fp: BinaryIO):
        self.fp = fp

    def __getattr__(self, key):
        return getattr(self.fp, key)

    def close(self):
        try:
            self.fp.close()
        except OSError:
            pass

    def write(self, s):
        try:
            return self.fp.write(s)
        except OSError as inst:
            if inst.errno != 0 and not win32.lasterrorwaspipeerror(inst):
                raise
            self.close()
            raise OSError(errno.EPIPE, 'Broken pipe')

    def flush(self):
        try:
            return self.fp.flush()
        except OSError as inst:
            if not win32.lasterrorwaspipeerror(inst):
                raise
            raise OSError(errno.EPIPE, 'Broken pipe')


def openhardlinks() -> bool:
    return True


def parsepatchoutput(output_line: bytes) -> bytes:
    """parses the output produced by patch and returns the filename"""
    pf = output_line[14:]
    if pf[0] == b'`':
        pf = pf[1:-1]  # Remove the quotes
    return pf


def sshargs(
    sshcmd: bytes, host: bytes, user: bytes | None, port: bytes | None
) -> bytes:
    '''Build argument list for ssh or Plink'''
    pflag = b'-P' if b'plink' in sshcmd.lower() else b'-p'
    args = (b"%s@%s" % (user, host)) if user else host
    if args.startswith(b'-') or args.startswith(b'/'):
        raise error.Abort(
            _(b'illegal ssh hostname or username starting with - or /: %s')
            % args
        )
    args = shellquote(args)
    if port:
        args = b'%s %s %s' % (pflag, shellquote(port), args)
    return args


def setflags(f: bytes, l: bool, x: bool) -> None:
    pass


def copymode(
    src: bytes,
    dst: bytes,
    mode: int | None = None,
    enforcewritable: bool = False,
) -> None:
    pass


def checkexec(path: bytes) -> bool:
    return False


def checklink(path: bytes) -> bool:
    return False


def setbinary(fd) -> None:
    # When run without console, pipes may expose invalid
    # fileno(), usually set to -1.
    fno = getattr(fd, 'fileno', None)
    if fno is not None and fno() >= 0:
        msvcrt.setmode(fno(), os.O_BINARY)  # pytype: disable=module-attr


def pconvert(path: bytes) -> bytes:
    return path.replace(pycompat.ossep, b'/')


def localpath(path: bytes) -> bytes:
    return path.replace(b'/', b'\\')


def normpath(path: bytes) -> bytes:
    return pconvert(os.path.normpath(path))


def normcase(path: bytes) -> bytes:
    return encoding.upper(path)  # NTFS compares via upper()


DRIVE_RE_B: Pattern[bytes] = re.compile(b'^[a-z]:')
DRIVE_RE_S: Pattern[str] = re.compile('^[a-z]:')


# TODO: why is this accepting str?
def abspath(path: AnyStr) -> AnyStr:
    abs_path = os.path.abspath(path)  # re-exports
    # Python on Windows is inconsistent regarding the capitalization of drive
    # letter and this cause issue with various path comparison along the way.
    # So we normalize the drive later to upper case here.
    #
    # See https://bugs.python.org/issue40368 for and example of this hell.
    if isinstance(abs_path, bytes):
        if DRIVE_RE_B.match(abs_path):
            abs_path = abs_path[0:1].upper() + abs_path[1:]
    elif DRIVE_RE_S.match(abs_path):
        abs_path = abs_path[0:1].upper() + abs_path[1:]
    return abs_path


# see posix.py for definitions
normcasespec: int = encoding.normcasespecs.upper
normcasefallback = encoding.upperfallback


def samestat(s1: os.stat_result, s2: os.stat_result) -> bool:
    return False


def shelltocmdexe(path: bytes, env: Mapping[bytes, bytes]) -> bytes:
    r"""Convert shell variables in the form $var and ${var} inside ``path``
    to %var% form.  Existing Windows style variables are left unchanged.

    The variables are limited to the given environment.  Unknown variables are
    left unchanged.

    >>> e = {b'var1': b'v1', b'var2': b'v2', b'var3': b'v3'}
    >>> # Only valid values are expanded
    >>> shelltocmdexe(b'cmd $var1 ${var2} %var3% $missing ${missing} %missing%',
    ...               e)
    'cmd %var1% %var2% %var3% $missing ${missing} %missing%'
    >>> # Single quote prevents expansion, as does \$ escaping
    >>> shelltocmdexe(b"cmd '$var1 ${var2} %var3%' \$var1 \${var2} \\", e)
    'cmd "$var1 ${var2} %var3%" $var1 ${var2} \\'
    >>> # $$ is not special. %% is not special either, but can be the end and
    >>> # start of consecutive variables
    >>> shelltocmdexe(b"cmd $$ %% %var1%%var2%", e)
    'cmd $$ %% %var1%%var2%'
    >>> # No double substitution
    >>> shelltocmdexe(b"$var1 %var1%", {b'var1': b'%var2%', b'var2': b'boom'})
    '%var1% %var1%'
    >>> # Tilde expansion
    >>> shelltocmdexe(b"~/dir ~\dir2 ~tmpfile \~/", {})
    '%USERPROFILE%/dir %USERPROFILE%\\dir2 ~tmpfile ~/'
    """
    if not any(c in path for c in b"$'~"):
        return path

    varchars = pycompat.sysbytes(string.ascii_letters + string.digits) + b'_-'

    res = b''
    index = 0
    pathlen = len(path)
    while index < pathlen:
        c = path[index : index + 1]
        if c == b'\'':  # no expansion within single quotes
            path = path[index + 1 :]
            pathlen = len(path)
            try:
                index = path.index(b'\'')
                res += b'"' + path[:index] + b'"'
            except ValueError:
                res += c + path
                index = pathlen - 1
        elif c == b'%':  # variable
            path = path[index + 1 :]
            pathlen = len(path)
            try:
                index = path.index(b'%')
            except ValueError:
                res += b'%' + path
                index = pathlen - 1
            else:
                var = path[:index]
                res += b'%' + var + b'%'
        elif c == b'$':  # variable
            if path[index + 1 : index + 2] == b'{':
                path = path[index + 2 :]
                pathlen = len(path)
                try:
                    index = path.index(b'}')
                    var = path[:index]

                    # See below for why empty variables are handled specially
                    if env.get(var, b'') != b'':
                        res += b'%' + var + b'%'
                    else:
                        res += b'${' + var + b'}'
                except ValueError:
                    res += b'${' + path
                    index = pathlen - 1
            else:
                var = b''
                index += 1
                c = path[index : index + 1]
                while c != b'' and c in varchars:
                    var += c
                    index += 1
                    c = path[index : index + 1]
                # Some variables (like HG_OLDNODE) may be defined, but have an
                # empty value.  Those need to be skipped because when spawning
                # cmd.exe to run the hook, it doesn't replace %VAR% for an empty
                # VAR, and that really confuses things like revset expressions.
                # OTOH, if it's left in Unix format and the hook runs sh.exe, it
                # will substitute to an empty string, and everything is happy.
                if env.get(var, b'') != b'':
                    res += b'%' + var + b'%'
                else:
                    res += b'$' + var

                if c != b'':
                    index -= 1
        elif (
            c == b'~'
            and index + 1 < pathlen
            and path[index + 1 : index + 2] in (b'\\', b'/')
        ):
            res += b"%USERPROFILE%"
        elif (
            c == b'\\'
            and index + 1 < pathlen
            and path[index + 1 : index + 2] in (b'$', b'~')
        ):
            # Skip '\', but only if it is escaping $ or ~
            res += path[index + 1 : index + 2]
            index += 1
        else:
            res += c

        index += 1
    return res


# A sequence of backslashes is special iff it precedes a double quote:
# - if there's an even number of backslashes, the double quote is not
#   quoted (i.e. it ends the quoted region)
# - if there's an odd number of backslashes, the double quote is quoted
# - in both cases, every pair of backslashes is unquoted into a single
#   backslash
# (See http://msdn2.microsoft.com/en-us/library/a1y7w461.aspx )
# So, to quote a string, we must surround it in double quotes, double
# the number of backslashes that precede double quotes and add another
# backslash before every double quote (being careful with the double
# quote we've appended to the end)
_quotere: Pattern[bytes] | None = None
_needsshellquote = None


def shellquote(s: bytes) -> bytes:
    r"""
    >>> shellquote(br'C:\Users\xyz')
    '"C:\\Users\\xyz"'
    >>> shellquote(br'C:\Users\xyz/mixed')
    '"C:\\Users\\xyz/mixed"'
    >>> # Would be safe not to quote too, since it is all double backslashes
    >>> shellquote(br'C:\\Users\\xyz')
    '"C:\\\\Users\\\\xyz"'
    >>> # But this must be quoted
    >>> shellquote(br'C:\\Users\\xyz/abc')
    '"C:\\\\Users\\\\xyz/abc"'
    """
    global _quotere
    if _quotere is None:
        _quotere = re.compile(br'(\\*)("|\\$)')
    global _needsshellquote
    if _needsshellquote is None:
        # ":" is also treated as "safe character", because it is used as a part
        # of path name on Windows.  "\" is also part of a path name, but isn't
        # safe because shlex.split() (kind of) treats it as an escape char and
        # drops it.  It will leave the next character, even if it is another
        # "\".
        _needsshellquote = re.compile(br'[^a-zA-Z0-9._:/-]').search
    if s and not _needsshellquote(s) and not _quotere.search(s):
        # "s" shouldn't have to be quoted
        return s
    return b'"%s"' % _quotere.sub(br'\1\1\\\2', s)


def _unquote(s: bytes) -> bytes:
    if s.startswith(b'"') and s.endswith(b'"'):
        return s[1:-1]
    return s


def shellsplit(s: bytes) -> list[bytes]:
    """Parse a command string in cmd.exe way (best-effort)"""
    return pycompat.maplist(_unquote, pycompat.shlexsplit(s, posix=False))


# if you change this stub into a real check, please try to implement the
# username and groupname functions above, too.
def isowner(st: os.stat_result) -> bool:
    return True


def findexe(command: bytes) -> bytes | None:
    """Find executable for command searching like cmd.exe does.
    If command is a basename then PATH is searched for command.
    PATH isn't searched if command is an absolute or relative path.
    An extension from PATHEXT is found and added if not present.
    If command isn't found None is returned."""
    pathext = encoding.environ.get(b'PATHEXT', b'.COM;.EXE;.BAT;.CMD')
    pathexts = [ext for ext in pathext.lower().split(pycompat.ospathsep)]
    if os.path.splitext(command)[1].lower() in pathexts:
        pathexts = [b'']

    def findexisting(pathcommand: bytes) -> bytes | None:
        """Will append extension (if needed) and return existing file"""
        for ext in pathexts:
            executable = pathcommand + ext
            if os.path.exists(executable):
                return executable
        return None

    if pycompat.ossep in command:
        return findexisting(command)

    for path in encoding.environ.get(b'PATH', b'').split(pycompat.ospathsep):
        executable = findexisting(os.path.join(path, command))
        if executable is not None:
            return executable
    return findexisting(os.path.expanduser(os.path.expandvars(command)))


_wantedkinds = {stat.S_IFREG, stat.S_IFLNK}


def statfiles(files: Sequence[bytes]) -> Iterator[os.stat_result | None]:
    """Stat each file in files. Yield each stat, or None if a file
    does not exist or has a type we don't care about.

    Cluster and cache stat per directory to minimize number of OS stat calls."""
    dircache = {}  # dirname -> filename -> status | None if file does not exist
    getkind = stat.S_IFMT
    for nf in files:
        nf = normcase(nf)
        dir, base = os.path.split(nf)
        if not dir:
            dir = b'.'
        cache = dircache.get(dir, None)
        if cache is None:
            try:
                dmap = {
                    normcase(n): s
                    for n, k, s in listdir(dir, True)
                    if getkind(s.st_mode) in _wantedkinds
                }
            except (FileNotFoundError, NotADirectoryError):
                dmap = {}
            cache = dircache.setdefault(dir, dmap)
        yield cache.get(base, None)


def username(uid: int | None = None) -> bytes | None:
    """Return the name of the user with the given uid.

    If uid is None, return the name of the current user."""
    if not uid:
        try:
            return pycompat.fsencode(getpass.getuser())
        except ModuleNotFoundError:
            # getpass.getuser() checks for a few environment variables first,
            # but if those aren't set, imports pwd and calls getpwuid(), none of
            # which exists on Windows.
            pass
    return None


def groupname(gid: int | None = None) -> bytes | None:
    """Return the name of the group with the given gid.

    If gid is None, return the name of the current group."""
    return None


def readlink(path: bytes) -> bytes:
    path_str = pycompat.fsdecode(path)
    try:
        link = os.readlink(path_str)
    except ValueError as e:
        # On py2, os.readlink() raises an AttributeError since it is
        # unsupported.  On py3, reading a non-link raises a ValueError.  Simply
        # treat this as the error the locking code has been expecting up to now
        # until an effort can be made to enable symlink support on Windows.
        raise AttributeError(e)
    return pycompat.fsencode(link)


def removedirs(name: bytes) -> None:
    """special version of os.removedirs that does not remove symlinked
    directories or junction points if they actually contain files"""
    if listdir(name):
        return
    os.rmdir(name)
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    while head and tail:
        try:
            if listdir(head):
                return
            os.rmdir(head)
        except (ValueError, OSError):
            break
        head, tail = os.path.split(head)


def rename(src: bytes, dst: bytes) -> None:
    '''atomically rename file src to dst, replacing dst if it exists'''
    try:
        os.rename(src, dst)
    except FileExistsError:
        unlink(dst)
        os.rename(src, dst)


def gethgcmd() -> list[bytes]:
    return [encoding.strtolocal(arg) for arg in [sys.executable] + sys.argv[:1]]


def groupmembers(name: bytes) -> list[bytes]:
    # Don't support groups on Windows for now
    raise KeyError


def isexec(f: bytes) -> bool:
    return False


class cachestat(int_misc.ICacheStat):
    stat: os.stat_result

    def __init__(self, path: bytes) -> None:
        self.stat = os.stat(path)

    def cacheable(self) -> bool:
        return bool(self.stat.st_ino)

    __hash__ = object.__hash__

    def __eq__(self, other: Any) -> bool:
        try:
            # Only dev, ino, size, mtime and atime are likely to change. Out
            # of these, we shouldn't compare atime but should compare the
            # rest. However, one of the other fields changing indicates
            # something fishy going on, so return False if anything but atime
            # changes.
            return (
                self.stat.st_ino == other.stat.st_ino
                and self.stat.st_dev == other.stat.st_dev
                and self.stat.st_nlink == other.stat.st_nlink
                and self.stat.st_uid == other.stat.st_uid
                and self.stat.st_gid == other.stat.st_gid
                and self.stat.st_size == other.stat.st_size
                and self.stat[stat.ST_MTIME] == other.stat[stat.ST_MTIME]
                and self.stat[stat.ST_CTIME] == other.stat[stat.ST_CTIME]
            )
        except AttributeError:
            return False

    def __ne__(self, other: Any) -> bool:
        return not self == other


def lookupreg(
    key: bytes,
    valname: bytes | None = None,
    scope: int | Iterable[int] | None = None,
) -> bytes | None:
    """Look up a key/value name in the Windows registry.

    valname: value name. If unspecified, the default value for the key
    is used.
    scope: optionally specify scope for registry lookup, this can be
    a sequence of scopes to look up in order. Default (CURRENT_USER,
    LOCAL_MACHINE).
    """
    if scope is None:
        # pytype: disable=module-attr
        scope = (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE)
        # pytype: enable=module-attr
    elif not isinstance(scope, (list, tuple)):
        scope = (scope,)
    for s in scope:
        try:
            # pytype: disable=module-attr
            with winreg.OpenKey(s, encoding.strfromlocal(key)) as hkey:
                # pytype: enable=module-attr
                name = None
                if valname is not None:
                    name = encoding.strfromlocal(valname)
                # pytype: disable=module-attr
                val = winreg.QueryValueEx(hkey, name)[0]
                # pytype: enable=module-attr

                # never let a Unicode string escape into the wild
                return encoding.unitolocal(val)
        except OSError:
            pass


expandglobs: bool = True


def statislink(st: os.stat_result | None) -> bool:
    '''check whether a stat result is a symlink'''
    return False


def statisexec(st: os.stat_result | None) -> bool:
    '''check whether a stat result is an executable file'''
    return False


def poll(fds) -> list:
    # see posix.py for description
    raise NotImplementedError()


def readpipe(pipe) -> bytes:
    """Read all available data from a pipe."""
    chunks = []
    while True:
        size = win32.peekpipe(pipe)
        if not size:
            break

        s = pipe.read(size)
        if not s:
            break
        chunks.append(s)

    return b''.join(chunks)


def bindunixsocket(sock, path: bytes) -> NoReturn:
    raise NotImplementedError('unsupported platform')
