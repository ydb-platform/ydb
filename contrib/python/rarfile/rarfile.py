# rarfile.py
#
# Copyright (c) 2005-2024  Marko Kreen <markokr@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""RAR archive reader.

This is Python module for Rar archive reading.  The interface
is made as :mod:`zipfile`-like as possible.

Basic logic:
 - Parse archive structure with Python.
 - Extract non-compressed files with Python
 - Extract compressed files with unrar.
 - Optionally write compressed data to temp file to speed up unrar,
   otherwise it needs to scan whole archive on each execution.

Example::

    import rarfile

    rf = rarfile.RarFile("myarchive.rar")
    for f in rf.infolist():
        print(f.filename, f.file_size)
        if f.filename == "README":
            print(rf.read(f))

Archive files can also be accessed via file-like object returned
by :meth:`RarFile.open`::

    import rarfile

    with rarfile.RarFile("archive.rar") as rf:
        with rf.open("README") as f:
            for ln in f:
                print(ln.strip())

For decompression to work, either ``unrar`` or ``unar`` tool must be in PATH.
"""

import errno
import io
import os
import re
import shutil
import struct
import sys
import warnings
from binascii import crc32, hexlify
from datetime import datetime, timezone
from hashlib import blake2s, pbkdf2_hmac, sha1, sha256
from pathlib import Path
from struct import Struct, pack, unpack
from subprocess import DEVNULL, PIPE, STDOUT, Popen
from tempfile import mkstemp

AES = None

# only needed for encrypted headers
try:
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.ciphers import (
            Cipher, algorithms, modes,
        )
        _have_crypto = 1
    except ImportError:
        from Crypto.Cipher import AES
        _have_crypto = 2
except ImportError:
    _have_crypto = 0


class AES_CBC_Decrypt:
    """Decrypt API"""
    def __init__(self, key, iv):
        if _have_crypto == 2:
            self.decrypt = AES.new(key, AES.MODE_CBC, iv).decrypt
        else:
            ciph = Cipher(algorithms.AES(key), modes.CBC(iv), default_backend())
            self.decrypt = ciph.decryptor().update


__version__ = "4.2"

# export only interesting items
__all__ = ["get_rar_version", "is_rarfile", "is_rarfile_sfx", "RarInfo", "RarFile", "RarExtFile"]

##
## Module configuration.  Can be tuned after importing.
##

#: executable for unrar tool
UNRAR_TOOL = "unrar"

#: executable for unar tool
UNAR_TOOL = "unar"

#: executable for bsdtar tool
BSDTAR_TOOL = "bsdtar"

#: executable for p7zip/7z tool
SEVENZIP_TOOL = "7z"

#: executable for alternative 7z tool
SEVENZIP2_TOOL = "7zz"

#: default fallback charset
DEFAULT_CHARSET = "windows-1252"

#: list of encodings to try, with fallback to DEFAULT_CHARSET if none succeed
TRY_ENCODINGS = ("utf8", "utf-16le")

#: whether to speed up decompression by using tmp archive
USE_EXTRACT_HACK = 1

#: limit the filesize for tmp archive usage
HACK_SIZE_LIMIT = 20 * 1024 * 1024

#: set specific directory for mkstemp() used by hack dir usage
HACK_TMP_DIR = None

#: Separator for path name components.  Always "/".
PATH_SEP = "/"

##
## rar constants
##

# block types
RAR_BLOCK_MARK = 0x72           # r
RAR_BLOCK_MAIN = 0x73           # s
RAR_BLOCK_FILE = 0x74           # t
RAR_BLOCK_OLD_COMMENT = 0x75    # u
RAR_BLOCK_OLD_EXTRA = 0x76      # v
RAR_BLOCK_OLD_SUB = 0x77        # w
RAR_BLOCK_OLD_RECOVERY = 0x78   # x
RAR_BLOCK_OLD_AUTH = 0x79       # y
RAR_BLOCK_SUB = 0x7a            # z
RAR_BLOCK_ENDARC = 0x7b         # {

# flags for RAR_BLOCK_MAIN
RAR_MAIN_VOLUME = 0x0001
RAR_MAIN_COMMENT = 0x0002
RAR_MAIN_LOCK = 0x0004
RAR_MAIN_SOLID = 0x0008
RAR_MAIN_NEWNUMBERING = 0x0010
RAR_MAIN_AUTH = 0x0020
RAR_MAIN_RECOVERY = 0x0040
RAR_MAIN_PASSWORD = 0x0080
RAR_MAIN_FIRSTVOLUME = 0x0100
RAR_MAIN_ENCRYPTVER = 0x0200

# flags for RAR_BLOCK_FILE
RAR_FILE_SPLIT_BEFORE = 0x0001
RAR_FILE_SPLIT_AFTER = 0x0002
RAR_FILE_PASSWORD = 0x0004
RAR_FILE_COMMENT = 0x0008
RAR_FILE_SOLID = 0x0010
RAR_FILE_DICTMASK = 0x00e0
RAR_FILE_DICT64 = 0x0000
RAR_FILE_DICT128 = 0x0020
RAR_FILE_DICT256 = 0x0040
RAR_FILE_DICT512 = 0x0060
RAR_FILE_DICT1024 = 0x0080
RAR_FILE_DICT2048 = 0x00a0
RAR_FILE_DICT4096 = 0x00c0
RAR_FILE_DIRECTORY = 0x00e0
RAR_FILE_LARGE = 0x0100
RAR_FILE_UNICODE = 0x0200
RAR_FILE_SALT = 0x0400
RAR_FILE_VERSION = 0x0800
RAR_FILE_EXTTIME = 0x1000
RAR_FILE_EXTFLAGS = 0x2000

# flags for RAR_BLOCK_ENDARC
RAR_ENDARC_NEXT_VOLUME = 0x0001
RAR_ENDARC_DATACRC = 0x0002
RAR_ENDARC_REVSPACE = 0x0004
RAR_ENDARC_VOLNR = 0x0008

# flags common to all blocks
RAR_SKIP_IF_UNKNOWN = 0x4000
RAR_LONG_BLOCK = 0x8000

# Host OS types
RAR_OS_MSDOS = 0    #: MSDOS (only in RAR3)
RAR_OS_OS2 = 1      #: OS2 (only in RAR3)
RAR_OS_WIN32 = 2    #: Windows
RAR_OS_UNIX = 3     #: UNIX
RAR_OS_MACOS = 4    #: MacOS (only in RAR3)
RAR_OS_BEOS = 5     #: BeOS (only in RAR3)

# Compression methods - "0".."5"
RAR_M0 = 0x30   #: No compression.
RAR_M1 = 0x31   #: Compression level `-m1` - Fastest compression.
RAR_M2 = 0x32   #: Compression level `-m2`.
RAR_M3 = 0x33   #: Compression level `-m3`.
RAR_M4 = 0x34   #: Compression level `-m4`.
RAR_M5 = 0x35   #: Compression level `-m5` - Maximum compression.

#
# RAR5 constants
#

RAR5_BLOCK_MAIN = 1
RAR5_BLOCK_FILE = 2
RAR5_BLOCK_SERVICE = 3
RAR5_BLOCK_ENCRYPTION = 4
RAR5_BLOCK_ENDARC = 5

RAR5_BLOCK_FLAG_EXTRA_DATA = 0x01
RAR5_BLOCK_FLAG_DATA_AREA = 0x02
RAR5_BLOCK_FLAG_SKIP_IF_UNKNOWN = 0x04
RAR5_BLOCK_FLAG_SPLIT_BEFORE = 0x08
RAR5_BLOCK_FLAG_SPLIT_AFTER = 0x10
RAR5_BLOCK_FLAG_DEPENDS_PREV = 0x20
RAR5_BLOCK_FLAG_KEEP_WITH_PARENT = 0x40

RAR5_MAIN_FLAG_ISVOL = 0x01
RAR5_MAIN_FLAG_HAS_VOLNR = 0x02
RAR5_MAIN_FLAG_SOLID = 0x04
RAR5_MAIN_FLAG_RECOVERY = 0x08
RAR5_MAIN_FLAG_LOCKED = 0x10

RAR5_FILE_FLAG_ISDIR = 0x01
RAR5_FILE_FLAG_HAS_MTIME = 0x02
RAR5_FILE_FLAG_HAS_CRC32 = 0x04
RAR5_FILE_FLAG_UNKNOWN_SIZE = 0x08

RAR5_COMPR_SOLID = 0x40

RAR5_ENC_FLAG_HAS_CHECKVAL = 0x01

RAR5_ENDARC_FLAG_NEXT_VOL = 0x01

RAR5_XFILE_ENCRYPTION = 1
RAR5_XFILE_HASH = 2
RAR5_XFILE_TIME = 3
RAR5_XFILE_VERSION = 4
RAR5_XFILE_REDIR = 5
RAR5_XFILE_OWNER = 6
RAR5_XFILE_SERVICE = 7

RAR5_XTIME_UNIXTIME = 0x01
RAR5_XTIME_HAS_MTIME = 0x02
RAR5_XTIME_HAS_CTIME = 0x04
RAR5_XTIME_HAS_ATIME = 0x08
RAR5_XTIME_UNIXTIME_NS = 0x10

RAR5_XENC_CIPHER_AES256 = 0

RAR5_XENC_CHECKVAL = 0x01
RAR5_XENC_TWEAKED = 0x02

RAR5_XHASH_BLAKE2SP = 0

RAR5_XREDIR_UNIX_SYMLINK = 1
RAR5_XREDIR_WINDOWS_SYMLINK = 2
RAR5_XREDIR_WINDOWS_JUNCTION = 3
RAR5_XREDIR_HARD_LINK = 4
RAR5_XREDIR_FILE_COPY = 5

RAR5_XREDIR_ISDIR = 0x01

RAR5_XOWNER_UNAME = 0x01
RAR5_XOWNER_GNAME = 0x02
RAR5_XOWNER_UID = 0x04
RAR5_XOWNER_GID = 0x08

RAR5_OS_WINDOWS = 0
RAR5_OS_UNIX = 1

DOS_MODE_ARCHIVE = 0x20
DOS_MODE_DIR = 0x10
DOS_MODE_SYSTEM = 0x04
DOS_MODE_HIDDEN = 0x02
DOS_MODE_READONLY = 0x01

RAR5_PW_CHECK_SIZE = 8
RAR5_PW_SUM_SIZE = 4

##
## internal constants
##

RAR_ID = b"Rar!\x1a\x07\x00"
RAR5_ID = b"Rar!\x1a\x07\x01\x00"

WIN32 = sys.platform == "win32"
BSIZE = 512 * 1024 if WIN32 else 64 * 1024

SFX_MAX_SIZE = 2 * 1024 * 1024
RAR_V3 = 3
RAR_V5 = 5

_BAD_CHARS = r"""\x00-\x1F<>|"?*"""
RC_BAD_CHARS_UNIX = re.compile(r"[%s]" % _BAD_CHARS)
RC_BAD_CHARS_WIN32 = re.compile(r"[%s:^\\]" % _BAD_CHARS)

FORCE_TOOL = False


def _find_sfx_header(xfile):
    sig = RAR_ID[:-1]
    buf = io.BytesIO()
    steps = (64, SFX_MAX_SIZE)

    with XFile(xfile) as fd:
        for step in steps:
            data = fd.read(step)
            if not data:
                break
            buf.write(data)
            curdata = buf.getvalue()
            findpos = 0
            while True:
                pos = curdata.find(sig, findpos)
                if pos < 0:
                    break
                if curdata[pos:pos + len(RAR_ID)] == RAR_ID:
                    return RAR_V3, pos
                if curdata[pos:pos + len(RAR5_ID)] == RAR5_ID:
                    return RAR_V5, pos
                findpos = pos + len(sig)
    return 0, 0


##
## Public interface
##


def get_rar_version(xfile):
    """Check quickly whether file is rar archive.
    """
    with XFile(xfile) as fd:
        buf = fd.read(len(RAR5_ID))
    if buf.startswith(RAR_ID):
        return RAR_V3
    elif buf.startswith(RAR5_ID):
        return RAR_V5
    return 0


def is_rarfile(xfile):
    """Check quickly whether file is rar archive.
    """
    try:
        return get_rar_version(xfile) > 0
    except OSError:
        # File not found or not accessible, ignore
        return False


def is_rarfile_sfx(xfile):
    """Check whether file is rar archive with support for SFX.

    It will read 2M from file.
    """
    return _find_sfx_header(xfile)[0] > 0


class Error(Exception):
    """Base class for rarfile errors."""


class BadRarFile(Error):
    """Incorrect data in archive."""


class NotRarFile(Error):
    """The file is not RAR archive."""


class BadRarName(Error):
    """Cannot guess multipart name components."""


class NoRarEntry(Error):
    """File not found in RAR"""


class PasswordRequired(Error):
    """File requires password"""


class NeedFirstVolume(Error):
    """Need to start from first volume.

    Attributes:

        current_volume
            Volume number of current file or None if not known
    """
    def __init__(self, msg, volume):
        super().__init__(msg)
        self.current_volume = volume


class NoCrypto(Error):
    """Cannot parse encrypted headers - no crypto available."""


class RarExecError(Error):
    """Problem reported by unrar/rar."""


class RarWarning(RarExecError):
    """Non-fatal error"""


class RarFatalError(RarExecError):
    """Fatal error"""


class RarCRCError(RarExecError):
    """CRC error during unpacking"""


class RarLockedArchiveError(RarExecError):
    """Must not modify locked archive"""


class RarWriteError(RarExecError):
    """Write error"""


class RarOpenError(RarExecError):
    """Open error"""


class RarUserError(RarExecError):
    """User error"""


class RarMemoryError(RarExecError):
    """Memory error"""


class RarCreateError(RarExecError):
    """Create error"""


class RarNoFilesError(RarExecError):
    """No files that match pattern were found"""


class RarUserBreak(RarExecError):
    """User stop"""


class RarWrongPassword(RarExecError):
    """Incorrect password"""


class RarUnknownError(RarExecError):
    """Unknown exit code"""


class RarSignalExit(RarExecError):
    """Unrar exited with signal"""


class RarCannotExec(RarExecError):
    """Executable not found."""


class UnsupportedWarning(UserWarning):
    """Archive uses feature that are unsupported by rarfile.

    .. versionadded:: 4.0
    """


class RarInfo:
    r"""An entry in rar archive.

    Timestamps as :class:`~datetime.datetime` are without timezone in RAR3,
    with UTC timezone in RAR5 archives.

    Attributes:

        filename
            File name with relative path.
            Path separator is "/".  Always unicode string.

        date_time
            File modification timestamp.   As tuple of (year, month, day, hour, minute, second).
            RAR5 allows archives where it is missing, it's None then.

        comment
            Optional file comment field.  Unicode string.  (RAR3-only)

        file_size
            Uncompressed size.

        compress_size
            Compressed size.

        compress_type
            Compression method: one of :data:`RAR_M0` .. :data:`RAR_M5` constants.

        extract_version
            Minimal Rar version needed for decompressing.  As (major*10 + minor),
            so 2.9 is 29.

            RAR3: 10, 20, 29

            RAR5 does not have such field in archive, it's simply set to 50.

        host_os
            Host OS type, one of RAR_OS_* constants.

            RAR3: :data:`RAR_OS_WIN32`, :data:`RAR_OS_UNIX`, :data:`RAR_OS_MSDOS`,
            :data:`RAR_OS_OS2`, :data:`RAR_OS_BEOS`.

            RAR5: :data:`RAR_OS_WIN32`, :data:`RAR_OS_UNIX`.

        mode
            File attributes. May be either dos-style or unix-style, depending on host_os.

        mtime
            File modification time.  Same value as :attr:`date_time`
            but as :class:`~datetime.datetime` object with extended precision.

        ctime
            Optional time field: creation time.  As :class:`~datetime.datetime` object.

        atime
            Optional time field: last access time.  As :class:`~datetime.datetime` object.

        arctime
            Optional time field: archival time.  As :class:`~datetime.datetime` object.
            (RAR3-only)

        CRC
            CRC-32 of uncompressed file, unsigned int.

            RAR5: may be None.

        blake2sp_hash
            Blake2SP hash over decompressed data.  (RAR5-only)

        volume
            Volume nr, starting from 0.

        volume_file
            Volume file name, where file starts.

        file_redir
            If not None, file is link of some sort.  Contains tuple of (type, flags, target).
            (RAR5-only)

            Type is one of constants:

                :data:`RAR5_XREDIR_UNIX_SYMLINK`
                    Unix symlink.
                :data:`RAR5_XREDIR_WINDOWS_SYMLINK`
                    Windows symlink.
                :data:`RAR5_XREDIR_WINDOWS_JUNCTION`
                    Windows junction.
                :data:`RAR5_XREDIR_HARD_LINK`
                    Hard link to target.
                :data:`RAR5_XREDIR_FILE_COPY`
                    Current file is copy of another archive entry.

            Flags may contain bits:

                :data:`RAR5_XREDIR_ISDIR`
                    Symlink points to directory.
    """

    # zipfile-compatible fields
    filename = None
    file_size = None
    compress_size = None
    date_time = None
    CRC = None
    volume = None
    orig_filename = None

    # optional extended time fields, datetime() objects.
    mtime = None
    ctime = None
    atime = None

    extract_version = None
    mode = None
    host_os = None
    compress_type = None

    # rar3-only fields
    comment = None
    arctime = None

    # rar5-only fields
    blake2sp_hash = None
    file_redir = None

    # internal fields
    flags = 0
    type = None

    # zipfile compat
    def is_dir(self):
        """Returns True if entry is a directory.

        .. versionadded:: 4.0
        """
        return False

    def is_symlink(self):
        """Returns True if entry is a symlink.

        .. versionadded:: 4.0
        """
        return False

    def is_file(self):
        """Returns True if entry is a normal file.

        .. versionadded:: 4.0
        """
        return False

    def needs_password(self):
        """Returns True if data is stored password-protected.
        """
        if self.type == RAR_BLOCK_FILE:
            return (self.flags & RAR_FILE_PASSWORD) > 0
        return False

    def isdir(self):
        """Returns True if entry is a directory.

        .. deprecated:: 4.0
        """
        return self.is_dir()


class RarFile:
    """Parse RAR structure, provide access to files in archive.

    Parameters:

        file
            archive file name or file-like object.
        mode
            only "r" is supported.
        charset
            fallback charset to use, if filenames are not already Unicode-enabled.
        info_callback
            debug callback, gets to see all archive entries.
        crc_check
            set to False to disable CRC checks
        errors
            Either "stop" to quietly stop parsing on errors,
            or "strict" to raise errors.  Default is "stop".
        part_only
            If True, read only single file and allow it to be middle-part
            of multi-volume archive.

            .. versionadded:: 4.0
    """

    #: File name, if available.  Unicode string or None.
    filename = None

    #: Archive comment.  Unicode string or None.
    comment = None

    def __init__(self, file, mode="r", charset=None, info_callback=None,
                 crc_check=True, errors="stop", part_only=False):
        if is_filelike(file):
            self.filename = getattr(file, "name", None)
        else:
            if isinstance(file, Path):
                file = str(file)
            self.filename = file
        self._rarfile = file

        self._charset = charset or DEFAULT_CHARSET
        self._info_callback = info_callback
        self._crc_check = crc_check
        self._part_only = part_only
        self._password = None
        self._file_parser = None

        if errors == "stop":
            self._strict = False
        elif errors == "strict":
            self._strict = True
        else:
            raise ValueError("Invalid value for errors= parameter.")

        if mode != "r":
            raise NotImplementedError("RarFile supports only mode=r")

        self._parse()

    def __enter__(self):
        """Open context."""
        return self

    def __exit__(self, typ, value, traceback):
        """Exit context."""
        self.close()

    def __iter__(self):
        """Iterate over members."""
        return iter(self.infolist())

    def setpassword(self, pwd):
        """Sets the password to use when extracting.
        """
        self._password = pwd
        if self._file_parser:
            if self._file_parser.has_header_encryption():
                self._file_parser = None
        if not self._file_parser:
            self._parse()
        else:
            self._file_parser.setpassword(self._password)

    def needs_password(self):
        """Returns True if any archive entries require password for extraction.
        """
        return self._file_parser.needs_password()

    def is_solid(self):
        """Returns True if archive uses solid compression.

        .. versionadded:: 4.2
        """
        return self._file_parser.is_solid()

    def namelist(self):
        """Return list of filenames in archive.
        """
        return [f.filename for f in self.infolist()]

    def infolist(self):
        """Return RarInfo objects for all files/directories in archive.
        """
        return self._file_parser.infolist()

    def volumelist(self):
        """Returns filenames of archive volumes.

        In case of single-volume archive, the list contains
        just the name of main archive file.
        """
        return self._file_parser.volumelist()

    def getinfo(self, name):
        """Return RarInfo for file.
        """
        return self._file_parser.getinfo(name)

    def getinfo_orig(self, name):
        """Return RarInfo for file source.

        RAR5: if name is hard-linked or copied file,
        returns original entry with original filename.

        .. versionadded:: 4.1
        """
        return self._file_parser.getinfo_orig(name)

    def open(self, name, mode="r", pwd=None):
        """Returns file-like object (:class:`RarExtFile`) from where the data can be read.

        The object implements :class:`io.RawIOBase` interface, so it can
        be further wrapped with :class:`io.BufferedReader`
        and :class:`io.TextIOWrapper`.

        On older Python where io module is not available, it implements
        only .read(), .seek(), .tell() and .close() methods.

        The object is seekable, although the seeking is fast only on
        uncompressed files, on compressed files the seeking is implemented
        by reading ahead and/or restarting the decompression.

        Parameters:

            name
                file name or RarInfo instance.
            mode
                must be "r"
            pwd
                password to use for extracting.
        """

        if mode != "r":
            raise NotImplementedError("RarFile.open() supports only mode=r")

        # entry lookup
        inf = self.getinfo(name)
        if inf.is_dir():
            raise io.UnsupportedOperation("Directory does not have any data: " + inf.filename)

        # check password
        if inf.needs_password():
            pwd = pwd or self._password
            if pwd is None:
                raise PasswordRequired("File %s requires password" % inf.filename)
        else:
            pwd = None

        return self._file_parser.open(inf, pwd)

    def read(self, name, pwd=None):
        """Return uncompressed data for archive entry.

        For longer files using :meth:`~RarFile.open` may be better idea.

        Parameters:

            name
                filename or RarInfo instance
            pwd
                password to use for extracting.
        """

        with self.open(name, "r", pwd) as f:
            return f.read()

    def close(self):
        """Release open resources."""
        pass

    def printdir(self, file=None):
        """Print archive file list to stdout or given file.
        """
        if file is None:
            file = sys.stdout
        for f in self.infolist():
            print(f.filename, file=file)

    def extract(self, member, path=None, pwd=None):
        """Extract single file into current directory.

        Parameters:

            member
                filename or :class:`RarInfo` instance
            path
                optional destination path
            pwd
                optional password to use
        """
        inf = self.getinfo(member)
        return self._extract_one(inf, path, pwd, True)

    def extractall(self, path=None, members=None, pwd=None):
        """Extract all files into current directory.

        Parameters:

            path
                optional destination path
            members
                optional filename or :class:`RarInfo` instance list to extract
            pwd
                optional password to use
        """
        if members is None:
            members = self.namelist()

        done = set()
        dirs = []
        for m in members:
            inf = self.getinfo(m)
            dst = self._extract_one(inf, path, pwd, not inf.is_dir())
            if inf.is_dir():
                if dst not in done:
                    dirs.append((dst, inf))
                    done.add(dst)
        if dirs:
            dirs.sort(reverse=True)
            for dst, inf in dirs:
                self._set_attrs(inf, dst)

    def testrar(self, pwd=None):
        """Read all files and test CRC.
        """
        for member in self.infolist():
            if member.is_file():
                with self.open(member, 'r', pwd) as f:
                    empty_read(f, member.file_size, BSIZE)

    def strerror(self):
        """Return error string if parsing failed or None if no problems.
        """
        if not self._file_parser:
            return "Not a RAR file"
        return self._file_parser.strerror()

    ##
    ## private methods
    ##

    def _parse(self):
        """Run parser for file type
        """
        ver, sfx_ofs = _find_sfx_header(self._rarfile)
        if ver == RAR_V3:
            p3 = RAR3Parser(self._rarfile, self._password, self._crc_check,
                            self._charset, self._strict, self._info_callback,
                            sfx_ofs, self._part_only)
            self._file_parser = p3  # noqa
        elif ver == RAR_V5:
            p5 = RAR5Parser(self._rarfile, self._password, self._crc_check,
                            self._charset, self._strict, self._info_callback,
                            sfx_ofs, self._part_only)
            self._file_parser = p5  # noqa
        else:
            raise NotRarFile("Not a RAR file")

        self._file_parser.parse()
        self.comment = self._file_parser.comment

    def _extract_one(self, info, path, pwd, set_attrs):
        fname = sanitize_filename(
            info.filename, os.path.sep, WIN32
        )

        if path is None:
            path = os.getcwd()
        else:
            path = os.fspath(path)
        dstfn = os.path.join(path, fname)

        dirname = os.path.dirname(dstfn)
        if dirname and dirname != ".":
            os.makedirs(dirname, exist_ok=True)

        if info.is_file():
            return self._make_file(info, dstfn, pwd, set_attrs)
        if info.is_dir():
            return self._make_dir(info, dstfn, pwd, set_attrs)
        if info.is_symlink():
            return self._make_symlink(info, dstfn, pwd, set_attrs)
        return None

    def _create_helper(self, name, flags, info):
        return os.open(name, flags)

    def _make_file(self, info, dstfn, pwd, set_attrs):
        def helper(name, flags):
            return self._create_helper(name, flags, info)
        with self.open(info, "r", pwd) as src:
            with open(dstfn, "wb", opener=helper) as dst:
                shutil.copyfileobj(src, dst)
        if set_attrs:
            self._set_attrs(info, dstfn)
        return dstfn

    def _make_dir(self, info, dstfn, pwd, set_attrs):
        os.makedirs(dstfn, exist_ok=True)
        if set_attrs:
            self._set_attrs(info, dstfn)
        return dstfn

    def _make_symlink(self, info, dstfn, pwd, set_attrs):
        target_is_directory = False
        if info.host_os == RAR_OS_UNIX:
            link_name = self.read(info, pwd)
            target_is_directory = (info.flags & RAR_FILE_DIRECTORY) == RAR_FILE_DIRECTORY
        elif info.file_redir:
            redir_type, redir_flags, link_name = info.file_redir
            if redir_type == RAR5_XREDIR_WINDOWS_JUNCTION:
                warnings.warn(f"Windows junction not supported - {info.filename}", UnsupportedWarning)
                return None
            target_is_directory = (redir_type & RAR5_XREDIR_ISDIR) > 0
        else:
            warnings.warn(f"Unsupported link type - {info.filename}", UnsupportedWarning)
            return None

        os.symlink(link_name, dstfn, target_is_directory=target_is_directory)
        return dstfn

    def _set_attrs(self, info, dstfn):
        if info.host_os == RAR_OS_UNIX:
            os.chmod(dstfn, info.mode & 0o777)
        elif info.host_os in (RAR_OS_WIN32, RAR_OS_MSDOS):
            # only keep R/O attr, except for dirs on win32
            if info.mode & DOS_MODE_READONLY and (info.is_file() or not WIN32):
                st = os.stat(dstfn)
                new_mode = st.st_mode & ~0o222
                os.chmod(dstfn, new_mode)

        if info.mtime:
            mtime_ns = to_nsecs(info.mtime)
            atime_ns = to_nsecs(info.atime) if info.atime else mtime_ns
            os.utime(dstfn, ns=(atime_ns, mtime_ns))


#
# File format parsing
#

class CommonParser:
    """Shared parser parts."""
    _main = None
    _hdrenc_main = None
    _needs_password = False
    _fd = None
    _expect_sig = None
    _parse_error = None
    _password = None
    comment = None

    def __init__(self, rarfile, password, crc_check, charset, strict,
                 info_cb, sfx_offset, part_only):
        self._rarfile = rarfile
        self._password = password
        self._crc_check = crc_check
        self._charset = charset
        self._strict = strict
        self._info_callback = info_cb
        self._info_list = []
        self._info_map = {}
        self._vol_list = []
        self._sfx_offset = sfx_offset
        self._part_only = part_only

    def is_solid(self):
        """Returns True if archive uses solid compression.
        """
        if self._main:
            if self._main.flags & RAR_MAIN_SOLID:
                return True
        return False

    def has_header_encryption(self):
        """Returns True if headers are encrypted
        """
        if self._hdrenc_main:
            return True
        if self._main:
            if self._main.flags & RAR_MAIN_PASSWORD:
                return True
        return False

    def setpassword(self, pwd):
        """Set cached password."""
        self._password = pwd

    def volumelist(self):
        """Volume files"""
        return self._vol_list

    def needs_password(self):
        """Is password required"""
        return self._needs_password

    def strerror(self):
        """Last error"""
        return self._parse_error

    def infolist(self):
        """List of RarInfo records.
        """
        return self._info_list

    def getinfo(self, member):
        """Return RarInfo for filename
        """
        if isinstance(member, RarInfo):
            fname = member.filename
        elif isinstance(member, Path):
            fname = str(member)
        else:
            fname = member

        if fname.endswith("/"):
            fname = fname.rstrip("/")

        try:
            return self._info_map[fname]
        except KeyError:
            raise NoRarEntry("No such file: %s" % fname) from None

    def getinfo_orig(self, member):
        inf = self.getinfo(member)
        if inf.file_redir:
            redir_type, redir_flags, redir_name = inf.file_redir
            # cannot leave to unrar as it expects copied file to exist
            if redir_type in (RAR5_XREDIR_FILE_COPY, RAR5_XREDIR_HARD_LINK):
                inf = self.getinfo(redir_name)
        return inf

    def parse(self):
        """Process file."""
        self._fd = None
        try:
            self._parse_real()
        finally:
            if self._fd:
                self._fd.close()
                self._fd = None

    def _parse_real(self):
        """Actually read file.
        """
        fd = XFile(self._rarfile)
        self._fd = fd
        fd.seek(self._sfx_offset, 0)
        sig = fd.read(len(self._expect_sig))
        if sig != self._expect_sig:
            raise NotRarFile("Not a Rar archive")

        volume = 0  # first vol (.rar) is 0
        more_vols = False
        endarc = False
        volfile = self._rarfile
        self._vol_list = [self._rarfile]
        raise_need_first_vol = False
        while True:
            if endarc:
                h = None    # don"t read past ENDARC
            else:
                h = self._parse_header(fd)
            if not h:
                if raise_need_first_vol:
                    # did not find ENDARC with VOLNR
                    raise NeedFirstVolume("Need to start from first volume", None)
                if more_vols and not self._part_only:
                    volume += 1
                    fd.close()
                    try:
                        volfile = self._next_volname(volfile)
                        fd = XFile(volfile)
                    except IOError:
                        self._set_error("Cannot open next volume: %s", volfile)
                        break
                    self._fd = fd
                    sig = fd.read(len(self._expect_sig))
                    if sig != self._expect_sig:
                        self._set_error("Invalid volume sig: %s", volfile)
                        break
                    more_vols = False
                    endarc = False
                    self._vol_list.append(volfile)
                    self._main = None
                    self._hdrenc_main = None
                    continue
                break
            h.volume = volume
            h.volume_file = volfile

            if h.type == RAR_BLOCK_MAIN and not self._main:
                self._main = h
                if volume == 0 and (h.flags & RAR_MAIN_NEWNUMBERING) and not self._part_only:
                    # RAR 2.x does not set FIRSTVOLUME,
                    # so check it only if NEWNUMBERING is used
                    if (h.flags & RAR_MAIN_FIRSTVOLUME) == 0:
                        if getattr(h, "main_volume_number", None) is not None:
                            # rar5 may have more info
                            raise NeedFirstVolume(
                                "Need to start from first volume (current: %r)"
                                % (h.main_volume_number,),
                                h.main_volume_number
                            )
                        # delay raise until we have volnr from ENDARC
                        raise_need_first_vol = True
                if h.flags & RAR_MAIN_PASSWORD:
                    self._needs_password = True
                    if not self._password:
                        break
            elif h.type == RAR_BLOCK_ENDARC:
                # use flag, but also allow RAR 2.x logic below to trigger
                if h.flags & RAR_ENDARC_NEXT_VOLUME:
                    more_vols = True
                endarc = True
                if raise_need_first_vol and (h.flags & RAR_ENDARC_VOLNR) > 0:
                    raise NeedFirstVolume(
                        "Need to start from first volume (current: %r)"
                        % (h.endarc_volnr,),
                        h.endarc_volnr
                    )
            elif h.type == RAR_BLOCK_FILE:
                # RAR 2.x does not write RAR_BLOCK_ENDARC
                if h.flags & RAR_FILE_SPLIT_AFTER:
                    more_vols = True
                # RAR 2.x does not set RAR_MAIN_FIRSTVOLUME
                if volume == 0 and h.flags & RAR_FILE_SPLIT_BEFORE:
                    if not self._part_only:
                        raise_need_first_vol = True

            if h.needs_password():
                self._needs_password = True

            # store it
            self.process_entry(fd, h)

            if self._info_callback:
                self._info_callback(h)

            # go to next header
            if h.add_size > 0:
                fd.seek(h.data_offset + h.add_size, 0)

    def process_entry(self, fd, item):
        """Examine item, add into lookup cache."""
        raise NotImplementedError()

    def _decrypt_header(self, fd):
        raise NotImplementedError("_decrypt_header")

    def _parse_block_header(self, fd):
        raise NotImplementedError("_parse_block_header")

    def _open_hack(self, inf, pwd):
        raise NotImplementedError("_open_hack")

    def _parse_header(self, fd):
        """Read single header
        """
        try:
            # handle encrypted headers
            if (self._main and self._main.flags & RAR_MAIN_PASSWORD) or self._hdrenc_main:
                if not self._password:
                    return None
                fd = self._decrypt_header(fd)

            # now read actual header
            return self._parse_block_header(fd)
        except struct.error:
            self._set_error("Broken header in RAR file")
            return None

    def _next_volname(self, volfile):
        """Given current vol name, construct next one
        """
        if is_filelike(volfile):
            raise IOError("Working on single FD")
        if self._main.flags & RAR_MAIN_NEWNUMBERING:
            return _next_newvol(volfile)
        return _next_oldvol(volfile)

    def _set_error(self, msg, *args):
        if args:
            msg = msg % args
        self._parse_error = msg
        if self._strict:
            raise BadRarFile(msg)

    def open(self, inf, pwd):
        """Return stream object for file data."""

        if inf.file_redir:
            redir_type, redir_flags, redir_name = inf.file_redir
            # cannot leave to unrar as it expects copied file to exist
            if redir_type in (RAR5_XREDIR_FILE_COPY, RAR5_XREDIR_HARD_LINK):
                inf = self.getinfo(redir_name)
                if not inf:
                    raise BadRarFile("cannot find copied file")
            elif redir_type in (
                RAR5_XREDIR_UNIX_SYMLINK, RAR5_XREDIR_WINDOWS_SYMLINK,
                RAR5_XREDIR_WINDOWS_JUNCTION,
            ):
                return io.BytesIO(redir_name.encode("utf8"))
        if inf.flags & RAR_FILE_SPLIT_BEFORE:
            raise NeedFirstVolume("Partial file, please start from first volume: " + inf.filename, None)

        # is temp write usable?
        use_hack = 1
        if not self._main:
            use_hack = 0
        elif self._main._must_disable_hack():
            use_hack = 0
        elif inf._must_disable_hack():
            use_hack = 0
        elif is_filelike(self._rarfile):
            pass
        elif inf.file_size > HACK_SIZE_LIMIT:
            use_hack = 0
        elif not USE_EXTRACT_HACK:
            use_hack = 0

        # now extract
        if inf.compress_type == RAR_M0 and (inf.flags & RAR_FILE_PASSWORD) == 0 and inf.file_redir is None:
            return self._open_clear(inf)
        elif use_hack:
            return self._open_hack(inf, pwd)
        elif is_filelike(self._rarfile):
            return self._open_unrar_membuf(self._rarfile, inf, pwd)
        else:
            return self._open_unrar(self._rarfile, inf, pwd)

    def _open_clear(self, inf):
        if FORCE_TOOL:
            return self._open_unrar(self._rarfile, inf)
        return DirectReader(self, inf)

    def _open_hack_core(self, inf, pwd, prefix, suffix):

        size = inf.compress_size + inf.header_size
        rf = XFile(inf.volume_file, 0)
        rf.seek(inf.header_offset)

        tmpfd, tmpname = mkstemp(suffix=".rar", dir=HACK_TMP_DIR)
        tmpf = os.fdopen(tmpfd, "wb")

        try:
            tmpf.write(prefix)
            while size > 0:
                if size > BSIZE:
                    buf = rf.read(BSIZE)
                else:
                    buf = rf.read(size)
                if not buf:
                    raise BadRarFile("read failed: " + inf.filename)
                tmpf.write(buf)
                size -= len(buf)
            tmpf.write(suffix)
            tmpf.close()
            rf.close()
        except BaseException:
            rf.close()
            tmpf.close()
            os.unlink(tmpname)
            raise

        return self._open_unrar(tmpname, inf, pwd, tmpname)

    def _open_unrar_membuf(self, memfile, inf, pwd):
        """Write in-memory archive to temp file, needed for solid archives.
        """
        tmpname = membuf_tempfile(memfile)
        return self._open_unrar(tmpname, inf, pwd, tmpname, force_file=True)

    def _open_unrar(self, rarfile, inf, pwd=None, tmpfile=None, force_file=False):
        """Extract using unrar
        """
        setup = tool_setup()

        # not giving filename avoids encoding related problems
        fn = None
        if not tmpfile or force_file:
            fn = inf.filename.replace("/", os.path.sep)

        # read from unrar pipe
        cmd = setup.open_cmdline(pwd, rarfile, fn)
        return PipeReader(self, inf, cmd, tmpfile)


#
# RAR3 format
#

class Rar3Info(RarInfo):
    """RAR3 specific fields."""
    extract_version = 15
    salt = None
    add_size = 0
    header_crc = None
    header_size = None
    header_offset = None
    data_offset = None
    _md_class = None
    _md_expect = None
    _name_size = None

    # make sure some rar5 fields are always present
    file_redir = None
    blake2sp_hash = None

    endarc_datacrc = None
    endarc_volnr = None

    def _must_disable_hack(self):
        if self.type == RAR_BLOCK_FILE:
            if self.flags & RAR_FILE_PASSWORD:
                return True
            elif self.flags & (RAR_FILE_SPLIT_BEFORE | RAR_FILE_SPLIT_AFTER):
                return True
        elif self.type == RAR_BLOCK_MAIN:
            if self.flags & (RAR_MAIN_SOLID | RAR_MAIN_PASSWORD):
                return True
        return False

    def is_dir(self):
        """Returns True if entry is a directory."""
        if self.type == RAR_BLOCK_FILE and not self.is_symlink():
            return (self.flags & RAR_FILE_DIRECTORY) == RAR_FILE_DIRECTORY
        return False

    def is_symlink(self):
        """Returns True if entry is a symlink."""
        return (
            self.type == RAR_BLOCK_FILE and
            self.host_os == RAR_OS_UNIX and
            self.mode & 0xF000 == 0xA000
        )

    def is_file(self):
        """Returns True if entry is a normal file."""
        return (
            self.type == RAR_BLOCK_FILE and
            not (self.is_dir() or self.is_symlink())
        )


class RAR3Parser(CommonParser):
    """Parse RAR3 file format.
    """
    _expect_sig = RAR_ID
    _last_aes_key = (None, None, None)   # (salt, key, iv)

    def _decrypt_header(self, fd):
        if not _have_crypto:
            raise NoCrypto("Cannot parse encrypted headers - no crypto")
        salt = fd.read(8)
        if self._last_aes_key[0] == salt:
            key, iv = self._last_aes_key[1:]
        else:
            key, iv = rar3_s2k(self._password, salt)
            self._last_aes_key = (salt, key, iv)
        return HeaderDecrypt(fd, key, iv)

    def _parse_block_header(self, fd):
        """Parse common block header
        """
        h = Rar3Info()
        h.header_offset = fd.tell()

        # read and parse base header
        buf = fd.read(S_BLK_HDR.size)
        if not buf:
            return None
        if len(buf) < S_BLK_HDR.size:
            self._set_error("Unexpected EOF when reading header")
            return None
        t = S_BLK_HDR.unpack_from(buf)
        h.header_crc, h.type, h.flags, h.header_size = t

        # read full header
        if h.header_size > S_BLK_HDR.size:
            hdata = buf + fd.read(h.header_size - S_BLK_HDR.size)
        else:
            hdata = buf
        h.data_offset = fd.tell()

        # unexpected EOF?
        if len(hdata) != h.header_size:
            self._set_error("Unexpected EOF when reading header")
            return None

        pos = S_BLK_HDR.size

        # block has data assiciated with it?
        if h.flags & RAR_LONG_BLOCK:
            h.add_size, pos = load_le32(hdata, pos)
        else:
            h.add_size = 0

        # parse interesting ones, decide header boundaries for crc
        if h.type == RAR_BLOCK_MARK:
            return h
        elif h.type == RAR_BLOCK_MAIN:
            pos += 6
            if h.flags & RAR_MAIN_ENCRYPTVER:
                pos += 1
            crc_pos = pos
            if h.flags & RAR_MAIN_COMMENT:
                self._parse_subblocks(h, hdata, pos)
        elif h.type == RAR_BLOCK_FILE:
            pos = self._parse_file_header(h, hdata, pos - 4)
            crc_pos = pos
            if h.flags & RAR_FILE_COMMENT:
                pos = self._parse_subblocks(h, hdata, pos)
        elif h.type == RAR_BLOCK_SUB:
            pos = self._parse_file_header(h, hdata, pos - 4)
            crc_pos = h.header_size
        elif h.type == RAR_BLOCK_OLD_AUTH:
            pos += 8
            crc_pos = pos
        elif h.type == RAR_BLOCK_OLD_EXTRA:
            pos += 7
            crc_pos = pos
        elif h.type == RAR_BLOCK_ENDARC:
            if h.flags & RAR_ENDARC_DATACRC:
                h.endarc_datacrc, pos = load_le32(hdata, pos)
            if h.flags & RAR_ENDARC_VOLNR:
                h.endarc_volnr = S_SHORT.unpack_from(hdata, pos)[0]
                pos += 2
            crc_pos = h.header_size
        else:
            crc_pos = h.header_size

        # check crc
        if h.type == RAR_BLOCK_OLD_SUB:
            crcdat = hdata[2:] + fd.read(h.add_size)
        else:
            crcdat = hdata[2:crc_pos]

        calc_crc = crc32(crcdat) & 0xFFFF

        # return good header
        if h.header_crc == calc_crc:
            return h

        # header parsing failed.
        self._set_error("Header CRC error (%02x): exp=%x got=%x (xlen = %d)",
                        h.type, h.header_crc, calc_crc, len(crcdat))

        # instead panicing, send eof
        return None

    def _parse_file_header(self, h, hdata, pos):
        """Read file-specific header
        """
        fld = S_FILE_HDR.unpack_from(hdata, pos)
        pos += S_FILE_HDR.size

        h.compress_size = fld[0]
        h.file_size = fld[1]
        h.host_os = fld[2]
        h.CRC = fld[3]
        h.date_time = parse_dos_time(fld[4])
        h.mtime = to_datetime(h.date_time)
        h.extract_version = fld[5]
        h.compress_type = fld[6]
        h._name_size = name_size = fld[7]
        h.mode = fld[8]

        h._md_class = CRC32Context
        h._md_expect = h.CRC

        if h.flags & RAR_FILE_LARGE:
            h1, pos = load_le32(hdata, pos)
            h2, pos = load_le32(hdata, pos)
            h.compress_size |= h1 << 32
            h.file_size |= h2 << 32
            h.add_size = h.compress_size

        name, pos = load_bytes(hdata, name_size, pos)
        if h.flags & RAR_FILE_UNICODE and b"\0" in name:
            # stored in custom encoding
            nul = name.find(b"\0")
            h.orig_filename = name[:nul]
            u = UnicodeFilename(h.orig_filename, name[nul + 1:])
            h.filename = u.decode()

            # if parsing failed fall back to simple name
            if u.failed:
                h.filename = self._decode(h.orig_filename)
        elif h.flags & RAR_FILE_UNICODE:
            # stored in UTF8
            h.orig_filename = name
            h.filename = name.decode("utf8", "replace")
        else:
            # stored in random encoding
            h.orig_filename = name
            h.filename = self._decode(name)

        # change separator, set dir suffix
        h.filename = h.filename.replace("\\", "/").rstrip("/")
        if h.is_dir():
            h.filename = h.filename + "/"

        if h.flags & RAR_FILE_SALT:
            h.salt, pos = load_bytes(hdata, 8, pos)
        else:
            h.salt = None

        # optional extended time stamps
        if h.flags & RAR_FILE_EXTTIME:
            pos = _parse_ext_time(h, hdata, pos)
        else:
            h.mtime = h.atime = h.ctime = h.arctime = None

        return pos

    def _parse_subblocks(self, h, hdata, pos):
        """Find old-style comment subblock
        """
        while pos < len(hdata):
            # ordinary block header
            t = S_BLK_HDR.unpack_from(hdata, pos)
            ___scrc, stype, sflags, slen = t
            pos_next = pos + slen
            pos += S_BLK_HDR.size

            # corrupt header
            if pos_next < pos:
                break

            # followed by block-specific header
            if stype == RAR_BLOCK_OLD_COMMENT and pos + S_COMMENT_HDR.size <= pos_next:
                declen, ver, meth, crc = S_COMMENT_HDR.unpack_from(hdata, pos)
                pos += S_COMMENT_HDR.size
                data = hdata[pos: pos_next]
                cmt = rar3_decompress(ver, meth, data, declen, sflags,
                                      crc, self._password)
                if not self._crc_check or (crc32(cmt) & 0xFFFF == crc):
                    h.comment = self._decode_comment(cmt)

            pos = pos_next
        return pos

    def _read_comment_v3(self, inf, pwd=None):

        # read data
        with XFile(inf.volume_file) as rf:
            rf.seek(inf.data_offset)
            data = rf.read(inf.compress_size)

        # decompress
        cmt = rar3_decompress(inf.extract_version, inf.compress_type, data,
                              inf.file_size, inf.flags, inf.CRC, pwd, inf.salt)

        # check crc
        if self._crc_check:
            crc = crc32(cmt)
            if crc != inf.CRC:
                return None

        return self._decode_comment(cmt)

    def _decode(self, val):
        for c in TRY_ENCODINGS:
            try:
                return val.decode(c)
            except UnicodeError:
                pass
        return val.decode(self._charset, "replace")

    def _decode_comment(self, val):
        return self._decode(val)

    def process_entry(self, fd, item):
        if item.type == RAR_BLOCK_FILE:
            # use only first part
            if item.flags & RAR_FILE_VERSION:
                pass    # skip old versions
            elif (item.flags & RAR_FILE_SPLIT_BEFORE) == 0:
                self._info_map[item.filename.rstrip("/")] = item
                self._info_list.append(item)
            elif len(self._info_list) > 0:
                # final crc is in last block
                old = self._info_list[-1]
                old.CRC = item.CRC
                old._md_expect = item._md_expect
                old.compress_size += item.compress_size

        # parse new-style comment
        if item.type == RAR_BLOCK_SUB and item.filename == "CMT":
            if item.flags & (RAR_FILE_SPLIT_BEFORE | RAR_FILE_SPLIT_AFTER):
                pass
            elif item.flags & RAR_FILE_SOLID:
                # file comment
                cmt = self._read_comment_v3(item, self._password)
                if len(self._info_list) > 0:
                    old = self._info_list[-1]
                    old.comment = cmt
            else:
                # archive comment
                cmt = self._read_comment_v3(item, self._password)
                self.comment = cmt

        if item.type == RAR_BLOCK_MAIN:
            if item.flags & RAR_MAIN_COMMENT:
                self.comment = item.comment
            if item.flags & RAR_MAIN_PASSWORD:
                self._needs_password = True

    # put file compressed data into temporary .rar archive, and run
    # unrar on that, thus avoiding unrar going over whole archive
    def _open_hack(self, inf, pwd):
        # create main header: crc, type, flags, size, res1, res2
        prefix = RAR_ID + S_BLK_HDR.pack(0x90CF, 0x73, 0, 13) + b"\0" * (2 + 4)
        return self._open_hack_core(inf, pwd, prefix, b"")


#
# RAR5 format
#

class Rar5Info(RarInfo):
    """Shared fields for RAR5 records.
    """
    extract_version = 50
    header_crc = None
    header_size = None
    header_offset = None
    data_offset = None

    # type=all
    block_type = None
    block_flags = None
    add_size = 0
    block_extra_size = 0

    # type=MAIN
    volume_number = None
    _md_class = None
    _md_expect = None

    def _must_disable_hack(self):
        return False


class Rar5BaseFile(Rar5Info):
    """Shared sturct for file & service record.
    """
    type = -1
    file_flags = None
    file_encryption = (0, 0, 0, b"", b"", b"")
    file_compress_flags = None
    file_redir = None
    file_owner = None
    file_version = None
    blake2sp_hash = None

    def _must_disable_hack(self):
        if self.flags & RAR_FILE_PASSWORD:
            return True
        if self.block_flags & (RAR5_BLOCK_FLAG_SPLIT_BEFORE | RAR5_BLOCK_FLAG_SPLIT_AFTER):
            return True
        if self.file_compress_flags & RAR5_COMPR_SOLID:
            return True
        if self.file_redir:
            return True
        return False


class Rar5FileInfo(Rar5BaseFile):
    """RAR5 file record.
    """
    type = RAR_BLOCK_FILE

    def is_symlink(self):
        """Returns True if entry is a symlink."""
        # pylint: disable=unsubscriptable-object
        return (
            self.file_redir is not None and
            self.file_redir[0] in (
                RAR5_XREDIR_UNIX_SYMLINK,
                RAR5_XREDIR_WINDOWS_SYMLINK,
                RAR5_XREDIR_WINDOWS_JUNCTION,
            )
        )

    def is_file(self):
        """Returns True if entry is a normal file."""
        return not (self.is_dir() or self.is_symlink())

    def is_dir(self):
        """Returns True if entry is a directory."""
        if not self.file_redir:
            if self.file_flags & RAR5_FILE_FLAG_ISDIR:
                return True
        return False


class Rar5ServiceInfo(Rar5BaseFile):
    """RAR5 service record.
    """
    type = RAR_BLOCK_SUB


class Rar5MainInfo(Rar5Info):
    """RAR5 archive main record.
    """
    type = RAR_BLOCK_MAIN
    main_flags = None
    main_volume_number = None

    def _must_disable_hack(self):
        if self.main_flags & RAR5_MAIN_FLAG_SOLID:
            return True
        return False


class Rar5EncryptionInfo(Rar5Info):
    """RAR5 archive header encryption record.
    """
    type = RAR5_BLOCK_ENCRYPTION
    encryption_algo = None
    encryption_flags = None
    encryption_kdf_count = None
    encryption_salt = None
    encryption_check_value = None

    def needs_password(self):
        return True


class Rar5EndArcInfo(Rar5Info):
    """RAR5 end of archive record.
    """
    type = RAR_BLOCK_ENDARC
    endarc_flags = None


class RAR5Parser(CommonParser):
    """Parse RAR5 format.
    """
    _expect_sig = RAR5_ID
    _hdrenc_main = None

    # AES encrypted headers
    _last_aes256_key = (-1, None, None)   # (kdf_count, salt, key)

    def _get_utf8_password(self):
        pwd = self._password
        if isinstance(pwd, str):
            return pwd.encode("utf8")
        return pwd

    def _gen_key(self, kdf_count, salt):
        if self._last_aes256_key[:2] == (kdf_count, salt):
            return self._last_aes256_key[2]
        if kdf_count > 24:
            raise BadRarFile("Too large kdf_count")
        pwd = self._get_utf8_password()
        key = pbkdf2_hmac("sha256", pwd, salt, 1 << kdf_count)
        self._last_aes256_key = (kdf_count, salt, key)
        return key

    def _decrypt_header(self, fd):
        if not _have_crypto:
            raise NoCrypto("Cannot parse encrypted headers - no crypto")
        h = self._hdrenc_main
        key = self._gen_key(h.encryption_kdf_count, h.encryption_salt)
        iv = fd.read(16)
        return HeaderDecrypt(fd, key, iv)

    def _parse_block_header(self, fd):
        """Parse common block header
        """
        header_offset = fd.tell()

        preload = 4 + 1
        start_bytes = fd.read(preload)
        if len(start_bytes) < preload:
            self._set_error("Unexpected EOF when reading header")
            return None
        while start_bytes[-1] & 0x80:
            b = fd.read(1)
            if not b:
                self._set_error("Unexpected EOF when reading header")
                return None
            start_bytes += b
        header_crc, pos = load_le32(start_bytes, 0)
        hdrlen, pos = load_vint(start_bytes, pos)
        if hdrlen > 2 * 1024 * 1024:
            return None
        header_size = pos + hdrlen

        # read full header, check for EOF
        hdata = start_bytes + fd.read(header_size - len(start_bytes))
        if len(hdata) != header_size:
            self._set_error("Unexpected EOF when reading header")
            return None
        data_offset = fd.tell()

        calc_crc = crc32(memoryview(hdata)[4:])
        if header_crc != calc_crc:
            # header parsing failed.
            self._set_error("Header CRC error: exp=%x got=%x (xlen = %d)",
                            header_crc, calc_crc, len(hdata))
            return None

        block_type, pos = load_vint(hdata, pos)

        if block_type == RAR5_BLOCK_MAIN:
            h, pos = self._parse_block_common(Rar5MainInfo(), hdata)
            h = self._parse_main_block(h, hdata, pos)
        elif block_type == RAR5_BLOCK_FILE:
            h, pos = self._parse_block_common(Rar5FileInfo(), hdata)
            h = self._parse_file_block(h, hdata, pos)
        elif block_type == RAR5_BLOCK_SERVICE:
            h, pos = self._parse_block_common(Rar5ServiceInfo(), hdata)
            h = self._parse_file_block(h, hdata, pos)
        elif block_type == RAR5_BLOCK_ENCRYPTION:
            h, pos = self._parse_block_common(Rar5EncryptionInfo(), hdata)
            h = self._parse_encryption_block(h, hdata, pos)
        elif block_type == RAR5_BLOCK_ENDARC:
            h, pos = self._parse_block_common(Rar5EndArcInfo(), hdata)
            h = self._parse_endarc_block(h, hdata, pos)
        else:
            h = None
        if h:
            h.header_offset = header_offset
            h.data_offset = data_offset
        return h

    def _parse_block_common(self, h, hdata):
        h.header_crc, pos = load_le32(hdata, 0)
        hdrlen, pos = load_vint(hdata, pos)
        h.header_size = hdrlen + pos
        h.block_type, pos = load_vint(hdata, pos)
        h.block_flags, pos = load_vint(hdata, pos)

        if h.block_flags & RAR5_BLOCK_FLAG_EXTRA_DATA:
            h.block_extra_size, pos = load_vint(hdata, pos)
        if h.block_flags & RAR5_BLOCK_FLAG_DATA_AREA:
            h.add_size, pos = load_vint(hdata, pos)

        h.compress_size = h.add_size

        if h.block_flags & RAR5_BLOCK_FLAG_SKIP_IF_UNKNOWN:
            h.flags |= RAR_SKIP_IF_UNKNOWN
        if h.block_flags & RAR5_BLOCK_FLAG_DATA_AREA:
            h.flags |= RAR_LONG_BLOCK
        return h, pos

    def _parse_main_block(self, h, hdata, pos):
        h.main_flags, pos = load_vint(hdata, pos)
        if h.main_flags & RAR5_MAIN_FLAG_HAS_VOLNR:
            h.main_volume_number, pos = load_vint(hdata, pos)

        h.flags |= RAR_MAIN_NEWNUMBERING
        if h.main_flags & RAR5_MAIN_FLAG_SOLID:
            h.flags |= RAR_MAIN_SOLID
        if h.main_flags & RAR5_MAIN_FLAG_ISVOL:
            h.flags |= RAR_MAIN_VOLUME
        if h.main_flags & RAR5_MAIN_FLAG_RECOVERY:
            h.flags |= RAR_MAIN_RECOVERY
        if self._hdrenc_main:
            h.flags |= RAR_MAIN_PASSWORD
        if h.main_flags & RAR5_MAIN_FLAG_HAS_VOLNR == 0:
            h.flags |= RAR_MAIN_FIRSTVOLUME

        return h

    def _parse_file_block(self, h, hdata, pos):
        h.file_flags, pos = load_vint(hdata, pos)
        h.file_size, pos = load_vint(hdata, pos)
        h.mode, pos = load_vint(hdata, pos)

        if h.file_flags & RAR5_FILE_FLAG_HAS_MTIME:
            h.mtime, pos = load_unixtime(hdata, pos)
            h.date_time = h.mtime.timetuple()[:6]
        if h.file_flags & RAR5_FILE_FLAG_HAS_CRC32:
            h.CRC, pos = load_le32(hdata, pos)
            h._md_class = CRC32Context
            h._md_expect = h.CRC

        h.file_compress_flags, pos = load_vint(hdata, pos)
        h.file_host_os, pos = load_vint(hdata, pos)
        h.orig_filename, pos = load_vstr(hdata, pos)
        h.filename = h.orig_filename.decode("utf8", "replace").rstrip("/")

        # use compatible values
        if h.file_host_os == RAR5_OS_WINDOWS:
            h.host_os = RAR_OS_WIN32
        else:
            h.host_os = RAR_OS_UNIX
        h.compress_type = RAR_M0 + ((h.file_compress_flags >> 7) & 7)

        if h.block_extra_size:
            # allow 1 byte of garbage
            while pos < len(hdata) - 1:
                xsize, pos = load_vint(hdata, pos)
                xdata, pos = load_bytes(hdata, xsize, pos)
                self._process_file_extra(h, xdata)

        if h.block_flags & RAR5_BLOCK_FLAG_SPLIT_BEFORE:
            h.flags |= RAR_FILE_SPLIT_BEFORE
        if h.block_flags & RAR5_BLOCK_FLAG_SPLIT_AFTER:
            h.flags |= RAR_FILE_SPLIT_AFTER
        if h.file_flags & RAR5_FILE_FLAG_ISDIR:
            h.flags |= RAR_FILE_DIRECTORY
        if h.file_compress_flags & RAR5_COMPR_SOLID:
            h.flags |= RAR_FILE_SOLID

        if h.is_dir():
            h.filename = h.filename + "/"
        return h

    def _parse_endarc_block(self, h, hdata, pos):
        h.endarc_flags, pos = load_vint(hdata, pos)
        if h.endarc_flags & RAR5_ENDARC_FLAG_NEXT_VOL:
            h.flags |= RAR_ENDARC_NEXT_VOLUME
        return h

    def _check_password(self, check_value, kdf_count_shift, salt):
        if len(check_value) != RAR5_PW_CHECK_SIZE + RAR5_PW_SUM_SIZE:
            return

        hdr_check = check_value[:RAR5_PW_CHECK_SIZE]
        hdr_sum = check_value[RAR5_PW_CHECK_SIZE:]
        sum_hash = sha256(hdr_check).digest()
        if sum_hash[:RAR5_PW_SUM_SIZE] != hdr_sum:
            return

        kdf_count = (1 << kdf_count_shift) + 32
        pwd = self._get_utf8_password()
        pwd_hash = pbkdf2_hmac("sha256", pwd, salt, kdf_count)

        pwd_check = bytearray(RAR5_PW_CHECK_SIZE)
        len_mask = RAR5_PW_CHECK_SIZE - 1
        for i, v in enumerate(pwd_hash):
            pwd_check[i & len_mask] ^= v

        if pwd_check != hdr_check:
            raise RarWrongPassword()

    def _parse_encryption_block(self, h, hdata, pos):
        h.encryption_algo, pos = load_vint(hdata, pos)
        h.encryption_flags, pos = load_vint(hdata, pos)
        h.encryption_kdf_count, pos = load_byte(hdata, pos)
        h.encryption_salt, pos = load_bytes(hdata, 16, pos)
        if h.encryption_flags & RAR5_ENC_FLAG_HAS_CHECKVAL:
            h.encryption_check_value, pos = load_bytes(hdata, 12, pos)
        if h.encryption_algo != RAR5_XENC_CIPHER_AES256:
            raise BadRarFile("Unsupported header encryption cipher")
        if h.encryption_check_value and self._password:
            self._check_password(h.encryption_check_value, h.encryption_kdf_count, h.encryption_salt)
        self._hdrenc_main = h
        return h

    def _process_file_extra(self, h, xdata):
        xtype, pos = load_vint(xdata, 0)
        if xtype == RAR5_XFILE_TIME:
            self._parse_file_xtime(h, xdata, pos)
        elif xtype == RAR5_XFILE_ENCRYPTION:
            self._parse_file_encryption(h, xdata, pos)
        elif xtype == RAR5_XFILE_HASH:
            self._parse_file_hash(h, xdata, pos)
        elif xtype == RAR5_XFILE_VERSION:
            self._parse_file_version(h, xdata, pos)
        elif xtype == RAR5_XFILE_REDIR:
            self._parse_file_redir(h, xdata, pos)
        elif xtype == RAR5_XFILE_OWNER:
            self._parse_file_owner(h, xdata, pos)
        elif xtype == RAR5_XFILE_SERVICE:
            pass
        else:
            pass

    # extra block for file time record
    def _parse_file_xtime(self, h, xdata, pos):
        tflags, pos = load_vint(xdata, pos)

        ldr = load_windowstime
        if tflags & RAR5_XTIME_UNIXTIME:
            ldr = load_unixtime

        if tflags & RAR5_XTIME_HAS_MTIME:
            h.mtime, pos = ldr(xdata, pos)
            h.date_time = h.mtime.timetuple()[:6]
        if tflags & RAR5_XTIME_HAS_CTIME:
            h.ctime, pos = ldr(xdata, pos)
        if tflags & RAR5_XTIME_HAS_ATIME:
            h.atime, pos = ldr(xdata, pos)

        if tflags & RAR5_XTIME_UNIXTIME_NS:
            if tflags & RAR5_XTIME_HAS_MTIME:
                nsec, pos = load_le32(xdata, pos)
                h.mtime = to_nsdatetime(h.mtime, nsec)
            if tflags & RAR5_XTIME_HAS_CTIME:
                nsec, pos = load_le32(xdata, pos)
                h.ctime = to_nsdatetime(h.ctime, nsec)
            if tflags & RAR5_XTIME_HAS_ATIME:
                nsec, pos = load_le32(xdata, pos)
                h.atime = to_nsdatetime(h.atime, nsec)

    # just remember encryption info
    def _parse_file_encryption(self, h, xdata, pos):
        algo, pos = load_vint(xdata, pos)
        flags, pos = load_vint(xdata, pos)
        kdf_count, pos = load_byte(xdata, pos)
        salt, pos = load_bytes(xdata, 16, pos)
        iv, pos = load_bytes(xdata, 16, pos)
        checkval = None
        if flags & RAR5_XENC_CHECKVAL:
            checkval, pos = load_bytes(xdata, 12, pos)
        if flags & RAR5_XENC_TWEAKED:
            h._md_expect = None
            h._md_class = NoHashContext

        h.file_encryption = (algo, flags, kdf_count, salt, iv, checkval)
        h.flags |= RAR_FILE_PASSWORD

    def _parse_file_hash(self, h, xdata, pos):
        hash_type, pos = load_vint(xdata, pos)
        if hash_type == RAR5_XHASH_BLAKE2SP:
            h.blake2sp_hash, pos = load_bytes(xdata, 32, pos)
            if (h.file_encryption[1] & RAR5_XENC_TWEAKED) == 0:
                h._md_class = Blake2SP
                h._md_expect = h.blake2sp_hash

    def _parse_file_version(self, h, xdata, pos):
        flags, pos = load_vint(xdata, pos)
        version, pos = load_vint(xdata, pos)
        h.file_version = (flags, version)

    def _parse_file_redir(self, h, xdata, pos):
        redir_type, pos = load_vint(xdata, pos)
        redir_flags, pos = load_vint(xdata, pos)
        redir_name, pos = load_vstr(xdata, pos)
        redir_name = redir_name.decode("utf8", "replace")
        h.file_redir = (redir_type, redir_flags, redir_name)

    def _parse_file_owner(self, h, xdata, pos):
        user_name = group_name = user_id = group_id = None

        flags, pos = load_vint(xdata, pos)
        if flags & RAR5_XOWNER_UNAME:
            user_name, pos = load_vstr(xdata, pos)
        if flags & RAR5_XOWNER_GNAME:
            group_name, pos = load_vstr(xdata, pos)
        if flags & RAR5_XOWNER_UID:
            user_id, pos = load_vint(xdata, pos)
        if flags & RAR5_XOWNER_GID:
            group_id, pos = load_vint(xdata, pos)

        h.file_owner = (user_name, group_name, user_id, group_id)

    def process_entry(self, fd, item):
        if item.block_type == RAR5_BLOCK_FILE:
            if item.file_version:
                pass    # skip old versions
            elif (item.block_flags & RAR5_BLOCK_FLAG_SPLIT_BEFORE) == 0:
                # use only first part
                self._info_map[item.filename.rstrip("/")] = item
                self._info_list.append(item)
            elif len(self._info_list) > 0:
                # final crc is in last block
                old = self._info_list[-1]
                old.CRC = item.CRC
                old._md_expect = item._md_expect
                old.blake2sp_hash = item.blake2sp_hash
                old.compress_size += item.compress_size
        elif item.block_type == RAR5_BLOCK_SERVICE:
            if item.filename == "CMT":
                self._load_comment(fd, item)

    def _load_comment(self, fd, item):
        if item.block_flags & (RAR5_BLOCK_FLAG_SPLIT_BEFORE | RAR5_BLOCK_FLAG_SPLIT_AFTER):
            return None
        if item.compress_type != RAR_M0:
            return None

        if item.flags & RAR_FILE_PASSWORD:
            algo, ___flags, kdf_count, salt, iv, ___checkval = item.file_encryption
            if algo != RAR5_XENC_CIPHER_AES256:
                return None
            key = self._gen_key(kdf_count, salt)
            f = HeaderDecrypt(fd, key, iv)
            cmt = f.read(item.file_size)
        else:
            # archive comment
            with self._open_clear(item) as cmtstream:
                cmt = cmtstream.read()

        # rar bug? - appends zero to comment
        cmt = cmt.split(b"\0", 1)[0]
        self.comment = cmt.decode("utf8")
        return None

    def _open_hack(self, inf, pwd):
        # len, type, blk_flags, flags
        main_hdr = b"\x03\x01\x00\x00"
        endarc_hdr = b"\x03\x05\x00\x00"
        main_hdr = S_LONG.pack(crc32(main_hdr)) + main_hdr
        endarc_hdr = S_LONG.pack(crc32(endarc_hdr)) + endarc_hdr
        return self._open_hack_core(inf, pwd, RAR5_ID + main_hdr, endarc_hdr)


##
## Utility classes
##

class UnicodeFilename:
    """Handle RAR3 unicode filename decompression.
    """
    def __init__(self, name, encdata):
        self.std_name = bytearray(name)
        self.encdata = bytearray(encdata)
        self.pos = self.encpos = 0
        self.buf = bytearray()
        self.failed = 0

    def enc_byte(self):
        """Copy encoded byte."""
        try:
            c = self.encdata[self.encpos]
            self.encpos += 1
            return c
        except IndexError:
            self.failed = 1
            return 0

    def std_byte(self):
        """Copy byte from 8-bit representation."""
        try:
            return self.std_name[self.pos]
        except IndexError:
            self.failed = 1
            return ord("?")

    def put(self, lo, hi):
        """Copy 16-bit value to result."""
        self.buf.append(lo)
        self.buf.append(hi)
        self.pos += 1

    def decode(self):
        """Decompress compressed UTF16 value."""
        hi = self.enc_byte()
        flagbits = 0
        while self.encpos < len(self.encdata):
            if flagbits == 0:
                flags = self.enc_byte()
                flagbits = 8
            flagbits -= 2
            t = (flags >> flagbits) & 3
            if t == 0:
                self.put(self.enc_byte(), 0)
            elif t == 1:
                self.put(self.enc_byte(), hi)
            elif t == 2:
                self.put(self.enc_byte(), self.enc_byte())
            else:
                n = self.enc_byte()
                if n & 0x80:
                    c = self.enc_byte()
                    for _ in range((n & 0x7f) + 2):
                        lo = (self.std_byte() + c) & 0xFF
                        self.put(lo, hi)
                else:
                    for _ in range(n + 2):
                        self.put(self.std_byte(), 0)
        return self.buf.decode("utf-16le", "replace")


class RarExtFile(io.RawIOBase):
    """Base class for file-like object that :meth:`RarFile.open` returns.

    Provides public methods and common crc checking.

    Behaviour:
     - no short reads - .read() and .readinfo() read as much as requested.
     - no internal buffer, use io.BufferedReader for that.
    """
    name = None     #: Filename of the archive entry
    mode = "rb"
    _parser = None
    _inf = None
    _fd = None
    _remain = 0
    _returncode = 0
    _md_context = None
    _seeking = False

    def _open_extfile(self, parser, inf):
        self.name = inf.filename
        self._parser = parser
        self._inf = inf

        if self._fd:
            self._fd.close()
        if self._seeking:
            md_class = NoHashContext
        else:
            md_class = self._inf._md_class or NoHashContext
        self._md_context = md_class()
        self._fd = None
        self._remain = self._inf.file_size

    def read(self, n=-1):
        """Read all or specified amount of data from archive entry."""

        # sanitize count
        if n is None or n < 0:
            n = self._remain
        elif n > self._remain:
            n = self._remain
        if n == 0:
            return b""

        buf = []
        orig = n
        while n > 0:
            # actual read
            data = self._read(n)
            if not data:
                break
            buf.append(data)
            self._md_context.update(data)
            self._remain -= len(data)
            n -= len(data)
        data = b"".join(buf)
        if n > 0:
            raise BadRarFile("Failed the read enough data: req=%d got=%d" % (orig, len(data)))

        # done?
        if not data or self._remain == 0:
            # self.close()
            self._check()
        return data

    def _check(self):
        """Check final CRC."""
        final = self._md_context.digest()
        exp = self._inf._md_expect
        if exp is None:
            return
        if final is None:
            return
        if self._returncode:
            check_returncode(self._returncode, "", tool_setup().get_errmap())
        if self._remain != 0:
            raise BadRarFile("Failed the read enough data")
        if final != exp:
            raise BadRarFile("Corrupt file - CRC check failed: %s - exp=%r got=%r" % (
                self._inf.filename, exp, final))

    def _read(self, cnt):
        """Actual read that gets sanitized cnt."""
        raise NotImplementedError("_read")

    def close(self):
        """Close open resources."""

        super().close()

        if self._fd:
            self._fd.close()
            self._fd = None

    def __del__(self):
        """Hook delete to make sure tempfile is removed."""
        self.close()

    def readinto(self, buf):
        """Zero-copy read directly into buffer.

        Returns bytes read.
        """
        raise NotImplementedError("readinto")

    def tell(self):
        """Return current reading position in uncompressed data."""
        return self._inf.file_size - self._remain

    def seek(self, offset, whence=0):
        """Seek in data.

        On uncompressed files, the seeking works by actual
        seeks so it's fast.  On compressed files its slow
        - forward seeking happens by reading ahead,
        backwards by re-opening and decompressing from the start.
        """

        # disable crc check when seeking
        if not self._seeking:
            self._md_context = NoHashContext()
            self._seeking = True

        fsize = self._inf.file_size
        cur_ofs = self.tell()

        if whence == 0:     # seek from beginning of file
            new_ofs = offset
        elif whence == 1:   # seek from current position
            new_ofs = cur_ofs + offset
        elif whence == 2:   # seek from end of file
            new_ofs = fsize + offset
        else:
            raise ValueError("Invalid value for whence")

        # sanity check
        if new_ofs < 0:
            new_ofs = 0
        elif new_ofs > fsize:
            new_ofs = fsize

        # do the actual seek
        if new_ofs >= cur_ofs:
            self._skip(new_ofs - cur_ofs)
        else:
            # reopen and seek
            self._open_extfile(self._parser, self._inf)
            self._skip(new_ofs)
        return self.tell()

    def _skip(self, cnt):
        """Read and discard data"""
        empty_read(self, cnt, BSIZE)

    def readable(self):
        """Returns True"""
        return True

    def writable(self):
        """Returns False.

        Writing is not supported.
        """
        return False

    def seekable(self):
        """Returns True.

        Seeking is supported, although it's slow on compressed files.
        """
        return True

    def readall(self):
        """Read all remaining data"""
        # avoid RawIOBase default impl
        return self.read()


class PipeReader(RarExtFile):
    """Read data from pipe, handle tempfile cleanup."""

    def __init__(self, parser, inf, cmd, tempfile=None):
        super().__init__()
        self._cmd = cmd
        self._proc = None
        self._tempfile = tempfile
        self._open_extfile(parser, inf)

    def _close_proc(self):
        if not self._proc:
            return
        for f in (self._proc.stdout, self._proc.stderr, self._proc.stdin):
            if f:
                f.close()
        self._proc.wait()
        self._returncode = self._proc.returncode
        self._proc = None

    def _open_extfile(self, parser, inf):
        super()._open_extfile(parser, inf)

        # stop old process
        self._close_proc()

        # launch new process
        self._returncode = 0
        self._proc = custom_popen(self._cmd)
        self._fd = self._proc.stdout

    def _read(self, cnt):
        """Read from pipe."""

        # normal read is usually enough
        data = self._fd.read(cnt)
        if len(data) == cnt or not data:
            return data

        # short read, try looping
        buf = [data]
        cnt -= len(data)
        while cnt > 0:
            data = self._fd.read(cnt)
            if not data:
                break
            cnt -= len(data)
            buf.append(data)
        return b"".join(buf)

    def close(self):
        """Close open resources."""

        self._close_proc()
        super().close()

        if self._tempfile:
            try:
                os.unlink(self._tempfile)
            except OSError:
                pass
            self._tempfile = None

    def readinto(self, buf):
        """Zero-copy read directly into buffer."""
        cnt = len(buf)
        if cnt > self._remain:
            cnt = self._remain
        vbuf = memoryview(buf)
        res = got = 0
        while got < cnt:
            res = self._fd.readinto(vbuf[got: cnt])
            if not res:
                break
            self._md_context.update(vbuf[got: got + res])
            self._remain -= res
            got += res
        return got


class DirectReader(RarExtFile):
    """Read uncompressed data directly from archive.
    """
    _cur = None
    _cur_avail = None
    _volfile = None

    def __init__(self, parser, inf):
        super().__init__()
        self._open_extfile(parser, inf)

    def _open_extfile(self, parser, inf):
        super()._open_extfile(parser, inf)

        self._volfile = self._inf.volume_file
        self._fd = XFile(self._volfile, 0)
        self._fd.seek(self._inf.header_offset, 0)
        self._cur = self._parser._parse_header(self._fd)
        self._cur_avail = self._cur.add_size

    def _skip(self, cnt):
        """RAR Seek, skipping through rar files to get to correct position
        """

        while cnt > 0:
            # next vol needed?
            if self._cur_avail == 0:
                if not self._open_next():
                    break

            # fd is in read pos, do the read
            if cnt > self._cur_avail:
                cnt -= self._cur_avail
                self._remain -= self._cur_avail
                self._cur_avail = 0
            else:
                self._fd.seek(cnt, 1)
                self._cur_avail -= cnt
                self._remain -= cnt
                cnt = 0

    def _read(self, cnt):
        """Read from potentially multi-volume archive."""

        pos = self._fd.tell()
        need = self._cur.data_offset + self._cur.add_size - self._cur_avail
        if pos != need:
            self._fd.seek(need, 0)

        buf = []
        while cnt > 0:
            # next vol needed?
            if self._cur_avail == 0:
                if not self._open_next():
                    break

            # fd is in read pos, do the read
            if cnt > self._cur_avail:
                data = self._fd.read(self._cur_avail)
            else:
                data = self._fd.read(cnt)
            if not data:
                break

            # got some data
            cnt -= len(data)
            self._cur_avail -= len(data)
            buf.append(data)

        if len(buf) == 1:
            return buf[0]
        return b"".join(buf)

    def _open_next(self):
        """Proceed to next volume."""

        # is the file split over archives?
        if (self._cur.flags & RAR_FILE_SPLIT_AFTER) == 0:
            return False

        if self._fd:
            self._fd.close()
            self._fd = None

        # open next part
        self._volfile = self._parser._next_volname(self._volfile)
        fd = open(self._volfile, "rb", 0)
        self._fd = fd
        sig = fd.read(len(self._parser._expect_sig))
        if sig != self._parser._expect_sig:
            raise BadRarFile("Invalid signature")

        # loop until first file header
        while True:
            cur = self._parser._parse_header(fd)
            if not cur:
                raise BadRarFile("Unexpected EOF")
            if cur.type in (RAR_BLOCK_MARK, RAR_BLOCK_MAIN):
                if cur.add_size:
                    fd.seek(cur.add_size, 1)
                continue
            if cur.orig_filename != self._inf.orig_filename:
                raise BadRarFile("Did not found file entry")
            self._cur = cur
            self._cur_avail = cur.add_size
            return True

    def readinto(self, buf):
        """Zero-copy read directly into buffer."""
        got = 0
        vbuf = memoryview(buf)
        while got < len(buf):
            # next vol needed?
            if self._cur_avail == 0:
                if not self._open_next():
                    break

            # length for next read
            cnt = len(buf) - got
            if cnt > self._cur_avail:
                cnt = self._cur_avail

            # read into temp view
            res = self._fd.readinto(vbuf[got: got + cnt])
            if not res:
                break
            self._md_context.update(vbuf[got: got + res])
            self._cur_avail -= res
            self._remain -= res
            got += res
        return got


class HeaderDecrypt:
    """File-like object that decrypts from another file"""
    def __init__(self, f, key, iv):
        self.f = f
        self.ciph = AES_CBC_Decrypt(key, iv)
        self.buf = b""

    def tell(self):
        """Current file pos - works only on block boundaries."""
        return self.f.tell()

    def read(self, cnt=None):
        """Read and decrypt."""
        if cnt > 8 * 1024:
            raise BadRarFile("Bad count to header decrypt - wrong password?")

        # consume old data
        if cnt <= len(self.buf):
            res = self.buf[:cnt]
            self.buf = self.buf[cnt:]
            return res
        res = self.buf
        self.buf = b""
        cnt -= len(res)

        # decrypt new data
        blklen = 16
        while cnt > 0:
            enc = self.f.read(blklen)
            if len(enc) < blklen:
                break
            dec = self.ciph.decrypt(enc)
            if cnt >= len(dec):
                res += dec
                cnt -= len(dec)
            else:
                res += dec[:cnt]
                self.buf = dec[cnt:]
                cnt = 0

        return res


class XFile:
    """Input may be filename or file object.
    """
    __slots__ = ("_fd", "_need_close")

    def __init__(self, xfile, bufsize=1024):
        if is_filelike(xfile):
            self._need_close = False
            self._fd = xfile
            self._fd.seek(0)
        else:
            self._need_close = True
            self._fd = open(xfile, "rb", bufsize)

    def read(self, n=None):
        """Read from file."""
        return self._fd.read(n)

    def tell(self):
        """Return file pos."""
        return self._fd.tell()

    def seek(self, ofs, whence=0):
        """Move file pos."""
        return self._fd.seek(ofs, whence)

    def readinto(self, buf):
        """Read into buffer."""
        return self._fd.readinto(buf)

    def close(self):
        """Close file object."""
        if self._need_close:
            self._fd.close()

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        self.close()


class NoHashContext:
    """No-op hash function."""
    def __init__(self, data=None):
        """Initialize"""
    def update(self, data):
        """Update data"""
    def digest(self):
        """Final hash"""
    def hexdigest(self):
        """Hexadecimal digest."""


class CRC32Context:
    """Hash context that uses CRC32."""
    __slots__ = ["_crc"]

    def __init__(self, data=None):
        self._crc = 0
        if data:
            self.update(data)

    def update(self, data):
        """Process data."""
        self._crc = crc32(data, self._crc)

    def digest(self):
        """Final hash."""
        return self._crc

    def hexdigest(self):
        """Hexadecimal digest."""
        return "%08x" % self.digest()


class Blake2SP:
    """Blake2sp hash context.
    """
    __slots__ = ["_thread", "_buf", "_cur", "_digest"]
    digest_size = 32
    block_size = 64
    parallelism = 8

    def __init__(self, data=None):
        self._buf = b""
        self._cur = 0
        self._digest = None
        self._thread = []

        for i in range(self.parallelism):
            ctx = self._blake2s(i, 0, i == (self.parallelism - 1))
            self._thread.append(ctx)

        if data:
            self.update(data)

    def _blake2s(self, ofs, depth, is_last):
        return blake2s(node_offset=ofs, node_depth=depth, last_node=is_last,
                       depth=2, inner_size=32, fanout=self.parallelism)

    def _add_block(self, blk):
        self._thread[self._cur].update(blk)
        self._cur = (self._cur + 1) % self.parallelism

    def update(self, data):
        """Hash data.
        """
        view = memoryview(data)
        bs = self.block_size
        if self._buf:
            need = bs - len(self._buf)
            if len(view) < need:
                self._buf += view.tobytes()
                return
            self._add_block(self._buf + view[:need].tobytes())
            view = view[need:]
        while len(view) >= bs:
            self._add_block(view[:bs])
            view = view[bs:]
        self._buf = view.tobytes()

    def digest(self):
        """Return final digest value.
        """
        if self._digest is None:
            if self._buf:
                self._add_block(self._buf)
                self._buf = b""
            ctx = self._blake2s(0, 1, True)
            for t in self._thread:
                ctx.update(t.digest())
            self._digest = ctx.digest()
        return self._digest

    def hexdigest(self):
        """Hexadecimal digest."""
        return hexlify(self.digest()).decode("ascii")


class Rar3Sha1:
    """Emulate buggy SHA1 from RAR3.
    """
    digest_size = 20
    block_size = 64

    _BLK_BE = struct.Struct(b">16L")
    _BLK_LE = struct.Struct(b"<16L")

    __slots__ = ("_nbytes", "_md", "_rarbug")

    def __init__(self, data=b"", rarbug=False):
        self._md = sha1()
        self._nbytes = 0
        self._rarbug = rarbug
        self.update(data)

    def update(self, data):
        """Process more data."""
        self._md.update(data)
        bufpos = self._nbytes & 63
        self._nbytes += len(data)

        if self._rarbug and len(data) > 64:
            dpos = self.block_size - bufpos
            while dpos + self.block_size <= len(data):
                self._corrupt(data, dpos)
                dpos += self.block_size

    def digest(self):
        """Return final state."""
        return self._md.digest()

    def hexdigest(self):
        """Return final state as hex string."""
        return self._md.hexdigest()

    def _corrupt(self, data, dpos):
        """Corruption from SHA1 core."""
        ws = list(self._BLK_BE.unpack_from(data, dpos))
        for t in range(16, 80):
            tmp = ws[(t - 3) & 15] ^ ws[(t - 8) & 15] ^ ws[(t - 14) & 15] ^ ws[(t - 16) & 15]
            ws[t & 15] = ((tmp << 1) | (tmp >> (32 - 1))) & 0xFFFFFFFF
        self._BLK_LE.pack_into(data, dpos, *ws)


##
## Utility functions
##

S_LONG = Struct("<L")
S_SHORT = Struct("<H")
S_BYTE = Struct("<B")

S_BLK_HDR = Struct("<HBHH")
S_FILE_HDR = Struct("<LLBLLBBHL")
S_COMMENT_HDR = Struct("<HBBH")


def load_vint(buf, pos):
    """Load RAR5 variable-size int."""
    limit = min(pos + 11, len(buf))
    res = ofs = 0
    while pos < limit:
        b = buf[pos]
        res += ((b & 0x7F) << ofs)
        pos += 1
        ofs += 7
        if b < 0x80:
            return res, pos
    raise BadRarFile("cannot load vint")


def load_byte(buf, pos):
    """Load single byte"""
    end = pos + 1
    if end > len(buf):
        raise BadRarFile("cannot load byte")
    return S_BYTE.unpack_from(buf, pos)[0], end


def load_le32(buf, pos):
    """Load little-endian 32-bit integer"""
    end = pos + 4
    if end > len(buf):
        raise BadRarFile("cannot load le32")
    return S_LONG.unpack_from(buf, pos)[0], end


def load_bytes(buf, num, pos):
    """Load sequence of bytes"""
    end = pos + num
    if end > len(buf):
        raise BadRarFile("cannot load bytes")
    return buf[pos: end], end


def load_vstr(buf, pos):
    """Load bytes prefixed by vint length"""
    slen, pos = load_vint(buf, pos)
    return load_bytes(buf, slen, pos)


def load_dostime(buf, pos):
    """Load LE32 dos timestamp"""
    stamp, pos = load_le32(buf, pos)
    tup = parse_dos_time(stamp)
    return to_datetime(tup), pos


def load_unixtime(buf, pos):
    """Load LE32 unix timestamp"""
    secs, pos = load_le32(buf, pos)
    dt = datetime.fromtimestamp(secs, timezone.utc)
    return dt, pos


def load_windowstime(buf, pos):
    """Load LE64 windows timestamp"""
    # unix epoch (1970) in seconds from windows epoch (1601)
    unix_epoch = 11644473600
    val1, pos = load_le32(buf, pos)
    val2, pos = load_le32(buf, pos)
    secs, n1secs = divmod((val2 << 32) | val1, 10000000)
    dt = datetime.fromtimestamp(secs - unix_epoch, timezone.utc)
    dt = to_nsdatetime(dt, n1secs * 100)
    return dt, pos


#
# volume numbering
#

_rc_num = re.compile('^[0-9]+$')


def _next_newvol(volfile):
    """New-style next volume
    """
    name, ext = os.path.splitext(volfile)
    if ext.lower() in ("", ".exe", ".sfx"):
        volfile = name + ".rar"
    i = len(volfile) - 1
    while i >= 0:
        if "0" <= volfile[i] <= "9":
            return _inc_volname(volfile, i, False)
        if volfile[i] in ("/", os.sep):
            break
        i -= 1
    raise BadRarName("Cannot construct volume name: " + volfile)



def _next_oldvol(volfile):
    """Old-style next volume
    """
    name, ext = os.path.splitext(volfile)
    if ext.lower() in ("", ".exe", ".sfx"):
        ext = ".rar"
    sfx = ext[2:]
    if _rc_num.match(sfx):
        ext = _inc_volname(ext, len(ext) - 1, True)
    else:
        # .rar -> .r00
        ext = ext[:2] + "00"
    return name + ext


def _inc_volname(volfile, i, inc_chars):
    """increase digits with carry, otherwise just increment char
    """
    fn = list(volfile)
    while i >= 0:
        if fn[i] == "9":
            fn[i] = "0"
            i -= 1
            if i < 0:
                fn.insert(0, "1")
        elif "0" <= fn[i] < "9" or inc_chars:
            fn[i] = chr(ord(fn[i]) + 1)
            break
        else:
            fn.insert(i + 1, "1")
            break
    return "".join(fn)


def _parse_ext_time(h, data, pos):
    """Parse all RAR3 extended time fields
    """
    # flags and rest of data can be missing
    flags = 0
    if pos + 2 <= len(data):
        flags = S_SHORT.unpack_from(data, pos)[0]
        pos += 2

    mtime, pos = _parse_xtime(flags >> 3 * 4, data, pos, h.mtime)
    h.ctime, pos = _parse_xtime(flags >> 2 * 4, data, pos)
    h.atime, pos = _parse_xtime(flags >> 1 * 4, data, pos)
    h.arctime, pos = _parse_xtime(flags >> 0 * 4, data, pos)
    if mtime:
        h.mtime = mtime
        h.date_time = mtime.timetuple()[:6]
    return pos


def _parse_xtime(flag, data, pos, basetime=None):
    """Parse one RAR3 extended time field
    """
    res = None
    if flag & 8:
        if not basetime:
            basetime, pos = load_dostime(data, pos)

        # load second fractions of 100ns units
        rem = 0
        cnt = flag & 3
        for _ in range(cnt):
            b, pos = load_byte(data, pos)
            rem = (b << 16) | (rem >> 8)

        # dostime has room for 30 seconds only, correct if needed
        if flag & 4 and basetime.second < 59:
            basetime = basetime.replace(second=basetime.second + 1)

        res = to_nsdatetime(basetime, rem * 100)
    return res, pos


def is_filelike(obj):
    """Filename or file object?
    """
    if isinstance(obj, (bytes, str, Path)):
        return False
    res = True
    for a in ("read", "tell", "seek"):
        res = res and hasattr(obj, a)
    if not res:
        raise ValueError("Invalid object passed as file")
    return True


def rar3_s2k(pwd, salt):
    """String-to-key hash for RAR3.
    """
    if not isinstance(pwd, str):
        pwd = pwd.decode("utf8")
    seed = bytearray(pwd.encode("utf-16le") + salt)
    h = Rar3Sha1(rarbug=True)
    iv = b""
    for i in range(16):
        for j in range(0x4000):
            cnt = S_LONG.pack(i * 0x4000 + j)
            h.update(seed)
            h.update(cnt[:3])
            if j == 0:
                iv += h.digest()[19:20]
    key_be = h.digest()[:16]
    key_le = pack("<LLLL", *unpack(">LLLL", key_be))
    return key_le, iv


def rar3_decompress(vers, meth, data, declen=0, flags=0, crc=0, pwd=None, salt=None):
    """Decompress blob of compressed data.

    Used for data with non-standard header - eg. comments.
    """
    # already uncompressed?
    if meth == RAR_M0 and (flags & RAR_FILE_PASSWORD) == 0:
        return data

    # take only necessary flags
    flags = flags & (RAR_FILE_PASSWORD | RAR_FILE_SALT | RAR_FILE_DICTMASK)
    flags |= RAR_LONG_BLOCK

    # file header
    fname = b"data"
    date = ((2010 - 1980) << 25) + (12 << 21) + (31 << 16)
    mode = DOS_MODE_ARCHIVE
    fhdr = S_FILE_HDR.pack(len(data), declen, RAR_OS_MSDOS, crc,
                           date, vers, meth, len(fname), mode)
    fhdr += fname
    if salt:
        fhdr += salt

    # full header
    hlen = S_BLK_HDR.size + len(fhdr)
    hdr = S_BLK_HDR.pack(0, RAR_BLOCK_FILE, flags, hlen) + fhdr
    hcrc = crc32(hdr[2:]) & 0xFFFF
    hdr = S_BLK_HDR.pack(hcrc, RAR_BLOCK_FILE, flags, hlen) + fhdr

    # archive main header
    mh = S_BLK_HDR.pack(0x90CF, RAR_BLOCK_MAIN, 0, 13) + b"\0" * (2 + 4)

    # decompress via temp rar
    setup = tool_setup()
    tmpfd, tmpname = mkstemp(suffix=".rar", dir=HACK_TMP_DIR)
    tmpf = os.fdopen(tmpfd, "wb")
    try:
        tmpf.write(RAR_ID + mh + hdr + data)
        tmpf.close()

        curpwd = (flags & RAR_FILE_PASSWORD) and pwd or None
        cmd = setup.open_cmdline(curpwd, tmpname)
        p = custom_popen(cmd)
        return p.communicate()[0]
    finally:
        tmpf.close()
        os.unlink(tmpname)


def sanitize_filename(fname, pathsep, is_win32):
    """Make filename safe for write access.
    """
    if is_win32:
        if len(fname) > 1 and fname[1] == ":":
            fname = fname[2:]
        rc = RC_BAD_CHARS_WIN32
    else:
        rc = RC_BAD_CHARS_UNIX
    if rc.search(fname):
        fname = rc.sub("_", fname)

    parts = []
    for seg in fname.split("/"):
        if seg in ("", ".", ".."):
            continue
        if is_win32 and seg[-1] in (" ", "."):
            seg = seg[:-1] + "_"
        parts.append(seg)
    return pathsep.join(parts)


def empty_read(src, size, blklen):
    """Read and drop fixed amount of data.
    """
    while size > 0:
        if size > blklen:
            res = src.read(blklen)
        else:
            res = src.read(size)
        if not res:
            raise BadRarFile("cannot load data")
        size -= len(res)


def to_datetime(t):
    """Convert 6-part time tuple into datetime object.
    """
    # extract values
    year, mon, day, h, m, s = t

    # assume the values are valid
    try:
        return datetime(year, mon, day, h, m, s)
    except ValueError:
        pass

    # sanitize invalid values
    mday = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    mon = max(1, min(mon, 12))
    day = max(1, min(day, mday[mon]))
    h = min(h, 23)
    m = min(m, 59)
    s = min(s, 59)
    return datetime(year, mon, day, h, m, s)


def parse_dos_time(stamp):
    """Parse standard 32-bit DOS timestamp.
    """
    sec, stamp = stamp & 0x1F, stamp >> 5
    mn, stamp = stamp & 0x3F, stamp >> 6
    hr, stamp = stamp & 0x1F, stamp >> 5
    day, stamp = stamp & 0x1F, stamp >> 5
    mon, stamp = stamp & 0x0F, stamp >> 4
    yr = (stamp & 0x7F) + 1980
    return (yr, mon, day, hr, mn, sec * 2)


# pylint: disable=arguments-differ,signature-differs
class nsdatetime(datetime):
    """Datetime that carries nanoseconds.

    Arithmetic operations will lose nanoseconds.

    .. versionadded:: 4.0
    """
    __slots__ = ("nanosecond",)
    nanosecond: int     #: Number of nanoseconds, 0 <= nanosecond <= 999999999

    def __new__(cls, year, month=None, day=None, hour=0, minute=0, second=0,
                microsecond=0, tzinfo=None, *, fold=0, nanosecond=0):
        usec, mod = divmod(nanosecond, 1000) if nanosecond else (microsecond, 0)
        if mod == 0:
            return datetime(year, month, day, hour, minute, second, usec, tzinfo, fold=fold)
        self = super().__new__(cls, year, month, day, hour, minute, second, usec, tzinfo, fold=fold)
        self.nanosecond = nanosecond
        return self

    def isoformat(self, sep="T", timespec="auto"):
        """Formats with nanosecond precision by default.
        """
        if timespec == "auto":
            pre, post = super().isoformat(sep, "microseconds").split(".", 1)
            return f"{pre}.{self.nanosecond:09d}{post[6:]}"
        return super().isoformat(sep, timespec)

    def astimezone(self, tz=None):
        """Convert to new timezone.
        """
        tmp = super().astimezone(tz)
        return self.__class__(tmp.year, tmp.month, tmp.day, tmp.hour, tmp.minute, tmp.second,
                              nanosecond=self.nanosecond, tzinfo=tmp.tzinfo, fold=tmp.fold)

    def replace(self, year=None, month=None, day=None, hour=None, minute=None, second=None,
                microsecond=None, tzinfo=None, *, fold=None, nanosecond=None):
        """Return new timestamp with specified fields replaced.
        """
        return self.__class__(
            self.year if year is None else year,
            self.month if month is None else month,
            self.day if day is None else day,
            self.hour if hour is None else hour,
            self.minute if minute is None else minute,
            self.second if second is None else second,
            nanosecond=((self.nanosecond if microsecond is None else microsecond * 1000)
                        if nanosecond is None else nanosecond),
            tzinfo=self.tzinfo if tzinfo is None else tzinfo,
            fold=self.fold if fold is None else fold)

    def __hash__(self):
        return hash((super().__hash__(), self.nanosecond)) if self.nanosecond else super().__hash__()

    def __eq__(self, other):
        return super().__eq__(other) and self.nanosecond == (
            other.nanosecond if isinstance(other, nsdatetime) else other.microsecond * 1000)

    def __gt__(self, other):
        return super().__gt__(other) or (super().__eq__(other) and self.nanosecond > (
            other.nanosecond if isinstance(other, nsdatetime) else other.microsecond * 1000))

    def __lt__(self, other):
        return not (self > other or self == other)

    def __ge__(self, other):
        return not self < other

    def __le__(self, other):
        return not self > other

    def __ne__(self, other):
        return not self == other


def to_nsdatetime(dt, nsec):
    """Apply nanoseconds to datetime.
    """
    if not nsec:
        return dt
    return nsdatetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
                      tzinfo=dt.tzinfo, fold=dt.fold, nanosecond=nsec)


def to_nsecs(dt):
    """Convert datatime instance to nanoseconds.
    """
    secs = int(dt.timestamp())
    nsecs = dt.nanosecond if isinstance(dt, nsdatetime) else dt.microsecond * 1000
    return secs * 1000000000 + nsecs


def custom_popen(cmd):
    """Disconnect cmd from parent fds, read only from stdout.
    """
    creationflags = 0x08000000 if WIN32 else 0  # CREATE_NO_WINDOW
    try:
        p = Popen(cmd, bufsize=0, stdout=PIPE, stderr=STDOUT, stdin=DEVNULL,
                  creationflags=creationflags)
    except OSError as ex:
        if ex.errno == errno.ENOENT:
            raise RarCannotExec("Unrar not installed?") from None
        if ex.errno == errno.EACCES or ex.errno == errno.EPERM:
            raise RarCannotExec("Cannot execute unrar") from None
        raise
    return p


def check_returncode(code, out, errmap):
    """Raise exception according to unrar exit code.
    """
    if code == 0:
        return

    if code > 0 and code < len(errmap):
        exc = errmap[code]
    elif code == 255:
        exc = RarUserBreak
    elif code < 0:
        exc = RarSignalExit
    else:
        exc = RarUnknownError

    # format message
    if out:
        msg = "%s [%d]: %s" % (exc.__doc__, code, out)
    else:
        msg = "%s [%d]" % (exc.__doc__, code)

    raise exc(msg)


def membuf_tempfile(memfile):
    """Write in-memory file object to real file.
    """
    memfile.seek(0, 0)

    tmpfd, tmpname = mkstemp(suffix=".rar", dir=HACK_TMP_DIR)
    tmpf = os.fdopen(tmpfd, "wb")

    try:
        shutil.copyfileobj(memfile, tmpf, BSIZE)
        tmpf.close()
    except BaseException:
        tmpf.close()
        os.unlink(tmpname)
        raise
    return tmpname


#
# Find working command-line tool
#

class ToolSetup:
    def __init__(self, setup):
        self.setup = setup

    def check(self):
        cmdline = self.get_cmdline("check_cmd", None)
        try:
            p = custom_popen(cmdline)
            out, _ = p.communicate()
            return p.returncode == 0
        except RarCannotExec:
            return False

    def open_cmdline(self, pwd, rarfn, filefn=None):
        cmdline = self.get_cmdline("open_cmd", pwd)
        cmdline.append(rarfn)
        if filefn:
            self.add_file_arg(cmdline, filefn)
        return cmdline

    def get_errmap(self):
        return self.setup["errmap"]

    def get_cmdline(self, key, pwd, nodash=False):
        cmdline = list(self.setup[key])
        cmdline[0] = globals()[cmdline[0]]
        if key == "check_cmd":
            return cmdline
        self.add_password_arg(cmdline, pwd)
        if not nodash:
            cmdline.append("--")
        return cmdline

    def add_file_arg(self, cmdline, filename):
        cmdline.append(filename)

    def add_password_arg(self, cmdline, pwd):
        """Append password switch to commandline.
        """
        if pwd is not None:
            if not isinstance(pwd, str):
                pwd = pwd.decode("utf8")
            args = self.setup["password"]
            if args is None:
                tool = self.setup["open_cmd"][0]
                raise RarCannotExec(f"{tool} does not support passwords")
            elif isinstance(args, str):
                cmdline.append(args + pwd)
            else:
                cmdline.extend(args)
                cmdline.append(pwd)
        else:
            cmdline.extend(self.setup["no_password"])


UNRAR_CONFIG = {
    "open_cmd": ("UNRAR_TOOL", "p", "-inul"),
    "check_cmd": ("UNRAR_TOOL", "-inul", "-?"),
    "password": "-p",
    "no_password": ("-p-",),
    # map return code to exception class, codes from rar.txt
    "errmap": [None,
               RarWarning, RarFatalError, RarCRCError, RarLockedArchiveError,    # 1..4
               RarWriteError, RarOpenError, RarUserError, RarMemoryError,        # 5..8
               RarCreateError, RarNoFilesError, RarWrongPassword]                # 9..11
}

# Problems with unar RAR backend:
# - Does not support RAR2 locked files [fails to read]
# - Does not support RAR5 Blake2sp hash [reading works]
UNAR_CONFIG = {
    "open_cmd": ("UNAR_TOOL", "-q", "-o", "-"),
    "check_cmd": ("UNAR_TOOL", "-version"),
    "password": ("-p",),
    "no_password": ("-p", ""),
    "errmap": [None],
}

# Problems with libarchive RAR backend:
# - Does not support solid archives.
# - Does not support password-protected archives.
# - Does not support RARVM-based compression filters.
BSDTAR_CONFIG = {
    "open_cmd": ("BSDTAR_TOOL", "-x", "--to-stdout", "-f"),
    "check_cmd": ("BSDTAR_TOOL", "--version"),
    "password": None,
    "no_password": (),
    "errmap": [None],
}

SEVENZIP_CONFIG = {
    "open_cmd": ("SEVENZIP_TOOL", "e", "-so", "-bb0"),
    "check_cmd": ("SEVENZIP_TOOL", "i"),
    "password": "-p",
    "no_password": ("-p",),
    "errmap": [None,
               RarWarning, RarFatalError, None, None,           # 1..4
               None, None, RarUserError, RarMemoryError]        # 5..8
}

SEVENZIP2_CONFIG = {
    "open_cmd": ("SEVENZIP2_TOOL", "e", "-so", "-bb0"),
    "check_cmd": ("SEVENZIP2_TOOL", "i"),
    "password": "-p",
    "no_password": ("-p",),
    "errmap": [None,
               RarWarning, RarFatalError, None, None,           # 1..4
               None, None, RarUserError, RarMemoryError]        # 5..8
}

CURRENT_SETUP = None


def tool_setup(unrar=True, unar=True, bsdtar=True, sevenzip=True, sevenzip2=True, force=False):
    """Pick a tool, return cached ToolSetup.
    """
    global CURRENT_SETUP
    if force:
        CURRENT_SETUP = None
    if CURRENT_SETUP is not None:
        return CURRENT_SETUP
    lst = []
    if unrar:
        lst.append(UNRAR_CONFIG)
    if unar:
        lst.append(UNAR_CONFIG)
    if sevenzip:
        lst.append(SEVENZIP_CONFIG)
    if sevenzip2:
        lst.append(SEVENZIP2_CONFIG)
    if bsdtar:
        lst.append(BSDTAR_CONFIG)

    for conf in lst:
        setup = ToolSetup(conf)
        if setup.check():
            CURRENT_SETUP = setup
            break
    if CURRENT_SETUP is None:
        raise RarCannotExec("Cannot find working tool")
    return CURRENT_SETUP


def main(args):
    """Minimal command-line interface for rarfile module.
    """
    import argparse
    p = argparse.ArgumentParser(description=main.__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("-l", "--list", metavar="<rarfile>",
                   help="Show archive listing")
    g.add_argument("-e", "--extract", nargs=2,
                   metavar=("<rarfile>", "<output_dir>"),
                   help="Extract archive into target dir")
    g.add_argument("-t", "--test", metavar="<rarfile>",
                   help="Test if a archive is valid")
    cmd = p.parse_args(args)

    if cmd.list:
        with RarFile(cmd.list) as rf:
            rf.printdir()
    elif cmd.test:
        with RarFile(cmd.test) as rf:
            rf.testrar()
    elif cmd.extract:
        with RarFile(cmd.extract[0]) as rf:
            rf.extractall(cmd.extract[1])


if __name__ == "__main__":
    main(sys.argv[1:])

