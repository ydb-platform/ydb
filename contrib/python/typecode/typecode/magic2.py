#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0 AND MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/aboutcode-org/typecode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
# This code was in part derived from the python-magic library:

# Copyright (c) 2001-2014 Adam Hupp
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import ctypes.util
import glob
import os
import sys
import warnings

from commoncode import command
from commoncode.system import on_windows

"""
magic2 is minimal and specialized wrapper around a vendored libmagic file
identification library. This is NOT thread-safe. It is based on python-magic
by Adam Hup and adapted to the specific needs of ScanCode.
"""

# Tracing flag
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(" ".join(isinstance(a, str) and a or repr(a) for a in args))


#
# Cached detectors
#
detectors = {}

# libmagic flags
MAGIC_NONE = 0
MAGIC_MIME = 16
MAGIC_MIME_ENCODING = 1024
MAGIC_NO_CHECK_ELF = 65536
MAGIC_NO_CHECK_TEXT = 131072
MAGIC_NO_CHECK_CDF = 262144

DETECT_TYPE = MAGIC_NONE
DETECT_MIME = MAGIC_NONE | MAGIC_MIME
DETECT_ENC = MAGIC_NONE | MAGIC_MIME | MAGIC_MIME_ENCODING

# keys for plugin-provided locations
TYPECODE_LIBMAGIC_DLL = "typecode.libmagic.dll"
TYPECODE_LIBMAGIC_DB = "typecode.libmagic.db"

TYPECODE_LIBMAGIC_PATH_ENVVAR = "TYPECODE_LIBMAGIC_PATH"
TYPECODE_LIBMAGIC_DB_PATH_ENVVAR = "TYPECODE_LIBMAGIC_DB_PATH"

if TRACE:

    def file_type(location):
        return _detect(location, DETECT_TYPE)

else:

    def file_type(location):
        """ "
        Return the detected filetype for file at `location` or an empty string if
        nothing found or an error occurred.
        """
        try:
            return _detect(location, DETECT_TYPE)
        except:
            # TODO: log errors
            return ""


class NoMagicLibError(Exception):
    """
    Raised when no libmagic library is found.
    """


def load_lib_failover():
    """
    Return a loaded libmagic from well known system installation locations.
    This is a function originally from python-magic.
    """
    libmagic = None
    # Let's try to find magic or magic1
    dll = (
        ctypes.util.find_library("magic")
        or ctypes.util.find_library("magic1")
        or ctypes.util.find_library("cygmagic-1")
        or ctypes.util.find_library("libmagic-1")
        # for MSYS2
        or ctypes.util.find_library("msys-magic-1")
    )
    # necessary because find_library returns None if it doesn't find the library
    if dll:
        libmagic = ctypes.CDLL(dll)

    if not (libmagic and libmagic._name):
        windows_dlls = [
            "magic1.dll",
            "cygmagic-1.dll",
            "libmagic-1.dll",
            "msys-magic-1.dll",
        ]
        platform_to_lib = {
            "darwin": (
                [
                    "/opt/local/lib/libmagic.dylib",
                    "/usr/local/lib/libmagic.dylib",
                ]
                +
                # Assumes there will only be one version installed when using brew
                glob.glob("/usr/local/Cellar/libmagic/*/lib/libmagic.dylib")
                + glob.glob("/opt/homebrew/Cellar/libmagic/*/lib/libmagic.dylib")
            ),
            "win32": windows_dlls,
            "cygwin": windows_dlls,
            "linux": ["libmagic.so.1"],
        }
        # fallback for some Linuxes (e.g. Alpine) where library search does not
        # work # flake8:noqa
        platform = "linux" if sys.platform.startswith("linux") else sys.platform
        for dll in platform_to_lib.get(platform, []):
            try:
                libmagic = ctypes.CDLL(dll)
                break
            except OSError:
                pass

    if libmagic and libmagic._name:
        return libmagic


def load_lib():
    """
    Return the libmagic shared library object loaded from either:
    - an environment variable ``TYPECODE_LIBMAGIC_PATH``
    - a plugin-provided path,
    - well known system names and locations,
    - the system PATH.
    Raise an NoMagicLibError if no libmagic can be found.
    """
    from plugincode.location_provider import get_location

    # try the environment first
    dll_loc = os.environ.get(TYPECODE_LIBMAGIC_PATH_ENVVAR)

    if TRACE and dll_loc:
        logger_debug("load_lib:", "got environ magic location:", dll_loc)

    # try a plugin-provided path second
    if not dll_loc:
        dll_loc = get_location(TYPECODE_LIBMAGIC_DLL)

        if TRACE and dll_loc:
            logger_debug("load_lib:", "got plugin magic location:", dll_loc)

    # try well known locations
    if not dll_loc:
        failover_lib = load_lib_failover()
        if failover_lib:
            warnings.warn(
                "System libmagic found in typical location is used. "
                "Install instead a typecode-libmagic plugin for best support."
            )
            return failover_lib

    # try the PATH
    if not dll_loc:
        dll = "libmagic.dll" if on_windows else "libmagic.so"
        dll_loc = command.find_in_path(dll)

        if dll_loc:
            warnings.warn(
                "libmagic found in the PATH. "
                "Install instead a typecode-libmagic plugin for best support."
            )

        if TRACE and dll_loc:
            logger_debug("load_lib:", "got path magic location:", dll_loc)

    if not dll_loc or not os.path.isfile(dll_loc):
        raise NoMagicLibError(
            "CRITICAL: libmagic DLL and its magic database are not installed. "
            "Unable to continue: you need to install a valid typecode-libmagic "
            "plugin with a valid and proper libmagic and magic DB available.\n"
            f"OR set the {TYPECODE_LIBMAGIC_PATH_ENVVAR} and "
            f"{TYPECODE_LIBMAGIC_DB_PATH_ENVVAR} environment variables.\n"
            f"OR install libmagic in typical common locations.\n"
            f"OR have a libmagic in the system PATH.\n"
        )
    return command.load_shared_library(dll_loc)


def get_magicdb_location(_cache=[]):
    """
    Return the location of the magicdb loaded from either:
    - an environment variable ``TYPECODE_LIBMAGIC_DB_PATH``,
    - a plugin-provided path,
    - the system PATH.
    Trigger a warning if no magicdb file is found.
    """
    if _cache:
        return _cache[0]

    from plugincode.location_provider import get_location

    # try the environment first
    magicdb_loc = os.environ.get(TYPECODE_LIBMAGIC_DB_PATH_ENVVAR)

    if TRACE and magicdb_loc:
        logger_debug("get_magicdb_location:", "got environ magicdb location:", magicdb_loc)

    # try a plugin-provided path second
    if not magicdb_loc:
        magicdb_loc = get_location(TYPECODE_LIBMAGIC_DB)

        if TRACE and magicdb_loc:
            logger_debug("get_magicdb_location:", "got plugin magicdb location:", magicdb_loc)

    # try the PATH
    if not magicdb_loc:
        db = "magic.mgc"
        magicdb_loc = command.find_in_path(db)

        if magicdb_loc:
            warnings.warn(
                "magicdb found in the PATH. "
                "Install instead a typecode-libmagic plugin for best support.\n"
                f"OR set the {TYPECODE_LIBMAGIC_DB_PATH_ENVVAR} environment variable."
            )

        if TRACE and magicdb_loc:
            logger_debug("get_magicdb_location:", "got path magicdb location:", magicdb_loc)

    if not magicdb_loc:
        warnings.warn(
            "Libmagic magic database not found. "
            "A default will be used if possible. "
            "Install instead a typecode-libmagic plugin for best support.\n"
            f"OR set the {TYPECODE_LIBMAGIC_DB_PATH_ENVVAR} environment variable."
        )
        return

    _cache.append(magicdb_loc)
    return magicdb_loc


def mime_type(location):
    """ "
    Return the detected mimetype for file at `location` or an empty string if
    nothing found or an error occurred.
    """
    try:
        return _detect(location, DETECT_MIME)
    except:
        # TODO: log errors
        return ""


def encoding(location):
    """ "
    Return the detected encoding for file at `location` or an empty string.
    Raise an exception on errors.
    """
    return _detect(location, DETECT_ENC)


def _detect(location, flags):
    """ "
    Return the detected type using `flags` of file at `location` or an empty
    string. Raise an exception on errors.
    """
    try:
        detector = detectors[flags]
    except KeyError:
        detector = Detector(flags=flags)
        detectors[flags] = detector
    val = detector.get(location)
    val = val or ""
    val = val.decode("ascii", "ignore").strip()
    return " ".join(val.split())


class MagicException(Exception):
    pass


class Detector(object):
    def __init__(self, flags, magic_db_location=None):
        """
        Create a new libmagic detector.
        flags - the libmagic flags
        magic_file - use a mime database other than the vendored default
        """
        self.flags = flags
        self.cookie = _magic_open(self.flags)
        if not magic_db_location:
            # Caveat emptor: this may be empty in which case a default will be tried
            magic_db_location = get_magicdb_location()

        # Note: this location must always be FS-encoded bytes on all OSes
        if magic_db_location and not isinstance(magic_db_location, bytes):
            magic_db_location = os.fsencode(magic_db_location)

        _magic_load(self.cookie, magic_db_location)

    def get(self, location):
        """
        Return the magic type info from a file at `location`. The value
        returned depends on the flags passed to the object. If this fails
        attempt to get it using a UTF-encoded location or from loading the
        first 16K of the file. Raise a MagicException on error.
        """
        assert location
        try:
            # first use the path as is
            return _magic_file(self.cookie, location)
        except:
            # then try to get a utf-8 encoded path: Rationale:
            # https://docs.python.org/2/library/ctypes.html#ctypes.set_conversion_mode ctypes
            # encode strings to byte as ASCII or MBCS depending on the OS The
            # location string may therefore be mangled and the file not accessible
            # anymore by libmagic in some cases.
            try:
                uloc = os.fsencode(location)
                return _magic_file(self.cookie, uloc)
            except:
                # if all fails, read the start of the file instead
                with open(location, "rb") as fd:
                    buf = fd.read(16384)
                return _magic_buffer(self.cookie, buf, len(buf))

    def __del__(self):
        """
        During shutdown magic_close may have been cleared already so make sure
        it exists before using it.
        """
        if self.cookie and _magic_close:
            _magic_close(self.cookie)


# Main ctypes proxy
libmagic = load_lib()


def libmagic_version():
    return _magic_version()


def check_error(result, func, args):  # NOQA
    """
    ctypes error handler/checker:  Check for errors and raise an exception or
    return the result otherwise.
    """
    is_int = isinstance(result, int)
    is_bytes = isinstance(result, bytes)
    is_text = isinstance(result, str)
    if (
        result is None
        or (is_int and result < 0)
        or (is_bytes and str(result, encoding="utf-8", errors="ignore").startswith("cannot open"))
        or (is_text and result.startswith("cannot open"))
    ):
        err = _magic_error(args[0])
        raise MagicException(err)
    else:
        return result


# ctypes functions aliases.
_magic_open = libmagic.magic_open
_magic_open.restype = ctypes.c_void_p
_magic_open.argtypes = [ctypes.c_int]

_magic_close = libmagic.magic_close
_magic_close.restype = None
_magic_close.argtypes = [ctypes.c_void_p]

_magic_error = libmagic.magic_error
_magic_error.restype = ctypes.c_char_p
_magic_error.argtypes = [ctypes.c_void_p]

_magic_file = libmagic.magic_file
_magic_file.restype = ctypes.c_char_p
_magic_file.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_magic_file.errcheck = check_error

_magic_buffer = libmagic.magic_buffer
_magic_buffer.restype = ctypes.c_char_p
_magic_buffer.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
_magic_buffer.errcheck = check_error

_magic_load = libmagic.magic_load
_magic_load.restype = ctypes.c_int
_magic_load.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_magic_load.errcheck = check_error

_magic_version = libmagic.magic_version
_magic_version.restype = ctypes.c_int
_magic_version.argtypes = []
