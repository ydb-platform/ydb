#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial
import locale
import logging
import mmap
import os
import warnings

import ctypes.util
from ctypes import c_char_p, c_wchar_p
from ctypes import c_int, c_longlong
from ctypes import c_size_t, c_ssize_t
from ctypes import c_void_p
from ctypes import POINTER
from ctypes import create_string_buffer

import attr

from commoncode import command
from commoncode import fileutils
from commoncode import paths
from commoncode import text
from commoncode.system import on_windows

import extractcode
from extractcode import ExtractError
from extractcode import ExtractErrorPasswordProtected

logger = logging.getLogger(__name__)

TRACE = False
TRACE_DEEP = False

if TRACE or TRACE_DEEP:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
libarchive2 is a minimal and specialized wrapper around a vendored libarchive
archive extraction library. It only deals with archive extraction and does not
know how to create archives.

Its main purpose is to try hard to extract files from archives on multiple OSes
and makes some compromises in doing so:

- special files and links may be skipped entirely and not extracted at all.

- relative paths are resolved to ensure that files are always extracted under a
  root extraction directory.

- files and directories may be renamed if they are not unique (ignoring case) in
  their extraction directory.

- files and directories are renamed by "transliterating" their names to plain
  ASCII if their name contain non-ASCI characters.

- files and directories are renamed if they contain characters or names that are
  not portable on common OSes (e.g. COM1, ":", "*", etc)

- permissions and modes are ignored entirely when extracting files to ensure
  that extracted files are always readable.

It is inspired from several libarchive bindings such as libarchive_c and
python-libarchive for Python and other similar wrappers for Ruby such as
ffi-libarchive.
"""

# keys for plugin-provided locations
EXTRACTCODE_LIBARCHIVE_DLL = 'extractcode.libarchive.dll'

EXTRACTCODE_LIBARCHIVE_PATH_ENVVAR = 'EXTRACTCODE_LIBARCHIVE_PATH'

_LIBRARY_NAME = 'libarchive'


def load_lib():
    """
    Return the libarchive shared library object loaded from either:
    - an environment variable ``EXTRACTCODE_LIBARCHIVE_PATH``
    - a plugin-provided path,
    - the system PATH.
    Raise an Exception if no libarchive can be found.
    """
    from plugincode.location_provider import get_location

    # try the environment first
    dll_loc = os.environ.get(EXTRACTCODE_LIBARCHIVE_PATH_ENVVAR)

    # try a plugin-provided path second
    if not dll_loc:
        dll_loc = get_location(EXTRACTCODE_LIBARCHIVE_DLL)

    # try the standard locations with find_library
    if not dll_loc:
        libarch_loc = ctypes.util.find_library(_LIBRARY_NAME)
        libarchive = ctypes.cdll.LoadLibrary(libarch_loc)
        if libarchive:
            warnings.warn(
                'Using "libarchive" library found in a system location. '
                'Install instead a extractcode-libarchive plugin for best support.'
            )
            return libarchive

    # try the PATH
    if not dll_loc:
        dll = 'libarchive.dll' if on_windows else 'libarchive.so'
        dll_loc = command.find_in_path(dll)
        if dll_loc:
            warnings.warn(
                'Using "libarchive" library found in the PATH. '
                'Install instead a extractcode-libarchive plugin for best support.'
            )

    if not dll_loc or not os.path.isfile(dll_loc):
        raise Exception(
            'CRITICAL: libarchive DLL is not installed. '
            'Unable to continue: you need to install a valid extractcode-libarchive '
            'plugin with a valid libarchive DLL available. '
            f'OR set the {EXTRACTCODE_LIBARCHIVE_PATH_ENVVAR} environment variable. '
            'OR install libarchive as a system package. '
            'OR ensure libarchive is available in the system PATH.'
    )
    return command.load_shared_library(dll_loc)


def set_env_with_tz():
    # NOTE: this is important to avoid timezone differences
    os.environ['TZ'] = 'UTC'


set_env_with_tz()

# NOTE: this is important to avoid locale-specific errors on various OS
locale.setlocale(locale.LC_ALL, '')

# load and initialize the shared library
libarchive = load_lib()


def extract(location, target_dir, skip_symlinks=True):
    """
    Extract files from a libarchive-supported archive file at `location` in the
    `target_dir` directory. `skip_symlinks` by default.
    Return a list of warning messages if any or an empty list.
    Raise Exceptions on errors.
    """
    assert location
    assert target_dir
    abs_location = os.path.abspath(os.path.expanduser(location))
    abs_target_dir = os.path.abspath(os.path.expanduser(target_dir))
    warnings = []

    set_env_with_tz()

    for entry in list_entries(abs_location):
        logger.debug('processing entry: {}'.format(entry))
        if not entry:
            continue

        if entry.is_empty():
            if TRACE:
                logger.debug('Skipping empty: {}'.format(entry))
            continue

        if entry.warnings:
            if not entry.is_empty():
                entry_path = entry.path
                msgs = ['%(entry_path)r: ' % locals()]
            else:
                msgs = ['No path available: ']

            messages = (w for w in entry.warnings if w and w.strip())
            messages = map(text.as_unicode, messages)
            messages = (w.strip('"\' ') for w in messages)
            msgs.extend(w for w in messages if w)
            msgs = '\n'.join(msgs) or 'No message provided'

            if msgs not in warnings:
                warnings.append(msgs)

            if TRACE:
                logger.debug('\n'.join(msgs))

        if not (entry.isdir or entry.isfile):
            # skip special files and links
            if TRACE:
                logger.debug('skipping: {}'.format(entry))

            if entry.issym and not skip_symlinks:
                raise NotImplemented(
                    'extraction of symlinks with libarchive is not yet implemented.')
            continue

        if TRACE:
            logger.debug('  writing.....')

        _target_path = entry.write(abs_target_dir, transform_path=partial(paths.safe_path, preserve_spaces=True))

    return warnings


def list_entries(location):
    """
    Return an archive entries list for the archive file at `location`.
    """
    assert location
    abs_location = os.path.abspath(os.path.expanduser(location))
    assert os.path.isfile(abs_location)

    # TODO: harden error handling
    with Archive(abs_location) as archive:
        for entry in archive:
            yield entry


class Archive(object):
    """
    Represent an iterable archive containing a list of Entry objects.

    Archive is designed to be used as a context manager with the "with" syntax:

        with Archive('some.tgz') as archive:
            for entry in archive:
                # do something with entry
    """

    def __init__(self, location, uncompress=True, extract=True, block_size=10240):
        """
        Build an Archive object from file at `location`.

        If `uncompress` is True, the archive will be uncompressed first if
        compressed. (e.g. a tar.gz will be ungzipped).

        If `extract` is True, the archive will be extracted if this is an
        archive. (e.g. a cpio will be extracted).

        If both are True, the archive will be uncompressed then extracted as
        needed. (e.g. a tar.xz will be unxzed then untarred at once).
        """
        msg = 'At least one of `uncompress` or `extract` flag is required.'
        assert uncompress or extract, msg
        self.location = location
        self.uncompress = uncompress
        self.extract = extract
        self.block_size = block_size
        # pointer to the libarchive structure
        self.archive_struct = None

    def open(self):
        """
        Open the archive for reading. You must call close() when done to free up
        resources and avoid leaks. Or use instead the Archive class as a context
        manager with the "with" keyword.
        """
        # first close any existing opened struct for this file
        self.close()
        self.archive_struct = archive_reader()
        if self.uncompress:
            use_all_filters(self.archive_struct)
        if extract:
            use_all_formats(self.archive_struct)
        try:
            # TODO: ensure that we have proper exceptions raised?
            open_file(self.archive_struct, self.location, self.block_size)
        except:
            open_file_w(self.archive_struct, self.location, self.block_size)
        return self

    def close(self):
        """
        Release any memory held by the underlying librachive for this archive.
        You must call close() when done with an archive to free up resources and
        avoid leaks.
        """
        if self.archive_struct:
            free_archive(self.archive_struct)
            self.archive_struct = None

    def iter(self):
        """
        Yield Entry(ies) for this archive.
        """
        assert self.archive_struct, 'Archive must be used as a context manager.'
        entry_struct = new_entry()
        try:
            while True:
                entry = None
                warnings = []
                try:
                    r = next_entry(self.archive_struct, entry_struct)
                    if r == ARCHIVE_EOF:
                        return
                    entry = Entry(self, entry_struct)
                except ArchiveWarning as aw:
                    if not entry:
                        entry = Entry(self, entry_struct)
                    if aw.msg and aw.msg not in entry.warnings:
                        entry.warnings.append(aw.msg)

                finally:
                    if entry:
                        entry.warnings.extend(warnings)
                        yield entry
        finally:
            if entry_struct:
                free_entry(entry_struct)

    def __enter__(self):
        return self.open()

    def __exit__(self, _type, _value, _traceback):
        return self.close()

    def __iter__(self):
        return self.iter()


@attr.attrs
class Entry(object):
    """
    Represent an Archive entry.

    The attribute names are loosely based on the stdlib module tarfile.Tarfile
    class attributes. Some attributes are not handled on purpose because they
    are never used: things such as modes/perms/users/groups are never restored
    by design to ensure extracted files are readable/writable and owned by the
    extracting user.
    """
    # TODO: re-check if users/groups may have some value for origin determination?

    # an archive object
    archive = attr.ib(repr=False)
    entry_struct = attr.ib(default=None, repr=False)
    # all paths are byte strings not unicode
    path = attr.ib(default=None)
    # bytes
    size = attr.ib(default=0)
    isfile = attr.ib(default=False)
    isdir = attr.ib(default=False)
    isspecial = attr.ib(default=False)
    issym = attr.ib(default=False)
    symlink_path = attr.ib(default=None)

    islnk = attr.ib(default=False)
    hardlink_path = attr.ib(default=None)

    isblk = attr.ib(default=False, repr=False)
    ischr = attr.ib(default=False, repr=False)
    isfifo = attr.ib(default=False, repr=False)
    issock = attr.ib(default=False, repr=False)

    # sec since epoch
    time = attr.ib(default=None, repr=False)

    # list of strings
    warnings = attr.ib(default=attr.Factory(list), repr=False)

    def __attrs_post_init__(self, *args, **kwargs):
        if not self.entry_struct:
            return

        filetype = entry_type(self.entry_struct)
        if filetype:
            self.isfile = filetype & AE_IFMT == AE_IFREG
            self.isdir = filetype & AE_IFMT == AE_IFDIR
            self.isblk = filetype & AE_IFMT == AE_IFBLK
            self.ischr = filetype & AE_IFMT == AE_IFCHR
            self.isfifo = filetype & AE_IFMT == AE_IFIFO
            self.issock = filetype & AE_IFMT == AE_IFSOCK
            self.isspecial = self.ischr or self.isblk or self.isfifo or self.issock
        else:
            # on some windows ar lib entries there is no type. This is a bug
            # we use isfile then
            self.isfile = True

        self.size = entry_size(self.entry_struct) or 0
        self.time = entry_time(self.entry_struct) or 0
        self.path = self.get_path(entry_path, entry_path_w)
        self.issym = filetype & AE_IFMT == AE_IFLNK

        # FIXME: could there be cases with link path and symlink is False?
        if self.issym:
            self.symlink_path = self.get_path(symlink_path, symlink_path_w)

        self.hardlink_path = self.get_path(hardlink_path, hardlink_path_w)
        # hardlinks do not have a filetype: we test the path instead
        self.islnk = bool(self.hardlink_path)

    def is_empty(self):
        return not self.archive or not self.entry_struct

    def get_path(self, func, func_w):
        """
        Return a path calling first the path function `func` then the wide char
        equivalent `func_w` if `func` did not provide a path.

        The path returned is either byte (on Python 2) or unicode string (Python
        3) On Python 2, if a path is unicode its bytes are converted to
        UTF-8-encoded bytes.
        """
        path = func(self.entry_struct)
        if not path:
            path = func_w(self.entry_struct)
        if not isinstance(path, str):
            path = text.as_unicode(path)

        return path

    def write(self, target_dir, transform_path=lambda x: x, skip_links=True):
        """
        Write entry to a file or directory saved relatively to the `target_dir`
        and return the path where the file or directory was written or None if
        nothing was written to disk. `transform_path` is a callable taking a
        path and returning a transformed path such as resolving relative paths,
        transliterating non-portable characters or other path transformations.
        The default is a no-op lambda.
        """
        if TRACE:
            logger.debug('writing entry: {}'.format(self))

        if not self.archive.archive_struct:
            raise ArchiveErrorIllegalOperationOnClosedArchive()
        # skip links and special files
        if not (self.isfile or self.isdir):
            return

        if skip_links and self.issym:
            return

        if skip_links and self.issym:
            return
        if not skip_links and self.issym:
            raise NotImplemented(
                'extraction of sym links with librarchive is not yet implemented.')

        abs_target_dir = os.path.abspath(os.path.expanduser(target_dir))
        # TODO: return some warning when original path has been transformed
        clean_path = transform_path(self.path)

        if self.isdir:
            # TODO: also rename directories to a new name if needed segment by segment
            dir_path = os.path.join(abs_target_dir, clean_path)
            fileutils.create_dir(dir_path)
            return dir_path

        # note: here isfile=True
        # create parent directories if needed
        target_path = os.path.join(abs_target_dir, clean_path)
        parent_path = os.path.dirname(target_path)

        # TODO: also rename directories to a new name if needed segment by segment
        fileutils.create_dir(parent_path)

        # TODO: return some warning when original path has been renamed?
        unique_path = extractcode.new_name(target_path, is_dir=False)
        if TRACE:
            logger.debug(
                f'path: \ntarget_path: {target_path}\n'
                f'unique_path: {unique_path}',
            )

        with open(unique_path, 'wb') as target:
            for content in self.get_content():
                if TRACE_DEEP:
                    logger.debug('    chunk: {}'.format(repr(content)))
                target.write(content)

        os.utime(unique_path, (self.time, self.time))

        return target_path

    def get_content(self):
        """
        Yield the content of this archive as bytes.
        """
        chunk_len = mmap.PAGESIZE
        sbuffer = create_string_buffer(chunk_len)
        archive_struct = self.archive.archive_struct
        red_len = 1
        while red_len:
            red_len = read_entry_data(archive_struct, sbuffer, chunk_len)
            yield sbuffer.raw[0:red_len]


class ArchiveException(ExtractError):

    def __init__(
        self,
        rc=None,
        archive_struct=None,
        archive_func=None,
        root_ex=None,
    ):
        self.root_ex = root_ex
        if root_ex and isinstance(root_ex, ArchiveException):
            self.rc = root_ex.rc
            self.errno = root_ex.errno
            msg = root_ex.args or []
            msg = map(text.as_unicode, msg)
            msg = u'\n'.join(msg)
            self.msg = msg or None
            self.func = root_ex.func
        else:
            self.rc = rc
            self.errno = archive_struct and errno(archive_struct) or None
            msg = archive_struct and err_msg(archive_struct) or ''
            self.msg = msg and text.as_unicode(msg) or 'Unknown error'
            self.func = archive_func and archive_func.__name__ or None

    def __str__(self):
        if TRACE:
            msg = (
                '%(msg)r: in function %(func)r with rc=%(rc)r, '
                'errno=%(errno)r, root_ex=%(root_ex)r')
            return msg % self.__dict__
        return self.msg or ''


class ArchiveWarning(ArchiveException):
    pass


class ArchiveErrorRetryable(ArchiveException):
    pass


class ArchiveError(ArchiveException):
    pass


class ArchiveErrorFatal(ArchiveException):
    pass


class ArchiveErrorFailedToWriteEntry(ArchiveException):
    pass


class ArchiveErrorPasswordProtected(
    ArchiveException,
    ExtractErrorPasswordProtected,
):
    pass


class ArchiveErrorIllegalOperationOnClosedArchive(ArchiveException):
    pass

#################################################
# ctypes defintion of the interface to libarchive
#################################################


def errcheck(rc, archive_func, args, null=False):
    """
    ctypes error check handler for functions returning int, or null if null is
    True.
    """
    if null:
        if rc is None:
            archive_struct = args and len(args) > 1 and args[0] or None
            raise ArchiveError(rc, archive_struct, archive_func)
        else:
            return rc

    if rc >= ARCHIVE_OK:
        return rc

    archive_struct = args[0]
    if rc == ARCHIVE_RETRY:
        raise ArchiveErrorRetryable(rc, archive_struct, archive_func)

    if rc == ARCHIVE_WARN:
        raise ArchiveWarning(rc, archive_struct, archive_func)

    # anything else is a serious error, in general not recoverable.
    raise ArchiveError(rc, archive_struct, archive_func)


errcheck_null = partial(errcheck, null=True)

# libarchive return codes
ARCHIVE_EOF = 1
ARCHIVE_OK = 0
ARCHIVE_RETRY = -10
ARCHIVE_WARN = -20
ARCHIVE_FAILED = -25
ARCHIVE_FATAL = -30

# libarchive stat/file types
AE_IFREG = 0o0100000  # Regular file
AE_IFLNK = 0o0120000  # Symbolic link
AE_IFSOCK = 0o0140000  # Socket
AE_IFCHR = 0o0020000  # Character device
AE_IFBLK = 0o0060000  # Block device
AE_IFDIR = 0o0040000  # Directory
AE_IFIFO = 0o0010000  # Named pipe (fifo)

AE_IFMT = 0o0170000  # Format mask

#####################################
# libarchive C functions declarations
#####################################

# NOTE: these declaration come with verbose doc to help with debugging and
# tracing lower level errors and issues. Some comments and the function
# signatures are copied from libarchve.
#
# NOTE: String data in libarchive can be set or accessed as wide character
# strings or narrow char strings. The functions that use wide character strings
# are suffixed with _w. These are different representations of the same data:
# For example, if you store a narrow string and read the corresponding wide
# string, the object will transparently convert formats using the current
# locale. Similarly, if you store a wide string and then store a narrow string
# for the same data, the previously-set wide string will be discarded in favor
# of the new data.

"""
To read an archive, you must first obtain an initialized struct archive object
from archive_read_new()

Allocates and initializes a struct archive object suitable for reading from an
archive. NULL is returned on error.
"""
# struct archive * archive_read_new(void);
archive_reader = libarchive.archive_read_new
archive_reader.argtypes = []
archive_reader.restype = c_void_p
archive_reader.errcheck = errcheck_null

"""
Given a struct archive object, you can enable support for formats and filters.
Enables support for all available formats except the "raw" format.

Return ARCHIVE_OK on success, or ARCHIVE_FATAL.
Detailed error codes and textual descriptions are available from the
archive_errno() and archive_error_string() functions.
"""

# int archive_read_support_format_all(struct archive *);
use_all_formats = libarchive.archive_read_support_format_all
use_all_formats.argtypes = [c_void_p]
use_all_formats.restype = c_int
use_all_formats.errcheck = errcheck

"""
Given a struct archive object, you can enable support for formats and filters.

Enables support for the "raw" format.
The "raw" format handler allows libarchive to be used to read arbitrary
data. It treats any data stream as an archive with a single entry. The
pathname of this entry is "data ;" all other entry fields are unset. This is
not enabled by archive_read_support_format_all() in order to avoid erroneous
handling of damaged archives.
"""
# int archive_read_support_format_raw(struct archive *);
use_raw_formats = libarchive.archive_read_support_format_raw
use_raw_formats.argtypes = [c_void_p]
use_raw_formats.restype = c_int
use_raw_formats.errcheck = errcheck

"""
Given a struct archive object, you can enable support for formats and filters.

Enables all available decompression filters.
Return ARCHIVE_OK if the compression is fully supported, ARCHIVE_WARN if the
compression is supported only through an external program.
Detailed error codes and textual descriptions are available from the
archive_errno() and archive_error_string() functions.
"""
# int archive_read_support_filter_all(struct archive *);
use_all_filters = libarchive.archive_read_support_filter_all
use_all_filters.argtypes = [c_void_p]
use_all_filters.restype = c_int
use_all_filters.errcheck = errcheck

"""
Once formats and filters have been set, you open an archive filename for
actual reading.

Freeze the settings, open the archive, and prepare for reading entries.
Accepts a simple filename and a block size. A NULL filename represents
standard input.

Return ARCHIVE_OK on success, or ARCHIVE_FATAL.
Once you have finished reading data from the archive, you should call
archive_read_close() to close the archive, then call archive_read_free() to
release all resources, including all memory allocated by the library.
"""
# int archive_read_open_filename(struct archive *, const char *filename, size_t block_size);
open_file = libarchive.archive_read_open_filename
open_file.argtypes = [c_void_p, c_char_p, c_size_t]
open_file.restype = c_int
open_file.errcheck = errcheck

"""
Wide char version of archive_read_open_filename.
"""
# int archive_read_open_filename_w(struct archive *, const wchar_t *_filename, size_t _block_size);
open_file_w = libarchive.archive_read_open_filename_w
open_file_w.argtypes = [c_void_p, c_wchar_p, c_size_t]
open_file_w.restype = c_int
open_file_w.errcheck = errcheck

"""
When done with reading an archive you must free its resources.

Invokes archive_read_close() if it was not invoked manually, then release all
resources.
Return ARCHIVE_OK on success, or ARCHIVE_FATAL.
"""
# int  archive_read_free(struct archive *);
free_archive = libarchive.archive_read_free
free_archive.argtypes = [c_void_p]
free_archive.restype = c_int
free_archive.errcheck = errcheck

#
# entry level functions
#
"""
You can think of a struct archive_entry as a heavy-duty version of struct stat
: it includes everything from struct stat plus associated pathname, textual
group and user names, etc. These objects are used by ManPageLibarchive3 to
represent the metadata associated with a particular entry in an archive.
"""

"""
Allocate and return a blank struct archive_entry object.
"""
# struct archive_entry * archive_entry_new(void);
new_entry = libarchive.archive_entry_new
new_entry.argtypes = []
new_entry.restype = c_void_p
new_entry.errcheck = errcheck_null

"""
Given an opened archive struct object, you can iterate through the archive
entries. An entry has a header with various data and usually a payload that is
the archived content.

Read the header for the next entry and populate the provided struct
archive_entry.

Return ARCHIVE_OK (the operation succeeded), ARCHIVE_WARN (the operation
succeeded but a non-critical error was encountered), ARCHIVE_EOF (end-of-
archive was encountered), ARCHIVE_RETRY (the operation failed but can be
retried), and ARCHIVE_FATAL (there was a fatal error; the archive should be
closed immediately).
"""
# int archive_read_next_header2(struct archive *, struct archive_entry *);
next_entry = libarchive.archive_read_next_header2
next_entry.argtypes = [c_void_p, c_void_p]
next_entry.restype = c_int
next_entry.errcheck = errcheck

"""
Read data associated with the header just read. Internally, this is a
convenience function that calls archive_read_data_block() and fills any gaps
with nulls so that callers see a single continuous stream of data.
"""
# ssize_t archive_read_data(struct archive *, void *buff, size_t len);
read_entry_data = libarchive.archive_read_data
read_entry_data.argtypes = [c_void_p, c_void_p, c_size_t]
read_entry_data.restype = c_ssize_t
read_entry_data.errcheck = errcheck

"""
Return the next available block of data for this entry. Unlike
archive_read_data(), the archive_read_data_block() function avoids copying
data and allows you to correctly handle sparse files, as supported by some
archive formats. The library guarantees that offsets will increase and that
blocks will not overlap. Note that the blocks returned from this function can
be much larger than the block size read from disk, due to compression and
internal buffer optimizations.
"""
# int archive_read_data_block(struct archive *, const void **buff, size_t *len, off_t *offset);
read_entry_data_block = libarchive.archive_read_data_block
read_entry_data_block.argtypes = [c_void_p, POINTER(c_void_p), POINTER(c_size_t), POINTER(c_longlong)]
read_entry_data_block.restype = c_int
read_entry_data_block.errcheck = errcheck

"""
Releases the struct archive_entry object.
The struct entry object must be freed when no longer needed.
"""
# void archive_entry_free(struct archive_entry *);
free_entry = libarchive.archive_entry_free
free_entry.argtypes = [c_void_p]
free_entry.restype = None

#
# Entry attributes: path, type, size, etc. are collected with these functions:
#

"""
The functions archive_entry_filetype() and archive_entry_set_filetype() get
respectively set the filetype. The file type is one of the following
constants:
AE_IFREG    Regular file
AE_IFLNK    Symbolic link
AE_IFSOCK   Socket
AE_IFCHR    Character device
AE_IFBLK    Block device
AE_IFDIR    Directory
AE_IFIFO    Named pipe (fifo)

Not all file types are supported by all platforms. The constants used by
stat(2) may have different numeric values from the corresponding constants
above.
"""
# struct archive_entry * archive_entry_filetype(struct archive_entry *);
# TODO: check for nulls
entry_type = libarchive.archive_entry_filetype
entry_type.argtypes = [c_void_p]
entry_type.restype = c_int

"""
This function retrieves the mtime field in an archive_entry. (modification
time).

The timestamps are truncated automatically depending on the archive format
(for archiving) or the filesystem capabilities (for restoring).
All timestamp fields are optional.
"""
# time_t archive_entry_mtime(struct archive_entry *);
entry_time = libarchive.archive_entry_mtime
entry_time.argtypes = [c_void_p]
entry_time.restype = c_int

"""
Path in the archive.

char *        Multibyte strings in the current locale.
wchar_t *     Wide character strings in the current locale.
"""
# const char * archive_entry_pathname(struct archive_entry *a);
# TODO: check for nulls
entry_path = libarchive.archive_entry_pathname
entry_path.argtypes = [c_void_p]
entry_path.restype = c_char_p

# const wchar_t * archive_entry_pathname_w(struct archive_entry *a);
# TODO: check for nulls?
entry_path_w = libarchive.archive_entry_pathname_w
entry_path_w.argtypes = [c_void_p]
entry_path_w.restype = c_wchar_p

# int64_t archive_entry_size(struct archive_entry *a);
entry_size = libarchive.archive_entry_size
entry_size.argtypes = [c_void_p]
entry_size.restype = c_longlong
entry_size.errcheck = errcheck

"""
Destination of the hardlink.
"""
# const char * archive_entry_hardlink(struct archive_entry *a);
hardlink_path = libarchive.archive_entry_hardlink
hardlink_path.argtypes = [c_void_p]
hardlink_path.restype = c_char_p

# const wchar_t * archive_entry_hardlink_w(struct archive_entry *a);
hardlink_path_w = libarchive.archive_entry_hardlink_w
hardlink_path_w.argtypes = [c_void_p]
hardlink_path_w.restype = c_wchar_p

"""
The number of references (hardlinks) can be obtained by calling
archive_entry_nlinks()
"""
# unsigned int archive_entry_nlink(struct archive_entry *a);
hardlink_count = libarchive.archive_entry_nlink
hardlink_count.argtypes = [c_void_p]
hardlink_count.restype = c_int

"""
The functions archive_entry_dev() and archive_entry_ino64() are used by
ManPageArchiveEntryLinkify3 to find hardlinks. The pair of device and inode is
supposed to identify hardlinked files.
"""
# int64_t archive_entry_ino64(struct archive_entry *a);
# dev_t archive_entry_dev(struct archive_entry *a);
# int archive_entry_dev_is_set(struct archive_entry *a);

"""
Destination of the symbolic link.
"""
# const char * archive_entry_symlink(struct archive_entry *);
symlink_path = libarchive.archive_entry_symlink
symlink_path.argtypes = [c_void_p]
symlink_path.restype = c_char_p
symlink_path.errcheck = errcheck_null

# const wchar_t * archive_entry_symlink_w(struct archive_entry *);
symlink_path_w = libarchive.archive_entry_symlink_w
symlink_path_w.argtypes = [c_void_p]
symlink_path_w.restype = c_wchar_p
symlink_path_w.errcheck = errcheck_null

#
# Utilities and error handling: not all are defined for now
#

"""
Returns a numeric error code (see errno(2)) indicating the reason for the most
recent error return. Note that this can not be reliably used to detect whether
an error has occurred. It should be used only after another libarchive
function has returned an error status.
"""
# int archive_errno(struct archive *);
errno = libarchive.archive_errno
errno.argtypes = [c_void_p]
errno.restype = c_int

"""
Returns a textual error message suitable for display. The error message here
is usually more specific than that obtained from passing the result of
archive_errno() to strerror(3).
"""
# const char * archive_error_string(struct archive *);
err_msg = libarchive.archive_error_string
err_msg.argtypes = [c_void_p]
err_msg.restype = c_char_p

"""
Returns a count of the number of files processed by this archive object. The
count is incremented by calls to ManPageArchiveWriteHeader3 or
ManPageArchiveReadNextHeader3.
"""
# int archive_file_count(struct archive *);

"""
Returns a numeric code identifying the indicated filter. See
archive_filter_count() for details of the numbering.
"""
# int archive_filter_code(struct archive *, int);

"""
Returns the number of filters in the current pipeline. For read archive
handles, these filters are added automatically by the automatic format
detection.
"""
# int archive_filter_count(struct archive *, int);

"""
Synonym for archive_filter_code(a,(0)).
"""
# int archive_compression(struct archive *);

"""
Returns a textual name identifying the indicated filter. See
archive_filter_count() for details of the numbering.
"""
# const char * archive_filter_name(struct archive *, int);

"""
Synonym for archive_filter_name(a,(0)).
"""
# const char * archive_compression_name(struct archive *);

"""
Returns a numeric code indicating the format of the current archive entry.
This value is set by a successful call to archive_read_next_header(). Note
that it is common for this value to change from entry to entry. For example, a
tar archive might have several entries that utilize GNU tar extensions and
several entries that do not. These entries will have different format codes.
"""
# int archive_format(struct archive *);

"""
A textual description of the format of the current entry.
"""
# const char * archive_format_name(struct archive *);
