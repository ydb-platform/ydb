#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND Python-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import errno
import os
import ntpath
import posixpath
import shutil
import stat
import sys
import tempfile

from os import fsdecode

try:
    from scancode_config import scancode_temp_dir as _base_temp_dir
except ImportError:
    _base_temp_dir = None

from commoncode import filetype
from commoncode.filetype import is_rwx

# this exception is not available on posix
try:
    WindowsError  # NOQA
except NameError:

    class WindowsError(Exception):
        pass

import logging
logger = logging.getLogger(__name__)

TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))

"""
File, paths and directory utility functions.
"""

#
# DIRECTORIES
#


def create_dir(location):
    """
    Create directory and all sub-directories recursively at location ensuring these
    are readable and writeable.
    Raise Exceptions if it fails to create the directory.
    """

    if os.path.exists(location):
        if not os.path.isdir(location):
            err = ('Cannot create directory: existing file '
                   'in the way ''%(location)s.')
            raise OSError(err % locals())
    else:
        # may fail on win if the path is too long
        # FIXME: consider using UNC ?\\ paths

        try:
            os.makedirs(location)
            chmod(location, RW, recurse=False)

        # avoid multi-process TOCTOU conditions when creating dirs
        # the directory may have been created since the exist check
        except WindowsError as e:
            # [Error 183] Cannot create a file when that file already exists
            if e and e.winerror == 183:
                if not os.path.isdir(location):
                    raise
            else:
                raise
        except (IOError, OSError) as o:
            if o.errno == errno.EEXIST:
                if not os.path.isdir(location):
                    raise
            else:
                raise


def get_temp_dir(base_dir=_base_temp_dir, prefix=''):
    """
    Return the path to a new existing unique temporary directory, created under
    the `base_dir` base directory using the `prefix` prefix.
    If `base_dir` is not provided, use the 'SCANCODE_TMP' env var or the system
    temp directory.

    WARNING: do not change this code without changing scancode_config.py too
    """

    has_base = bool(base_dir)
    if not has_base:
        base_dir = os.getenv('SCANCODE_TMP')
        if not base_dir:
            base_dir = tempfile.gettempdir()

    if not os.path.exists(base_dir):
        create_dir(base_dir)

    if not has_base:
        prefix = 'scancode-tk-'

    return tempfile.mkdtemp(prefix=prefix, dir=base_dir)

#
# PATHS AND NAMES MANIPULATIONS
#

# TODO: move these functions to paths.py


def prepare_path(pth):
    """
    Return the `pth` path string either as encoded bytes if on Linux and using
    Python 2 or as a unicode/text otherwise.
    """
    if not isinstance(pth, str):
        return fsdecode(pth)
    return pth


def is_posixpath(location):
    """
    Return True if the `location` path is likely a POSIX-like path using POSIX path
    separators (slash or "/")or has no path separator.

    Return False if the `location` path is likely a Windows-like path using backslash
    as path separators (e.g. "\").
    """
    has_slashes = '/' in location
    has_backslashes = '\\' in location
    # windows paths with drive
    if location:
        drive, _ = ntpath.splitdrive(location)
        if drive:
            return False

    # a path is always POSIX unless it contains ONLY backslahes
    # which is a rough approximation (it could still be posix)
    is_posix = True
    if has_backslashes and not has_slashes:
        is_posix = False
    return is_posix


def as_posixpath(location):
    """
    Return a POSIX-like path using POSIX path separators (slash or "/") for a
    `location` path. This converts Windows paths to look like POSIX paths: Python
    accepts gracefully POSIX paths on Windows.
    """
    location = prepare_path(location)
    return location.replace('\\', '/')


def as_winpath(location):
    """
    Return a Windows-like path using Windows path separators (backslash or "\") for a
    `location` path.
    """
    location = prepare_path(location)
    return location.replace('/', '\\')


def split_parent_resource(path, force_posix=False):
    """
    Return a tuple of (parent directory path, resource name).
    """
    use_posix = force_posix or is_posixpath(path)
    splitter = use_posix and posixpath or ntpath
    path_no_trailing_speps = path.rstrip('\\/')
    return splitter.split(path_no_trailing_speps)


def resource_name(path, force_posix=False):
    """
    Return the resource name (file name or directory name) from `path` which
    is the last path segment.
    """
    _left, right = split_parent_resource(path, force_posix)
    return right or ''


def file_name(path, force_posix=False):
    """
    Return the file name (or directory name) of a path.
    """
    return resource_name(path, force_posix)


def parent_directory(path, force_posix=False):
    """
    Return the parent directory path of a file or directory `path`.
    """
    left, _right = split_parent_resource(path, force_posix)
    use_posix = force_posix or is_posixpath(path)
    sep = '/' if use_posix else '\\'
    trail = sep if left != sep else ''
    return left + trail


def file_base_name(path, force_posix=False):
    """
    Return the file base name for a path. The base name is the base name of
    the file minus the extension. For a directory return an empty string.
    """
    return splitext(path, force_posix)[0]


def file_extension(path, force_posix=False):
    """
    Return the file extension for a path.
    """
    return splitext(path, force_posix)[1]


def splitext_name(file_name, is_file=True):
    """
    Return a tuple of Unicode strings (basename, extension) for a file name. The
    basename is the file name minus its extension. Return an empty extension
    string for a directory. Not the same as os.path.splitext_name.
    """

    if not file_name:
        return '', ''
    file_name = fsdecode(file_name)

    if not is_file:
        return file_name, ''

    if file_name.startswith('.') and '.' not in file_name[1:]:
        # .dot files base name is the full name and they do not have an extension
        return file_name, ''

    base_name, extension = posixpath.splitext(file_name)
    # handle composed extensions of tar.gz, bz, zx,etc
    if base_name.endswith('.tar'):
        base_name, extension2 = posixpath.splitext(base_name)
        extension = extension2 + extension
    return base_name, extension


# TODO: FIXME: this is badly broken!!!!
def splitext(path, force_posix=False):
    """
    Return a tuple of strings (basename, extension) for a path. The basename is
    the file name minus its extension. Return an empty extension string for a
    directory.
    """
    base_name = ''
    extension = ''
    if not path:
        return base_name, extension

    is_dir = path.endswith(('\\', '/',))
    path = as_posixpath(path).strip('/')
    name = resource_name(path, force_posix)
    if is_dir:
        # directories never have an extension
        base_name = name
        extension = ''
    elif name.startswith('.') and '.' not in name[1:]:
        # .dot files base name is the full name and they do not have an extension
        base_name = name
        extension = ''
    else:
        base_name, extension = posixpath.splitext(name)
        # handle composed extensions of tar.gz, tar.bz2, zx,etc
        if base_name.endswith('.tar'):
            base_name, extension2 = posixpath.splitext(base_name)
            extension = extension2 + extension
    return base_name, extension

#
# DIRECTORY AND FILES WALKING/ITERATION
#


def ignore_nothing(_):
    return False


def walk(location, ignored=None, follow_symlinks=False):
    """
    Walk location returning the same tuples as os.walk but with a different
    behavior:
     - always walk top-down, breadth-first.
     - ignore and do not follow symlinks unless `follow_symlinks` is True,
     - always ignore special files (FIFOs, etc.)
     - optionally ignore files and directories by invoking the `ignored`
       callable on files and directories returning True if it should be ignored.
     - location is a directory or a file: for a file, the file is returned.

    If `follow_symlinks` is True, then symlinks will not be ignored and be
    collected like regular files and directories
    """
    # TODO: consider using the new "scandir" module for some speed-up.

    is_ignored = ignored(location) if ignored else False
    if is_ignored:
        if TRACE:
            logger_debug('walk: ignored:', location, is_ignored)
        return

    if filetype.is_file(location, follow_symlinks=follow_symlinks) :
        yield parent_directory(location), [], [file_name(location)]

    elif filetype.is_dir(location, follow_symlinks=follow_symlinks):
        dirs = []
        files = []
        # TODO: consider using scandir
        for name in os.listdir(location):
            loc = os.path.join(location, name)
            if filetype.is_special(loc) or (ignored and ignored(loc)):
                if (follow_symlinks
                        and filetype.is_link(loc)
                        and not filetype.is_broken_link(location)):
                    pass
                else:
                    if TRACE:
                        ign = ignored and ignored(loc)
                        logger_debug('walk: ignored:', loc, ign)
                    continue
            # special files and symlinks are always ignored
            if filetype.is_dir(loc, follow_symlinks=follow_symlinks):
                dirs.append(name)
            elif filetype.is_file(loc, follow_symlinks=follow_symlinks):
                files.append(name)
        yield location, dirs, files

        for dr in dirs:
            for tripple in walk(os.path.join(location, dr), ignored, follow_symlinks=follow_symlinks):
                yield tripple


def resource_iter(location, ignored=ignore_nothing, with_dirs=True, follow_symlinks=False):
    """
    Return an iterable of paths at `location` recursively.

    :param location: a file or a directory.
    :param ignored: a callable accepting a location argument and returning True
                    if the location should be ignored.
    :return: an iterable of file and directory locations.
    """
    for top, dirs, files in walk(location, ignored, follow_symlinks=follow_symlinks):
        if with_dirs:
            for d in dirs:
                yield os.path.join(top, d)
        for f in files:
            yield os.path.join(top, f)
#
# COPY
#


def copytree(src, dst):
    """
    Copy recursively the `src` directory to the `dst` directory. If `dst` is an
    existing directory, files in `dst` may be overwritten during the copy.
    Preserve timestamps.
    Ignores:
     -`src` permissions: `dst` files are created with the default permissions.
     - all special files such as FIFO or character devices and symlinks.

    Raise an shutil.Error with a list of reasons.

    This function is similar to and derived from the Python shutil.copytree
    function. See fileutils.py.ABOUT for details.
    """
    if not filetype.is_readable(src):
        chmod(src, R, recurse=False)

    names = os.listdir(src)

    if not os.path.exists(dst):
        os.makedirs(dst)

    errors = []
    errors.extend(copytime(src, dst))

    for name in names:
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)

        # skip anything that is not a regular file, dir or link
        if not filetype.is_regular(srcname):
            continue

        if not filetype.is_readable(srcname):
            chmod(srcname, R, recurse=False)
        try:
            if os.path.isdir(srcname):
                copytree(srcname, dstname)
            elif filetype.is_file(srcname):
                copyfile(srcname, dstname)
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error as err:
            errors.extend(err.args[0])
        except EnvironmentError as why:
            errors.append((srcname, dstname, str(why)))

    if errors:
        raise shutil.Error(errors)


def copyfile(src, dst):
    """
    Copy src file to dst file preserving timestamps.
    Ignore permissions and special files.

    Similar to and derived from Python shutil module. See fileutils.py.ABOUT
    for details.
    """
    if not filetype.is_regular(src):
        return
    if not filetype.is_readable(src):
        chmod(src, R, recurse=False)
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    shutil.copyfile(src, dst)
    copytime(src, dst)


def copytime(src, dst):
    """
    Copy timestamps from `src` to `dst`.

    Similar to and derived from Python shutil module. See fileutils.py.ABOUT
    for details.
    """
    errors = []
    st = os.stat(src)
    if hasattr(os, 'utime'):
        try:
            os.utime(dst, (st.st_atime, st.st_mtime))
        except OSError as why:
            if WindowsError is not None and isinstance(why, WindowsError):
                # File access times cannot be copied on Windows
                pass
            else:
                errors.append((src, dst, str(why)))
    return errors

#
# PERMISSIONS
#


# modes: read, write, executable
R = stat.S_IRUSR
RW = stat.S_IRUSR | stat.S_IWUSR
RX = stat.S_IRUSR | stat.S_IXUSR
RWX = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR


# FIXME: This was an expensive operation that used to recurse of the parent directory
def chmod(location, flags, recurse=False):
    """
    Update permissions for `location` with with `flags`. `flags` is one of R,
    RW, RX or RWX with the same semantics as in the chmod command. Update is
    done recursively if `recurse`.
    """
    if not location or not os.path.exists(location):
        return
    location = os.path.abspath(location)

    new_flags = flags
    if filetype.is_dir(location):
        # POSIX dirs need to be executable to be readable,
        # and to be writable so we can change perms of files inside
        new_flags = RWX

    # FIXME: do we really need to change the parent directory perms?
    # FIXME: may just check them instead?
    parent = os.path.dirname(location)
    current_stat = stat.S_IMODE(os.stat(parent).st_mode)
    if not is_rwx(parent):
        os.chmod(parent, current_stat | RWX)

    if filetype.is_regular(location):
        current_stat = stat.S_IMODE(os.stat(location).st_mode)
        os.chmod(location, current_stat | new_flags)

    if recurse:
        chmod_tree(location, flags)


def chmod_tree(location, flags):
    """
    Update permissions recursively in a directory tree `location`.
    """
    if filetype.is_dir(location):
        for top, dirs, files in walk(location):
            for d in dirs:
                chmod(os.path.join(top, d), flags, recurse=False)
            for f in files:
                chmod(os.path.join(top, f), flags, recurse=False)

#
# DELETION
#


def _rm_handler(function, path, excinfo):  # NOQA
    """
    shutil.rmtree handler invoked on error when deleting a directory tree.
    This retries deleting once before giving up.
    """
    if TRACE:
        logger_debug('_rm_handler:', 'path:', path, 'excinfo:', excinfo)
    if function in (os.rmdir, os.listdir):
        try:
            chmod(path, RW, recurse=True)
            shutil.rmtree(path, True)
        except Exception:
            pass

        if os.path.exists(path):
            logger.warning('Failed to delete directory %s', path)

    elif function == os.remove:
        try:
            delete(path, _err_handler=None)
        except:
            pass

        if os.path.exists(path):
            logger.warning('Failed to delete file %s', path)


def delete(location, _err_handler=_rm_handler):
    """
    Delete a directory or file at `location` recursively. Similar to "rm -rf"
    in a shell or a combo of os.remove and shutil.rmtree.
    This function is design to never fails: instead it logs warnings if the
    file or directory was not deleted correctly.
    """
    if not location:
        return

    if os.path.exists(location) or filetype.is_broken_link(location):
        if filetype.is_dir(location):
            shutil.rmtree(location, False, _rm_handler)
        else:
            os.remove(location)
