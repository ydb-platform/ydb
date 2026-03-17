"""This submodule contains file system utilities for Prance."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2019 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


# Re-define an error for backwards compatibility
FileNotFoundError = FileNotFoundError  # pragma: no cover


# The following constant and function are taken from
# https://stackoverflow.com/questions/9532499/check-whether-a-path-is-valid-in-python-without-creating-a-file-at-the-paths-ta

# Sadly, Python fails to provide the following magic number for us.
_ERROR_INVALID_NAME = 123
"""
Windows-specific error code indicating an invalid pathname.

See Also
----------
https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382%28v=vs.85%29.aspx
    Official listing of all such codes.
"""


# Following Microsoft documentation, set the default read size for detecting
# a file encoding to a multiple of 4k that seems to work well on various OSes
# and volume sizes.
# https://support.microsoft.com/en-us/help/140365/default-cluster-size-for-ntfs-fat-and-exfat
_READ_CHUNK_SIZE = 64 * 1024
"""
Default read size for detecting file encoding.
"""


def is_pathname_valid(pathname):
    """
    Test whether a path name is valid.

    :return: True if the passed pathname is valid on the current OS, False
        otherwise.
    :rtype: bool
    """
    import errno
    import os

    # If this pathname is either not a string or is but is empty, this pathname
    # is invalid.
    try:
        if not isinstance(pathname, str) or not pathname:
            return False

        # Strip this pathname's Windows-specific drive specifier (e.g., `C:\`)
        # if any. Since Windows prohibits path components from containing `:`
        # characters, failing to strip this `:`-suffixed prefix would
        # erroneously invalidate all valid absolute Windows pathnames.
        _, pathname = os.path.splitdrive(pathname)

        # Directory guaranteed to exist. If the current OS is Windows, this is
        # the drive to which Windows was installed (e.g., the "%SYSTEMDRIVE%"
        # environment variable); else, the typical root directory.
        # The %systemdrive% (typically c:) is the partition with
        # the %systemroot% (typically Windows) directory.
        import sys

        root_dirname = (
            os.environ.get("SYSTEMDRIVE", "C:")
            if sys.platform == "win32"
            else os.path.sep
        )
        assert os.path.isdir(root_dirname)  # ...Murphy and her ironclad Law

        # Append a path separator to this directory if needed.
        root_dirname = root_dirname.rstrip(os.path.sep) + os.path.sep

        # Test whether each path component split from this pathname is valid or
        # not, ignoring non-existent and non-readable path components.
        for pathname_part in pathname.split(os.path.sep):
            try:
                os.lstat(root_dirname + pathname_part)
            except OSError as exc:
                # If an OS-specific exception is raised, its error code
                # indicates whether this pathname is valid or not. Unless this
                # is the case, this exception implies an ignorable kernel or
                # filesystem complaint (e.g., path not found or inaccessible).
                #
                # Only the following exceptions indicate invalid pathnames:
                #
                # * Instances of the Windows-specific "WindowsError" class
                #   defining the "winerror" attribute whose value is
                #   "_ERROR_INVALID_NAME". Under Windows, "winerror" is more
                #   fine-grained and hence useful than the generic "errno"
                #   attribute. When a too-long pathname is passed, for example,
                #   "errno" is "ENOENT" (i.e., no such file or directory) rather
                #   than "ENAMETOOLONG" (i.e., file name too long).
                # * Instances of the cross-platform "OSError" class defining the
                #   generic "errno" attribute whose value is either:
                #   * Under most POSIX-compatible OSes, "ENAMETOOLONG".
                #   * Under some edge-case OSes (e.g., SunOS, *BSD), "ERANGE".
                if hasattr(exc, "winerror"):  # pragma: nocover
                    if exc.winerror == _ERROR_INVALID_NAME:
                        return False
                elif exc.errno in {errno.ENAMETOOLONG, errno.ERANGE}:
                    return False
    # If a "TypeError" exception was raised, it almost certainly has the
    # error message "embedded NUL character" indicating an invalid pathname.
    except TypeError:  # pragma: nocover
        return False
    # Null-bytes may also cause this, and they are invalid.
    except ValueError:
        return False
    # If no exception was raised, all path components and hence this
    # pathname itself are valid. (Praise be to the curmudgeonly python.)
    else:
        return True
    # If any other exception was raised, this is an unrelated fatal issue
    # (e.g., a bug). Permit this exception to unwind the call stack.
    #
    # Did we mention this should be shipped with Python already?


def from_posix(fname):
    """
    Convert a path from posix-like, to the platform format.

    :param str fname: The filename in posix-like format.
    :return: The filename in the format of the platform.
    :rtype: str
    """
    import sys

    if sys.platform == "win32":  # pragma: nocover
        if fname[0] == "/":
            fname = fname[1:]
        fname = fname.replace("/", "\\")
    return fname


def to_posix(fname):
    """
    Convert a path to posix-like format.

    :param str fname: The filename to convert to posix format.
    :return: The filename in posix-like format.
    :rtype: str
    """
    import sys

    if sys.platform == "win32":  # pragma: nocover
        import os.path

        if os.path.isabs(fname):
            fname = "/" + fname
        fname = fname.replace("\\", "/")
    return fname


def abspath(filename, relative_to=None):
    """
    Return the absolute path of a file relative to a reference file.

    If no reference file is given, this function works identical to
    `canonical_filename`.

    :param str filename: The filename to make absolute.
    :param str relative_to: [optional] the reference file name.
    :return: The absolute path
    :rtype: str
    """
    # Create filename relative to the reference, if it exists.
    import os.path

    fname = from_posix(filename)
    if relative_to and not os.path.isabs(fname):
        relative_to = from_posix(relative_to)
        if os.path.isdir(relative_to):
            fname = os.path.join(relative_to, fname)
        else:
            fname = os.path.join(os.path.dirname(relative_to), fname)

    # Make the result canonical
    fname = canonical_filename(fname)
    return to_posix(fname)


def canonical_filename(filename):
    """
    Return the canonical version of a file name.

    The canonical version is defined as the absolute path, and all file system
    links dereferenced.

    :param str filename: The filename to make canonical.
    :return: The canonical filename.
    :rtype: str
    """
    import os.path

    path = from_posix(filename)
    while True:
        path = os.path.abspath(path)
        try:
            p = os.path.dirname(path)
            # os.readlink doesn't exist in windows python2.7
            try:
                deref_path = os.readlink(path)
            except AttributeError:  # pragma: no cover
                return path
            path = os.path.join(p, deref_path)
        except OSError:
            return path


def detect_encoding(filename, default_to_utf8=True, **kwargs):
    """
    Detect the named file's character encoding.

    If the first parts of the file appear to be ASCII, this function returns
    'UTF-8', as that's a safe superset of ASCII. This can be switched off by
    changing the `default_to_utf8` parameter.

    :param str filename: The name of the file to detect the encoding of.
    :param bool default_to_utf8: Defaults to True. Set to False to disable
        treating ASCII files as UTF-8.
    :param bool read_all: Keyword argument; if True, reads the entire file
        for encoding detection.
    :return: The file encoding.
    :rtype: str
    """
    # Read some of the file
    import os.path

    filename = from_posix(filename)
    file_len = os.path.getsize(filename)
    read_len = min(_READ_CHUNK_SIZE, file_len)

    # ... unless we're supposed to!
    if kwargs.get("read_all", False):
        read_len = file_len

    # Read the first read_len bytes raw, so we can detect the encoding
    with open(filename, "rb") as raw_handle:
        raw = raw_handle.read(read_len)

    # Detect the encoding the file specifies, if any.
    import codecs

    if raw.startswith(codecs.BOM_UTF8):
        encoding = "utf-8-sig"
    else:
        # Detect encoding using the best detector available
        try:
            # First try ICU. ICU will report ASCII in the first 32 Bytes as
            # ISO-8859-1, which isn't exactly wrong, but maybe optimistic.
            import icu

            encoding = icu.CharsetDetector(raw).detect().getName().lower()
        except ImportError:  # pragma: nocover
            # If that doesn't work, try chardet - it's not got native components,
            # which is a bonus in some environments, but it's not as precise.
            import chardet

            encoding = chardet.detect(raw)["encoding"].lower()

            # Chardet is more brutal in that it reports ASCII if none of the first
            # Bytes contain high bits. To emulate ICU, we just bump up the detected
            # encoding.
            if encoding == "ascii":
                encoding = "iso-8859-1"

        # Both chardet and ICU may detect ISO-8859-x, which may not be possible
        # to decode as UTF-8. So whatever they report, we'll try decoding as
        # UTF-8 before reporting it.
        if default_to_utf8 and encoding in ("ascii", "iso-8859-1", "windows-1252"):
            # Try decoding as utf-8
            try:
                raw.decode("utf-8")
                # If this worked... well there's no guarantee it's utf-8, to be
                # honest.
                encoding = "utf-8"
            except UnicodeDecodeError:
                # Decoding as utf-8 failed, so we can't default to it.
                pass

    return encoding


def read_file(filename, encoding=None):
    """
    Read and decode a file, taking BOMs into account.

    :param str filename: The name of the file to read.
    :param str encoding: The encoding to use. If not given, detect_encoding is
        used to determine the encoding.
    :return: The file contents.
    :rtype: unicode string
    """
    filename = from_posix(filename)
    if not encoding:
        # Detect encoding
        encoding = detect_encoding(filename)

    # Finally, read the file in the detected encoding
    with open(filename, encoding=encoding) as handle:
        return handle.read()


def write_file(filename, contents, encoding=None):
    """
    Write a file with the given encoding.

    The default encoding is 'utf-8'. It's recommended not to change that for
    JSON or YAML output.

    :param str filename: The name of the file to read.
    :param str contents: The file contents to write.
    :param str encoding: The encoding to use. If not given, detect_encoding is
        used to determine the encoding.
    """
    if not encoding:
        encoding = "utf-8"

    fname = from_posix(filename)
    with open(fname, mode="w", encoding=encoding) as handle:
        handle.write(contents)
