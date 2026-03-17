# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

import gzip
from hashlib import md5
import json
import lzma
from typing import Any, Callable, Iterator
import os
import functools
import re
import sys
from contextlib import contextmanager
from lxml import etree  # nosec # We only write XML files

from .logging import LOGGER
from .version import __version__

REGEX_VERSION_POSTFIX = re.compile(r"(.+?)(?:\.post\d+)?\.dev.+$")
PRETTY_JSON_INDENT = 4
GZIP_SUFFIX = ".gz"
LZMA_SUFFIX = ".xz"


class LoopChecker:
    """Class for checking if a directory was already scanned."""

    def __init__(self) -> None:
        self._seen = set[tuple[int, int]]()

    def already_visited(self, path: str) -> bool:
        """Check if the path was already checked."""
        st = os.stat(path)
        key = (st.st_dev, st.st_ino)
        if key in self._seen:
            return True

        self._seen.add(key)
        return False


@functools.lru_cache(maxsize=1)
def is_fs_case_insensitive() -> bool:
    """Check if the file system is case insensitive."""
    cwd = os.getcwd()
    # Guessing if file system is case insensitive.
    # The working directory is not the root and accessible in upper and lower case
    # and pointing to same file.
    ret = (
        (cwd != os.path.sep)
        and os.path.exists(cwd.upper())
        and os.path.exists(cwd.lower())
        and os.path.samefile(cwd.upper(), cwd.lower())
    )
    LOGGER.debug("File system is case %s.", "insensitive" if ret else "sensitive")

    return ret


@functools.lru_cache(maxsize=None)
def fix_case_of_path(path: str) -> str:
    """Fix casing of filenames for cas insensitive file systems."""
    rest, cur = os.path.split(path)
    # e.g path = ".." happens if original path is like "../dir/subdir/file.cpp"
    if not rest:
        return cur
    if not cur:  # e.g path = "C:/"
        return rest.upper()  # Always use uppercase drive letter

    try:
        cur_lower = cur.lower()
        matched_filename = [f for f in os.listdir(rest) if f.lower() == cur_lower]
        if len(matched_filename) > 1:
            raise RuntimeError(
                "Seems that we have a case sensitive filesystem, can't fix file case"
            )

        if len(matched_filename) == 1:
            path = os.path.join(fix_case_of_path(rest), matched_filename[0])
    except FileNotFoundError:
        LOGGER.warning("Can not fix case of path because %s not found.", rest)

    return path.replace("\\", "/")


def get_version_for_report() -> str:
    """Get the printable version for the report."""
    version = __version__
    if match := REGEX_VERSION_POSTFIX.match(version):
        version = f"{match.group(1)}+main"
    return version


def search_file(
    predicate: Callable[[str], bool],
    path: str,
    exclude_directory: list[re.Pattern[str]],
) -> Iterator[str]:
    """
    Given a search path, recursively descend to find files that satisfy a
    predicate.
    """
    if path is None or path == ".":
        path = os.getcwd()
    elif not os.path.exists(path):
        raise IOError("Unknown directory '" + path + "'")

    loop_checker = LoopChecker()
    for root, dirs, files in os.walk(os.path.abspath(path), followlinks=True):
        # Check if we've already visited 'root' through the magic of symlinks
        if loop_checker.already_visited(root):
            dirs[:] = []
            continue

        dirs[:] = [
            d
            for d in sorted(dirs)
            if not any(exc.match(os.path.join(root, d)) for exc in exclude_directory)
        ]
        root = os.path.abspath(root)

        for name in sorted(files):
            if predicate(name):
                yield os.path.abspath(os.path.join(root, name))


def commonpath(files: list[str]) -> str:
    r"""Find the common prefix of all files.

    This differs from the standard library os.path.commonpath():
     - We first normalize all paths to a realpath.
     - We return a path with a trailing path separator.

    No common path exists under the following circumstances:
     - on Windows when the paths have different drives.
       E.g.: commonpath([r'C:\foo', r'D:\foo']) == ''
     - when the `files` are empty.

    Arguments:
        files (list): the input paths, may be relative or absolute.

    Returns: str
        The common prefix directory as a relative path.
        Always ends with a path separator.
        Returns the empty string if no common path exists.
    """
    if not files:
        return ""

    if len(files) == 1:
        prefix_path = str(os.path.dirname(os.path.realpath(files[0])))
    else:
        split_paths = [os.path.realpath(path).split(os.path.sep) for path in files]
        # We only have to compare the lexicographically minimum and maximum
        # paths to find the common prefix of all, e.g.:
        #   /a/b/c/d  <- min
        #   /a/b/d
        #   /a/c/a    <- max
        #
        # compare:
        # https://github.com/python/cpython/blob/3.6/Lib/posixpath.py#L487
        min_path = min(split_paths)
        max_path = max(split_paths)
        common = min_path  # assume that min_path is a prefix of max_path
        for i in range(min(len(min_path), len(max_path))):
            if min_path[i] != max_path[i]:
                common = min_path[:i]  # disproven, slice for actual prefix
                break
        prefix_path = os.path.sep.join(common)

    LOGGER.debug("Common prefix path is %r.", prefix_path)

    # make the path relative and add a trailing slash
    if prefix_path:
        prefix_path = os.path.join(
            os.path.relpath(prefix_path, os.path.realpath(os.getcwd())), ""
        )
        LOGGER.debug("Common relative prefix path is %r.", prefix_path)
    return prefix_path


@contextmanager
def open_text_for_writing(
    filename: str | None, default_filename: str | None = None, **kwargs: Any
) -> Iterator[Any]:
    """Context manager to open and close a file for text writing.

    Stdout is used if `filename` is None or '-'.
    """
    if filename is not None and filename.endswith(os.sep):
        if default_filename is None:
            raise AssertionError(
                "If filename is a directory a default filename is mandatory."
            )
        filename += default_filename

    if filename is not None and filename != "-":
        if filename.endswith(GZIP_SUFFIX):
            with gzip.open(filename, "wt", **kwargs) as fh_out:
                yield fh_out
        elif filename.endswith(LZMA_SUFFIX):
            with lzma.open(filename, "wt", **kwargs) as fh_out:
                yield fh_out
        else:
            with open(filename, "wt", **kwargs) as fh_out:  # pylint: disable=unspecified-encoding
                yield fh_out
    else:
        encoding = kwargs.get("encoding", "utf-8").lower()
        old_encoding = sys.stdout.encoding
        try:
            if old_encoding != encoding:
                sys.stdout.reconfigure(encoding=encoding)  # type: ignore[union-attr]
            yield sys.stdout
        finally:
            if old_encoding != encoding:
                sys.stdout.reconfigure(encoding=old_encoding)  # type: ignore[union-attr]


@contextmanager
def open_binary_for_writing(
    filename: str | None,
    default_filename: str,
    **kwargs: Any,
) -> Iterator[Any]:
    """Context manager to open and close a file for binary writing.

    Stdout is used if `filename` is None or '-'.
    """
    if filename is not None and filename.endswith(os.sep):
        filename += default_filename

    if filename is not None and filename != "-":
        if filename.endswith(GZIP_SUFFIX):
            with gzip.open(filename, "wb", **kwargs) as fh_out:
                yield fh_out
        elif filename.endswith(LZMA_SUFFIX):
            with lzma.open(filename, "wb", **kwargs) as fh_out:
                yield fh_out
        else:
            # files in write binary mode for UTF-8
            with open(filename, "wb", **kwargs) as fh_out:
                yield fh_out
    else:
        yield sys.stdout.buffer


def write_json_output(
    json_dict: dict[str, Any],
    *,
    pretty: bool,
    filename: str | None,
    default_filename: str,
    **kwargs: Any,
) -> None:
    """Helper function to output JSON dictionary to a file/STDOUT."""
    with open_text_for_writing(filename, default_filename, **kwargs) as fh:
        json.dump(json_dict, fh, indent=PRETTY_JSON_INDENT if pretty else None)


def write_xml_output(
    root: Any,
    *,
    doctype: str | None = None,
    pretty: bool,
    filename: str | None,
    default_filename: str,
    **kwargs: Any,
) -> None:
    """Helper function to output XML format dictionary to a file/STDOUT"""
    with open_binary_for_writing(filename, default_filename, **kwargs) as fh:
        fh.write(
            etree.tostring(
                root,
                pretty_print=pretty,
                encoding="utf-8",
                xml_declaration=True,
                doctype=doctype,  # type: ignore [arg-type]
            )
        )


@contextmanager
def chdir(directory: str) -> Iterator[None]:
    """Context for doing something in a locked directory."""
    current_dir = os.getcwd()
    os.chdir(directory)
    try:
        yield
    finally:
        os.chdir(current_dir)


def force_unix_separator(path: str) -> str:
    """Get the filename with / independent from OS."""
    return path.replace("\\", "/")


def get_md5_hexdigest(data: bytes) -> str:
    """Get the MD5 digest of the given bytes."""
    return md5(data, usedforsecurity=False).hexdigest()  # nosec # Not used for security


def read_source_file(
    source_encoding: str, filename: str, max_line_number: int
) -> list[str]:
    """Read in the source file and fill up lines if needed."""
    source_lines: list[bytes] = []
    try:
        with open(filename, "rb") as fh_in:
            source_lines = fh_in.read().splitlines()
        lines = len(source_lines)
        if lines < max_line_number:
            LOGGER.warning(
                "File %s has %d line(s) but coverage data has %d line(s).",
                filename,
                lines,
                max_line_number,
            )
            # GCOV itself adds the /*EOF*/ in the text report if there is no data and we used the same.
            source_lines += [b"/*EOF*/"] * (max_line_number - lines)
    except OSError as e:
        if filename.endswith("<stdin>"):
            message = (
                f"Got unreadable source file '{filename}', replacing with empty lines."
            )
            LOGGER.info(message)
        else:
            # The exception contains the source file name,
            # e.g. [Errno 2] No such file or directory: 'xy.txt'
            message = f"Can't read file, using empty lines: {e}"
            LOGGER.warning(message)
            # If we can't read the file we use as first line the error
            # and use empty lines for the rest of the lines.
        source_lines = [b""] * max_line_number
        source_lines[0] = f"/* {message} */".encode()

    encoded_source_lines = [
        line.decode(source_encoding, errors="replace") for line in source_lines
    ]

    return encoded_source_lines
