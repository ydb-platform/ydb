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

import functools
import platform
import re
import os

from .logging import LOGGER
from .utils import force_unix_separator, is_fs_case_insensitive


class Filter:
    """Base class for a filename filter."""

    def __init__(self, pattern: str) -> None:
        flags = re.IGNORECASE if is_fs_case_insensitive() else 0
        self.pattern = re.compile(pattern, flags)

    def match(self, path: str) -> bool:
        """Return True if the given path (always with /) matches the regular expression."""
        os_independent_path = force_unix_separator(path)
        if self.pattern.match(os_independent_path):
            LOGGER.debug("  Filter %s matched for path %s.", self, os_independent_path)
            return True
        return False

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.pattern.pattern})"


class AbsoluteFilter(Filter):
    """Class for a filename filter which matches against the real path of a file."""

    def match(self, path: str) -> bool:
        """Return True if the given path with all symlinks resolved matches the filter."""
        path = os.path.realpath(path)
        return super().match(path)


class RelativeFilter(Filter):
    """Class for a filename filter which matches against the relative paths of a file."""

    def __init__(self, root: str, pattern: str) -> None:
        super().__init__(pattern)
        self.root = os.path.realpath(root)

    def match(self, path: str) -> bool:
        """Return True if the given path with all symlinks resolved matches the filter."""
        path = os.path.realpath(path)

        # On Windows, a relative path can never cross drive boundaries.
        # If so, the relative filter cannot match.
        if platform.system() == "Windows":
            path_drive, _ = os.path.splitdrive(path)
            root_drive, _ = os.path.splitdrive(self.root)
            if path_drive != root_drive:  # pragma: no cover
                return False

        relpath = os.path.relpath(path, self.root)
        return super().match(relpath)

    def __str__(self) -> str:
        return f"RelativeFilter({self.pattern.pattern} root={self.root})"


class AlwaysMatchFilter(Filter):
    """Class for a filter which matches for all files."""

    def __init__(self) -> None:
        super().__init__("")

    def match(self, path: str) -> bool:
        """Return always True."""
        LOGGER.debug("  Filter %s matched.", self)
        return True


class DirectoryPrefixFilter(Filter):
    """Class for a filename filter which matches for all files in a directory."""

    def __init__(self, directory: str) -> None:
        os_independent_path = force_unix_separator(directory)
        pattern = re.escape(f"{os_independent_path}/")
        super().__init__(pattern)

    def match(self, path: str) -> bool:
        """Return True if the given path matches the filter."""
        path = os.path.normpath(path)
        return super().match(path)


@functools.cache
def __is_file_matching_any(filename: str, filters: tuple[Filter, ...]) -> bool:
    """Check if filename matches any of the given filters.

    The filename is tested against all filters in the list.
    The first matching filter causes a True result.

    filename (str): the file path to match
    filters (list of Filter): the filters to test against

    returns:
        True when filename is matching any filter.
    """

    if any(f.match(filename) for f in filters):
        return True

    LOGGER.debug("  No filter matched.")
    return False


def is_file_excluded(
    filter_type: str,
    filename: str,
    include_filter: tuple[Filter, ...],
    exclude_filter: tuple[Filter, ...],
) -> bool:
    """Apply inclusion/exclusion filters to filename.

    The include_filter are tested against
    the given (relative) filename.
    The exclude_filter are tested against
    the stripped, given (relative), and absolute filenames.

    filename (str): the absolute file path to match
    include_filter (list of FilterOption): ANY of these filters must match
    exclude_filter (list of FilterOption): NONE of these filters must match

    returns:
        True when filename is not matching a include filter or matches an exclude filter.
    """

    LOGGER.debug("Check if %s is included (%s)...", filename, filter_type)
    is_included = __is_file_matching_any(filename, include_filter)
    if is_included and exclude_filter:
        LOGGER.debug("Check for exclusion...")
        is_included = not __is_file_matching_any(filename, exclude_filter)

    if is_included:
        LOGGER.debug("--> File is included.")
        return False

    LOGGER.debug("--> File is excluded.")
    return True
