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

from __future__ import annotations
import os
import re
from typing import Any, ItemsView, Iterable, Iterator, Literal, ValuesView

from ..filter import is_file_excluded

from ..logging import LOGGER
from ..options import Options
from ..utils import commonpath, force_unix_separator

from .coverage import FileCoverage
from .coverage_dict import CoverageDict
from .merging import MergeOptions
from .stats import CoverageStat, DecisionCoverageStat, SummarizedStats


class ContainerBase:
    """Base class for coverage containers"""

    def sort_coverage(
        self,
        sort_key: Literal["filename", "uncovered-number", "uncovered-percent"],
        sort_reverse: bool,
        by_metric: Literal["line", "branch", "decision"],
        filename_uses_relative_pathname: bool = False,
    ) -> list[str]:
        """Sort a coverage dict.

        covdata (dict): the coverage dictionary
        sort_key ("filename", "uncovered-number", "uncovered-percent"): the values to sort by
        sort_reverse (bool): reverse order if True
        by_metric ("line", "branch", "decision"): select the metric to sort
        filename_uses_relative_pathname (bool): for html, we break down a pathname to the
            relative path, but not for other formats.

        returns: the sorted keys
        """

        basedir = commonpath(list(self.data.keys()))

        def key_filename(key: str) -> list[int | str]:
            def convert_to_int_if_possible(text: str) -> int | str:
                return int(text) if text.isdigit() else text

            key = (
                force_unix_separator(
                    os.path.relpath(os.path.realpath(key), os.path.realpath(basedir))
                )
                if filename_uses_relative_pathname
                else key
            ).casefold()

            return [
                convert_to_int_if_possible(part) for part in re.split(r"([0-9]+)", key)
            ]

        def coverage_stat(key: str) -> CoverageStat:
            cov: FileCoverage | CoverageContainerDirectory = self.data[key]
            if by_metric == "branch":
                return cov.branch_coverage()
            if by_metric == "decision":
                return cov.decision_coverage().to_coverage_stat
            return cov.line_coverage()

        def key_num_uncovered(key: str) -> int:
            stat = coverage_stat(key)
            uncovered = stat.total - stat.covered
            return uncovered

        def key_percent_uncovered(key: str) -> float:
            stat = coverage_stat(key)
            covered = stat.covered
            total = stat.total

            # No branches are always put directly after (or before when reversed)
            # files with 100% coverage (by assigning such files 110% coverage)
            return covered / total if total > 0 else 1.1

        if sort_key == "uncovered-number":
            # First sort filename alphabetical and then by the requested key
            return sorted(
                sorted(self.data, key=key_filename),
                key=key_num_uncovered,
                reverse=sort_reverse,
            )
        if sort_key == "uncovered-percent":
            # First sort filename alphabetical and then by the requested key
            return sorted(
                sorted(self.data, key=key_filename),
                key=key_percent_uncovered,
                reverse=sort_reverse,
            )

        # By default, we sort by filename alphabetically
        return sorted(self.data, key=key_filename, reverse=sort_reverse)


class CoverageContainer(ContainerBase):
    """Coverage container holding all the coverage data."""

    def __init__(self) -> None:
        self.data = CoverageDict[str, FileCoverage]()
        self.directories = list[CoverageContainerDirectory]()

    def __getitem__(self, key: str) -> FileCoverage:
        return self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def values(self) -> ValuesView[FileCoverage]:
        """Get the file coverage data objects."""
        return self.data.values()

    def items(self) -> ItemsView[str, FileCoverage]:
        """Get the file coverage data items."""
        return self.data.items()

    def serialize(self, options: Options) -> list[dict[str, Any]]:
        """Serialize the object."""
        return [value.serialize(options) for _, value in sorted(self.items())]

    @classmethod
    def deserialize(
        cls,
        data_sources: str,
        data_dicts_files: list[dict[str, Any]],
        options: Options,
        merge_options: MergeOptions,
    ) -> CoverageContainer:
        """Serialize the object."""
        covdata = CoverageContainer()
        for gcovr_file in data_dicts_files:
            if (
                filecov := FileCoverage.deserialize(
                    data_sources, gcovr_file, merge_options, options
                )
            ) is not None:
                covdata.insert_file_coverage(
                    filecov,
                    merge_options,
                )

        return covdata

    def merge_lines(self, options: Options) -> None:
        """Merge line coverage for same line number. Remove the function information on merged lines."""
        for filecov in self.values():
            filecov.merge_lines(
                is_file_excluded(
                    "trace",
                    filecov.filename,
                    options.trace_include_filter,
                    options.trace_exclude_filter,
                )
            )

    def merge(self, other: CoverageContainer, options: MergeOptions) -> None:
        """
        Merge CoverageContainer information and clear directory statistics.

        Do not use 'other' objects afterwards!
        """
        self.directories.clear()
        other.directories.clear()
        self.data.merge(other.data, options)

    def insert_file_coverage(
        self, filecov: FileCoverage, options: MergeOptions
    ) -> FileCoverage:
        """Add a file coverage item."""
        self.directories.clear()
        key = filecov.filename
        if key in self.data:
            self.data[key].merge(filecov, options)
        else:
            self.data[key] = filecov

        return filecov

    @property
    def stats(self) -> SummarizedStats:
        """Create a coverage statistic from a coverage data object."""
        stats = SummarizedStats.new_empty()
        for filecov in self.values():
            stats += filecov.stats
        return stats

    @staticmethod
    def _get_dirname(filename: str) -> str | None:
        """Get the directory name with a trailing path separator.

        >>> import os
        >>> CoverageContainer._get_dirname("bar/foobar.cpp".replace("/", os.sep)).replace(os.sep, "/")
        'bar/'
        >>> CoverageContainer._get_dirname("/foo/bar/A/B.cpp".replace("/", os.sep)).replace(os.sep, "/")
        '/foo/bar/A/'
        >>> CoverageContainer._get_dirname(os.sep) is None
        True
        """
        if filename == os.sep:
            return None
        return str(os.path.dirname(filename.rstrip(os.sep))) + os.sep

    def populate_directories(
        self, sorted_keys: Iterable[str], root_filter: re.Pattern[str]
    ) -> None:
        r"""Populate the list of directories and add accumulated stats.

        This function will accumulate statistics such that every directory
        above it will know the statistics associated with all files deep within a
        directory structure.

        Args:
            sorted_keys: The sorted keys for covdata
            root_filter: Information about the filter used with the root directory
        """

        # Get the directory coverage
        subdirs = dict[str, CoverageContainerDirectory]()
        for key in sorted_keys:
            filecov = self[key]
            dircov: CoverageContainerDirectory | None = None
            dirname: str | None = (
                os.path.dirname(filecov.filename)
                .replace("\\", os.sep)
                .replace("/", os.sep)
                .rstrip(os.sep)
            ) + os.sep
            while dirname is not None and root_filter.search(dirname + os.sep):
                if dirname not in subdirs:
                    subdirs[dirname] = CoverageContainerDirectory(dirname)
                if dircov is None:
                    subdirs[dirname][filecov.filename] = filecov
                else:
                    subdirs[dirname].data[dircov.filename] = dircov
                    subdirs[dircov.filename].parent_dirname = dirname
                subdirs[dirname].stats += filecov.stats
                dircov = subdirs[dirname]
                dirname = CoverageContainer._get_dirname(dirname)

        # Replace directories where only one sub container is available
        # with the content this sub container
        LOGGER.debug(
            "Replace directories with only one sub element with the content of this."
        )
        subdirs_to_remove = set()
        for dirname, covdata_dir in subdirs.items():
            # There is exact one element, replace current element with referenced element
            if len(covdata_dir) == 1:
                # Get the orphan item
                orphan_key, orphan_value = next(iter(covdata_dir.items()))
                # The only child is a File object
                if isinstance(orphan_value, FileCoverage):
                    # Replace the reference to ourself with our content
                    if covdata_dir.parent_dirname is not None:
                        LOGGER.debug(
                            "Move %s to %s.",
                            orphan_key,
                            covdata_dir.parent_dirname,
                        )
                        parent_covdata_dir = subdirs[covdata_dir.parent_dirname]
                        parent_covdata_dir[orphan_key] = orphan_value
                        del parent_covdata_dir[dirname]
                        subdirs_to_remove.add(dirname)
                else:
                    LOGGER.debug(
                        "Move content of %s to %s.",
                        orphan_value.dirname,
                        dirname,
                    )
                    # Replace the children with the orphan ones
                    covdata_dir.data = orphan_value.data
                    # Change the parent key of each new child element
                    for new_child_value in covdata_dir.values():
                        if isinstance(new_child_value, CoverageContainerDirectory):
                            new_child_value.parent_dirname = dirname
                    # Mark the key for removal.
                    subdirs_to_remove.add(orphan_key)

        for dirname in subdirs_to_remove:
            del subdirs[dirname]

        self.directories = list(subdirs.values())


class CoverageContainerDirectory(ContainerBase):
    """Represent coverage information about a directory."""

    __slots__ = "dirname", "parent_dirname", "data", "stats"

    def __init__(self, dirname: str) -> None:
        self.dirname: str = dirname
        self.parent_dirname: str | None = None
        self.data = CoverageDict[str, FileCoverage | CoverageContainerDirectory]()
        self.stats: SummarizedStats = SummarizedStats.new_empty()

    def __setitem__(
        self, key: str, item: FileCoverage | CoverageContainerDirectory
    ) -> None:
        self.data[key] = item

    def __getitem__(self, key: str) -> FileCoverage | CoverageContainerDirectory:
        return self.data[key]

    def __delitem__(self, key: str) -> None:
        del self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def values(self) -> ValuesView[FileCoverage | CoverageContainerDirectory]:
        """Get the file coverage data objects."""
        return self.data.values()

    def items(self) -> ItemsView[str, FileCoverage | CoverageContainerDirectory]:
        """Get the file coverage data items."""
        return self.data.items()

    def merge(self, other: CoverageContainerDirectory, options: MergeOptions) -> None:
        """
        Merge CoverageContainerDirectory information and clear directory statistics.

        Do not use 'other' objects afterwards!
        """
        self.data.merge(other.data, options)

    @property
    def filename(self) -> str:
        """Helpful function for when we use this DirectoryCoverage in a union with FileCoverage"""
        return self.dirname

    def line_coverage(self) -> CoverageStat:
        """A simple wrapper function necessary for sort_coverage()."""
        return self.stats.line

    def branch_coverage(self) -> CoverageStat:
        """A simple wrapper function necessary for sort_coverage()."""
        return self.stats.branch

    def decision_coverage(self) -> DecisionCoverageStat:
        """A simple wrapper function necessary for sort_coverage()."""
        return self.stats.decision
