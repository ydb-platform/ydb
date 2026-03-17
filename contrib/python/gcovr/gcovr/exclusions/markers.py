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

"""
Handle explicit exclusion markers in source code, e.g. ``GCOVR_EXCL_LINE``.
"""

from typing import Callable
import re

from .utils import (
    make_is_in_any_range_inclusive,
    apply_exclusion_ranges,
    get_function_exclude_ranges,
    get_functions_by_line,
)

from ..data_model.coverage import FileCoverage, FunctionCoverage
from ..logging import LOGGER


_EXCLUDE_FLAG = "_EXCL_"
_EXCLUDE_PATTERN_LINE = ""
_EXCLUDE_PATTERN_BRANCH = "BR_"
_EXCLUDE_PATTERN_SUFFIX_LINE = "LINE"
_EXCLUDE_PATTERN_SUFFIX_START = "START"
_EXCLUDE_PATTERN_SUFFIX_STOP = "STOP"
_EXCLUDE_PATTERN_SUFFIX_FUNCTION = "FUNCTION"
_EXCLUDE_PATTERN_SUFFIXES = [
    _EXCLUDE_PATTERN_SUFFIX_LINE,
    _EXCLUDE_PATTERN_SUFFIX_START,
    _EXCLUDE_PATTERN_SUFFIX_STOP,
    _EXCLUDE_PATTERN_SUFFIX_FUNCTION,
]
_EXCLUDE_PATTERN_SUFFIX_SOURCE_BRANCH_EXCLUSION = (
    f"{_EXCLUDE_FLAG}{_EXCLUDE_PATTERN_BRANCH}SOURCE"
)
_EXCLUDE_PATTERN_SUFFIX_BRANCH_WITHOUT_HIT_EXCLUSION = (
    rf"{_EXCLUDE_FLAG}{_EXCLUDE_PATTERN_BRANCH}WITHOUT_HIT:\s+((\d+)/(\d+))"
)

ExclusionPredicate = Callable[[int], bool]
FunctionListByLine = dict[int, list[FunctionCoverage]]


def get_markers_regex(exclude_pattern_prefix: str) -> re.Pattern[str]:
    """Get a regular expression matching all markers."""
    patterns = [
        f"{_EXCLUDE_FLAG}(?:{_EXCLUDE_PATTERN_BRANCH}|{_EXCLUDE_PATTERN_LINE})(?:{'|'.join(_EXCLUDE_PATTERN_SUFFIXES)})"
    ]
    patterns.append(_EXCLUDE_PATTERN_SUFFIX_SOURCE_BRANCH_EXCLUSION)
    patterns.append(_EXCLUDE_PATTERN_SUFFIX_BRANCH_WITHOUT_HIT_EXCLUSION)
    return re.compile(f"{exclude_pattern_prefix}(?:{'|'.join(patterns)})")


def apply_exclusion_markers(
    filecov: FileCoverage,
    *,
    lines: list[str],
    exclude_lines_by_pattern: list[re.Pattern[str]],
    exclude_branches_by_pattern: list[re.Pattern[str]],
    exclude_pattern_prefix: str,
    warn_excluded_lines_with_hits: bool,
    activate_trace_logging: bool,
) -> None:
    """
    Remove any coverage information that is excluded by explicit markers such as
    ``GCOVR_EXCL_LINE``.

    Modifies the input FileCoverage in place.

    Arguments:
        filecov: the coverage to filter
        lines: the source code lines (not raw gcov lines)
        exclude_lines_by_pattern: list of regular expressions to exclude
            individual lines
        exclude_branches_by_pattern: list of regular expressions to exclude
            individual branches
        exclude_pattern_prefix: string with prefix for _LINE/_START/_STOP markers.
    """

    _process_exclude_branch_source(
        lines=lines,
        exclude_pattern_prefix=exclude_pattern_prefix,
        filecov=filecov,
        activate_trace_logging=activate_trace_logging,
    )

    _process_exclude_branch_with_no_hit(
        lines=lines,
        exclude_pattern_prefix=exclude_pattern_prefix,
        filecov=filecov,
        activate_trace_logging=activate_trace_logging,
    )

    line_is_excluded, branch_is_excluded = _find_excluded_ranges(
        lines=lines,
        warnings=_ExclusionRangeWarnings(filecov.filename),
        exclude_lines_by_custom_patterns=exclude_lines_by_pattern,
        exclude_branches_by_custom_patterns=exclude_branches_by_pattern,
        exclude_pattern_prefix=exclude_pattern_prefix,
        filecov=filecov,
        activate_trace_logging=activate_trace_logging,
    )

    apply_exclusion_ranges(
        filecov,
        line_is_excluded=line_is_excluded,
        branch_is_excluded=branch_is_excluded,
        warn_excluded_lines_with_hits=warn_excluded_lines_with_hits,
    )


def _process_exclude_branch_source(
    lines: list[str],
    *,
    exclude_pattern_prefix: str,
    filecov: FileCoverage,
    activate_trace_logging: bool,
) -> None:
    """Scan through all lines to find source branch exclusion markers."""

    excl_pattern = f"(.*?)({exclude_pattern_prefix}{_EXCLUDE_PATTERN_SUFFIX_SOURCE_BRANCH_EXCLUSION})"
    excl_pattern_compiled = re.compile(excl_pattern)

    for lineno, code in enumerate(lines, 1):
        if _EXCLUDE_FLAG in code:
            columnno = 1
            for prefix, match in excl_pattern_compiled.findall(code):
                columnno += len(prefix)
                location = f"{filecov.filename}:{lineno}:{columnno}"
                linecovs = filecov.get_line(lineno)
                if linecovs is None:
                    LOGGER.error(
                        "Found marker for source branch exclusion at %s without coverage information",
                        location,
                    )
                else:
                    for linecov in linecovs.linecov():
                        if linecov.function_name is None or linecov.block_ids is None:
                            LOGGER.warning(
                                "Source branch exclusion at %s needs at least gcc-14 with supported JSON format.",
                                location,
                            )
                        elif not linecov.block_ids:
                            LOGGER.error(
                                "Source branch exclusion at %s found but no block ids defined at this line.",
                                location,
                            )
                        else:
                            function_name = linecov.function_name
                            block_ids = linecov.block_ids or []
                            # Check the lines which belong to the function
                            for cur_linecov in filecov.linecov():
                                if cur_linecov.function_name != function_name:
                                    continue
                                # Exclude the branch where the destination is one of the blocks of the line with the marker
                                for cur_branchcov in cur_linecov.branches():
                                    if cur_branchcov.destination_block_id in block_ids:
                                        if activate_trace_logging:
                                            branch_info = (
                                                f"{cur_branchcov.source_block_id}->{cur_branchcov.destination_block_id}"
                                                if cur_branchcov.branchno is None
                                                else f"{cur_branchcov.branchno}"
                                            )
                                            LOGGER.trace(
                                                "Source branch exclusion at %s is excluding branch %s of line %s",
                                                location,
                                                branch_info,
                                                cur_linecov.lineno,
                                            )
                                        cur_branchcov.excluded = True
                columnno += len(match)


def _process_exclude_branch_with_no_hit(
    lines: list[str],
    *,
    exclude_pattern_prefix: str,
    filecov: FileCoverage,
    activate_trace_logging: bool,
) -> None:
    """Scan through all lines to find exclusion markers for branches without a hit."""

    excl_pattern = rf"(.*?)({exclude_pattern_prefix}{_EXCLUDE_PATTERN_SUFFIX_BRANCH_WITHOUT_HIT_EXCLUSION})"
    excl_pattern_compiled = re.compile(excl_pattern)

    for lineno, code in enumerate(lines, 1):
        if _EXCLUDE_FLAG in code:
            columnno = 1
            for (
                prefix,
                match,
                stats_string,
                uncovered,
                total,
            ) in excl_pattern_compiled.findall(code):
                columnno += len(prefix)
                location = f"{filecov.filename}:{lineno}:{columnno}"
                linecovs = filecov.get_line(lineno)
                if linecovs is None:
                    LOGGER.error(
                        "Found marker for exclusion of branches without hits at %s without coverage information",
                        location,
                    )
                else:
                    for linecov in linecovs.linecov():
                        stats = linecov.branch_coverage()
                        expected_uncovered = stats.total - stats.covered
                        if (
                            str(stats.total) == total
                            and str(expected_uncovered) == uncovered
                        ):
                            if activate_trace_logging:
                                LOGGER.trace(
                                    "Exclusion of branches without hits at %s is excluding %s branch(es)",
                                    location,
                                    uncovered,
                                )
                            linecov.exclude_branches()
                        else:
                            LOGGER.error(
                                "Exclusion of branches without hits (%s) at %s is wrong. There %s %s out of %s branches uncovered",
                                stats_string,
                                location,
                                "is" if expected_uncovered <= 1 else "are",
                                expected_uncovered,
                                stats.total,
                            )
                columnno += len(match)


class _ExclusionRangeWarnings:
    r"""
    Log warnings related to exclusion marker processing.

    Example:
    >>> source = '''\
    ... some code
    ... foo // LCOV_EXCL_STOP
    ... bar // GCOVR_EXCL_START
    ... bar // GCOVR_EXCL_LINE
    ... baz // GCOV_EXCL_STOP
    ... "GCOVR_EXCL_START"
    ... '''
    >>> caplog = getfixture("caplog")
    >>> caplog.clear()
    >>> _ = apply_exclusion_markers(  # doctest: +NORMALIZE_WHITESPACE
    ...     FileCoverage("", filename="example.cpp"),
    ...     lines=source.strip().splitlines(),
    ...     exclude_lines_by_pattern=[],
    ...     exclude_branches_by_pattern=[],
    ...     exclude_pattern_prefix=r"[GL]COVR?",
    ...     warn_excluded_lines_with_hits=False,
    ...     activate_trace_logging=False)
    >>> for message in caplog.record_tuples:
    ...     print(f"{message[1]}: {message[2]}")
    30: mismatched coverage exclusion flags.
              LCOV_EXCL_STOP found on line 2 without corresponding LCOV_EXCL_START, when processing example.cpp.
    30: GCOVR_EXCL_LINE found on line 4 in excluded region started on line 3, when processing example.cpp.
    30: GCOVR_EXCL_START found on line 3 was terminated by GCOV_EXCL_STOP on line 5, when processing example.cpp.
    30: The coverage exclusion region start flag GCOVR_EXCL_START
              on line 6 did not have corresponding GCOVR_EXCL_STOP flag
              in file example.cpp.
    """

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def mismatched_start_stop(
        self, start_lineno: int, start: str, stop_lineno: int, stop: str
    ) -> None:
        """warn that start/stop region markers don't match"""
        LOGGER.warning(
            "%s found on line %d was terminated by %s on line %d, when processing %s.",
            start,
            start_lineno,
            stop,
            stop_lineno,
            self.filename,
        )

    def stop_without_start(self, lineno: int, expected_start: str, stop: str) -> None:
        """warn that a region was ended without corresponding start marker"""
        LOGGER.warning(
            "mismatched coverage exclusion flags.\n"
            "          %s found on line %d without corresponding %s, when processing %s.",
            stop,
            lineno,
            expected_start,
            self.filename,
        )

    def start_without_stop(self, lineno: int, start: str, expected_stop: str) -> None:
        """warn that a region was started but not closed"""
        LOGGER.warning(
            "The coverage exclusion region start flag %s\n"
            "          on line %d did not have corresponding %s flag\n"
            "          in file %s.",
            start,
            lineno,
            expected_stop,
            self.filename,
        )

    def line_after_start(self, lineno: int, start: str, start_lineno: int) -> None:
        """warn that a region was started but an excluded line was found"""
        LOGGER.warning(
            "%s found on line %d in excluded region started on line %d, when processing %s.",
            start,
            lineno,
            start_lineno,
            self.filename,
        )


def _process_exclusion_marker(
    lineno: int,
    columnno: int,
    flag: str,
    header: str,
    exclude_word: str,
    warnings: _ExclusionRangeWarnings,
    functions_by_line: FunctionListByLine,
    exclude_ranges: list[tuple[int, int]],
    exclusion_stack: list[tuple[str, int]],
) -> None:
    """
    Process the exclusion marker.

    Header is a marker name like LCOV or GCOVR.

    START flags are added to the exclusion stack
    STOP flags remove a marker from the exclusion stack
    """

    if flag == _EXCLUDE_PATTERN_SUFFIX_LINE:
        if exclusion_stack:
            warnings.line_after_start(
                lineno,
                f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_LINE}",
                exclusion_stack[-1][1],
            )
        else:
            exclude_ranges.append((lineno, lineno))
    elif flag == _EXCLUDE_PATTERN_SUFFIX_FUNCTION:
        exclude_ranges += get_function_exclude_ranges(
            warnings.filename, lineno, columnno, functions_by_line=functions_by_line
        )
    elif flag == _EXCLUDE_PATTERN_SUFFIX_START:
        exclusion_stack.append((header, lineno))
    elif flag == _EXCLUDE_PATTERN_SUFFIX_STOP:
        if not exclusion_stack:
            warnings.stop_without_start(
                lineno,
                f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_START}",
                f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_STOP}",
            )
        else:
            start_header, start_lineno = exclusion_stack.pop()
            if header != start_header:
                warnings.mismatched_start_stop(
                    start_lineno,
                    f"{start_header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_START}",
                    lineno,
                    f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_STOP}",
                )

            exclude_ranges.append((start_lineno, lineno - 1))


def _find_excluded_ranges(
    lines: list[str],
    *,
    warnings: _ExclusionRangeWarnings,
    exclude_pattern_prefix: str,
    exclude_lines_by_custom_patterns: list[re.Pattern[str]],
    exclude_branches_by_custom_patterns: list[re.Pattern[str]],
    filecov: FileCoverage,
    activate_trace_logging: bool = False,
) -> tuple[ExclusionPredicate, ExclusionPredicate]:
    """
    Scan through all lines to find line ranges and branch ranges covered by exclusion markers.

    Example:
    >>> from .utils import _lines_from_sparse
    >>> import re
    >>> lines = [
    ...     (11, '//PREFIX_EXCL_LINE'), (13, '//IGNORE_LINE'),
    ...     (15, '//PREFIX_EXCL_START'), (18, '//PREFIX_EXCL_STOP'),
    ...     (21, '//PREFIX_EXCL_BR_LINE'), (23, '//IGNORE_BR'),
    ...     (25, '//PREFIX_EXCL_BR_START'), (28, '//PREFIX_EXCL_BR_STOP')]
    >>> exclude_line, exclude_branch = _find_excluded_ranges(
    ...     _lines_from_sparse(lines), warnings=...,
    ...     exclude_lines_by_custom_patterns=[re.compile('.*IGNORE_LINE')],
    ...     exclude_branches_by_custom_patterns=[re.compile('.*IGNORE_BR')],
    ...     exclude_pattern_prefix='PREFIX',
    ...     filecov=None)
    >>> [lineno for lineno in range(30) if exclude_line(lineno)]
    [11, 13, 15, 16, 17]
    >>> [lineno for lineno in range(30) if exclude_branch(lineno)]
    [21, 23, 25, 26, 27]

    The stop marker line is NOT inclusive:
    >>> exclude_line, _ = _find_excluded_ranges(
    ...     _lines_from_sparse([(3, '// PREFIX_EXCL_START'), (7, '// PREFIX_EXCL_STOP')]),
    ...     warnings=...,
    ...     exclude_lines_by_custom_patterns=[],
    ...     exclude_branches_by_custom_patterns=[],
    ...     exclude_pattern_prefix='PREFIX',
    ...     filecov=None)
    >>> for lineno in range(1, 10):
    ...     print(f"{lineno}: {'excluded' if exclude_line(lineno) else 'code'}")
    1: code
    2: code
    3: excluded
    4: excluded
    5: excluded
    6: excluded
    7: code
    8: code
    9: code
    """

    functions_by_line: FunctionListByLine = get_functions_by_line(filecov)

    def find_range_impl(
        custom_patterns: list[re.Pattern[str]],
        exclude_word: str,
    ) -> ExclusionPredicate:
        excl_pattern = f"(.*?)(({exclude_pattern_prefix}){_EXCLUDE_FLAG}{exclude_word}({'|'.join(_EXCLUDE_PATTERN_SUFFIXES)}))"
        excl_pattern_compiled = re.compile(excl_pattern)

        # possibly overlapping inclusive (closed) ranges that describe exclusions regions
        exclude_ranges = list[tuple[int, int]]()
        exclusion_stack = list[tuple[str, int]]()

        for lineno, code in enumerate(lines, 1):
            if _EXCLUDE_FLAG in code:
                columnno = 1
                for prefix, match, header, flag in excl_pattern_compiled.findall(code):
                    columnno += len(prefix)
                    _process_exclusion_marker(
                        lineno,
                        columnno,
                        flag,
                        header,
                        exclude_word,
                        warnings,
                        functions_by_line,
                        exclude_ranges,
                        exclusion_stack,
                    )
                    columnno += len(match)

            if any(p.match(code) for p in custom_patterns):
                exclude_ranges.append((lineno, lineno))

        for header, lineno in exclusion_stack:
            warnings.start_without_stop(
                lineno,
                f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_START}",
                f"{header}{_EXCLUDE_FLAG}{exclude_word}{_EXCLUDE_PATTERN_SUFFIX_STOP}",
            )

        if activate_trace_logging:
            LOGGER.trace(
                "Exclusion ranges for pattern %r: %s", excl_pattern, exclude_ranges
            )

        return make_is_in_any_range_inclusive(exclude_ranges)

    return (
        find_range_impl(exclude_lines_by_custom_patterns, _EXCLUDE_PATTERN_LINE),
        find_range_impl(exclude_branches_by_custom_patterns, _EXCLUDE_PATTERN_BRANCH),
    )
