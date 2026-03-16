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

"""Utils for exclusion of lines and branches"""

from typing import Callable, Iterable

from ..data_model.coverage import FileCoverage, FunctionCoverage
from ..logging import LOGGER


ExclusionPredicate = Callable[[int], bool]
FunctionListByLine = dict[int, list[FunctionCoverage]]


def function_exclude_not_supported(
    filename: str | None = None,
    lineno: int | None = None,
    columnno: int | None = None,
) -> None:
    """warn that a function exclude isn't supported"""
    if filename is None:
        LOGGER.warning("Function exclusion not supported for this compiler.")
    else:
        LOGGER.warning(
            "Function exclude marker found on line %s:%s but not supported for this compiler, when processing %s.",
            lineno,
            columnno,
            filename,
        )


def function_exclude_not_at_function_line(
    filename: str, lineno: int, columnno: int
) -> None:
    """warn that a function exclude is found at a line where no function is defined"""
    LOGGER.warning(
        "Function exclude marker found on line %s:%s but no function definition found, when processing %s.",
        lineno,
        columnno,
        filename,
    )


def get_functions_by_line(filecov: FileCoverage) -> FunctionListByLine:
    """Get dict with the linenumber as key and the function defined in the line as value."""
    functions_by_line: FunctionListByLine = {}
    if filecov is not None:
        for functioncov in filecov.functioncov():
            if functioncov.start is not None:
                for lineno, _ in functioncov.start.items():
                    if lineno not in functions_by_line:
                        functions_by_line[lineno] = []
                    functions_by_line[lineno].append(functioncov)

    return functions_by_line


def get_function_exclude_ranges(
    filename: str, lineno: int, columnno: int, *, functions_by_line: FunctionListByLine
) -> list[tuple[int, int]]:
    """Get the exclude range for a given function."""
    exclude_ranges = []
    if functions_by_line:
        # Find the closest function definition in this line. Check end column if end line is on same line
        function_iter = iter(functions_by_line.get(lineno, []))
        for function in function_iter:
            if (
                function.start is not None
                and function.end is not None
                and columnno > function.start[lineno][1]
                and (
                    lineno < function.end[lineno][0]
                    or columnno < function.end[lineno][1]
                )
            ):
                lineno_end = function.end[lineno][0]
                included_ranges = []
                # Now we need to check for nested functions which are included
                for function in function_iter:
                    if function.end is not None:
                        included_ranges.append((lineno, function.end[lineno][0] + 1))
                for function_lineno in range(lineno + 1, lineno_end):
                    for function in functions_by_line.get(function_lineno, []):
                        if function.start is not None and function.end is not None:
                            included_ranges.append(
                                (
                                    function.start[function_lineno][0],
                                    function.end[function_lineno][0],
                                )
                            )
                if included_ranges:
                    last_include_end = lineno
                    for include_start, include_end in included_ranges:
                        # The exclusion end must be in the line before
                        exclude_ranges.append((last_include_end, include_start - 1))
                        # The next exclusion must start after the included line
                        last_include_end = include_end + 1
                    exclude_ranges.append((last_include_end, lineno_end))
                else:
                    exclude_ranges.append((lineno, lineno_end))
                break
        else:
            function_exclude_not_at_function_line(filename, lineno, columnno)
    else:
        function_exclude_not_supported(filename, lineno, columnno)

    return exclude_ranges


def apply_exclusion_ranges(
    filecov: FileCoverage,
    *,
    line_is_excluded: ExclusionPredicate,
    branch_is_excluded: ExclusionPredicate,
    warn_excluded_lines_with_hits: bool,
) -> None:
    """
    Remove any coverage information that is excluded by explicit markers such as
    ``GCOVR_EXCL_LINE``.

    Modifies the input FileCoverage in place.

    Arguments:
        filecov: the coverage to filter
        line_is_excluded: the function to check if a line is excluded
        branch_is_excluded: the function to check if the branches are excluded
    """

    for linecov_collection in filecov.lines():
        if line_is_excluded(linecov_collection.lineno):
            if warn_excluded_lines_with_hits and linecov_collection.count:
                LOGGER.warning(
                    "%s: Line with %d hit(s) excluded.",
                    linecov_collection.location,
                    linecov_collection.count,
                )
            linecov_collection.exclude()

        elif branch_is_excluded(linecov_collection.lineno):
            linecov_collection.exclude_branches()

    for functioncov in filecov.functioncov():
        for lineno in functioncov.reportable_linenos:
            if line_is_excluded(lineno):
                functioncov.exclude(lineno)


def make_is_in_any_range_inclusive(
    ranges: list[tuple[int, int]],
) -> ExclusionPredicate:
    """
    Create a function to check whether an input is in any range (inclusive).

    This function should provide reasonable performance
    if queries are mostly made in ascending order.

    Example:
    >>> select = make_is_in_any_range_inclusive([(3,3), (5,7)])
    >>> select(0)
    False
    >>> select(6)
    True
    >>> [x for x in range(10) if select(x)]
    [3, 5, 6, 7]
    """

    # values are likely queried in ascending order,
    # allowing the search to start with the first possible range
    ranges = sorted(ranges)
    hint_value = 0
    hint_index = 0

    def is_in_any_range(value: int) -> bool:
        nonlocal hint_value, hint_index

        # if the heuristic failed, restart search from the beginning
        if value < hint_value:
            hint_index = 0

        hint_value = value

        for i in range(hint_index, len(ranges)):
            start, end = ranges[i]
            hint_index = i

            # stop as soon as a too-large range is seen
            if value < start:
                break

            if start <= value <= end:
                return True
        else:
            hint_index = len(ranges)

        return False

    return is_in_any_range


def _lines_from_sparse(sparse: Iterable[tuple[int, str]]) -> list[str]:
    """
    Convert lineno–source tuples to a flat list, useful for tests.

    >>> _lines_from_sparse([(3, 'foo'), (2, 'bar'), (3, 'foo2')])
    ['', 'bar', 'foo2']
    """
    lines = list[str]()
    for lineno, source in sparse:
        lines.extend("" for _ in range(len(lines), lineno))
        lines[lineno - 1] = source
    return lines
