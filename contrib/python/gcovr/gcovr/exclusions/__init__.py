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
Handle exclusion markers and any other source code level filtering mechanisms.

The different mechanisms are exposed as separate passes/functions
that remove unwanted aspects from the coverage data.
Alternatively, they full suite of exclusion rules can be invoked
via ``apply_all_exclusions()``, which is configured via the usual options object.
"""

from dataclasses import dataclass, field
import re

from ..data_model.coverage import FileCoverage
from ..logging import LOGGER
from ..options import Options

from .markers import ExclusionPredicate, FunctionListByLine, apply_exclusion_markers
from .noncode import remove_unreachable_branches, remove_noncode_lines
from .utils import (
    make_is_in_any_range_inclusive,
    apply_exclusion_ranges,
    function_exclude_not_supported,
    get_function_exclude_ranges,
    get_functions_by_line,
)


@dataclass
class ExclusionOptions:
    """
    Options used by exclusion processing.

    The defaults are just for testing purposes.
    Otherwise, this class acts more like an interface,
    describing some options in "gcovr.configuration".
    """

    respect_exclusion_markers: bool = True
    warn_excluded_lines_with_hits: bool = False
    exclude_function: list[re.Pattern[str]] = field(default_factory=lambda: [])
    exclude_lines_by_pattern: list[re.Pattern[str]] = field(default_factory=lambda: [])
    exclude_branches_by_pattern: list[re.Pattern[str]] = field(
        default_factory=lambda: []
    )
    exclude_pattern_prefix: str = "PREFIX"
    exclude_throw_branches: bool = False
    exclude_unreachable_branches: bool = False
    exclude_function_lines: bool = False
    exclude_internal_functions: bool = False
    exclude_noncode_lines: bool = False


def get_exclusion_options_from_options(options: Options) -> ExclusionOptions:
    """Get the exclusion options."""

    return ExclusionOptions(
        respect_exclusion_markers=options.respect_exclusion_markers,
        warn_excluded_lines_with_hits=options.warn_excluded_lines_with_hits,
        exclude_function=options.exclude_function,
        exclude_lines_by_pattern=options.exclude_lines_by_pattern,
        exclude_branches_by_pattern=options.exclude_branches_by_pattern,
        exclude_pattern_prefix=options.exclude_pattern_prefix,
        exclude_throw_branches=options.exclude_throw_branches,
        exclude_unreachable_branches=(options.exclude_unreachable_branches),
        exclude_function_lines=options.exclude_function_lines,
        exclude_internal_functions=options.exclude_internal_functions,
        exclude_noncode_lines=options.exclude_noncode_lines,
    )


def apply_all_exclusions(
    filecov: FileCoverage,
    *,
    lines: list[str],
    options: ExclusionOptions,
    activate_trace_logging: bool = False,
) -> None:
    """
    Apply all available exclusion mechanisms, if they are enabled by the options.

    Modifies the FileCoverage in place.
    """

    if options.exclude_internal_functions:
        remove_internal_functions(
            filecov, activate_trace_logging=activate_trace_logging
        )

    if options.exclude_throw_branches:
        remove_throw_branches(filecov, activate_trace_logging=activate_trace_logging)

    if options.exclude_unreachable_branches:
        remove_unreachable_branches(
            filecov, lines=lines, activate_trace_logging=activate_trace_logging
        )

    if options.exclude_noncode_lines:
        remove_noncode_lines(
            filecov, lines=lines, activate_trace_logging=activate_trace_logging
        )

    if options.respect_exclusion_markers:
        apply_exclusion_markers(
            filecov,
            lines=lines,
            exclude_lines_by_pattern=options.exclude_lines_by_pattern,
            exclude_branches_by_pattern=options.exclude_branches_by_pattern,
            exclude_pattern_prefix=options.exclude_pattern_prefix,
            warn_excluded_lines_with_hits=options.warn_excluded_lines_with_hits,
            activate_trace_logging=activate_trace_logging,
        )

    if options.exclude_function_lines:
        exclude_function_definition_lines(
            filecov, activate_trace_logging=activate_trace_logging
        )

    if options.exclude_function:
        exclude_functions(
            filecov,
            options.exclude_function,
            activate_trace_logging=activate_trace_logging,
        )


def exclude_function_definition_lines(
    filecov: FileCoverage, activate_trace_logging: bool
) -> None:
    """Remove coverage for lines that contain a function definition."""
    # iterate over a shallow copy
    known_function_lines = set(
        (lineno, functioncov.name)
        for functioncov in filecov.functioncov()
        for lineno in functioncov.linenos
    )
    for linecov in list(filecov.linecov()):
        if (linecov.lineno, linecov.function_name) in known_function_lines or (
            linecov.lineno,
            linecov.demangled_function_name,
        ) in known_function_lines:
            if activate_trace_logging:
                LOGGER.trace(
                    "%s: Removing line of function definition for function %s",
                    linecov.location,
                    linecov.function_name,
                )
            linecov.exclude()


def exclude_functions(
    filecov: FileCoverage, patterns: list[re.Pattern[str]], activate_trace_logging: bool
) -> None:
    """Remove matching functions"""
    if filecov.functioncov():
        functions_by_line: FunctionListByLine = get_functions_by_line(filecov)

        exclude_ranges = []
        for lineno, functions in functions_by_line.items():
            for functioncov in functions:
                for pattern in patterns:
                    if any(
                        pattern.fullmatch(name)
                        for name in (
                            functioncov.name,
                            functioncov.demangled_name,
                        )
                        if name is not None
                    ):
                        if (
                            functioncov.start is None
                            or functioncov.start[lineno] is None
                        ):
                            function_exclude_not_supported()
                        else:
                            exclude_ranges += get_function_exclude_ranges(
                                filecov.filename,
                                lineno,
                                functioncov.start[lineno][1]
                                + 1,  # Cheat that the comment is after the definition
                                functions_by_line=functions_by_line,
                            )
                        break
            if activate_trace_logging:
                LOGGER.trace(
                    "Exclusion range for functions from CLI in %s: %s.",
                    filecov.filename,
                    str(exclude_ranges),
                )
        exclusion_predicate: ExclusionPredicate = make_is_in_any_range_inclusive(
            exclude_ranges
        )
        apply_exclusion_ranges(
            filecov,
            line_is_excluded=exclusion_predicate,
            branch_is_excluded=exclusion_predicate,
            warn_excluded_lines_with_hits=False,
        )


def remove_internal_functions(
    filecov: FileCoverage, activate_trace_logging: bool
) -> None:
    """Remove compiler-generated functions, e.g. for static initialization."""

    # Get all the functions first because we want to remove some of them which will else result in an error.
    for functioncov in list(filecov.functioncov()):
        if _function_can_be_excluded(
            functioncov.mangled_name, functioncov.demangled_name
        ):
            if activate_trace_logging:
                LOGGER.trace(
                    "%s: Removing symbol %s detected as compiler generated functions, e.g. for static initialization",
                    functioncov.location,
                    functioncov.name,
                )
            filecov.remove_function_coverage(functioncov)


def _function_can_be_excluded(*names: str | None) -> bool:
    """Special names for construction/destruction of static objects will be ignored"""
    return any(
        name is not None
        and (name.startswith("__") or name.startswith("_GLOBAL__sub_I_"))
        for name in names
    )


def remove_throw_branches(filecov: FileCoverage, activate_trace_logging: bool) -> None:
    """Remove branches annotated as "throw"."""
    for linecov in filecov.linecov():
        # iterate over shallow copy
        branchcov_list = list(linecov.branches())
        for branchcov in branchcov_list:
            if branchcov.throw:
                linecov.remove_branch(branchcov)
        if activate_trace_logging:
            removed_items = len(branchcov_list) - len(list(linecov.branches()))
            if removed_items > 0:
                LOGGER.trace(
                    "%s: Removing %d unreachable branch(es) detected as exception-only code",
                    linecov.location,
                    removed_items,
                )
