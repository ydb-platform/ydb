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

from dataclasses import dataclass, field

from ..options import Options


@dataclass
class MergeFunctionOptions:
    """Data class to store the function merge options."""

    ignore_function_lineno: bool = False
    merge_function_use_line_zero: bool = False
    merge_function_use_line_min: bool = False
    merge_function_use_line_max: bool = False
    separate_function: bool = False


FUNCTION_STRICT_MERGE_OPTIONS = MergeFunctionOptions()
FUNCTION_LINE_ZERO_MERGE_OPTIONS = MergeFunctionOptions(
    ignore_function_lineno=True,
    merge_function_use_line_zero=True,
)
FUNCTION_MIN_LINE_MERGE_OPTIONS = MergeFunctionOptions(
    ignore_function_lineno=True,
    merge_function_use_line_min=True,
)
FUNCTION_MAX_LINE_MERGE_OPTIONS = MergeFunctionOptions(
    ignore_function_lineno=True,
    merge_function_use_line_max=True,
)
SEPARATE_FUNCTION_MERGE_OPTIONS = MergeFunctionOptions(
    ignore_function_lineno=True,
    separate_function=True,
)


@dataclass
class MergeOptions:
    """Data class to store the merge options."""

    func_opts: MergeFunctionOptions = field(default_factory=MergeFunctionOptions)


DEFAULT_MERGE_OPTIONS = MergeOptions()


def get_merge_mode_from_options(options: Options) -> MergeOptions:
    """Get the function merge mode."""
    merge_opts = MergeOptions()
    if options.merge_mode_functions == "strict":
        merge_opts.func_opts = FUNCTION_STRICT_MERGE_OPTIONS
    elif options.merge_mode_functions == "merge-use-line-0":
        merge_opts.func_opts = FUNCTION_LINE_ZERO_MERGE_OPTIONS
    elif options.merge_mode_functions == "merge-use-line-min":
        merge_opts.func_opts = FUNCTION_MIN_LINE_MERGE_OPTIONS
    elif options.merge_mode_functions == "merge-use-line-max":
        merge_opts.func_opts = FUNCTION_MAX_LINE_MERGE_OPTIONS
    elif options.merge_mode_functions == "separate":
        merge_opts.func_opts = SEPARATE_FUNCTION_MERGE_OPTIONS
    else:
        raise AssertionError("Unknown functions merge mode.")

    return merge_opts
