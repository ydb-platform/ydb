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

import os
from ...data_model.container import CoverageContainer
from ...formats.base import BaseHandler
from ...options import (
    FilterOption,
    GcovrConfigOption,
    relative_path,
)


class GcovHandler(BaseHandler):
    """Class to handle GCOV intermediate format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options used for output
            "verbose",
            # Global options needed for processing
            "keep_intermediate_files",
            "delete_input_files",
            # Global options needed for report
            "show_decision",
            # Global options used for merging end exclusion processing.
            "exclude_directory",
            "exclude_noncode_lines",
            "exclude_throw_branches",
            "exclude_unreachable_branches",
            "exclude_function_lines",
            "exclude_internal_functions",
            "respect_exclusion_markers",
            "exclude_function",
            "exclude_lines_by_pattern",
            "exclude_branches_by_pattern",
            "exclude_pattern_prefix",
            "warn_excluded_lines_with_hits",
            "merge_mode_functions",
            # Local options
            GcovrConfigOption(
                "gcov_use_existing_files",
                ["-g", "--gcov-use-existing-files", "--use-gcov-files"],
                group="gcov_options",
                help="Use existing gcov files for analysis.",
                action="store_true",
            ),
            GcovrConfigOption(
                "gcov_ignore_errors",
                ["--gcov-ignore-errors"],
                group="gcov_options",
                choices=(
                    "all",
                    "source_not_found",
                    "output_error",
                    "no_working_dir_found",
                ),
                nargs="?",
                const="all",
                default=None,
                help=(
                    "Ignore errors from invoking GCOV command "
                    "instead of exiting with an error. "
                    "A report will be shown on stderr. "
                    "Default is '{default!s}'."
                ),
                type=str,
                action="append",
            ),
            GcovrConfigOption(
                "gcov_ignore_parse_errors",
                ["--gcov-ignore-parse-errors"],
                group="gcov_options",
                choices=(
                    "all",
                    "negative_hits.warn",
                    "negative_hits.warn_once_per_file",
                    "suspicious_hits.warn",
                    "suspicious_hits.warn_once_per_file",
                ),
                nargs="?",
                const="all",
                default=None,
                help=(
                    "Skip lines with parse errors in GCOV files "
                    "instead of exiting with an error. "
                    "A report will be shown on stderr. "
                    "Default is '{default!s}'."
                ),
                type=str,
                action="append",
            ),
            GcovrConfigOption(
                "gcov_suspicious_hits_threshold",
                ["--gcov-suspicious-hits-threshold"],
                config="gcov-suspicious-hits-threshold",
                group="gcov_options",
                help=(
                    "Set the threshold for detecting suspicious hits "
                    "in gcov output files. "
                    "Set to 0 to turn the detection of."
                ),
                type=int,
                default=2**32,
            ),
            GcovrConfigOption(
                "gcov_include_filter",
                ["--gcov-filter"],
                group="filter_options",
                help=(
                    "Keep only gcov data files that match this filter. "
                    "Can be specified multiple times."
                ),
                action="append",
                type=FilterOption,
                default=[],
            ),
            GcovrConfigOption(
                "gcov_exclude_filter",
                ["--gcov-exclude"],
                group="filter_options",
                help=(
                    "Exclude gcov data files that match this filter. "
                    "Can be specified multiple times."
                ),
                action="append",
                type=FilterOption,
                default=[],
            ),
            GcovrConfigOption(
                "gcov_cmd",
                ["--gcov-executable"],
                group="gcov_options",
                help=(
                    "Use a particular gcov executable. "
                    "Must match the compiler you are using, "
                    "e.g. 'llvm-cov gcov' for Clang. "
                    "Can include additional arguments. "
                    "Defaults to the GCOV environment variable, "
                    "or 'gcov': '{default!s}'."
                ),
                default=os.environ.get("GCOV", "gcov"),
            ),
            GcovrConfigOption(
                "gcov_objdir",
                ["--gcov-object-directory", "--object-directory"],
                group="gcov_options",
                help=(
                    "Override normal working directory detection. "
                    "Gcovr needs to identify the path between gcda files "
                    "and the directory where the compiler was originally run. "
                    "Normally, gcovr can guess correctly. "
                    "This option specifies either "
                    "the path from gcc to the gcda file (i.e. gcc's '-o' option), "
                    "or the path from the gcda file to gcc's working directory."
                ),
                type=relative_path,
            ),
            GcovrConfigOption(
                "gcov_parallel",
                ["-j"],
                config="gcov-parallel",
                group="gcov_options",
                help=(
                    "Set the number of threads to use in parallel. "
                    "0=Number of CPUs, negative number='all but N CPUs'."
                ),
                nargs="?",
                const=0,
                type=int,
                default=1,
            ),
        ]

    def validate_options(self) -> None:
        if self.options.gcov_objdir is not None and not os.path.exists(
            self.options.gcov_objdir
        ):
            raise RuntimeError(
                "Bad --gcov-object-directory option.\n"
                "\tThe specified directory does not exist."
            )

    def read_report(self) -> CoverageContainer:
        from .read import read_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        return read_report(self.options)
