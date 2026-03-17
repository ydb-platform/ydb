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
from ...options import GcovrConfigOption, OutputOrDefault
from ...utils import force_unix_separator


class JsonHandler(BaseHandler):
    """Class to handle own JSON tracefile format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options used for output
            "verbose",
            # Global options used for merging.
            "merge_mode_functions",
            "show_decision",
            # Local options
            GcovrConfigOption(
                "json",
                ["--json"],
                group="output_options",
                metavar="OUTPUT",
                help="Generate a JSON report. OUTPUT is optional and defaults to --output.",
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "json_pretty",
                ["--json-pretty"],
                group="output_options",
                help="Pretty-print the JSON report. Implies --json.",
                action="store_true",
            ),
            GcovrConfigOption(
                "json_summary",
                ["--json-summary"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Generate a JSON summary report. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "json_summary_pretty",
                ["--json-summary-pretty"],
                group="output_options",
                help="Pretty-print the JSON SUMMARY report. Implies --json-summary.",
                action="store_true",
            ),
            GcovrConfigOption(
                "json_base",
                ["--json-base"],
                group="output_options",
                metavar="PATH",
                help="Prepend the given path to all file paths in JSON report.",
                type=lambda p: force_unix_separator(os.path.normpath(p)),
                default=None,
            ),
            GcovrConfigOption(
                "json_tracefile",
                ["-a", "--json-add-tracefile", "--add-tracefile"],
                config="add-tracefile",
                help=(
                    "Combine the coverage data from JSON files. "
                    "Coverage files contains source files structure relative "
                    "to root directory. Those structures are combined "
                    "in the output relative to the current root directory. "
                    "Unix style wildcards can be used to add the pathnames "
                    "matching a specified pattern. In this case pattern "
                    "must be set in double quotation marks. "
                    "Option can be specified multiple times. "
                    "When option is used gcov is not run to collect "
                    "the new coverage data."
                ),
                action="append",
                default=[],
            ),
            GcovrConfigOption(
                "json_trace_data_source",
                ["--json-trace-data-source"],
                group="output_options",
                help="Write the data source to the tracefile.",
                action="store_true",
            ),
        ]

    def read_report(self) -> CoverageContainer:
        from .read import read_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        return read_report(self.options)

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)

    def write_summary_report(
        self, covdata: CoverageContainer, output_file: str
    ) -> None:
        from .write import write_summary_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_summary_report(covdata, output_file, self.options)
