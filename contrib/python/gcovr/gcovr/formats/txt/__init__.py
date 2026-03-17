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

from ...data_model.container import CoverageContainer
from ...formats.base import BaseHandler
from ...options import (
    GcovrConfigOption,
    GcovrDeprecatedConfigOptionAction,
    OutputOrDefault,
)


class UseBranchMetricAction(GcovrDeprecatedConfigOptionAction):
    """Argparse action for mapping deprecated option to new option."""

    option = "--txt-metric"
    config = "txt-metric"
    value = "branch"


class TxtHandler(BaseHandler):
    """Class to handle text format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options needed for report
            "show_calls",
            "show_decision",  # Only for summary report
            # Local options
            GcovrConfigOption(
                "txt_metric",
                ["--txt-metric"],
                config="txt-metric",
                group="output_options",
                help=("The metric type to report. Default is '{default!s}'."),
                choices=("line", "branch", "decision"),
                default="line",
            ),
            GcovrConfigOption(
                "txt_metric",
                ["-b", "--txt-branches", "--branches"],
                config="txt-branch",
                group="output_options",
                help=(
                    "Deprecated, please use '--txt-metric branch' instead."
                    "Report the branch coverage instead of the line coverage in text report."
                ),
                nargs=0,
                action=UseBranchMetricAction,
            ),
            GcovrConfigOption(
                "txt_report_covered",
                ["--txt-report-covered"],
                config="txt-covered",
                help="Report the covered lines instead of the uncovered.",
                action="store_true",
            ),
            GcovrConfigOption(
                "txt",
                ["--txt"],
                group="output_options",
                metavar="OUTPUT",
                help="Generate a text report. OUTPUT is optional and defaults to --output.",
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "txt_summary",
                ["-s", "--txt-summary", "--print-summary"],
                group="output_options",
                help=(
                    "Print a small report to stdout "
                    "with line & function & branch percentage coverage "
                    "optional parts are decision & call coverage. "
                    "This is in addition to other reports. "
                ),
                action="store_true",
            ),
        ]

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)

    def write_summary_report(
        self, covdata: CoverageContainer, output_file: str
    ) -> None:
        from .write import write_summary_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_summary_report(covdata, output_file, self.options)
