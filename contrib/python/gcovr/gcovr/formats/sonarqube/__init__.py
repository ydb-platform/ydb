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
from ...options import GcovrConfigOption, OutputOrDefault


class SonarqubeHandler(BaseHandler):
    """Class to handle Sonarqube format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options needed for report
            "show_decision",
            GcovrConfigOption(
                "sonarqube",
                ["--sonarqube"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Generate Sonarqube generic coverage report in this file name. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "sonarqube_pretty",
                ["--sonarqube-pretty"],
                group="output_options",
                help=("Pretty-print the Sonarqube XML report. Implies --sonarqube."),
                action="store_true",
            ),
            GcovrConfigOption(
                "sonarqube_metric",
                ["--sonarqube-metric"],
                config="sonarqube-metric",
                group="output_options",
                help=("The metric type to report. Default is '{default!s}'."),
                choices=("line", "branch", "condition", "decision"),
                default="branch",
            ),
        ]

    def validate_options(self) -> None:
        if (
            self.options.sonarqube_metric == "decision"
            and not self.options.show_decision
        ):
            raise RuntimeError(
                "--sonarqube-metric=decision needs the option --decisions."
            )

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)
