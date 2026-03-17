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

from ...options import GcovrConfigOption, OutputOrDefault
from ...formats.base import BaseHandler

from ...data_model.container import CoverageContainer


class CloverHandler(BaseHandler):
    """Class to handle Clover format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options used for merging.
            "merge_mode_functions",
            # Local options
            GcovrConfigOption(
                "clover",
                ["--clover"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Generate a Clover XML report. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "clover_pretty",
                ["--clover-pretty"],
                group="output_options",
                help=("Pretty-print the Clover XML report. Implies --clover."),
                action="store_true",
            ),
            GcovrConfigOption(
                "clover_project",
                ["--clover-project"],
                group="output_options",
                type=str,
                help=("The project name for the Clover XML report."),
            ),
        ]

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)
