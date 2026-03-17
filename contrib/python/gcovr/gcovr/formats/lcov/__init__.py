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


class UseLcovFormatVersion(GcovrDeprecatedConfigOptionAction):
    """Argparse action to map old option --lcov-format-v1 to new option --lcov-format-version=1.x."""

    option = "--lcov-format-version"
    config = "lcov_format_version"
    value = "1.x"


class LcovHandler(BaseHandler):
    """Class to handle LCOV format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            GcovrConfigOption(
                "lcov",
                ["--lcov"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Generate a LCOV info file. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "lcov_format_version",
                ["--lcov-format-version"],
                config="lcov_format_version",
                group="output_options",
                help="The format version to write.",
                choices=("1.x", "2.0"),
                default="2.0",
            ),
            GcovrConfigOption(
                "lcov_format_version",
                ["--lcov-format-1.x"],
                group="output_options",
                help="Deprecated, please use --lcov-format-version=1.x instead.",
                nargs=0,
                action=UseLcovFormatVersion,
            ),
            GcovrConfigOption(
                "lcov_comment",
                ["--lcov-comment"],
                group="output_options",
                metavar="COMMENT",
                help="The comment used in LCOV file.",
            ),
            GcovrConfigOption(
                "lcov_test_name",
                ["--lcov-test-name"],
                group="output_options",
                metavar="NAME",
                help=(
                    "The name used for TN in LCOV file, must not contain spaces. "
                    "Default is '{default!s}'."
                ),
                default="GCOVR_report",
            ),
        ]

    def validate_options(self) -> None:
        if (
            self.options.lcov_test_name is not None
            and " " in self.options.lcov_test_name
        ):
            raise RuntimeError(
                f"The LCOV test name must not contain spaces, got {self.options.lcov_test_name!r}."
            )

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)
