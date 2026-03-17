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


THEMES = (
    "green",
    "blue",
)


class MarkdownHandler(BaseHandler):
    """Class to handle markdown format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options needed for report
            "medium_threshold",
            "high_threshold",
            "medium_threshold_branch",
            "high_threshold_branch",
            "medium_threshold_line",
            "high_threshold_line",
            # Local options
            GcovrConfigOption(
                "markdown",
                ["--markdown"],
                group="output_options",
                metavar="OUTPUT",
                help="Generate a markdown report. OUTPUT is optional and defaults to --output.",
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "markdown_summary",
                ["--markdown-summary"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Generate a markdown summary report. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "markdown_theme",
                ["--markdown-theme"],
                group="output_options",
                type=str,
                choices=THEMES,
                metavar="THEME",
                help=(
                    "Override the default color theme for the Markdown report. "
                    "Default is {default!s}."
                ),
                default=THEMES[0],
            ),
            GcovrConfigOption(
                "markdown_title",
                ["--markdown-title"],
                group="output_options",
                type=str,
                metavar="TEXT",
                help=(
                    "Override the default title of the Markdown report. "
                    "Default is {default!s}."
                ),
                default="GCC Code Coverage Report",
            ),
            GcovrConfigOption(
                "markdown_heading_level",
                ["--markdown-heading-level"],
                group="output_options",
                type=int,
                metavar="INT",
                help=(
                    "Override the default heading level of the Markdown report. "
                    "This is useful if the report is embedded in another markdown file. "
                    "Default is {default!s}."
                ),
                default=1,
            ),
            GcovrConfigOption(
                "markdown_file_link",
                ["--markdown-file-link"],
                group="output_options",
                type=str,
                metavar="TEXT",
                help=(
                    "Link the files using given URL by replacing {{file}} with the current file."
                ),
                default=None,
            ),
        ]

    def validate_options(self) -> None:
        if self.options.markdown_heading_level < 1:
            raise RuntimeError("The markdown heading level must not be less than 0.")

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)

    def write_summary_report(
        self, covdata: CoverageContainer, output_file: str
    ) -> None:
        from .write import write_summary_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_summary_report(covdata, output_file, self.options)
