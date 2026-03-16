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
    OutputOrDefault,
    check_input_file,
)


THEMES = (
    "green",
    "blue",
    "github.blue",
    "github.green",
    "github.dark-green",
    "github.dark-blue",
)


class HtmlHandler(BaseHandler):
    """Class to handle HTML format."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        return [
            # Global options needed for report
            "show_calls",
            "show_decision",
            "medium_threshold",
            "high_threshold",
            "medium_threshold_branch",
            "high_threshold_branch",
            "medium_threshold_line",
            "high_threshold_line",
            # Needed for highlighting of GCOVR markers
            "exclude_pattern_prefix",
            # Local options
            GcovrConfigOption(
                "html",
                ["--html"],
                group="output_options",
                metavar="OUTPUT",
                help="Generate a HTML report. OUTPUT is optional and defaults to --output.",
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "html_details",
                ["--html-details"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Add annotated source code reports to the HTML report. "
                    "Implies --html, can not be used together with --html-nested. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "html_nested",
                ["--html-nested"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Add annotated source code reports to the HTML report. "
                    "A page is created for each directory that summarize subdirectories "
                    "with aggregated statistics. "
                    "Implies --html, can not be used together with --html-details. "
                    "OUTPUT is optional and defaults to --output."
                ),
                nargs="?",
                type=OutputOrDefault,
                default=None,
                const=OutputOrDefault(None),
            ),
            GcovrConfigOption(
                "html_single_page",
                ["--html-single-page"],
                group="output_options",
                help=(
                    "Use one single html output file containing all data in the "
                    "specified mode. If mode is 'js-enabled' (default) and javascript "
                    "is possible the page is interactive like the normal report. "
                    "If mode is 'static' all files are shown at once."
                ),
                action="store_true",
            ),
            GcovrConfigOption(
                "html_static_report",
                ["--html-static-report"],
                group="output_options",
                help="Create a static report without javascript.",
                action="store_true",
            ),
            GcovrConfigOption(
                "html_self_contained",
                ["--html-self-contained"],
                group="output_options",
                help=(
                    "Control whether the HTML report bundles resources like CSS styles. "
                    "Self-contained reports can be sent via email, "
                    "but conflict with the Content Security Policy of some web servers. "
                    "Defaults to self-contained reports unless --html-details or "
                    "--html-nested is used without --html-single-page."
                ),
                action="store_const",
                default=None,
                const=True,
                const_negate=False,
            ),
            GcovrConfigOption(
                "html_block_ids",
                ["--html-block-ids"],
                group="output_options",
                help=(
                    "Add the block ids to the HTML report for debugging the branch coverage."
                ),
                action="store_true",
            ),
            GcovrConfigOption(
                "html_template_dir",
                ["--html-template-dir"],
                group="output_options",
                metavar="OUTPUT",
                help=(
                    "Override the default Jinja2 template directory for the HTML report."
                ),
            ),
            GcovrConfigOption(
                "html_syntax_highlighting",
                ["--html-syntax-highlighting", "--html-details-syntax-highlighting"],
                group="output_options",
                help="Use syntax highlighting in HTML source page. Enabled by default.",
                action="store_const",
                default=True,
                const=True,
                const_negate=False,  # autogenerates --no-NAME with action const=False
            ),
            GcovrConfigOption(
                "html_theme",
                ["--html-theme"],
                group="output_options",
                type=str,
                choices=THEMES,
                metavar="THEME",
                help=(
                    "Override the default color theme for the HTML report. "
                    "Default is {default!s}."
                ),
                default=THEMES[0],
            ),
            GcovrConfigOption(
                "html_css",
                ["--html-css"],
                group="output_options",
                type=check_input_file,
                metavar="CSS",
                help="Override the default style sheet for the HTML report.",
                default=None,
            ),
            GcovrConfigOption(
                "html_title",
                ["--html-title"],
                group="output_options",
                metavar="TITLE",
                help="Use TITLE as title for the HTML report. Default is '{default!s}'.",
                default="GCC Code Coverage Report",
            ),
            GcovrConfigOption(
                "html_tab_size",
                ["--html-tab-size"],
                group="output_options",
                help="Used spaces for a tab in a source file. Default is {default!s}",
                type=int,
                default=4,
            ),
            GcovrConfigOption(
                "html_relative_anchors",
                ["--html-absolute-paths"],
                group="output_options",
                help=(
                    "Use absolute paths to link the --html-details reports. "
                    "Defaults to relative links."
                ),
                action="store_false",
            ),
            GcovrConfigOption(
                "html_encoding",
                ["--html-encoding"],
                group="output_options",
                help=(
                    "Override the declared HTML report encoding. "
                    "Defaults to {default!s}. "
                    "See also --source-encoding."
                ),
                default="UTF-8",
            ),
        ]

    def validate_options(self) -> None:
        if self.options.html_tab_size < 1:
            raise RuntimeError("Value of --html-tab-size should be greater 0.")

        if self.options.html_details and self.options.html_nested:
            raise RuntimeError(
                "--html-details and --html-nested can not be used together."
            )

        html_output = None
        if self.options.html and self.options.html.value:
            html_output = self.options.html.value
        elif self.options.html_details and self.options.html_details.value:
            html_output = self.options.html_details.value
        elif self.options.html_nested and self.options.html_nested.value:
            html_output = self.options.html_nested.value
        elif self.options.output and self.options.output.value:
            html_output = self.options.output.value

        if self.options.html_details and not html_output:
            raise RuntimeError(
                "a named output must be given, if the option --html-details is used."
            )

        if self.options.html_nested and not html_output:
            raise RuntimeError(
                "a named output must be given, if the option --html-nested is used."
            )

        if (
            html_output == "-"
            and not self.options.html_single_page
            and (self.options.html_details or self.options.html_nested)
        ):
            raise RuntimeError(
                "detailed reports can only be printed to STDOUT as --html-single-page."
            )

        if self.options.html_single_page and not (
            self.options.html_details or self.options.html_nested
        ):
            raise RuntimeError(
                "option --html-details or --html-nested is needed, if the option --html-single-page is used."
            )

        if self.options.html_self_contained is False and not html_output:
            raise RuntimeError(
                "can only disable --html-self-contained when a named output is given."
            )

        if (
            self.options.html_self_contained is False
            and html_output == "-"
            and not self.options.html_single_page
        ):
            raise RuntimeError("only self contained reports can be printed to STDOUT")

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        from .write import write_report  # pylint: disable=import-outside-toplevel # Lazy loading is intended here

        write_report(covdata, output_file, self.options)
