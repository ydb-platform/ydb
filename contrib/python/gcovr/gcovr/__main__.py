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
import re
import sys

from argparse import ArgumentError, ArgumentParser, Namespace
from typing import Any
import traceback

from .configuration import (
    argument_parser_setup,
    config_entries_from_dict,
    merge_options_and_set_defaults,
    parse_config_file,
    parse_config_into_dict,
)
from .data_model.container import CoverageContainer
from .exceptions import SanityCheckError
from .filter import (
    AlwaysMatchFilter,
    DirectoryPrefixFilter,
    Filter,
)
from .formats.gcov.read import GcovProgram
from .formats.gcov.workers import Workers
from .logging import configure_logging, update_logging, LOGGER
from .options import FilterOption
from .version import __version__

# formats
from . import formats as gcovr_formats

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

EXIT_SUCCESS = 0
EXIT_CMDLINE_ERROR = 1
EXIT_LINE_NOK = 2
EXIT_BRANCH_NOK = 4
EXIT_DECISION_NOK = 8
EXIT_FUNCTION_NOK = 16
EXIT_READ_ERROR = 64
EXIT_WRITE_ERROR = 128


#
# Exits with status 2 if below threshold
#
def get_exit_code(
    covdata: CoverageContainer,
    threshold_line: float,
    threshold_branch: float,
    threshold_decision: float,
    threshold_function: float,
) -> int:
    """Fail depending on the coverage result."""
    exit_code = 0

    if (
        threshold_line > 0.0
        or threshold_branch > 0.0
        or threshold_decision > 0.0
        or threshold_function > 0.0
    ):
        stats = covdata.stats

        line_nok = False
        if threshold_line > 0.0:
            # If there are no lines, mark as uncovered
            # (indicates no data at all, likely an error).
            percent_lines = stats.line.percent_or(0.0)

            if percent_lines < threshold_line:
                line_nok = True
                LOGGER.error(
                    "Failed minimum line coverage (got %s%%, minimum %s%%)",
                    percent_lines,
                    threshold_line,
                )

        branch_nok = False
        if threshold_branch > 0.0:
            # Allow data with no branches.
            percent_branches = stats.branch.percent_or(100.0)
            if percent_branches < threshold_branch:
                branch_nok = True
                LOGGER.error(
                    "Failed minimum branch coverage (got %s%%, minimum %s%%)",
                    percent_branches,
                    threshold_branch,
                )

        decision_nok = False
        if threshold_decision > 0.0:
            # Allow data with no decisions.
            percent_decision = stats.decision.percent_or(100.0)
            if percent_decision < threshold_decision:
                decision_nok = True
                LOGGER.error(
                    "Failed minimum decision coverage (got %s%%, minimum %s%%)",
                    percent_decision,
                    threshold_decision,
                )

        function_nok = False
        if threshold_function > 0.0:
            # Allow data with no functions.
            percent_function = stats.function.percent_or(100.0)
            if percent_function < threshold_function:
                function_nok = True
                LOGGER.error(
                    "Failed minimum function coverage (got %s%%, minimum %s%%)",
                    percent_function,
                    threshold_function,
                )

        if line_nok:
            exit_code |= EXIT_LINE_NOK
        if branch_nok:
            exit_code |= EXIT_BRANCH_NOK
        if decision_nok:
            exit_code |= EXIT_DECISION_NOK
        if function_nok:
            exit_code |= EXIT_FUNCTION_NOK

    return exit_code


def create_argument_parser() -> ArgumentParser:
    """Create the argument parser."""

    parser = ArgumentParser(add_help=False, exit_on_error=False)
    parser.usage = "gcovr [options] [search_paths...]"
    parser.description = (
        "A utility to run gcov and summarize the coverage in simple reports."
    )

    parser.epilog = "See <http://gcovr.com/> for the full manual."

    options = parser.add_argument_group("Options")
    options.add_argument(
        "-h", "--help", help="Show this help message, then exit.", action="help"
    )
    options.add_argument(
        "--version",
        help="Print the version number, then exit.",
        action="store_true",
        dest="version",
        default=False,
    )

    argument_parser_setup(parser, options)

    return parser


COPYRIGHT = (
    "Copyright (c) 2013-2026 the gcovr authors\n"
    "Copyright (c) 2013 Sandia Corporation.\n"
    "Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,\n"
    "the U.S. Government retains certain rights in this software.\n"
)


def find_config_name(root: str, filename: str) -> str | None:
    """Find the configuration to use."""
    if root:
        filename = os.path.join(root, filename)

    if os.path.isfile(filename):
        return filename

    return None


def load_config(partial_options: Namespace) -> dict[str, Any]:
    """Load a config file if configured or found by default names"""
    filename = getattr(partial_options, "config", None)
    if filename is not None:
        with open(filename, encoding="utf-8") as buf:
            return parse_config_into_dict(parse_config_file(buf, filename))

    root = getattr(partial_options, "root", "")
    if filename := find_config_name(root, "gcovr.cfg"):
        with open(filename, encoding="utf-8") as buf:
            return parse_config_into_dict(parse_config_file(buf, filename))

    if filename := find_config_name(root, "gcovr.toml"):
        with open(filename, "rb") as buf:
            data = tomllib.load(buf)
        return parse_config_into_dict(config_entries_from_dict(data, filename))

    if filename := find_config_name(root, "pyproject.toml"):
        with open(filename, "rb") as buf:
            data = tomllib.load(buf)
        if (gcovr_section := data.get("tool", {}).get("gcovr")) is not None:
            return parse_config_into_dict(
                config_entries_from_dict(gcovr_section, filename)
            )

    return {}


def main(args: list[str] | None = None) -> int:  # pylint: disable=too-many-return-statements
    """The main entry point of GCOVR."""
    configure_logging()
    try:
        parser = create_argument_parser()
        cli_options = parser.parse_args(args=args)
    except SystemExit as e:
        if e.code != 0:
            raise SanityCheckError("Exitcode must be 0.") from e
        return EXIT_SUCCESS
    except ArgumentError as e:
        sys.stderr.write(f"gcovr: error: {e}\n")
        return EXIT_CMDLINE_ERROR

    if cli_options.version:
        sys.stdout.write(f"gcovr {__version__}\n\n{COPYRIGHT}")
        return EXIT_SUCCESS

    # load the config
    cfg_options = load_config(cli_options)
    options = merge_options_and_set_defaults([cfg_options, cli_options.__dict__])

    # Reconfigure the logging.
    update_logging(options)
    LOGGER.debug("gcovr version %s", __version__)

    # We need to reset the stored information her for our test framework
    GcovProgram.reset()

    if options.sort_branches and options.sort_key not in [
        "uncovered-number",
        "uncovered-percent",
    ]:
        LOGGER.error(
            "the options --sort-branches without '--sort uncovered-number' or '--sort uncovered-percent' doesn't make sense."
        )
        return EXIT_CMDLINE_ERROR

    if options.html_title == "":
        LOGGER.error("an empty --html-title= is not allowed.")
        return EXIT_CMDLINE_ERROR

    for postfix in ["", "line", "branch"]:
        key_medium = "medium_threshold"
        key_high = "high_threshold"
        if postfix:
            key_medium += f"_{postfix}"
            key_high += f"_{postfix}"
        option_medium = f"--{key_medium.replace('_', '-')}"
        option_high = f"--{key_high.replace('_', '-')}"

        if getattr(options, key_medium) == 0:
            LOGGER.error("Value of %s should not be zero.", option_medium)
            return EXIT_CMDLINE_ERROR

        # Inherit the defaults from the global coverage values if not set
        if postfix:
            if getattr(options, key_medium) is None:
                setattr(
                    options,
                    key_medium,
                    options.medium_threshold,
                )
                # To get the correct option in the error message below.
                option_medium = "--medium-threshold"
            if getattr(options, key_high) is None:
                setattr(
                    options,
                    key_high,
                    options.high_threshold,
                )
                # To get the correct option in the error message below.
                option_medium = "--high-threshold"

        if getattr(options, key_medium) > getattr(options, key_high):
            LOGGER.error(
                "Value of %s=%s should be\nlower than or equal to the value of %s=%s.",
                option_medium,
                getattr(options, key_medium),
                option_high,
                getattr(options, key_high),
            )
            return EXIT_CMDLINE_ERROR

    try:
        gcovr_formats.validate_options(options)
    except RuntimeError as exc:
        LOGGER.error("%s", str(exc))
        return EXIT_CMDLINE_ERROR

    options.starting_dir = os.path.abspath(os.getcwd())
    options.root_dir = os.path.abspath(options.root)

    #
    # Setup filters
    #
    # The root filter isn't technically a filter,
    # but is used to turn absolute paths into relative paths
    options.root_filter = re.compile("^" + re.escape(options.root_dir + os.sep))

    def _setup_filter(
        option: str,
        patterns: list[FilterOption],
        default_filter: Filter | None = None,
    ) -> tuple[Filter, ...]:
        """Setup a filter and handle the exception."""
        try:
            filters = list[Filter]()
            if len(patterns):
                filters = [f.build_filter() for f in patterns]
            elif default_filter is not None:
                filters.append(default_filter)
            LOGGER.debug("Filters for %s: (%d)", option, len(filters))
            for f in filters:
                LOGGER.debug(" - %s", f)
            return tuple(filters)
        except re.error as e:
            # mypy is thinking that the pattern can be a byte string therefore we need to explicit use !s.
            # See also discussion https://github.com/gcovr/gcovr/pull/1028#discussion_r1855437452
            raise RuntimeError(
                f"Error setting up filter {option}='{e.pattern!s}': {e}"
            ) from None

    def _setup_pattern(option: str, patterns: list[str]) -> tuple[re.Pattern[str], ...]:
        """Setup a filter and handle the exception."""
        try:
            compiled_patterns = list[re.Pattern[str]]()
            if len(patterns):
                compiled_patterns = [re.compile(p) for p in patterns]
            LOGGER.debug("Patterns for %s: (%d)", option, len(compiled_patterns))
            for p in compiled_patterns:
                LOGGER.debug(" - %s", p)
            return tuple(compiled_patterns)
        except re.error as e:
            # mypy is thinking that the pattern can be a byte string therefore we need to explicit use !s.
            # See also discussion https://github.com/gcovr/gcovr/pull/1028#discussion_r1855437452
            raise RuntimeError(
                f"Error setting up pattern {option}='{e.pattern!s}': {e}"
            ) from None

    try:
        options.include_filter = _setup_filter(
            "--filter", options.include_filter, DirectoryPrefixFilter(options.root_dir)
        )
        options.exclude_filter = _setup_filter("--exclude", options.exclude_filter)
        options.include_search_filter = _setup_filter(
            "--include", options.include_search_filter
        )

        options.gcov_include_filter = _setup_filter(
            "--gcov-filter", options.gcov_include_filter, AlwaysMatchFilter()
        )
        options.gcov_exclude_filter = _setup_filter(
            "--gcov-exclude", options.gcov_exclude_filter
        )
        options.exclude_directory = _setup_filter(
            "--exclude-directory", options.exclude_directory
        )

        options.trace_include_filter = _setup_filter(
            "--trace-include", options.trace_include_filter
        )
        options.trace_exclude_filter = _setup_filter(
            "--trace-exclude", options.trace_exclude_filter
        )

        options.exclude_function = _setup_pattern(
            "--exclude-function",
            [
                p[1:-1] if p[0] == "/" and p[-1] == "/" else re.escape(p)
                for p in options.exclude_function
            ],
        )
        options.exclude_lines_by_pattern = _setup_pattern(
            "--exclude-lines-by-pattern", options.exclude_lines_by_pattern
        )
        options.exclude_branches_by_pattern = _setup_pattern(
            "--exclude-branches-by-pattern", options.exclude_branches_by_pattern
        )
    except RuntimeError as e:
        LOGGER.error("%s", e)
        return EXIT_CMDLINE_ERROR

    if options.fail_under_decision > 0.0 and not options.show_decision:
        LOGGER.error("--fail-under-decision need also option --decision.")
        return EXIT_CMDLINE_ERROR

    if options.show_decision:
        LOGGER.info(
            "Attention, the decision analysis is experimental. "
            "It uses a fragile heuristic and depends on the code format."
        )

    LOGGER.info("Reading coverage data...")
    try:
        covdata = gcovr_formats.read_reports(options)
    except Workers.WorkerThreadException as exc:
        LOGGER.error("Error occurred while reading reports: %s", exc)
        return EXIT_READ_ERROR
    except Exception:  # pylint: disable=broad-exception-caught
        LOGGER.error(
            "Error occurred while reading reports:\n%s", traceback.format_exc()
        )
        return EXIT_READ_ERROR

    LOGGER.info("Writing coverage report...")
    try:
        gcovr_formats.write_reports(covdata, options)
    except Exception:  # pylint: disable=broad-exception-caught
        LOGGER.error(
            "Error occurred while printing reports:\n%s", traceback.format_exc()
        )
        return EXIT_WRITE_ERROR

    return get_exit_code(
        covdata,
        options.fail_under_line,
        options.fail_under_branch,
        options.fail_under_decision,
        options.fail_under_function,
    )


if __name__ == "__main__":
    sys.exit(main())
