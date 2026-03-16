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

# cspell:ignore getpreferredencoding getfixture caplog

from __future__ import annotations
from argparse import _ArgumentGroup, ArgumentParser, ArgumentTypeError, SUPPRESS
from inspect import isclass
from locale import getpreferredencoding
from typing import Iterable, Any, Callable, TextIO
from dataclasses import dataclass
import datetime
import os
import re


from . import formats
from .exceptions import SanityCheckError
from .logging import LOGGER
from .options import (
    FilterOption,
    GcovrConfigOption,
    GcovrConfigOptionAction,
    GcovrDeprecatedConfigOptionAction,
    NonEmptyFilterOption,
    Options,
    OutputOrDefault,
    check_input_file,
    check_percentage,
    relative_path,
)
from .timestamps import parse_timestamp


def timestamp(value: str) -> datetime.datetime:
    """Get the current timestamp from a given string."""

    try:
        return parse_timestamp(value)
    except ValueError as ex:
        raise ArgumentTypeError(f"{ex}: {value!r}") from None


def source_date_epoch() -> datetime.datetime | None:
    """Load time from SOURCE_DATE_EPOCH, if it exists.
    See: <https://reproducible-builds.org/docs/source-date-epoch/>

    Examples:
    >>> monkeypatch = getfixture("monkeypatch")
    >>> caplog = getfixture("caplog")

    Example: can be empty
    >>> with monkeypatch.context() as mp:
    ...   mp.delenv("SOURCE_DATE_EPOCH", raising=False)
    ...   print(source_date_epoch())
    None

    Example: can contain timestamp
    >>> with monkeypatch.context() as mp:
    ...   mp.setenv("SOURCE_DATE_EPOCH", "1677067226")
    ...   print(source_date_epoch())
    2023-02-22 12:00:26+00:00

    Example: can contain invalid timestamp
    >>> with monkeypatch.context() as mp:
    ...   mp.setenv("SOURCE_DATE_EPOCH", "not a timestamp")
    ...   print(source_date_epoch())
    None
    >>> for m in caplog.messages: print(m)
    Ignoring invalid environment variable SOURCE_DATE_EPOCH='not a timestamp'
    """

    ts = os.environ.get("SOURCE_DATE_EPOCH")

    if ts:
        try:
            return datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc)
        except ValueError:
            LOGGER.warning(
                "Ignoring invalid environment variable SOURCE_DATE_EPOCH=%r",
                ts,
            )

    return None


def argument_parser_setup(
    parser: ArgumentParser, default_group: _ArgumentGroup
) -> None:
    r"""Add all options and groups to the given argparse parser."""

    # setup option groups
    groups = {}
    for group_def in GCOVR_CONFIG_OPTION_GROUPS:
        group = parser.add_argument_group(
            group_def["name"],
            description=group_def["description"],
        )
        groups[group_def["key"]] = group

    # create each option value
    for opt in GCOVR_CONFIG_OPTIONS:
        group = default_group if opt.group is None else groups[opt.group]

        kwargs = dict[str, Any](
            {
                "action": opt.action,
                "const": opt.const,
                "default": SUPPRESS,  # default will be assigned manually
                "help": opt.help,
                "metavar": opt.metavar,
            }
        )

        # To avoid store_const problems, optionally set choices, nargs, type:
        if opt.choices is not None:
            kwargs["choices"] = opt.choices
        if opt.nargs is not None:
            kwargs["nargs"] = opt.nargs
        if opt.type is not None:
            kwargs["type"] = opt.type

        # We only want to set dest and required for non-positional.
        if opt.flags:
            kwargs["dest"] = opt.name
            kwargs["required"] = opt.required  # only meaningful for flags
            group.add_argument(*opt.flags, **kwargs)

            # possibly add a negation flag
            if opt.const_negate is not None:
                kwargs["required"] = False
                kwargs["help"] = SUPPRESS  # don't show separate help entry
                kwargs["const"] = opt.const_negate
                group.add_argument(*opt.negate, **kwargs)

        elif opt.positional:
            group.add_argument(opt.name, **kwargs)

        else:
            raise SanityCheckError("Unexpected option.")


def parse_config_into_dict(
    config_entry_source: Iterable[ConfigEntry],
    all_options: Iterable[GcovrConfigOption] | None = None,
) -> dict[str, Any]:
    """Parse a config file and save the configuration in a dictionary."""
    cfg_dict = dict[str, Any]()

    if all_options is None:
        all_options = GCOVR_CONFIG_OPTIONS

    for cfg_entry in config_entry_source:
        for option in all_options:
            if option.config_keys is not None:
                if cfg_entry.key in option.config_keys:
                    value = _get_value_from_config_entry(cfg_entry, option)
                    _assign_value_to_dict(
                        cfg_dict,
                        value,
                        option,
                        cfg_entry_key=cfg_entry.key,
                        is_single_value=True,
                    )
                    break
        else:
            raise cfg_entry.error("unknown config option") from None

    return cfg_dict


def _get_value_from_config_entry(
    cfg_entry: ConfigEntry,
    option: GcovrConfigOption,
) -> Any:
    def get_boolean(silent_error: bool = False) -> bool | None:
        try:
            return cfg_entry.value_as_bool
        except ValueError:
            if silent_error:
                return None
            raise

    # special case: store_const expects a boolean
    if option.action == "store_const":
        use_const = get_boolean()
    # special case: nargs=? optionally expects a boolean
    elif option.nargs == "?" and option.choices is None:
        use_const = get_boolean(silent_error=True)
    else:
        use_const = None  # marker to continue with parsing

    if use_const is True:
        return option.const
    if use_const is False:
        return option.default
    if use_const is not None:
        raise SanityCheckError("Unexpected entry type.")

    # parse the value
    value: object
    if option.type is bool:
        value = cfg_entry.value_as_bool

    elif option.type is not None:
        if cfg_entry.filename is None:
            raise AssertionError(
                "Conversion function must derive base directory from filename"
            )
        basedir = os.path.dirname(cfg_entry.filename)
        converter = _get_converter_function(option.type, basedir=basedir)

        try:
            value = converter(cfg_entry.value)
        except (ValueError, ArgumentTypeError) as err:
            raise cfg_entry.error(str(err)) from None

    elif option.name == "json_tracefile":  # Special case for patterns
        if cfg_entry.filename is None:
            raise AssertionError(
                "Conversion function must derive base directory from filename"
            )
        basedir = os.path.dirname(cfg_entry.filename)
        value = os.path.join(basedir, cfg_entry.value)
    else:
        value = cfg_entry.value

    # verify choices:
    if option.choices is not None:
        if value not in option.choices:
            raise cfg_entry.error(  # pylint: disable=raising-format-tuple
                "must be one of ({}) but got {!r}",
                ", ".join(repr(choice) for choice in option.choices),
                value,
            )

    return value


def _get_converter_function(
    option_type: Callable[[str], Any],
    *,
    basedir: str,
) -> Callable[[str], Any]:
    """
    Obtain a converter function that corresponds to `option.type`.

    Usually, `option.type` already is that converter function.
    But sometimes, it needs extra arguments that are injected here.
    """

    if isclass(option_type) and issubclass(option_type, FilterOption):
        return lambda value: FilterOption(value, basedir)

    if option_type is check_input_file:
        return lambda value: check_input_file(value, basedir)

    if option_type is relative_path:
        return lambda value: relative_path(value, basedir)

    if option_type is OutputOrDefault:
        return lambda value: OutputOrDefault(value, basedir)

    return option_type


def _assign_value_to_dict(
    namespace: dict[str, Any],
    value: Any,
    option: GcovrConfigOption,
    is_single_value: bool,
    cfg_entry_key: str | None = None,
) -> None:
    if option.action == "append" or option.nargs == "*":
        append_target = namespace.setdefault(option.name, [])
        if is_single_value:
            append_target.append(value)
        else:
            append_target.extend(value)
        return

    if option.action in ("store", "store_const"):
        namespace[option.name] = value
        return

    if not isinstance(option.action, str) and issubclass(
        option.action, GcovrConfigOptionAction
    ):
        option.action(option.flags, option.name).store_config_key(
            namespace,
            value,
            config=cfg_entry_key,
        )
        return

    raise AssertionError(f"Unexpected action for {option.name}: {option.action!r}")


def merge_options_and_set_defaults(
    partial_namespaces: list[dict[str, Any]],
    all_options: list[GcovrConfigOption] | None = None,
) -> Options:
    """Merge all options into the namespace and set the default values for unused options."""
    if not partial_namespaces:
        raise AssertionError("At least one namespace required")

    if all_options is None:
        all_options = GCOVR_CONFIG_OPTIONS

    target = dict[str, Any]()
    for namespace in partial_namespaces:
        for option in all_options:
            if option.name not in namespace:
                continue

            _assign_value_to_dict(
                target, namespace[option.name], option, is_single_value=False
            )

    # if no value was provided, set the default.
    for option in all_options:
        target.setdefault(option.name, option.default)

    return Options(**target)


class UseSortUncoveredNumberAction(GcovrDeprecatedConfigOptionAction):
    """Argparse action to map old option --sort-uncovered to new option --sort=uncovered-number."""

    option = "--sort"
    config = "sort"
    value = "uncovered-number"


class UseSortUncoveredPercentAction(GcovrDeprecatedConfigOptionAction):
    """Argparse action to map old option --sort-percentage to new option --sort=uncovered-percent."""

    option = "--sort"
    config = "sort"
    value = "uncovered-percent"


GCOVR_CONFIG_OPTION_GROUPS = [
    {
        "key": "output_options",
        "name": "Output Options",
        "description": (
            "Gcovr prints a text report by default, but can switch to XML or HTML."
        ),
    },
    {
        "key": "filter_options",
        "name": "Filter Options",
        "description": (
            "Filters decide which files are included in the report. "
            "Any filter must match, and no exclude filter must match. "
            "A filter is a regular expression that matches a path. "
            "Filter paths use forward slashes, even on Windows. "
            "If the filter looks like an absolute path "
            "it is matched against an absolute path. "
            "Otherwise, the filter is matched against a relative path, "
            "where that path is relative to the current directory "
            "or if defined in a configuration file to the directory of the file."
        ),
    },
    {
        "key": "gcov_options",
        "name": "GCOV Options",
        "description": (
            "The 'gcov' tool turns raw coverage files (.gcda and .gcno) "
            "into .gcov files that are then processed by gcovr. "
            "The gcno files are generated by the compiler. "
            "The gcda files are generated when the instrumented program is "
            "executed."
        ),
    },
    {
        "key": "llvm_options",
        "name": "LLVM Options",
        "description": (
            "The 'llvm-profdata' tool turns raw coverage files (.profraw) "
            "into .profdata files which are then exported by 'llvm-cov' into a JSON string. "
        ),
    },
    {
        "key": "gcov_llvm_options",
        "name": "GCOV and LLVM Options",
        "description": ("Options which are applicable for GCOV and LLVM processing."),
    },
]


# Style guide for option descriptions:
# - Prefer complete sentences.
# - Phrase first sentence as a command:
#   “Print report”, not “Prints report”.
# - Must be readable on the command line,
#   AND parse as reStructured Text.

GCOVR_CONFIG_OPTIONS = [
    GcovrConfigOption(
        "verbose",
        ["-v", "--verbose"],
        help="Print progress messages. Please include this output in bug reports.",
        action="store_true",
    ),
    GcovrConfigOption(
        "no_color",
        ["--no-color"],
        help=(
            "Turn off colored logging."
            " Is also set if environment variable NO_COLOR is present."
            " Ignored if --force-color is used."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "force_color",
        ["--force-color"],
        help=(
            "Force colored logging, this is the default for a terminal."
            " Is also set if environment variable FORCE_COLOR is present."
            " Has precedence over --no-color."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "root",
        ["-r", "--root"],
        help=(
            "The root directory of your source files. "
            "Defaults to '{default!s}', the current directory. "
            "File names are reported relative to this root. "
            "The --root is the default --filter."
        ),
        default=".",
        type=relative_path,
    ),
    GcovrConfigOption(
        "config",
        ["--config"],
        config=False,
        help=(
            "Load that configuration file. "
            "Defaults to gcovr.cfg in the --root directory."
        ),
        type=relative_path,
    ),
    GcovrConfigOption(
        "respect_exclusion_markers",
        ["--no-markers"],
        help=(
            "Turn off exclusion markers. Any exclusion markers "
            "specified in source files will be ignored."
        ),
        action="store_false",
    ),
    GcovrConfigOption(
        "medium_threshold",
        ["--medium-threshold", "--html-medium-threshold"],
        group="output_options",
        type=check_percentage,
        metavar="MEDIUM",
        help=(
            "If the coverage is below MEDIUM, the value is marked "
            "as low coverage in the report. "
            "MEDIUM has to be lower than or equal to value of --high-threshold "
            "and greater than 0. "
            "If MEDIUM is equal to value of --high-threshold the report has "
            "only high and low coverage. Default is {default!s}."
        ),
        default=75.0,
    ),
    GcovrConfigOption(
        "high_threshold",
        ["--high-threshold", "--html-high-threshold"],
        group="output_options",
        type=check_percentage,
        metavar="HIGH",
        help=(
            "If the coverage is below HIGH, the value is marked "
            "as medium coverage in the report. "
            "HIGH has to be greater than or equal to value of --medium-threshold. "
            "If HIGH is equal to value of --medium-threshold the report has "
            "only high and low coverage. Default is {default!s}."
        ),
        default=90.0,
    ),
    GcovrConfigOption(
        "medium_threshold_branch",
        ["--medium-threshold-branch", "--html-medium-threshold-branch"],
        group="output_options",
        metavar="MEDIUM_BRANCH",
        type=check_percentage,
        help="If the coverage is below MEDIUM_BRANCH, the value is marked "
        "as low coverage in the report. "
        "MEDIUM_BRANCH has to be lower than or equal to value of --high-threshold-branch "
        "and greater than 0. "
        "If MEDIUM_BRANCH is equal to value of --medium-threshold-branch the report has "
        "only high and low coverage. Default is taken from --medium-threshold.",
        default=None,
    ),
    GcovrConfigOption(
        "high_threshold_branch",
        ["--high-threshold-branch", "--html-high-threshold-branch"],
        group="output_options",
        type=check_percentage,
        metavar="HIGH_BRANCH",
        help="If the coverage is below HIGH_BRANCH, the value is marked "
        "as medium coverage in the report. "
        "HIGH_BRANCH has to be greater than or equal to value of --medium-threshold-branch. "
        "If HIGH_BRANCH is equal to value of --medium-threshold-branch the report has "
        "only high and low coverage. Default is taken from --high-threshold.",
        default=None,
    ),
    GcovrConfigOption(
        "medium_threshold_line",
        ["--medium-threshold-line", "--html-medium-threshold-line"],
        group="output_options",
        metavar="MEDIUM_LINE",
        type=check_percentage,
        help="If the coverage is below MEDIUM_LINE, the value is marked "
        "as low coverage in the report. "
        "MEDIUM_LINE has to be lower than or equal to value of --high-threshold-line "
        "and greater than 0. "
        "If MEDIUM_LINE is equal to value of --medium-threshold-line the report has "
        "only high and low coverage. Default is taken from --medium-threshold.",
        default=None,
    ),
    GcovrConfigOption(
        "high_threshold_line",
        ["--high-threshold-line", "--html-high-threshold-line"],
        group="output_options",
        type=check_percentage,
        metavar="HIGH_LINE",
        help="If the coverage is below HIGH_LINE, the value is marked "
        "as medium coverage in the report. "
        "HIGH_LINE has to be greater than or equal to value of --medium-threshold-line. "
        "If HIGH_LINE is equal to value of --medium-threshold-line the report has "
        "only high and low coverage. Default is taken from --high-threshold.",
        default=None,
    ),
    GcovrConfigOption(
        "fail_under_line",
        ["--fail-under-line"],
        type=check_percentage,
        metavar="MIN",
        help=(
            "Exit with a status of 2 "
            "if the total line coverage is less than MIN. "
            "Can be ORed with exit status of '--fail-under-branch', "
            "'--fail-under-decision', and '--fail-under-function' option."
        ),
        default=0.0,
    ),
    GcovrConfigOption(
        "fail_under_branch",
        ["--fail-under-branch"],
        type=check_percentage,
        metavar="MIN",
        help=(
            "Exit with a status of 4 "
            "if the total branch coverage is less than MIN. "
            "Can be ORed with exit status of '--fail-under-line', "
            "'--fail-under-decision', and '--fail-under-function' option."
        ),
        default=0.0,
    ),
    GcovrConfigOption(
        "fail_under_decision",
        ["--fail-under-decision"],
        type=check_percentage,
        metavar="MIN",
        help=(
            "Exit with a status of 8 "
            "if the total decision coverage is less than MIN. "
            "Can be ORed with exit status of '--fail-under-line', "
            "'--fail-under-branch', and '--fail-under-function' option."
        ),
        default=0.0,
    ),
    GcovrConfigOption(
        "fail_under_function",
        ["--fail-under-function"],
        type=check_percentage,
        metavar="MIN",
        help=(
            "Exit with a status of 16 "
            "if the total function coverage is less than MIN. "
            "Can be ORed with exit status of '--fail-under-line', "
            "'--fail-under-branch', and '--fail-under-decision' option."
        ),
        default=0.0,
    ),
    GcovrConfigOption(
        "source_encoding",
        ["--source-encoding"],
        help=(
            "Select the source file encoding. "
            "Defaults to the system default encoding ({default!s})."
        ),
        default=getpreferredencoding(),
    ),
    GcovrConfigOption(
        "output",
        ["-o", "--output"],
        group="output_options",
        help=(
            "Print output to this filename. Defaults to stdout. "
            "Individual output formats can override this."
        ),
        type=OutputOrDefault,
        default=None,
    ),
    GcovrConfigOption(
        "show_decision",
        ["--decisions"],
        group="output_options",
        help="Report the decision coverage. For HTML, JSON, and the summary report.",
        action="store_true",
    ),
    GcovrConfigOption(
        "show_calls",
        ["--calls"],
        group="output_options",
        help="Report the calls coverage. For HTML and the summary report.",
        action="store_true",
    ),
    GcovrConfigOption(
        "sort_branches",
        ["--sort-branches"],
        group="output_options",
        help=(
            "Sort entries by branches instead of lines. Can only be used together "
            "with '--sort uncovered-number' or '--sort uncovered-percent'."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "sort_key",
        ["--sort"],
        config="sort",
        group="output_options",
        help=(
            "Sort entries by filename, number or percent of uncovered lines or branches"
            "(if the option --sort-branches is given). "
            "The default order is increasing and can be changed by --sort-reverse. "
            "The secondary sort key (if values are identical) is always the filename (ascending order). "
            "For CSV, HTML, JSON, LCOV and text report."
        ),
        choices=("filename", "uncovered-number", "uncovered-percent"),
        default="filename",
    ),
    GcovrConfigOption(
        "sort_key",
        ["-u", "--sort-uncovered"],
        group="output_options",
        help="Deprecated, please use '--sort uncovered-number' instead.",
        nargs=0,
        action=UseSortUncoveredNumberAction,
    ),
    GcovrConfigOption(
        "sort_key",
        ["-p", "--sort-percentage"],
        group="output_options",
        help="Deprecated, please use '--sort uncovered-percent' instead.",
        nargs=0,
        action=UseSortUncoveredPercentAction,
    ),
    GcovrConfigOption(
        "sort_reverse",
        ["--sort-reverse"],
        config="sort_reverse",
        group="output_options",
        help="Sort entries in reverse order (see --sort).",
        action="store_true",
    ),
    *formats.get_options(),
    GcovrConfigOption(
        "timestamp",
        ["--timestamp"],
        group="output_options",
        help=(
            "Override current time for reproducible reports. "
            "Can use `YYYY-MM-DD hh:mm:ss` or epoch notation. "
            "Used by HTML, Coveralls, and Cobertura reports. "
            "Default is taken from environment variable SOURCE_DATE_EPOCH "
            "(see https://reproducible-builds.org/docs/source-date-epoch) "
            "or current time."
        ),
        type=timestamp,
        default=source_date_epoch() or datetime.datetime.now(),
    ),
    GcovrConfigOption(
        "include_search_filter",
        ["-i", "--include"],
        group="filter_options",
        help=(
            "Include source files that match this filter. "
            "This is to ensure that files are in report even "
            "if no coverage data is found. Files are searched "
            "recursive from the --root directory. "
            "Can be specified multiple times."
        ),
        action="append",
        type=FilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "include_filter",
        ["-f", "--filter"],
        group="filter_options",
        help=(
            "Keep only source files that match this filter. "
            "Can be specified multiple times. "
            "Relative filters are relative to the current working directory "
            "or if defined in a configuration file. "
            "If no filters are provided, defaults to --root."
        ),
        action="append",
        type=FilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "exclude_filter",
        ["-e", "--exclude"],
        group="filter_options",
        help=(
            "Exclude source files that match this filter. "
            "Can be specified multiple times."
        ),
        action="append",
        type=NonEmptyFilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "exclude_directory",
        [
            "--exclude-directory",
            "--gcov-exclude-directory",
            "--gcov-exclude-directories",
            "--exclude-directories",
        ],
        group="filter_options",
        help=(
            "Exclude directories that match this regex "
            "while searching raw coverage files. "
            "Can be specified multiple times."
        ),
        action="append",
        type=NonEmptyFilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "keep_intermediate_files",
        ["-k", "--keep-intermediate-files", "--keep", "--gcov-keep"],
        config="keep-intermediate-files",
        group="gcov_llvm_options",
        help=(
            "Keep gcov/profdata files after processing. "
            "This applies both to files that were generated by gcovr, "
            "or were supplied via the --gcov-use-existing-files/--llvm-use-existing-files option. "
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "delete_input_files",
        ["-d", "--delete-input-files", "--delete", "--gcov-delete"],
        config="delete-input-files",
        group="gcov_llvm_options",
        help="Delete gcda/profraw files after processing, used gcno files are never deleted.",
        action="store_true",
    ),
    GcovrConfigOption(
        "trace_include_filter",
        ["--trace-include"],
        group="filter_options",
        help=(
            "Log output for files that match this filter. "
            "The output is logged without activating verbose mode. "
            "Can be specified multiple times."
        ),
        action="append",
        type=NonEmptyFilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "trace_exclude_filter",
        ["--trace-exclude"],
        group="filter_options",
        help=(
            "Do not log very verbose output for files that match this filter. "
            "Can be specified multiple times."
        ),
        action="append",
        type=NonEmptyFilterOption,
        default=[],
    ),
    GcovrConfigOption(
        "merge_mode_functions",
        ["--merge-mode-functions"],
        metavar="MERGE_MODE",
        group="gcov_llvm_options",
        choices=(
            "strict",
            "merge-use-line-0",
            "merge-use-line-min",
            "merge-use-line-max",
            "separate",
        ),
        default="strict",
        help=(
            "The merge mode for functions coverage from different gcov files for same sourcefile. "
            "Default is '{default!s}'."
        ),
    ),
    GcovrConfigOption(
        "merge_lines",
        ["--merge-lines"],
        group="gcov_options",
        help=(
            "Merge line coverage for same line coming from different functions, "
            "e.g. template instances. The branches, conditions and calls are merged "
            "accordingly."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "exclude_function_lines",
        ["--exclude-function-lines"],
        group="gcov_options",
        help="Exclude coverage from lines defining a function.",
        action="store_true",
    ),
    GcovrConfigOption(
        "exclude_function",
        ["--exclude-function"],
        help=(
            "Exclude coverage of functions. If function starts and end "
            "with '/' it is treated as a regular expression. "
            "This option needs at least GCC 14 with a supported version of "
            "JSON output format."
        ),
        action="append",
        type=str,
        default=[],
    ),
    GcovrConfigOption(
        "exclude_lines_by_pattern",
        ["--exclude-lines-by-pattern"],
        help="Exclude lines that match this regex. The regex must match the start of the line.",
        action="append",
        type=str,
        default=[],
    ),
    GcovrConfigOption(
        "exclude_branches_by_pattern",
        ["--exclude-branches-by-pattern"],
        help="Exclude branches that match this regex. The regex must match the start of the line.",
        action="append",
        type=str,
        default=[],
    ),
    GcovrConfigOption(
        "exclude_pattern_prefix",
        ["--exclude-pattern-prefix"],
        help=(
            "Define the regex prefix used in markers / line exclusions "
            "(i.e ..._EXCL_START, ..._EXCL_START, ..._EXCL_STOP)"
        ),
        type=str,
        default=r"[GL]COVR?",
    ),
    GcovrConfigOption(
        "warn_excluded_lines_with_hits",
        ["--warn-excluded-lines-with-hits"],
        help="Print a warning if a line excluded by comments has a hit counter != 0.",
        action="store_true",
    ),
    GcovrConfigOption(
        "exclude_internal_functions",
        ["--include-internal-functions"],
        group="gcov_options",
        help=(
            "Include function coverage of compiler internal functions "
            "(starting with '__' or '_GLOBAL__sub_I_')."
        ),
        action="store_false",
    ),
    GcovrConfigOption(
        "exclude_unreachable_branches",
        ["--exclude-unreachable-branches"],
        group="gcov_options",
        help=(
            "Remove branch coverage from lines without useful source code "
            "(often, compiler-generated 'dead' code)."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "exclude_noncode_lines",
        ["--exclude-noncode-lines"],
        config="exclude-noncode-lines",
        group="gcov_options",
        help="Remove coverage from lines which seem to be non-code.",
        action="store_true",
        const_negate=False,
    ),
    GcovrConfigOption(
        "exclude_throw_branches",
        ["--exclude-throw-branches"],
        group="gcov_options",
        help=(
            "For branch coverage, remove branches "
            "that the compiler generates for exception handling. "
            "This often leads to more 'sensible' coverage reports."
        ),
        action="store_true",
    ),
    GcovrConfigOption(
        "search_paths",
        config="search-path",
        positional=True,
        nargs="*",
        help=(
            "Search paths for coverage files. "
            "Defaults to --root and --gcov-object-directory. "
            "If path is a file it is used directly."
        ),
        type=relative_path,
    ),
]


CONFIG_HASH_COMMENT = re.compile(r"(?:^|\s+) [#] .* $", re.X)
CONFIG_SEMICOLON_COMMENT = re.compile(r"(?:^|\s+) [;] .* $", re.X)

# kebab-case word, separated from value (rest of line) by "=" with optional space
CONFIG_KV = re.compile(r"^((?=\w)[\w-]+) \s* = \s* (.*) $", re.X)

# "$" followed by word, open brace, or open parenthesis
CONFIG_POSSIBLE_VARIABLE = re.compile(r"[$][\w{(]")


def parse_config_file(
    open_file: TextIO,
    filename: str,
    first_lineno: int = 1,
) -> Iterable[ConfigEntry]:
    r"""
    Parse an ini-style configuration format.

    Yields: ConfigEntry

    Example: basic syntax.

    >>> import io
    >>> cfg = u'''
    ... # this is a comment
    ... key =   value  # trailing comment
    ... # the next line is empty
    ...
    ... key = can have multiple values
    ... another-key =  # can be empty
    ... optional=spaces
    ... '''
    >>> open_file = io.StringIO(cfg[1:])
    >>> for entry in parse_config_file(open_file, 'test.cfg'):
    ...     print(entry)
    test.cfg: 2: key = value
    test.cfg: 5: key = can have multiple values
    test.cfg: 6: another-key = # empty
    test.cfg: 7: optional = spaces
    """

    def error(pattern: str, *args: object, **kwargs: object) -> SyntaxError:
        # pylint: disable=cell-var-from-loop
        message = pattern.format(*args, **kwargs)
        message += f"\non this line: {line}"
        return SyntaxError(": ".join([filename, str(lineno), message]))

    for lineno, line in enumerate(open_file, first_lineno):
        line = line.rstrip()

        # strip (trailing) comments
        line = CONFIG_HASH_COMMENT.sub("", line)

        if CONFIG_SEMICOLON_COMMENT.search(line):
            raise error("semicolon comment ; ... is reserved")

        if line.isspace() or not line:  # skip empty lines
            continue

        match = CONFIG_KV.match(line)
        if not match:
            raise error('expected "key = value" entry')

        key: str = match.group(1).strip()
        value: str = match.group(2)

        if value.startswith('"'):
            raise error('leading quote " is reserved')
        if value.startswith("'"):
            raise error("leading quote ' is reserved")
        if value.endswith("\\"):
            raise error("trailing backslash \\ is reserved")
        if CONFIG_POSSIBLE_VARIABLE.search(value):
            raise error(
                "variable substitution syntax ({example}) is reserved",
                example="${var}, $(var), or $var",
            )

        yield ConfigEntry(key, value, filename=filename, lineno=lineno)


def config_entries_from_dict(
    config: dict[str, Any],
    filename: str,
) -> Iterable[ConfigEntry]:
    r"""
    Generate config entries from a dictionary

    Yields: ConfigEntry

    Example: basic syntax.

    >>> import io
    >>> cfg = {
    ...     'key': ['value', 'can have multiple values'],
    ...     'another-key': '',
    ...     'optional': 'spaces',
    ... }
    >>> for entry in config_entries_from_dict(cfg, 'test.cfg'):
    ...     print(entry)
    test.cfg: ??: key = value
    test.cfg: ??: key = can have multiple values
    test.cfg: ??: another-key = # empty
    test.cfg: ??: optional = spaces
    """

    for key, value in config.items():
        if isinstance(value, list):
            for inner_value in value:
                yield ConfigEntry(key, inner_value, filename=filename)
        else:
            yield ConfigEntry(key, value, filename=filename)


@dataclass
class ConfigEntry:
    """A "key = value" config file entry."""

    key: str
    """The key. There might be other entries with the same key."""

    value: str
    """The un-parsed value."""

    filename: str | None = None
    """Path of the config file, for error messages."""

    lineno: int | None = None
    """Line of the entry in the config file, for error messages."""

    def __str__(self) -> str:
        r"""
        Display the config entry.

        >>> print(ConfigEntry("the-key", "value",
        ...                   filename="foo.cfg", lineno=17))
        foo.cfg: 17: the-key = value
        """
        filename = self.filename or "<config>"
        lineno = self.lineno or "??"
        key = self.key
        value = self.value or "# empty"
        return f"{filename}: {lineno}: {key} = {value}"

    @property
    def value_as_bool(self) -> bool:
        r"""
        The value converted to a boolean.

        >>> ConfigEntry("k", "yes").value_as_bool
        True

        >>> ConfigEntry("k", "no").value_as_bool
        False

        >>> ConfigEntry("k", "foo").value_as_bool
        Traceback (most recent call last):
        ValueError: <config>: ??: k: boolean option must be "yes" or "no"
        """
        if isinstance(self.value, bool):
            return self.value
        value = self.value
        if value == "yes":
            return True
        if value == "no":
            return False
        raise self.error('boolean option must be "yes" or "no"')

    def error(self, pattern: str, *args: object, **kwargs: object) -> ValueError:
        r"""
        Format but NOT RAISE a ValueError.

        >>> entry = ConfigEntry('jobs', 'nun', lineno=3)
        >>> raise entry.error("expected number but got {value!r}")
        Traceback (most recent call last):
        ValueError: <config>: 3: jobs: expected number but got 'nun'
        """
        filename = self.filename or "<config>"
        lineno = str(self.lineno or "??")
        kwargs.update(key=self.key, value=self.value)
        message = pattern.format(*args, **kwargs)
        return ValueError(": ".join([filename, lineno, self.key, message]))
