"""
Program entry point for sarif-tools on the command line.
"""

import argparse
import os
import sys

from sarif import loader, sarif_file, __version__ as SARIF_TOOLS_PACKAGE_VERSION
from sarif.filter.general_filter import load_filter_file

from sarif.operations import (
    blame_op,
    codeclimate_op,
    copy_op,
    csv_op,
    diff_op,
    html_op,
    emacs_op,
    info_op,
    ls_op,
    summary_op,
    trend_op,
    upgrade_filter_op,
    word_op,
)


def main():
    """
    Entry point function.
    """
    args, unknown_args = ARG_PARSER.parse_known_args()

    if args.debug:
        _print_version()
        print(f"Running code from {__file__}")
        known_args_summary = ", ".join(
            f"{key}={getattr(args, key)}" for key in vars(args)
        )
        print(f"Known arguments: {known_args_summary}")
        if args.version:
            return 0
    elif args.version:
        _print_version()
        return 0

    if unknown_args:
        if any(
            unknown_arg.startswith("--blame-filter")
            or unknown_arg.startswith("-b=")
            or unknown_arg == "-b"
            for unknown_arg in unknown_args
        ):
            print("ERROR: --blame-filter was removed in v2.0.0.")
            print(
                "Run the upgrade-filter command to convert your blame filter to the new filter format, then pass via --filter option."
            )
        args = ARG_PARSER.parse_args()

    exitcode = args.func(args)
    return exitcode


def _create_arg_parser():
    cmd_list = "commands:\n"
    max_cmd_length = max(len(cmd) for cmd in _COMMANDS)
    col_width = max_cmd_length + 2
    for cmd, cmd_attributes in _COMMANDS.items():
        cmd_list += cmd.ljust(col_width) + cmd_attributes["desc"] + "\n"
    cmd_list += "Run `sarif <COMMAND> --help` for command-specific help."
    parser = argparse.ArgumentParser(
        prog="sarif",
        description="Process sets of SARIF files",
        epilog=cmd_list,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.set_defaults(func=_usage_command)
    subparsers = parser.add_subparsers(dest="command", help="command")
    subparser = {}
    for cmd, cmd_attributes in _COMMANDS.items():
        subparser[cmd] = subparsers.add_parser(cmd, description=cmd_attributes["desc"])
        subparser[cmd].set_defaults(func=cmd_attributes["fn"])

    # Common options
    parser.add_argument("--version", "-v", action="store_true")
    parser.add_argument(
        "--debug", action="store_true", help="Print information useful for debugging"
    )
    parser.add_argument(
        "--check",
        "-x",
        type=str,
        choices=sarif_file.SARIF_SEVERITIES_WITH_NONE,
        help="Exit with error code if there are any issues of the specified level "
        + "(or for diff, an increase in issues at that level).",
    )

    for cmd in [
        "blame",
        "codeclimate",
        "csv",
        "html",
        "emacs",
        "summary",
        "word",
        "upgrade-filter",
    ]:
        subparser[cmd].add_argument(
            "--output", "-o", type=str, metavar="PATH", help="Output file or directory"
        )
    for cmd in ["copy", "diff", "info", "ls", "trend", "usage"]:
        subparser[cmd].add_argument(
            "--output", "-o", type=str, metavar="FILE", help="Output file"
        )

    for cmd in [
        "codeclimate",
        "copy",
        "csv",
        "diff",
        "summary",
        "html",
        "emacs",
        "trend",
        "word",
    ]:
        subparser[cmd].add_argument(
            "--filter",
            "-b",
            type=str,
            metavar="FILE",
            help="Specify the filter file to apply.  See README for format.",
        )

    # Command-specific options
    subparser["blame"].add_argument(
        "--code",
        "-c",
        metavar="PATH",
        type=str,
        help="Path to git repository; if not specified, the current working directory is used",
    )
    subparser["copy"].add_argument(
        "--timestamp",
        "-t",
        action="store_true",
        help='Append current timestamp to output filename in the "yyyymmddThhmmssZ" format used by '
        "the `sarif trend` command",
    )
    # codeclimate and csv default to no trimming
    for cmd in ["codeclimate", "csv"]:
        subparser[cmd].add_argument(
            "--autotrim",
            "-a",
            action="store_true",
            help="Strip off the common prefix of paths in the CSV output",
        )
    # word and html default to trimming
    for cmd in ["html", "emacs", "word"]:
        subparser[cmd].add_argument(
            "--no-autotrim",
            "-n",
            action="store_true",
            help="Do not strip off the common prefix of paths in the output document",
        )
        subparser[cmd].add_argument(
            "--image",
            type=str,
            help="Image to include at top of file - SARIF logo by default",
        )
    # codeclimate, csv, html and word allow trimmable paths to be specified
    for cmd in ["codeclimate", "csv", "word", "html", "emacs"]:
        subparser[cmd].add_argument(
            "--trim",
            metavar="PREFIX",
            action="append",
            type=str,
            help="Prefix to strip from issue paths, e.g. the checkout directory on the build agent",
        )
    # Most commands take an arbitrary list of SARIF files or directories
    for cmd in _COMMANDS:
        if cmd not in ["diff", "upgrade-filter", "usage", "version"]:
            subparser[cmd].add_argument(
                "files_or_dirs",
                metavar="file_or_dir",
                type=str,
                nargs="*",
                default=["."],
                help="A SARIF file or a directory containing SARIF files",
            )
    subparser["diff"].add_argument(
        "old_file_or_dir",
        type=str,
        nargs=1,
        help="An old SARIF file or a directory containing the old SARIF files",
    )
    subparser["diff"].add_argument(
        "new_file_or_dir",
        type=str,
        nargs=1,
        help="A new SARIF file or a directory containing the new SARIF files",
    )

    subparser["trend"].add_argument(
        "--dateformat",
        "-f",
        type=str,
        choices=["dmy", "mdy", "ymd"],
        default="dmy",
        help="Date component order to use in output CSV.  Default is `dmy`",
    )

    subparser["upgrade-filter"].add_argument(
        "files_or_dirs",
        metavar="file",
        type=str,
        nargs="*",
        default=["."],
        help="A v1-style blame-filter file",
    )

    return parser


def _check(input_files: sarif_file.SarifFileSet, check_level):
    ret = 0
    if check_level:
        for severity in sarif_file.SARIF_SEVERITIES_WITH_NONE:
            ret += input_files.get_report().get_issue_count_for_severity(severity)
            if severity == check_level:
                break
    if ret > 0:
        sys.stderr.write(
            f"Check: exiting with return code {ret} due to issues at or above {check_level} "
            "severity\n"
        )
    return ret


def _init_filtering(input_files, args):
    if args.filter:
        filters = load_filter_file(args.filter)
        input_files.init_general_filter(*filters)


def _init_path_prefix_stripping(input_files, args, strip_by_default):
    if strip_by_default:
        autotrim = not args.no_autotrim
    else:
        autotrim = args.autotrim
    trim_paths = args.trim
    if autotrim or trim_paths:
        input_files.init_path_prefix_stripping(autotrim, trim_paths)


def _ensure_dir(dir_path):
    """
    Create directory if it does not exist
    """
    if dir_path and not os.path.isdir(dir_path):
        os.makedirs(dir_path)


def _prepare_output(
    input_files: sarif_file.SarifFileSet, output_arg, output_file_extension: str
):
    """
    Returns (output, output_multiple_files)
    output is args.output, or if that wasn't specified, a default output file based on the inputs
    and the file extension.
    output_multiple_files determines whether to output one file per input plus a totals file.
    It is false if there is only one input file, or args.output is a file that exists,
    or args.output ends with the expected file extension.
    """
    input_file_count = len(input_files)
    if input_file_count == 0:
        return ("static_analysis_output" + output_file_extension, False)
    if input_file_count == 1:
        derived_output_filename = (
            input_files[0].get_file_name_without_extension() + output_file_extension
        )
        if output_arg:
            if os.path.isdir(output_arg):
                return (os.path.join(output_arg, derived_output_filename), False)
            _ensure_dir(os.path.dirname(output_arg))
            return (output_arg, False)
        return (derived_output_filename, False)
    # Multiple input files
    if output_arg:
        if os.path.isfile(output_arg) or output_arg.strip().upper().endswith(
            output_file_extension.upper()
        ):
            # Output single file, even though there are multiple input files.
            _ensure_dir(os.path.dirname(output_arg))
            return (output_arg, False)
        _ensure_dir(output_arg)
        return (output_arg, True)
    return (os.getcwd(), True)


####################################### Command handlers #######################################


def _blame_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".sarif")
    blame_op.enhance_with_blame(
        input_files, args.code or os.getcwd(), output, multiple_file_output
    )
    return _check(input_files, args.check)


def _codeclimate_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_path_prefix_stripping(input_files, args, strip_by_default=False)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".json")
    codeclimate_op.generate(input_files, output, multiple_file_output)
    return _check(input_files, args.check)


def _copy_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    _init_filtering(input_files, args)
    output = args.output or "out.sarif"
    output_sarif_file_set = copy_op.generate_sarif(
        input_files,
        output,
        args.timestamp,
        SARIF_TOOLS_PACKAGE_VERSION,
        " ".join(sys.argv),
    )
    return _check(output_sarif_file_set, args.check)


def _csv_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_path_prefix_stripping(input_files, args, strip_by_default=False)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".csv")
    csv_op.generate_csv(input_files, output, multiple_file_output)
    return _check(input_files, args.check)


def _diff_command(args):
    old_sarif = loader.load_sarif_files(args.old_file_or_dir[0])
    new_sarif = loader.load_sarif_files(args.new_file_or_dir[0])
    _init_filtering(old_sarif, args)
    _init_filtering(new_sarif, args)
    return diff_op.print_diff(old_sarif, new_sarif, args.output, args.check)


def _html_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_path_prefix_stripping(input_files, args, strip_by_default=True)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".html")
    html_op.generate_html(input_files, args.image, output, multiple_file_output)
    return _check(input_files, args.check)


def _emacs_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_path_prefix_stripping(input_files, args, strip_by_default=True)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".txt")
    emacs_op.generate_compile(input_files, output, multiple_file_output)
    return _check(input_files, args.check)


def _info_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    info_op.generate_info(input_files, args.output)
    if args.check:
        return _check(input_files, args.check)
    return 0


def _ls_command(args):
    ls_op.print_ls(args.files_or_dirs, args.output)
    if args.check:
        input_files = loader.load_sarif_files(*args.files_or_dirs)
        return _check(input_files, args.check)
    return 0


def _summary_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = (None, False)
    if args.output:
        (output, multiple_file_output) = _prepare_output(
            input_files, args.output, ".txt"
        )
    summary_op.generate_summary(input_files, output, multiple_file_output)
    return _check(input_files, args.check)


def _trend_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_filtering(input_files, args)
    if args.output:
        _ensure_dir(os.path.dirname(args.output))
        output = args.output
    else:
        output = "static_analysis_trend.csv"
    trend_op.generate_trend_csv(input_files, output, args.dateformat)
    return _check(input_files, args.check)


def _upgrade_filter_command(args):
    old_filter_files = args.files_or_dirs
    single_output_file = None
    output_dir = None
    if len(old_filter_files) == 1:
        if args.output and os.path.isdir(args.output):
            output_dir = args.output
        else:
            single_output_file = args.output or old_filter_files[0] + ".yaml"
    elif args.output:
        output_dir = args.output
    else:
        output_dir = os.path.dirname(args.output)
    for old_filter_file in old_filter_files:
        output_file = single_output_file or os.path.join(
            output_dir, os.path.basename(old_filter_file) + ".yaml"
        )
        upgrade_filter_op.upgrade_filter_file(old_filter_file, output_file)
    return 0


def _usage_command(args):
    if hasattr(args, "output") and args.output:
        with open(args.output, "w", encoding="utf-8") as file_out:
            ARG_PARSER.print_help(file_out)
        print("Wrote usage instructions to", args.output)
    else:
        ARG_PARSER.print_help()
    if args.check:
        sys.stderr.write("Spurious --check argument")
        return 1
    return 0


def _version_command(args):
    _print_version(not args.version)


def _print_version(bare=False):
    print(
        SARIF_TOOLS_PACKAGE_VERSION
        if bare
        else f"SARIF tools v{SARIF_TOOLS_PACKAGE_VERSION}"
    )


def _word_command(args):
    input_files = loader.load_sarif_files(*args.files_or_dirs)
    input_files.init_default_line_number_1()
    _init_path_prefix_stripping(input_files, args, strip_by_default=True)
    _init_filtering(input_files, args)
    (output, multiple_file_output) = _prepare_output(input_files, args.output, ".docx")
    word_op.generate_word_docs_from_sarif_inputs(
        input_files, args.image, output, multiple_file_output
    )
    return _check(input_files, args.check)


_COMMANDS = {
    "blame": {
        "fn": _blame_command,
        "desc": "Enhance SARIF file with information from `git blame`",
    },
    "codeclimate": {
        "fn": _codeclimate_command,
        "desc": "Write a JSON representation in Code Climate format of SARIF file(s) "
        "for viewing as a Code Quality report in GitLab UI",
    },
    "copy": {
        "fn": _copy_command,
        "desc": "Write a new SARIF file containing optionally-filtered data from other SARIF file(s)",
    },
    "csv": {
        "fn": _csv_command,
        "desc": "Write a CSV file listing the issues from the SARIF files(s) specified",
    },
    "diff": {
        "fn": _diff_command,
        "desc": "Find the difference between two [sets of] SARIF files",
    },
    "emacs": {
        "fn": _emacs_command,
        "desc": "Write a representation of SARIF file(s) for viewing in emacs",
    },
    "html": {
        "fn": _html_command,
        "desc": "Write an HTML representation of SARIF file(s) for viewing in a web browser",
    },
    "info": {
        "fn": _info_command,
        "desc": "Print information about SARIF file(s) structure",
    },
    "ls": {
        "fn": _ls_command,
        "desc": "List all SARIF files in the directories specified",
    },
    "summary": {
        "fn": _summary_command,
        "desc": "Write a text summary with the counts of issues from the SARIF files(s) specified",
    },
    "trend": {
        "fn": _trend_command,
        "desc": "Write a CSV file with time series data from SARIF files with "
        '"yyyymmddThhmmssZ" timestamps in their filenames',
    },
    "upgrade-filter": {
        "fn": _upgrade_filter_command,
        "desc": "Upgrade a sarif-tools v1-style blame filter file to a v2-style filter YAML file",
    },
    "usage": {
        "fn": _usage_command,
        "desc": "(Command optional) - print usage and exit",
    },
    "version": {"fn": _version_command, "desc": "Print version and exit"},
    "word": {
        "fn": _word_command,
        "desc": "Produce MS Word .docx summaries of the SARIF files specified",
    },
}

ARG_PARSER = _create_arg_parser()
