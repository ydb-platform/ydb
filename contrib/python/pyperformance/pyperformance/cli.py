import argparse
import logging
import os.path
import sys

from pyperf import _hooks

from pyperformance import __version__, _utils, is_dev, is_installed
from pyperformance.commands import (
    cmd_compare,
    cmd_compile,
    cmd_compile_all,
    cmd_list,
    cmd_list_groups,
    cmd_run,
    cmd_show,
    cmd_upload,
    cmd_venv_create,
    cmd_venv_recreate,
    cmd_venv_remove,
    cmd_venv_show,
)


def comma_separated(values):
    values = [value.strip() for value in values.split(",")]
    return list(filter(None, values))


def check_positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("Argument must a be positive integer.")
    return value


def filter_opts(cmd, *, allow_no_benchmarks=False):
    cmd.add_argument("--manifest", help="benchmark manifest file to use")

    cmd.add_argument(
        "-b",
        "--benchmarks",
        metavar="BM_LIST",
        default="<default>",
        help=(
            "Comma-separated list of benchmarks or groups to run.  Can"
            " contain both positive and negative arguments:"
            "  --benchmarks=run_this,also_this,-not_this.  If"
            " there are no positive arguments, we'll run all"
            " benchmarks except the negative arguments. "
            " Otherwise we run only the positive arguments."
        ),
    )
    cmd.set_defaults(allow_no_benchmarks=allow_no_benchmarks)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="pyperformance",
        description=(
            "Compares the performance of baseline_python with"
            " changed_python and prints a report."
        ),
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {__version__}"
    )

    subparsers = parser.add_subparsers(dest="action")
    cmds = []

    # run
    cmd = subparsers.add_parser("run", help="Run benchmarks on the running python")
    cmds.append(cmd)
    cmd.add_argument(
        "-r",
        "--rigorous",
        action="store_true",
        help=("Spend longer running tests to get more accurate results"),
    )
    cmd.add_argument(
        "-f", "--fast", action="store_true", help="Get rough answers quickly"
    )
    cmd.add_argument(
        "--debug-single-value",
        action="store_true",
        help="Debug: fastest mode, only compute a single value",
    )
    cmd.add_argument("-v", "--verbose", action="store_true", help="Print more output")
    cmd.add_argument(
        "-m",
        "--track-memory",
        action="store_true",
        help="Track memory usage. This only works on Linux.",
    )
    cmd.add_argument(
        "--affinity",
        metavar="CPU_LIST",
        default=None,
        help=(
            "Specify CPU affinity for benchmark runs. This "
            "way, benchmarks can be forced to run on a given "
            "CPU to minimize run to run variation."
        ),
    )
    cmd.add_argument(
        "-o",
        "--output",
        metavar="FILENAME",
        help="Run the benchmarks on only one interpreter and "
        "write benchmark into FILENAME. "
        "Provide only baseline_python, not changed_python.",
    )
    cmd.add_argument(
        "--append",
        metavar="FILENAME",
        help="Add runs to an existing file, or create it if it doesn't exist",
    )
    cmd.add_argument(
        "--min-time",
        metavar="MIN_TIME",
        help="Minimum duration in seconds of a single "
        "value, used to calibrate the number of loops",
    )
    cmd.add_argument(
        "--same-loops",
        help="Use the same number of loops as a previous run "
        "(i.e., don't recalibrate). Should be a path to a "
        ".json file from a previous run.",
    )
    cmd.add_argument(
        "--timeout",
        help="Specify a timeout in seconds for a single "
        "benchmark run (default: disabled)",
        type=check_positive,
    )
    hook_names = list(_hooks.get_hook_names())
    cmd.add_argument(
        "--hook",
        action="append",
        choices=hook_names,
        metavar=f"{', '.join(x for x in hook_names if not x.startswith('_'))}",
        help="Apply the given pyperf hook(s) when running each benchmark",
    )
    cmd.add_argument(
        "--warmups",
        type=int,
        default=None,
        help="number of skipped values per run used to warmup the benchmark",
    )
    filter_opts(cmd)

    # show
    cmd = subparsers.add_parser("show", help="Display a benchmark file")
    cmd.add_argument("filename", metavar="FILENAME")

    # compare
    cmd = subparsers.add_parser("compare", help="Compare two benchmark files")
    cmds.append(cmd)
    cmd.add_argument("-v", "--verbose", action="store_true", help="Print more output")
    cmd.add_argument(
        "-O",
        "--output_style",
        metavar="STYLE",
        choices=("normal", "table"),
        default="normal",
        help=(
            "What style the benchmark output should take."
            " Valid options are 'normal' and 'table'."
            " Default is normal."
        ),
    )
    cmd.add_argument(
        "--csv",
        metavar="CSV_FILE",
        action="store",
        default=None,
        help=(
            "Name of a file the results will be written to,"
            " as a three-column CSV file containing minimum"
            " runtimes for each benchmark."
        ),
    )
    cmd.add_argument("baseline_filename", metavar="baseline_file.json")
    cmd.add_argument("changed_filename", metavar="changed_file.json")

    # list
    cmd = subparsers.add_parser("list", help="List benchmarks of the running Python")
    cmds.append(cmd)
    filter_opts(cmd)

    # list_groups
    cmd = subparsers.add_parser(
        "list_groups", help="List benchmark groups of the running Python"
    )
    cmds.append(cmd)
    cmd.add_argument("--manifest", help="benchmark manifest file to use")
    cmd.add_argument("--tags", action="store_true")
    cmd.add_argument("--no-tags", dest="tags", action="store_false")
    cmd.set_defaults(tags=True)

    # compile
    cmd = subparsers.add_parser(
        "compile",
        help="Compile and install CPython and run benchmarks on installed Python",
    )
    cmd.add_argument("config_file", help="Configuration filename")
    cmd.add_argument("revision", help="Python benchmarked revision")
    cmd.add_argument("branch", nargs="?", help="Git branch")
    cmd.add_argument("--patch", help="Patch file")
    cmd.add_argument(
        "-U", "--no-update", action="store_true", help="Don't update the Git repository"
    )
    cmd.add_argument(
        "-T",
        "--no-tune",
        action="store_true",
        help="Don't run 'pyperf system tune' to tune the system for benchmarks",
    )
    cmds.append(cmd)

    # compile_all
    cmd = subparsers.add_parser(
        "compile_all",
        help="Compile and install CPython and run benchmarks "
        "on installed Python on all branches and revisions "
        "of CONFIG_FILE",
    )
    cmd.add_argument("config_file", help="Configuration filename")
    cmds.append(cmd)

    # upload
    cmd = subparsers.add_parser(
        "upload", help="Upload JSON results to a Codespeed website"
    )
    cmd.add_argument("config_file", help="Configuration filename")
    cmd.add_argument("json_file", help="JSON filename")
    cmds.append(cmd)

    # venv
    venv_common = argparse.ArgumentParser(add_help=False)
    venv_common.add_argument("--venv", help="Path to the virtual environment")
    cmd = subparsers.add_parser(
        "venv", parents=[venv_common], help="Actions on the virtual environment"
    )
    cmd.set_defaults(venv_action="show")
    venvsubs = cmd.add_subparsers(dest="venv_action")
    cmd = venvsubs.add_parser("show", parents=[venv_common])
    cmds.append(cmd)
    cmd = venvsubs.add_parser("create", parents=[venv_common])
    filter_opts(cmd, allow_no_benchmarks=True)
    cmds.append(cmd)
    cmd = venvsubs.add_parser("recreate", parents=[venv_common])
    filter_opts(cmd, allow_no_benchmarks=True)
    cmds.append(cmd)
    cmd = venvsubs.add_parser("remove", parents=[venv_common])
    cmds.append(cmd)

    for cmd in cmds:
        cmd.add_argument(
            "--inherit-environ",
            metavar="VAR_LIST",
            type=comma_separated,
            help=(
                "Comma-separated list of environment variable "
                "names that are inherited from the parent "
                "environment when running benchmarking "
                "subprocesses."
            ),
        )
        cmd.add_argument(
            "-p",
            "--python",
            help="Python executable (default: use running Python)",
            default=sys.executable,
        )

    options = parser.parse_args()

    if options.action == "run" and options.debug_single_value:
        options.fast = True

    if not options.action:
        # an action is mandatory
        parser.print_help()
        sys.exit(1)

    if hasattr(options, "python"):
        # Replace "~" with the user home directory
        options.python = os.path.expanduser(options.python)
        # Try to get the absolute path to the binary
        abs_python = os.path.abspath(options.python)
        if not abs_python:
            print(
                "ERROR: Unable to locate the Python executable: %r" % options.python,
                flush=True,
            )
            sys.exit(1)
        options.python = abs_python

    if hasattr(options, "benchmarks"):
        if options.benchmarks == "<NONE>":
            if not options.allow_no_benchmarks:
                parser.error("--benchmarks cannot be empty")
            options.benchmarks = None

    return (parser, options)


def _manifest_from_options(options):
    from pyperformance import _manifest

    return _manifest.load_manifest(options.manifest)


def _benchmarks_from_options(options):
    if not getattr(options, "benchmarks", None):
        return None
    manifest = _manifest_from_options(options)
    return _select_benchmarks(options.benchmarks, manifest)


def _select_benchmarks(raw, manifest):
    from pyperformance import _benchmark_selections

    # Get the raw list of benchmarks.
    entries = raw.lower()

    def parse_entry(o, s):
        return _benchmark_selections.parse_selection(s, op=o)

    parsed = _utils.parse_selections(entries, parse_entry)
    parsed_infos = list(parsed)

    # Disallow negative groups.
    for op, _, kind, parsed in parsed_infos:
        if callable(parsed):
            continue
        name = parsed.name if kind == "benchmark" else parsed
        if name in manifest.groups and op == "-":
            raise ValueError(f"negative groups not supported: -{parsed.name}")

    # Get the selections.
    selected = []
    this_python_version = ".".join(map(str, sys.version_info[:3]))
    for bench in _benchmark_selections.iter_selections(manifest, parsed_infos):
        if isinstance(bench, str):
            logging.warning(f"no benchmark named {bench!r}")
            continue
        # Filter out any benchmarks that can't be run on the Python version we're running
        if this_python_version in bench.python:
            selected.append(bench)

    return selected


def _main():
    if not is_installed():
        # Always require a local checkout to be installed.
        print("ERROR: pyperformance should not be run without installing first")
        if is_dev():
            print("(consider using the dev.py script)")
        sys.exit(1)

    parser, options = parse_args()

    if options.action == "venv":
        from . import _pythoninfo, _venv

        if not options.venv:
            info = _pythoninfo.get_info(options.python)
            root = _venv.get_venv_root(python=info)
        else:
            root = options.venv
            info = None

        action = options.venv_action
        if action == "create":
            benchmarks = _benchmarks_from_options(options)
            cmd_venv_create(options, root, info, benchmarks)
        elif action == "recreate":
            benchmarks = _benchmarks_from_options(options)
            cmd_venv_recreate(options, root, info, benchmarks)
        elif action == "remove":
            cmd_venv_remove(options, root)
        elif action == "show":
            cmd_venv_show(options, root)
        else:
            print(f"ERROR: unsupported venv command action {action!r}")
            parser.print_help()
            sys.exit(1)
    elif options.action == "compile":
        cmd_compile(options)
        sys.exit()
    elif options.action == "compile_all":
        cmd_compile_all(options)
        sys.exit()
    elif options.action == "upload":
        cmd_upload(options)
        sys.exit()
    elif options.action == "show":
        cmd_show(options)
        sys.exit()
    elif options.action == "run":
        benchmarks = _benchmarks_from_options(options)
        cmd_run(options, benchmarks)
    elif options.action == "compare":
        cmd_compare(options)
    elif options.action == "list":
        benchmarks = _benchmarks_from_options(options)
        cmd_list(options, benchmarks)
    elif options.action == "list_groups":
        manifest = _manifest_from_options(options)
        cmd_list_groups(manifest, showtags=options.tags)
    else:
        parser.print_help()
        sys.exit(1)


def main():
    try:
        _main()
    except KeyboardInterrupt:
        print("Benchmark suite interrupted: exit!", flush=True)
        sys.exit(1)
