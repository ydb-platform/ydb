import sys
from argparse import ArgumentParser
from glob import iglob
from itertools import chain

from . import JUnitXml, version


def merge(paths, output, suite_name=""):
    """Merge XML reports."""
    result = JUnitXml()
    for path in paths:
        result += JUnitXml.fromfile(path)

    result.update_statistics()
    if suite_name:
        result.name = suite_name
    result.write(sys.stdout if output == "-" else output)
    return 0


def verify(paths):
    """Verify if none of the testcases failed or errored."""
    # We could grab the number of failures and errors from the statistics of the root element
    # or from the test suites elements, but those attributes are not guaranteed to be present
    # or correct. So we'll just loop over all the testcases.
    for path in paths:
        xml = JUnitXml.fromfile(path)
        for suite in xml:
            for case in suite:
                if not case.is_passed and not case.is_skipped:
                    return 1
    return 0


def _parser(prog_name=None):  # pragma: no cover
    """Create the CLI arg parser."""
    parser = ArgumentParser(description="Junitparser CLI helper.", prog=prog_name)

    parser.add_argument(
        "-v", "--version", action="version", version="%(prog)s " + version
    )

    command_parser = parser.add_subparsers(dest="command", help="command")
    command_parser.required = True

    # an abstract object that defines common arguments used by multiple commands
    abstract_parser = ArgumentParser(add_help=False)
    abstract_parser.add_argument(
        "--glob",
        help="Treat original XML path(s) as glob(s).",
        dest="paths_are_globs",
        action="store_true",
        default=False,
    )
    abstract_parser.add_argument("paths", help="Original XML path(s).", nargs="+")

    # command: merge
    merge_parser = command_parser.add_parser(
        "merge",
        help="Merge JUnit XML format reports with junitparser.",
        parents=[abstract_parser],
    )
    merge_parser.add_argument(
        "output", help='Merged XML Path, setting to "-" will output to the console'
    )
    merge_parser.add_argument(
        "--suite-name",
        help="Name added to <testsuites>.",
    )

    # command: verify
    verify_parser = command_parser.add_parser(  # noqa: F841
        "verify",
        help="Return a non-zero exit code if one of the testcases failed or errored.",
        parents=[abstract_parser],
    )

    return parser


def main(args=None, prog_name=None):
    """CLI's main runner."""
    args = _parser(prog_name=prog_name).parse_args(args)
    paths = (
        chain.from_iterable(iglob(path) for path in args.paths)
        if args.paths_are_globs
        else args.paths
    )
    if args.command == "merge":
        return merge(paths, args.output, args.suite_name)
    if args.command == "verify":
        return verify(paths)
    return 255
