"""\
Python           _              _
   _ __  ___  __| |___ _ _ _ _ (_)______
  | '  \\/ _ \\/ _` / -_) '_| ' \\| |_ / -_)
  |_|_|_\\___/\\__,_\\___|_| |_||_|_/__\\___|\
"""


import logging
import optparse
import os
import sys

from fissix import refactor
from fissix.main import StdoutRefactoringTool, warn

from libmodernize import __version__
from libmodernize.fixes import fissix_fix_names, opt_in_fix_names, six_fix_names

usage = (
    __doc__
    + """\
 %s

Usage: modernize [options] file|dir ...
"""
    % __version__
)


def format_usage(usage):
    """Method that doesn't output "Usage:" prefix"""
    return usage


def main(args=None):
    """Main program.

    Returns a suggested exit status (0, 1, 2).
    """
    # Set up option parser
    parser = optparse.OptionParser(usage=usage, version="modernize %s" % __version__)
    parser.formatter.format_usage = format_usage
    parser.add_option(
        "-v", "--verbose", action="store_true", help="Show more verbose logging."
    )
    parser.add_option(
        "--no-diffs", action="store_true", help="Don't show diffs of the refactoring."
    )
    parser.add_option(
        "-l", "--list-fixes", action="store_true", help="List standard transformations."
    )
    parser.add_option(
        "-d", "--doctests_only", action="store_true", help="Fix up doctests only."
    )
    parser.add_option(
        "-f",
        "--fix",
        action="append",
        default=[],
        help="Each FIX specifies a transformation; "
        "'-f default' includes default fixers.",
    )
    parser.add_option(
        "--fixers-here",
        action="store_true",
        help="Add current working directory to python path (so fixers can be found)",
    )
    parser.add_option(
        "-j",
        "--processes",
        action="store",
        default=1,
        type="int",
        help="Run fissix concurrently.",
    )
    parser.add_option(
        "-x",
        "--nofix",
        action="append",
        default=[],
        help="Prevent a fixer from being run.",
    )
    parser.add_option(
        "-p",
        "--print-function",
        action="store_true",
        help="Modify the grammar so that print() is a function.",
    )
    parser.add_option(
        "-w", "--write", action="store_true", help="Write back modified files."
    )
    parser.add_option(
        "-n",
        "--nobackups",
        action="store_true",
        default=False,
        help="Don't write backups for modified files.",
    )
    parser.add_option(
        "--six-unicode",
        action="store_true",
        default=False,
        help="Wrap unicode literals in six.u().",
    )
    parser.add_option(
        "--future-unicode",
        action="store_true",
        default=False,
        help="Use 'from __future__ import unicode_literals'"
        "(only useful for Python 2.6+).",
    )
    parser.add_option(
        "--no-six",
        action="store_true",
        default=False,
        help="Exclude fixes that depend on the six package.",
    )
    parser.add_option(
        "--enforce",
        action="store_true",
        default=False,
        help="Returns non-zero exit code if any fixers had to be applied.  "
        "Useful for enforcing Python 3 compatibility.",
    )

    fixer_pkg = "libmodernize.fixes"
    avail_fixes = set(refactor.get_fixers_from_package(fixer_pkg))
    avail_fixes.update(fissix_fix_names)

    # Parse command line arguments
    refactor_stdin = False
    flags = {}
    options, args = parser.parse_args(args)
    if not options.write and options.no_diffs:
        warn("Not writing files and not printing diffs; that's not very useful.")
    if not options.write and options.nobackups:
        parser.error("Can't use '-n' without '-w'.")
    if options.list_fixes:
        print(
            "Standard transformations available for the "
            "-f/--fix and -x/--nofix options:"
        )
        for fixname in sorted(avail_fixes):
            print("    {}  ({})".format(fixname, fixname.split(".fix_", 1)[1]))
        print()
        if not args:
            return 0
    if not args:
        print("At least one file or directory argument required.", file=sys.stderr)
        print("Use --help to show usage.", file=sys.stderr)
        return 2
    if "-" in args:
        refactor_stdin = True
        if options.write:
            print("Can't write to stdin.", file=sys.stderr)
            return 2
    if options.print_function:
        flags["print_function"] = True
    if options.fixers_here:
        sys.path.append(os.getcwd())

    # Set up logging handler
    level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(format="%(name)s: %(message)s", level=level)

    # Initialize the refactoring tool
    unwanted_fixes = set()
    splitfixes = []
    for fix in options.nofix:
        splitfixes.extend(fix.split(","))
    for fix in splitfixes:
        matched = None
        for tgt in avail_fixes:
            if tgt == fix or tgt.endswith(f".fix_{fix}"):
                matched = tgt
                unwanted_fixes.add(matched)
                break
        else:
            print(f"Error: fix '{fix}' was not found", file=sys.stderr)
            return 2

    default_fixes = avail_fixes.difference(opt_in_fix_names)

    # Remove unicode fixers depending on command line options
    if options.six_unicode:
        unwanted_fixes.add("libmodernize.fixes.fix_unicode_future")
    elif options.future_unicode:
        unwanted_fixes.add("libmodernize.fixes.fix_unicode")
    else:
        unwanted_fixes.add("libmodernize.fixes.fix_unicode")
        unwanted_fixes.add("libmodernize.fixes.fix_unicode_future")

    if options.no_six:
        unwanted_fixes.update(six_fix_names)

    explicit = set()
    if options.fix:
        default_present = False
        splitfixes = []
        for fix in options.fix:
            splitfixes.extend(fix.split(","))
        for fix in splitfixes:
            if fix == "default":
                default_present = True
            else:
                matched = None
                for tgt in avail_fixes:
                    if tgt == fix or tgt.endswith(f".fix_{fix}"):
                        matched = tgt
                        explicit.add(matched)
                        break
                else:
                    # A non-standard fix -- trust user to have supplied path
                    explicit.add(fix)
        requested = default_fixes.union(explicit) if default_present else explicit
    else:
        requested = default_fixes
    fixer_names = requested.difference(unwanted_fixes)  # Filter out unwanted fixers
    explicit = explicit.intersection(
        fixer_names
    )  # Filter `explicit` fixers vs remaining fixers

    print(" Loading the following fixers:", file=sys.stderr)
    if fixer_names:
        for fixname in sorted(fixer_names):
            print(
                "    {}  ({})".format(fixname, fixname.split(".fix_", 1)[1]),
                file=sys.stderr,
            )
    else:
        print("    (None)", file=sys.stderr)
    print(" Applying the following explicit transformations:", file=sys.stderr)
    if explicit:
        for fixname in sorted(explicit):
            print(
                "    {}  ({})".format(fixname, fixname.split(".fix_", 1)[1]),
                file=sys.stderr,
            )
    else:
        print("    (None)", file=sys.stderr)
    print(file=sys.stderr)

    # Refactor all files and directories passed as arguments
    rt = StdoutRefactoringTool(
        sorted(fixer_names),
        flags,
        sorted(explicit),
        options.nobackups,
        not options.no_diffs,
    )
    if not rt.errors:
        if refactor_stdin:
            rt.refactor_stdin()
        else:
            try:
                rt.refactor(
                    args, options.write, options.doctests_only, options.processes
                )
            except refactor.MultiprocessingUnsupported:  # pragma: no cover
                assert options.processes > 1
                print("Sorry, -j isn't supported on this platform.", file=sys.stderr)
                return 1
        rt.summarize()

    # Return error status (0 if rt.errors is zero)
    return_code = int(bool(rt.errors))
    # If we are enforcing python 3 compatibility, return a non-zero exit code
    # if we had to modify any files.
    if options.enforce and rt.files:
        return_code |= 2
    return return_code
