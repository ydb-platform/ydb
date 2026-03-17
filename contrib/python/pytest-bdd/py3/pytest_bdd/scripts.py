"""pytest-bdd scripts."""

import argparse
import os.path
import re

import glob2

from .generation import generate_code, parse_feature_files

MIGRATE_REGEX = re.compile(r"\s?(\w+)\s\=\sscenario\((.+)\)", flags=re.MULTILINE)


def migrate_tests(args):
    """Migrate outdated tests to the most recent form."""
    path = args.path
    for file_path in glob2.iglob(os.path.join(os.path.abspath(path), "**", "*.py")):
        migrate_tests_in_file(file_path)


def migrate_tests_in_file(file_path):
    """Migrate all bdd-based tests in the given test file."""
    try:
        with open(file_path, "r+") as fd:
            content = fd.read()
            new_content = MIGRATE_REGEX.sub(r"\n@scenario(\2)\ndef \1():\n    pass\n", content)
            if new_content != content:
                # the regex above potentially causes the end of the file to
                # have an extra newline
                new_content = new_content.rstrip("\n") + "\n"
                fd.seek(0)
                fd.write(new_content)
                print(f"migrated: {file_path}")
            else:
                print(f"skipped: {file_path}")
    except OSError:
        pass


def check_existense(file_name):
    """Check file or directory name for existence."""
    if not os.path.exists(file_name):
        raise argparse.ArgumentTypeError(f"{file_name} is an invalid file or directory name")
    return file_name


def print_generated_code(args):
    """Print generated test code for the given filenames."""
    features, scenarios, steps = parse_feature_files(args.files)
    code = generate_code(features, scenarios, steps)
    print(code)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(prog="pytest-bdd")
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    parser_generate = subparsers.add_parser("generate", help="generate help")
    parser_generate.add_argument(
        "files",
        metavar="FEATURE_FILE",
        type=check_existense,
        nargs="+",
        help="Feature files to generate test code with",
    )
    parser_generate.set_defaults(func=print_generated_code)

    parser_migrate = subparsers.add_parser("migrate", help="migrate help")
    parser_migrate.add_argument("path", metavar="PATH", help="Migrate outdated tests to the most recent form")
    parser_migrate.set_defaults(func=migrate_tests)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
