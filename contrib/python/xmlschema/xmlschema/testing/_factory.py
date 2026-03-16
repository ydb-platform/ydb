#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors
"""
Test factory for creating test cases from lists of paths to XSD or XML files.

The list of cases can be defined within files named "testfiles". These are text files
that contain a list of relative paths to XSD or XML files, that are used to dinamically
build a set of test classes. Each path is followed by a list of options that defines a
custom setting for each test.
"""
import re
import argparse
import os
import fileinput
import logging
import platform
import sys
import unittest

from xmlschema.cli import xsd_version_number, defuse_data
from xmlschema.validators import XMLSchema10, XMLSchema11
from ._observers import ObservedXMLSchema10, ObservedXMLSchema11

logger = logging.getLogger(__file__)


def get_test_args(args_line):
    """Returns the list of arguments from provided text line."""
    try:
        args_line, _ = args_line.split('#', 1)  # Strip optional ending comment
    except ValueError:
        pass
    return re.split(r'(?<!\\) ', args_line.strip())


def get_test_program_args_parser(prog=None):
    """
    Gets an argument parser for building test scripts for schemas and xml files.
    The returned parser has many arguments of unittest's TestProgram plus some
    arguments for selecting testfiles and XML schema options.
    """
    parser = argparse.ArgumentParser(os.path.basename(prog), add_help=True)
    default_testfiles = os.path.join(os.path.dirname(prog), 'test_cases/testfiles')

    # unittest's arguments
    parser.add_argument('-v', '--verbose', dest='verbosity', default=1,
                        action='store_const', const=2, help='Verbose output')
    parser.add_argument('-q', '--quiet', dest='verbosity',
                        action='store_const', const=0, help='Quiet output')
    parser.add_argument('--locals', dest='tb_locals', action='store_true',
                        help='Show local variables in tracebacks')
    parser.add_argument('-f', '--failfast', dest='failfast',
                        action='store_true', help='Stop on first fail or error')
    parser.add_argument('-c', '--catch', dest='catchbreak',
                        action='store_true', help='Catch Ctrl-C and display results so far')
    parser.add_argument('-b', '--buffer', dest='buffer', action='store_true',
                        help='Buffer stdout and stderr during tests')
    parser.add_argument('-k', dest='patterns', action='append', default=list(),
                        help='Only run tests which match the given substring')

    # xmlschema's arguments (removed by the test script)
    parser.add_argument('--lxml', dest='lxml', action='store_true', default=False,
                        help='Check also with lxml.etree.XMLSchema (for XSD 1.0)')
    parser.add_argument('--codegen', action="store_true", default=False,
                        help="Test code generation with XML data bindings module.")
    parser.add_argument('--random', dest='random', action='store_true', default=False,
                        help='Execute the test cases in random order.')
    parser.add_argument('testfiles', type=str, nargs='*', default=default_testfiles,
                        help="Test files containing a list of cases to build and run.")
    return parser


def parse_xmlschema_args(argv=None):
    """Parse CLI arguments removing xmlschema's additional arguments after parsing."""
    if argv is None:
        argv = sys.argv

    prog, args = argv[0], argv[1:]
    parser = get_test_program_args_parser(prog)
    args = parser.parse_args(args)

    # Clean argv of xmlschema arguments and
    _argv = sys.argv.copy()
    argv.clear()
    argv.append(prog)
    for item in _argv[1:]:
        if item.endswith('testfiles'):
            continue
        elif item not in ('--lxml', '--codegen', '--random', '-f',
                          '--failfast', '-c', '--catch', '-b', '--buffer'):
            argv.append(item)

    return args


def run_xmlschema_tests(target=None, args=None):
    if target is not None:
        header_template = "Test xmlschema {} with Python {} on platform {}"
        header = header_template.format(
            target, platform.python_version(), platform.platform()
        )
        print('{0}\n{1}\n{0}'.format("*" * len(header), header))

    if args is None:
        unittest.main()
    else:
        unittest.main(
            argv=sys.argv,
            verbosity=args.verbosity,
            failfast=args.failfast,
            catchbreak=args.catchbreak,
            buffer=args.buffer
        )


def get_test_line_args_parser():
    """Gets an arguments parser for uncommented on not blank "testfiles" lines."""

    parser = argparse.ArgumentParser(add_help=True)
    parser.usage = "TEST_FILE [OPTIONS]\nTry 'TEST_FILE --help' for more information."
    parser.add_argument('filename', metavar='TEST_FILE', type=str,
                        help="Test filename (relative path).")
    parser.add_argument(
        '-L', dest='locations', nargs=2, type=str, default=None, action='append',
        metavar="URI-URL", help="Schema location hint overrides."
    )
    parser.add_argument(
        '--version', dest='version', metavar='VERSION', type=xsd_version_number, default='1.0',
        help="XSD schema version to use for the test case (default is 1.0)."
    )
    parser.add_argument(
        '--errors', type=int, default=0, metavar='NUM',
        help="Number of errors expected (default=0)."
    )
    parser.add_argument(
        '--warnings', type=int, default=0, metavar='NUM',
        help="Number of warnings expected (default=0)."
    )
    parser.add_argument(
        '--inspect', action="store_true", default=False,
        help="Inspect using an observed custom schema class."
    )
    parser.add_argument(
        '--defuse', metavar='(always, remote, never)', type=defuse_data, default='remote',
        help="Define when to use the defused XML data loaders."
    )
    parser.add_argument(
        '--timeout', type=int, default=300, metavar='SEC',
        help="Timeout for fetching resources (default=300)."
    )
    parser.add_argument(
        '--validation-only', action="store_true", default=False,
        help="Skip decode/encode tests on XML data."
    )
    parser.add_argument(
        '--no-pickle', action="store_true", default=False,
        help="Skip pickling/unpickling test on schema (max recursion exceeded)."
    )
    parser.add_argument('--skip-location-loader', action="store_true", default=False,
                        help="Skip test with alternative LocationSchemaLoader.")
    parser.add_argument(
        '--lax-encode', action="store_true", default=False,
        help="Use lax mode on encode checks (for cases where test data uses default or "
             "fixed values or some test data are skipped by wildcards processContents). "
             "Ignored on schema tests."
    )
    parser.add_argument(
        '--debug', action="store_true", default=False,
        help="Activate the debug mode (only the cases with --debug are executed).",
    )
    parser.add_argument(
        '--codegen', action="store_true", default=False,
        help="Test code generation with XML data bindings module. For default "
             "test code generation if the same command option is provided.",
    )
    return parser


def xmlschema_tests_factory(test_class_builder, testfiles, suffix,
                            check_with_lxml=False, codegen=False, verbosity=1):
    """
    Factory function for file based schema/validation cases.

    :param test_class_builder: the test class builder function.
    :param testfiles: a single or a list of testfiles indexes.
    :param suffix: the suffix ('xml' or 'xsd') to consider for cases.
    :param check_with_lxml: if `True` compare with lxml XMLSchema class, \
    reporting anomalies. Works only for XSD 1.0 tests.
    :param codegen: if `True` is provided checks code generation with XML data \
    bindings module for all tests. For default is `False` and code generation \
    is tested only for the cases where the same option is provided.
    :param verbosity: the unittest's verbosity, can be 0, 1 or 2.
    :return: a list of test classes.
    """
    test_classes = {}
    test_num = 0
    debug_mode = False
    line_buffer = []
    test_line_parser = get_test_line_args_parser()

    for line in fileinput.input(testfiles):
        line = line.strip()
        if not line or line[0] == '#':
            if not line_buffer:
                continue
            else:
                raise SyntaxError("Empty continuation at line %d!" % fileinput.filelineno())
        elif '#' in line:
            line = line.split('#', 1)[0].rstrip()

        # Process line continuations
        if line[-1] == '\\':
            line_buffer.append(line[:-1].strip())
            continue
        elif line_buffer:
            line_buffer.append(line)
            line = ' '.join(line_buffer)
            del line_buffer[:]

        test_args = test_line_parser.parse_args(get_test_args(line))
        if test_args.locations is not None:
            test_args.locations = {k.strip('\'"'): v for k, v in test_args.locations}
        if codegen:
            test_args.codegen = True

        test_file = os.path.join(os.path.dirname(fileinput.filename()), test_args.filename)
        if os.path.isdir(test_file):
            logger.debug("Skip %s: is a directory.", test_file)
            continue
        elif os.path.splitext(test_file)[1].lower() != '.%s' % suffix:
            logger.debug("Skip %s: wrong suffix.", test_file)
            continue
        elif not os.path.isfile(test_file):
            logger.error("Skip %s: is not a file.", test_file)
            continue

        test_num += 1

        # Debug mode activation
        if debug_mode:
            if not test_args.debug:
                continue
        elif test_args.debug:
            debug_mode = True
            msg = "Debug mode activated: discard previous %r test classes."
            logger.debug(msg, len(test_classes))
            test_classes.clear()

        if test_args.version == '1.0':
            schema_class = ObservedXMLSchema10 if test_args.inspect else XMLSchema10
            test_class = test_class_builder(
                test_file, test_args, test_num, schema_class, check_with_lxml
            )
        else:
            schema_class = ObservedXMLSchema11 if test_args.inspect else XMLSchema11
            test_class = test_class_builder(
                test_file, test_args, test_num, schema_class, check_with_lxml=False
            )

        test_classes[test_class.__name__] = test_class
        if verbosity == 2:
            print(f"Create case {test_class.__name__} for file {os.path.relpath(test_file)}")

        logger.debug("Add XSD %s test class %r.", test_args.version, test_class.__name__)

    if line_buffer:
        raise ValueError("Not completed line continuation at the end!")

    return test_classes
