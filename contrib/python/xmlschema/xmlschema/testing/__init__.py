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
Subpackage with unittest extensions for xmlschema.

Includes common classes and helpers for building test scripts for xmlschema. The main
part is a test factory for creating test cases from lists of paths to XSD or XML files.
The list of cases can be defined within files named "testfiles". These are text files
that contain a list of relative paths to XSD or XML files, that are used to dinamically
build a set of test classes. Each path is followed by a list of options that defines a
custom setting for each test.
"""
from urllib.request import urlopen
from urllib.error import URLError

from ._helpers import iter_nested_items, etree_elements_assert_equal
from ._test_case_classes import XMLSchemaTestCase, XsdValidatorTestCase
from ._builders import make_schema_test_class, make_validation_test_class
from ._factory import get_test_args, xsd_version_number, defuse_data, \
    get_test_program_args_parser, parse_xmlschema_args, run_xmlschema_tests, \
    get_test_line_args_parser, xmlschema_tests_factory
from ._observers import SchemaObserver, ObservedXMLSchema10, ObservedXMLSchema11


def has_network_access(*locations):
    for url in locations:
        try:
            urlopen(url, timeout=10)
        except (URLError, OSError):
            pass
        else:
            return True
    return False


SKIP_REMOTE_TESTS = not has_network_access('https://github.com/')


__all__ = [
    'XsdValidatorTestCase', 'make_schema_test_class', 'make_validation_test_class',
    'get_test_args', 'xsd_version_number', 'defuse_data', 'get_test_program_args_parser',
    'parse_xmlschema_args', 'run_xmlschema_tests', 'get_test_line_args_parser',
    'xmlschema_tests_factory', 'SchemaObserver', 'ObservedXMLSchema10',
    'ObservedXMLSchema11', 'has_network_access', 'iter_nested_items',
    'etree_elements_assert_equal', 'SKIP_REMOTE_TESTS', 'XMLSchemaTestCase'
]
