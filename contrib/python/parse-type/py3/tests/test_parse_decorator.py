#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=invalid-name, missing-docstring, too-few-public-methods
"""
Integrated into :mod:`parse` module.
"""

from __future__ import absolute_import
import unittest
import parse
from parse_type import build_type_dict
from .parse_type_test import ParseTypeTestCase


# -----------------------------------------------------------------------------
# TEST CASE: TestParseTypeWithPatternDecorator
# -----------------------------------------------------------------------------
class TestParseTypeWithPatternDecorator(ParseTypeTestCase):
    r"""
    Test the pattern decorator for type-converter (parse_type) functions.

        >>> def parse_number(text):
        ...     return int(text)
        >>> parse_number.pattern = r"\d+"

    is equivalent to:

        >>> import parse
        >>> @parse.with_pattern(r"\d+")
        ... def parse_number(text):
        ...     return int(text)

        >>> assert hasattr(parse_number, "pattern")
        >>> assert parse_number.pattern == r"\d+"
    """

    def assert_decorated_with_pattern(self, func, expected_pattern):
        self.assertTrue(callable(func))
        self.assertTrue(hasattr(func, "pattern"))
        self.assertEqual(func.pattern, expected_pattern)

    def assert_converter_call(self, func, text, expected_value):
        value = func(text)
        self.assertEqual(value, expected_value)

    # -- TESTS:
    def test_function_with_pattern_decorator(self):
        @parse.with_pattern(r"\d+")
        def parse_number(text):
            return int(text)

        self.assert_decorated_with_pattern(parse_number, r"\d+")
        self.assert_converter_call(parse_number, "123", 123)

    def test_classmethod_with_pattern_decorator(self):
        choice_pattern = r"Alice|Bob|Charly"
        class C(object):
            @classmethod
            @parse.with_pattern(choice_pattern)
            def parse_choice(cls, text):
                return text

        self.assert_decorated_with_pattern(C.parse_choice, choice_pattern)
        self.assert_converter_call(C.parse_choice, "Alice", "Alice")

    def test_staticmethod_with_pattern_decorator(self):
        choice_pattern = r"Alice|Bob|Charly"
        class S(object):
            @staticmethod
            @parse.with_pattern(choice_pattern)
            def parse_choice(text):
                return text

        self.assert_decorated_with_pattern(S.parse_choice, choice_pattern)
        self.assert_converter_call(S.parse_choice, "Bob", "Bob")

    def test_decorated_function_with_parser(self):
        # -- SETUP:
        @parse.with_pattern(r"\d+")
        def parse_number(text):
            return int(text)

        parse_number.name = "Number" #< For test automation.
        more_types = build_type_dict([parse_number])
        schema = "Test: {number:Number}"
        parser = parse.Parser(schema, more_types)

        # -- PERFORM TESTS:
        # pylint: disable=bad-whitespace
        self.assert_match(parser, "Test: 1",   "number", 1)
        self.assert_match(parser, "Test: 42",  "number", 42)
        self.assert_match(parser, "Test: 123", "number", 123)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Test: x",    "number")  # Not a Number.
        self.assert_mismatch(parser, "Test: -1",   "number")  # Negative.
        self.assert_mismatch(parser, "Test: a, b", "number")  # List of ...

    def test_decorated_classmethod_with_parser(self):
        # -- SETUP:
        class C(object):
            @classmethod
            @parse.with_pattern(r"Alice|Bob|Charly")
            def parse_person(cls, text):
                return text

        more_types = {"Person": C.parse_person}
        schema = "Test: {person:Person}"
        parser = parse.Parser(schema, more_types)

        # -- PERFORM TESTS:
        # pylint: disable=bad-whitespace
        self.assert_match(parser, "Test: Alice", "person", "Alice")
        self.assert_match(parser, "Test: Bob",   "person", "Bob")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Test: ", "person")        # Missing.
        self.assert_mismatch(parser, "Test: BAlice", "person")  # Similar1.
        self.assert_mismatch(parser, "Test: Boby", "person")    # Similar2.
        self.assert_mismatch(parser, "Test: a",    "person")    # INVALID ...

# -----------------------------------------------------------------------------
# MAIN:
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()


# Copyright (c) 2012-2013 by Jens Engel (https://github/jenisys/parse_type)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
