# -*- coding: utf-8 -*-

from __future__ import absolute_import
from parse_type import TypeBuilder
from enum import Enum
try:
    import unittest2 as unittest
except ImportError:
    import unittest


# -----------------------------------------------------------------------------
# TEST SUPPORT FOR: TypeBuilder Tests
# -----------------------------------------------------------------------------
# -- PROOF-OF-CONCEPT DATATYPE:
def parse_number(text):
    return int(text)
parse_number.pattern = r"\d+"   # Provide better regexp pattern than default.
parse_number.name = "Number"    # For testing only.

# -- ENUM DATATYPE:
parse_yesno = TypeBuilder.make_enum({
    "yes":  True,   "no":  False,
    "on":   True,   "off": False,
    "true": True,   "false": False,
})
parse_yesno.name = "YesNo"      # For testing only.

# -- ENUM CLASS:
class Color(Enum):
    red = 1
    green = 2
    blue = 3

parse_color = TypeBuilder.make_enum(Color)
parse_color.name = "Color"

# -- CHOICE DATATYPE:
parse_person_choice = TypeBuilder.make_choice(["Alice", "Bob", "Charly"])
parse_person_choice.name = "PersonChoice"      # For testing only.


# -----------------------------------------------------------------------------
# ABSTRACT TEST CASE:
# -----------------------------------------------------------------------------
class TestCase(unittest.TestCase):

    # -- PYTHON VERSION BACKWARD-COMPATIBILTY:
    if not hasattr(unittest.TestCase, "assertIsNone"):
        def assertIsNone(self, obj, msg=None):
            self.assert_(obj is None, msg)

        def assertIsNotNone(self, obj, msg=None):
            self.assert_(obj is not None, msg)


class ParseTypeTestCase(TestCase):
    """
    Common test case base class for :mod:`parse_type` tests.
    """

    def assert_match(self, parser, text, param_name, expected):
        """
        Check that a parser can parse the provided text and extracts the
        expected value for a parameter.

        :param parser: Parser to use
        :param text:   Text to parse
        :param param_name: Name of parameter
        :param expected:   Expected value of parameter.
        :raise: AssertionError on failures.
        """
        result = parser.parse(text)
        self.assertIsNotNone(result)
        self.assertEqual(result[param_name], expected)

    def assert_mismatch(self, parser, text, param_name=None):
        """
        Check that a parser cannot extract the parameter from the provided text.
        A parse mismatch has occured.

        :param parser: Parser to use
        :param text:   Text to parse
        :param param_name: Name of parameter
        :raise: AssertionError on failures.
        """
        result = parser.parse(text)
        self.assertIsNone(result)

    def ensure_can_parse_all_enum_values(self, parser, type_converter,
                                         schema, name):
        # -- ENSURE: Known enum values are correctly extracted.
        for value_name, value in type_converter.mappings.items():
            text = schema % value_name
            self.assert_match(parser, text, name,  value)

    def ensure_can_parse_all_choices(self, parser, type_converter, schema, name):
        transform = getattr(type_converter, "transform", None)
        for choice_value in type_converter.choices:
            text = schema % choice_value
            expected_value = choice_value
            if transform:
                assert callable(transform)
                expected_value = transform(choice_value)
            self.assert_match(parser, text, name,  expected_value)

    def ensure_can_parse_all_choices2(self, parser, type_converter, schema, name):
        transform = getattr(type_converter, "transform", None)
        for index, choice_value in enumerate(type_converter.choices):
            text = schema % choice_value
            if transform:
                assert callable(transform)
                expected_value = (index, transform(choice_value))
            else:
                expected_value = (index, choice_value)
            self.assert_match(parser, text, name, expected_value)



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
