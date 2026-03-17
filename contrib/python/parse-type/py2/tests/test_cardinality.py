#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test suite to test the :mod:`parse_type.cardinality` module.
"""

from __future__ import absolute_import
from .parse_type_test import ParseTypeTestCase, parse_number
from parse_type import Cardinality, TypeBuilder, build_type_dict
from parse import Parser
import parse
import unittest

# -----------------------------------------------------------------------------
# TEST CASE: TestCardinality
# -----------------------------------------------------------------------------
class TestCardinality(ParseTypeTestCase):

    def test_enum_basics(self):
        assert Cardinality.optional is Cardinality.zero_or_one
        assert Cardinality.many0 is Cardinality.zero_or_more
        assert Cardinality.many  is Cardinality.one_or_more

    def check_pattern_for_cardinality_one(self, pattern, new_pattern):
        expected_pattern = Cardinality.one.make_pattern(pattern)
        self.assertEqual(pattern, new_pattern)
        self.assertEqual(new_pattern, expected_pattern)

    def check_pattern_for_cardinality_zero_or_one(self, pattern, new_pattern):
        expected_pattern = Cardinality.zero_or_one.schema % pattern
        self.assertNotEqual(pattern, new_pattern)
        self.assertEqual(new_pattern, expected_pattern)

    def check_pattern_for_cardinality_zero_or_more(self, pattern, new_pattern):
        expected_pattern = Cardinality.zero_or_more.make_pattern(pattern)
        self.assertNotEqual(pattern, new_pattern)
        self.assertEqual(new_pattern, expected_pattern)

    def check_pattern_for_cardinality_one_or_more(self, pattern, new_pattern):
        expected_pattern = Cardinality.one_or_more.make_pattern(pattern)
        self.assertNotEqual(pattern, new_pattern)
        self.assertEqual(new_pattern, expected_pattern)

    def check_pattern_for_cardinality_optional(self, pattern, new_pattern):
        expected = Cardinality.optional.make_pattern(pattern)
        self.assertEqual(new_pattern, expected)
        self.check_pattern_for_cardinality_zero_or_one(pattern, new_pattern)

    def check_pattern_for_cardinality_many0(self, pattern, new_pattern):
        expected = Cardinality.many0.make_pattern(pattern)
        self.assertEqual(new_pattern, expected)
        self.check_pattern_for_cardinality_zero_or_more(pattern, new_pattern)

    def check_pattern_for_cardinality_many(self, pattern, new_pattern):
        expected = Cardinality.many.make_pattern(pattern)
        self.assertEqual(new_pattern, expected)
        self.check_pattern_for_cardinality_one_or_more(pattern, new_pattern)

    def test_make_pattern(self):
        data = [
            (Cardinality.one, r"\d+", r"\d+"),
            (Cardinality.one, r"\w+", None),
            (Cardinality.zero_or_one, r"\w+", None),
            (Cardinality.one_or_more, r"\w+", None),
            (Cardinality.optional, "XXX", Cardinality.zero_or_one.make_pattern("XXX")),
            (Cardinality.many0, "XXX", Cardinality.zero_or_more.make_pattern("XXX")),
            (Cardinality.many,  "XXX", Cardinality.one_or_more.make_pattern("XXX")),
        ]
        for cardinality, pattern, expected_pattern in data:
            if expected_pattern is None:
                expected_pattern = cardinality.make_pattern(pattern)
            new_pattern = cardinality.make_pattern(pattern)
            self.assertEqual(new_pattern, expected_pattern)

            name = cardinality.name
            checker = getattr(self, "check_pattern_for_cardinality_%s" % name)
            checker(pattern, new_pattern)

    def test_make_pattern_for_zero_or_one(self):
        patterns  = [r"\d",    r"\d+",    r"\w+",    r"XXX" ]
        expecteds = [r"(\d)?", r"(\d+)?", r"(\w+)?", r"(XXX)?" ]
        for pattern, expected in zip(patterns, expecteds):
            new_pattern = Cardinality.zero_or_one.make_pattern(pattern)
            self.assertEqual(new_pattern, expected)
            self.check_pattern_for_cardinality_zero_or_one(pattern, new_pattern)

    def test_make_pattern_for_zero_or_more(self):
        pattern  = "XXX"
        expected = r"(XXX)?(\s*,\s*(XXX))*"
        new_pattern = Cardinality.zero_or_more.make_pattern(pattern)
        self.assertEqual(new_pattern, expected)
        self.check_pattern_for_cardinality_zero_or_more(pattern, new_pattern)

    def test_make_pattern_for_one_or_more(self):
        pattern  = "XXX"
        expected = r"(XXX)(\s*,\s*(XXX))*"
        new_pattern = Cardinality.one_or_more.make_pattern(pattern)
        self.assertEqual(new_pattern, expected)
        self.check_pattern_for_cardinality_one_or_more(pattern, new_pattern)

    def test_is_many(self):
        is_many_true_valueset = set(
            [Cardinality.zero_or_more, Cardinality.one_or_more])

        for cardinality in Cardinality:
            expected = cardinality in is_many_true_valueset
            self.assertEqual(cardinality.is_many(), expected)


# -----------------------------------------------------------------------------
# TEST CASE: CardinalityTypeBuilderTest
# -----------------------------------------------------------------------------
class CardinalityTypeBuilderTest(ParseTypeTestCase):

    def check_parse_number_with_zero_or_one(self, parse_candidate,
                                            type_name="OptionalNumber"):
        schema = "Optional: {number:%s}" % type_name
        type_dict = {
            "Number":  parse_number,
            type_name: parse_candidate,
        }
        parser = parse.Parser(schema, type_dict)

        # -- PERFORM TESTS:
        self.assert_match(parser, "Optional: ",   "number", None)
        self.assert_match(parser, "Optional: 1",  "number", 1)
        self.assert_match(parser, "Optional: 42", "number", 42)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Optional: x",   "number")  # Not a Number.
        self.assert_mismatch(parser, "Optional: -1",  "number")  # Negative.
        self.assert_mismatch(parser, "Optional: a, b", "number") # List of ...

    def check_parse_number_with_optional(self, parse_candidate,
                                         type_name="OptionalNumber"):
        self.check_parse_number_with_zero_or_one(parse_candidate, type_name)

    def check_parse_number_with_zero_or_more(self, parse_candidate,
                                             type_name="Numbers0"):
        schema = "List: {numbers:%s}" % type_name
        type_dict = {
            type_name: parse_candidate,
        }
        parser = parse.Parser(schema, type_dict)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: ",        "numbers", [ ])
        self.assert_match(parser, "List: 1",       "numbers", [ 1 ])
        self.assert_match(parser, "List: 1, 2",    "numbers", [ 1, 2 ])
        self.assert_match(parser, "List: 1, 2, 3", "numbers", [ 1, 2, 3 ])

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "List: x",  "numbers")  # Not a Number.
        self.assert_mismatch(parser, "List: -1", "numbers")  # Negative.
        self.assert_mismatch(parser, "List: 1,", "numbers")  # Trailing sep.
        self.assert_mismatch(parser, "List: a, b", "numbers") # List of ...

    def check_parse_number_with_one_or_more(self, parse_candidate,
                                            type_name="Numbers"):
        schema = "List: {numbers:%s}" % type_name
        type_dict = {
            "Number":  parse_number,
            type_name: parse_candidate,
        }
        parser = parse.Parser(schema, type_dict)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: 1",       "numbers", [ 1 ])
        self.assert_match(parser, "List: 1, 2",    "numbers", [ 1, 2 ])
        self.assert_match(parser, "List: 1, 2, 3", "numbers", [ 1, 2, 3 ])

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "List: ",   "numbers")  # Zero items.
        self.assert_mismatch(parser, "List: x",  "numbers")  # Not a Number.
        self.assert_mismatch(parser, "List: -1", "numbers")  # Negative.
        self.assert_mismatch(parser, "List: 1,", "numbers")  # Trailing sep.
        self.assert_mismatch(parser, "List: a, b", "numbers") # List of ...

    def check_parse_choice_with_optional(self, parse_candidate):
        # Choice (["red", "green", "blue"])
        schema = "Optional: {color:OptionalChoiceColor}"
        parser = parse.Parser(schema, dict(OptionalChoiceColor=parse_candidate))

        # -- PERFORM TESTS:
        self.assert_match(parser, "Optional: ",      "color", None)
        self.assert_match(parser, "Optional: red",   "color", "red")
        self.assert_match(parser, "Optional: green", "color", "green")
        self.assert_match(parser, "Optional: blue",  "color", "blue")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Optional: r",    "color")  # Not a Color.
        self.assert_mismatch(parser, "Optional: redx", "color")  # Similar.
        self.assert_mismatch(parser, "Optional: red, blue", "color") # List of ...


    def check_parse_number_with_many(self, parse_candidate, type_name="Numbers"):
        self.check_parse_number_with_one_or_more(parse_candidate, type_name)

    def check_parse_number_with_many0(self, parse_candidate,
                                      type_name="Numbers0"):
        self.check_parse_number_with_zero_or_more(parse_candidate, type_name)


# -----------------------------------------------------------------------------
# TEST CASE: TestTypeBuilder4Cardinality
# -----------------------------------------------------------------------------
class TestTypeBuilder4Cardinality(CardinalityTypeBuilderTest):

    def test_with_zero_or_one_basics(self):
        parse_opt_number = TypeBuilder.with_zero_or_one(parse_number)
        self.assertEqual(parse_opt_number.pattern, r"(\d+)?")

    def test_with_zero_or_one__number(self):
        parse_opt_number = TypeBuilder.with_zero_or_one(parse_number)
        self.check_parse_number_with_zero_or_one(parse_opt_number)

    def test_with_optional__number(self):
        # -- ALIAS FOR: zero_or_one
        parse_opt_number = TypeBuilder.with_optional(parse_number)
        self.check_parse_number_with_optional(parse_opt_number)

    def test_with_optional__choice(self):
        # -- ALIAS FOR: zero_or_one
        parse_color = TypeBuilder.make_choice(["red", "green", "blue"])
        parse_opt_color = TypeBuilder.with_optional(parse_color)
        self.check_parse_choice_with_optional(parse_opt_color)

    def test_with_zero_or_more_basics(self):
        parse_numbers = TypeBuilder.with_zero_or_more(parse_number)
        self.assertEqual(parse_numbers.pattern, r"(\d+)?(\s*,\s*(\d+))*")

    def test_with_zero_or_more__number(self):
        parse_numbers = TypeBuilder.with_zero_or_more(parse_number)
        self.check_parse_number_with_zero_or_more(parse_numbers)

    def test_with_zero_or_more__choice(self):
        parse_color  = TypeBuilder.make_choice(["red", "green", "blue"])
        parse_colors = TypeBuilder.with_zero_or_more(parse_color)
        parse_colors.name = "Colors0"

        extra_types = build_type_dict([ parse_colors ])
        schema = "List: {colors:Colors0}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: ",           "colors", [ ])
        self.assert_match(parser, "List: green",      "colors", [ "green" ])
        self.assert_match(parser, "List: red, green", "colors", [ "red", "green" ])

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "List: x",  "colors")  # Not a Color.
        self.assert_mismatch(parser, "List: black", "colors")  # Unknown
        self.assert_mismatch(parser, "List: red,",  "colors")  # Trailing sep.
        self.assert_mismatch(parser, "List: a, b",  "colors")  # List of ...

    def test_with_one_or_more_basics(self):
        parse_numbers = TypeBuilder.with_one_or_more(parse_number)
        self.assertEqual(parse_numbers.pattern, r"(\d+)(\s*,\s*(\d+))*")

    def test_with_one_or_more_basics_with_other_separator(self):
        parse_numbers2 = TypeBuilder.with_one_or_more(parse_number, listsep=';')
        self.assertEqual(parse_numbers2.pattern, r"(\d+)(\s*;\s*(\d+))*")

        parse_numbers2 = TypeBuilder.with_one_or_more(parse_number, listsep=':')
        self.assertEqual(parse_numbers2.pattern, r"(\d+)(\s*:\s*(\d+))*")

    def test_with_one_or_more(self):
        parse_numbers = TypeBuilder.with_one_or_more(parse_number)
        self.check_parse_number_with_one_or_more(parse_numbers)

    def test_with_many(self):
        # -- ALIAS FOR: one_or_more
        parse_numbers = TypeBuilder.with_many(parse_number)
        self.check_parse_number_with_many(parse_numbers)

    def test_with_many0(self):
        # -- ALIAS FOR: one_or_more
        parse_numbers = TypeBuilder.with_many0(parse_number)
        self.check_parse_number_with_many0(parse_numbers)

    def test_with_one_or_more_choice(self):
        parse_color  = TypeBuilder.make_choice(["red", "green", "blue"])
        parse_colors = TypeBuilder.with_one_or_more(parse_color)
        parse_colors.name = "Colors"

        extra_types = build_type_dict([ parse_colors ])
        schema = "List: {colors:Colors}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: green",      "colors", [ "green" ])
        self.assert_match(parser, "List: red, green", "colors", [ "red", "green" ])

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "List: ",   "colors")  # Zero items.
        self.assert_mismatch(parser, "List: x",  "colors")  # Not a Color.
        self.assert_mismatch(parser, "List: black", "colors")  # Unknown
        self.assert_mismatch(parser, "List: red,",  "colors")  # Trailing sep.
        self.assert_mismatch(parser, "List: a, b",  "colors")  # List of ...

    def test_with_one_or_more_enum(self):
        parse_color  = TypeBuilder.make_enum({"red": 1, "green":2, "blue": 3})
        parse_colors = TypeBuilder.with_one_or_more(parse_color)
        parse_colors.name = "Colors"

        extra_types = build_type_dict([ parse_colors ])
        schema = "List: {colors:Colors}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: green",      "colors", [ 2 ])
        self.assert_match(parser, "List: red, green", "colors", [ 1, 2 ])

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "List: ",   "colors")  # Zero items.
        self.assert_mismatch(parser, "List: x",  "colors")  # Not a Color.
        self.assert_mismatch(parser, "List: black", "colors")  # Unknown
        self.assert_mismatch(parser, "List: red,",  "colors")  # Trailing sep.
        self.assert_mismatch(parser, "List: a, b",  "colors")  # List of ...

    def test_with_one_or_more_with_other_separator(self):
        parse_numbers2 = TypeBuilder.with_one_or_more(parse_number, listsep=';')
        parse_numbers2.name = "Numbers2"

        extra_types = build_type_dict([ parse_numbers2 ])
        schema = "List: {numbers:Numbers2}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "List: 1",       "numbers", [ 1 ])
        self.assert_match(parser, "List: 1; 2",    "numbers", [ 1, 2 ])
        self.assert_match(parser, "List: 1; 2; 3", "numbers", [ 1, 2, 3 ])

    def test_with_cardinality_one(self):
        parse_number2 = TypeBuilder.with_cardinality(Cardinality.one, parse_number)
        assert parse_number2 is parse_number

    def test_with_cardinality_zero_or_one(self):
        parse_opt_number = TypeBuilder.with_cardinality(
                Cardinality.zero_or_one, parse_number)
        self.check_parse_number_with_zero_or_one(parse_opt_number)

    def test_with_cardinality_zero_or_more(self):
        parse_many0_numbers = TypeBuilder.with_cardinality(
                Cardinality.zero_or_more, parse_number)
        self.check_parse_number_with_zero_or_more(parse_many0_numbers)

    def test_with_cardinality_one_or_more(self):
        parse_many_numbers = TypeBuilder.with_cardinality(
                Cardinality.one_or_more, parse_number)
        self.check_parse_number_with_one_or_more(parse_many_numbers)

    def test_with_cardinality_optional(self):
        parse_opt_number = TypeBuilder.with_cardinality(
                Cardinality.optional, parse_number)
        self.check_parse_number_with_optional(parse_opt_number)

    def test_with_cardinality_many0(self):
        parse_many0_numbers = TypeBuilder.with_cardinality(
                Cardinality.many0, parse_number)
        self.check_parse_number_with_zero_or_more(parse_many0_numbers)

    def test_with_cardinality_many(self):
        parse_many_numbers = TypeBuilder.with_cardinality(
                Cardinality.many, parse_number)
        self.check_parse_number_with_many(parse_many_numbers)

    def test_parse_with_optional_and_named_fields(self):
        parse_opt_number = TypeBuilder.with_optional(parse_number)
        parse_opt_number.name = "Number?"

        type_dict = build_type_dict([parse_opt_number, parse_number])
        schema = "Numbers: {number1:Number?} {number2:Number}"
        parser = parse.Parser(schema, type_dict)

        # -- CASE: Optional number is present
        result = parser.parse("Numbers: 34 12")
        expected = dict(number1=34, number2=12)
        self.assertIsNotNone(result)
        self.assertEqual(result.named, expected)

        # -- CASE: Optional number is missing
        result = parser.parse("Numbers:  12")
        expected = dict(number1=None, number2=12)
        self.assertIsNotNone(result)
        self.assertEqual(result.named, expected)

    def test_parse_with_optional_and_unnamed_fields(self):
        # -- ENSURE: Cardinality.optional.group_count is correct
        # REQUIRES: Parser := parse_type.Parser with group_count support
        parse_opt_number = TypeBuilder.with_optional(parse_number)
        parse_opt_number.name = "Number?"

        type_dict = build_type_dict([parse_opt_number, parse_number])
        schema = "Numbers: {:Number?} {:Number}"
        parser = Parser(schema, type_dict)

        # -- CASE: Optional number is present
        result = parser.parse("Numbers: 34 12")
        expected = (34, 12)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

        # -- CASE: Optional number is missing
        result = parser.parse("Numbers:  12")
        expected = (None, 12)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

    def test_parse_with_many_and_unnamed_fields(self):
        # -- ENSURE: Cardinality.one_or_more.group_count is correct
        # REQUIRES: Parser := parse_type.Parser with group_count support
        parse_many_numbers = TypeBuilder.with_many(parse_number)
        parse_many_numbers.name = "Number+"

        type_dict = build_type_dict([parse_many_numbers, parse_number])
        schema = "Numbers: {:Number+} {:Number}"
        parser = Parser(schema, type_dict)

        # -- CASE:
        result = parser.parse("Numbers: 1, 2, 3 42")
        expected = ([1, 2, 3], 42)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

        result = parser.parse("Numbers: 3 43")
        expected = ([ 3 ], 43)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

    def test_parse_with_many0_and_unnamed_fields(self):
        # -- ENSURE: Cardinality.zero_or_more.group_count is correct
        # REQUIRES: Parser := parse_type.Parser with group_count support
        parse_many0_numbers = TypeBuilder.with_many0(parse_number)
        parse_many0_numbers.name = "Number*"

        type_dict = build_type_dict([parse_many0_numbers, parse_number])
        schema = "Numbers: {:Number*} {:Number}"
        parser = Parser(schema, type_dict)

        # -- CASE: Optional numbers are present
        result = parser.parse("Numbers: 1, 2, 3 42")
        expected = ([1, 2, 3], 42)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

        # -- CASE: Optional numbers are missing := EMPTY-LIST
        result = parser.parse("Numbers:  43")
        expected = ([ ], 43)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))


# class TestParserWithManyTypedFields(ParseTypeTestCase):

    #parse_variant1 = TypeBuilder.make_variant([parse_number, parse_yesno])
    #parse_variant1.name = "Number_or_YesNo"
    #parse_variant2 = TypeBuilder.make_variant([parse_color, parse_person_choice])
    #parse_variant2.name = "Color_or_PersonChoice"
    #TYPE_CONVERTERS = [
    #    parse_number,
    #    parse_yesno,
    #    parse_color,
    #    parse_person_choice,
    #    parse_variant1,
    #    parse_variant2,
    #]
    #
#    def test_parse_with_many_named_fields(self):
#        type_dict = build_type_dict(self.TYPE_CONVERTERS)
#        schema = """\
#Number:   {number:Number}
#YesNo:    {answer:YesNo}
#Color:    {color:Color}
#Person:   {person:PersonChoice}
#Variant1: {variant1:Number_or_YesNo}
#Variant2: {variant2:Color_or_PersonChoice}
#"""
#        parser = parse.Parser(schema, type_dict)
#
#        text = """\
#Number:   12
#YesNo:    yes
#Color:    red
#Person:   Alice
#Variant1: 42
#Variant2: Bob
#"""
#        expected = dict(
#            number=12,
#            answer=True,
#            color=Color.red,
#            person="Alice",
#            variant1=42,
#            variant2="Bob"
#        )
#
#        result = parser.parse(text)
#        self.assertIsNotNone(result)
#        self.assertEqual(result.named, expected)

#    def test_parse_with_many_unnamed_fields(self):
#        type_dict = build_type_dict(self.TYPE_CONVERTERS)
#        schema = """\
#Number:   {:Number}
#YesNo:    {:YesNo}
#Color:    {:Color}
#Person:   {:PersonChoice}
#"""
#        # -- OMIT: XFAIL, due to group_index delta counting => Parser problem.
#        # Variant2: {:Color_or_PersonChoice}
#        # Variant1: {:Number_or_YesNo}
#        parser = parse.Parser(schema, type_dict)
#
#        text = """\
#Number:   12
#YesNo:    yes
#Color:    red
#Person:   Alice
#"""
#        # SKIP: Variant2: Bob
#        # SKIP: Variant1: 42
#        expected = [ 12, True, Color.red, "Alice", ] # -- SKIP: "Bob", 42 ]
#
#        result = parser.parse(text)
#        self.assertIsNotNone(result)
#        self.assertEqual(result.fixed, tuple(expected))



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
