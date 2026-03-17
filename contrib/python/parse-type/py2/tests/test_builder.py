#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test suite  for parse_type.py

REQUIRES: parse >= 1.8.4 ('pattern' attribute support)
"""

from __future__ import absolute_import
import re
import unittest
import parse
from .parse_type_test import ParseTypeTestCase
from .parse_type_test \
    import parse_number, parse_yesno, parse_person_choice, parse_color, Color
from parse_type import TypeBuilder, build_type_dict
from enum import Enum


# -----------------------------------------------------------------------------
# TEST CASE: TestTypeBuilder4Enum
# -----------------------------------------------------------------------------
class TestTypeBuilder4Enum(ParseTypeTestCase):

    TYPE_CONVERTERS = [ parse_yesno ]

    def test_parse_enum_yesno(self):
        extra_types = build_type_dict([ parse_yesno ])
        schema = "Answer: {answer:YesNo}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.ensure_can_parse_all_enum_values(parser,
                parse_yesno, "Answer: %s", "answer")

        # -- VALID:
        self.assert_match(parser, "Answer: yes", "answer", True)
        self.assert_match(parser, "Answer: no",  "answer", False)

        # -- IGNORE-CASE: In parsing, calls type converter function !!!
        self.assert_match(parser, "Answer: YES", "answer", True)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __YES__", "answer")
        self.assert_mismatch(parser, "Answer: yes ",    "answer")
        self.assert_mismatch(parser, "Answer: yes ZZZ", "answer")

    def test_make_enum_with_dict(self):
        parse_nword = TypeBuilder.make_enum({"one": 1, "two": 2, "three": 3})
        parse_nword.name = "NumberAsWord"

        extra_types = build_type_dict([ parse_nword ])
        schema = "Answer: {number:NumberAsWord}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.ensure_can_parse_all_enum_values(parser,
            parse_nword, "Answer: %s", "number")

        # -- VALID:
        self.assert_match(parser, "Answer: one", "number", 1)
        self.assert_match(parser, "Answer: two", "number", 2)

        # -- IGNORE-CASE: In parsing, calls type converter function !!!
        self.assert_match(parser, "Answer: THREE", "number", 3)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __one__", "number")
        self.assert_mismatch(parser, "Answer: one ",    "number")
        self.assert_mismatch(parser, "Answer: one_",    "number")
        self.assert_mismatch(parser, "Answer: one ZZZ", "number")

    def test_make_enum_with_enum_class(self):
        """
        Use :meth:`parse_type.TypeBuilder.make_enum()` with enum34 classes.
        """
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        parse_color = TypeBuilder.make_enum(Color)
        parse_color.name = "Color"
        schema = "Answer: {color:Color}"
        parser = parse.Parser(schema, dict(Color=parse_color))

        # -- PERFORM TESTS:
        self.ensure_can_parse_all_enum_values(parser,
                parse_color, "Answer: %s", "color")

        # -- VALID:
        self.assert_match(parser, "Answer: red",   "color", Color.red)
        self.assert_match(parser, "Answer: green", "color", Color.green)
        self.assert_match(parser, "Answer: blue",  "color", Color.blue)

        # -- IGNORE-CASE: In parsing, calls type converter function !!!
        self.assert_match(parser, "Answer: RED", "color", Color.red)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __RED__", "color")
        self.assert_mismatch(parser, "Answer: red ",    "color")
        self.assert_mismatch(parser, "Answer: redx",    "color")
        self.assert_mismatch(parser, "Answer: redx ZZZ", "color")


# -----------------------------------------------------------------------------
# TEST CASE: TestTypeBuilder4Choice
# -----------------------------------------------------------------------------
class TestTypeBuilder4Choice(ParseTypeTestCase):

    def test_parse_choice_persons(self):
        extra_types = build_type_dict([ parse_person_choice ])
        schema = "Answer: {answer:PersonChoice}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "Answer: Alice", "answer", "Alice")
        self.assert_match(parser, "Answer: Bob",   "answer", "Bob")
        self.ensure_can_parse_all_choices(parser,
                    parse_person_choice, "Answer: %s", "answer")

        # -- IGNORE-CASE: In parsing, calls type converter function !!!
        # SKIP-WART: self.assert_match(parser, "Answer: BOB", "answer", "BOB")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __Alice__", "answer")
        self.assert_mismatch(parser, "Answer: Alice ",    "answer")
        self.assert_mismatch(parser, "Answer: Alice ZZZ", "answer")

    def test_make_choice(self):
        parse_choice = TypeBuilder.make_choice(["one", "two", "three"])
        parse_choice.name = "NumberWordChoice"
        extra_types = build_type_dict([ parse_choice ])
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "Answer: one", "answer", "one")
        self.assert_match(parser, "Answer: two", "answer", "two")
        self.ensure_can_parse_all_choices(parser,
                    parse_choice, "Answer: %s", "answer")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __one__", "answer")
        self.assert_mismatch(parser, "Answer: one ",    "answer")
        self.assert_mismatch(parser, "Answer: one ZZZ", "answer")

    def test_make_choice__anycase_accepted_case_sensitity(self):
        # -- NOTE: strict=False => Disable errors due to case-mismatch.
        parse_choice = TypeBuilder.make_choice(["one", "two", "three"],
                                               strict=False)
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice))

        # -- PERFORM TESTS:
        # NOTE: Parser uses re.IGNORECASE flag => Any case accepted.
        self.assert_match(parser, "Answer: one",   "answer", "one")
        self.assert_match(parser, "Answer: TWO",   "answer", "TWO")
        self.assert_match(parser, "Answer: Three", "answer", "Three")

    def test_make_choice__samecase_match_or_error(self):
        # -- NOTE: strict=True => Enable errors due to case-mismatch.
        parse_choice = TypeBuilder.make_choice(["One", "TWO", "three"],
                                               strict=True)
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice))

        # -- PERFORM TESTS: Case matches.
        # NOTE: Parser uses re.IGNORECASE flag => Any case accepted.
        self.assert_match(parser, "Answer: One",   "answer", "One")
        self.assert_match(parser, "Answer: TWO",   "answer", "TWO")
        self.assert_match(parser, "Answer: three", "answer", "three")

        # -- PERFORM TESTS: EXACT-CASE MISMATCH
        case_mismatch_input_data = ["one", "ONE", "Two", "two", "Three" ]
        for input_value in case_mismatch_input_data:
            input_text = "Answer: %s" % input_value
            with self.assertRaises(ValueError):
                parser.parse(input_text)

    def test_make_choice__anycase_accepted_lowercase_enforced(self):
        # -- NOTE: strict=True => Enable errors due to case-mismatch.
        parse_choice = TypeBuilder.make_choice(["one", "two", "three"],
                            transform=lambda x: x.lower(), strict=True)
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice))

        # -- PERFORM TESTS:
        # NOTE: Parser uses re.IGNORECASE flag
        # => Any case accepted, but result is in lower case.
        self.assert_match(parser, "Answer: one",   "answer", "one")
        self.assert_match(parser, "Answer: TWO",   "answer", "two")
        self.assert_match(parser, "Answer: Three", "answer", "three")

    def test_make_choice__with_transform(self):
        transform = lambda x: x.upper()
        parse_choice = TypeBuilder.make_choice(["ONE", "two", "Three"],
                                               transform)
        self.assertSequenceEqual(parse_choice.choices, ["ONE", "TWO", "THREE"])
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice))

        # -- PERFORM TESTS:
        self.assert_match(parser, "Answer: one", "answer", "ONE")
        self.assert_match(parser, "Answer: two", "answer", "TWO")
        self.ensure_can_parse_all_choices(parser,
                    parse_choice, "Answer: %s", "answer")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __one__", "answer")
        self.assert_mismatch(parser, "Answer: one ",    "answer")
        self.assert_mismatch(parser, "Answer: one ZZZ", "answer")

    def test_make_choice2(self):
        # -- strict=False: Disable errors due to case mismatch.
        parse_choice2 = TypeBuilder.make_choice2(["zero", "one", "two"],
                                                 strict=False)
        parse_choice2.name = "NumberWordChoice2"
        extra_types = build_type_dict([ parse_choice2 ])
        schema = "Answer: {answer:NumberWordChoice2}"
        parser = parse.Parser(schema, extra_types)

        # -- PERFORM TESTS:
        self.assert_match(parser, "Answer: zero", "answer", (0, "zero"))
        self.assert_match(parser, "Answer: one",  "answer", (1, "one"))
        self.assert_match(parser, "Answer: two",  "answer", (2, "two"))
        self.ensure_can_parse_all_choices2(parser,
                parse_choice2, "Answer: %s", "answer")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Answer: __one__", "answer")
        self.assert_mismatch(parser, "Answer: one ",    "answer")
        self.assert_mismatch(parser, "Answer: one ZZZ", "answer")

    def test_make_choice2__with_transform(self):
        transform = lambda x: x.lower()
        parse_choice2 = TypeBuilder.make_choice2(["ZERO", "one", "Two"],
                                        transform=transform)
        self.assertSequenceEqual(parse_choice2.choices, ["zero", "one", "two"])
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice2))

        # -- PERFORM TESTS:
        # NOTE: Parser uses re.IGNORECASE => Any case is accepted.
        self.assert_match(parser, "Answer: zERO", "answer", (0, "zero"))
        self.assert_match(parser, "Answer: ONE", "answer",  (1, "one"))
        self.assert_match(parser, "Answer: Two", "answer",  (2, "two"))

    def test_make_choice2__samecase_match_or_error(self):
        # -- NOTE: strict=True => Enable errors due to case-mismatch.
        parse_choice2 = TypeBuilder.make_choice2(["Zero", "one", "TWO"],
                                                 strict=True)
        schema = "Answer: {answer:NumberWordChoice}"
        parser = parse.Parser(schema, dict(NumberWordChoice=parse_choice2))

        # -- PERFORM TESTS: Case matches.
        # NOTE: Parser uses re.IGNORECASE flag => Any case accepted.
        self.assert_match(parser, "Answer: Zero", "answer", (0, "Zero"))
        self.assert_match(parser, "Answer: one",  "answer", (1, "one"))
        self.assert_match(parser, "Answer: TWO",  "answer", (2, "TWO"))

        # -- PERFORM TESTS: EXACT-CASE MISMATCH
        case_mismatch_input_data = ["zero", "ZERO", "One", "ONE", "two" ]
        for input_value in case_mismatch_input_data:
            input_text = "Answer: %s" % input_value
            with self.assertRaises(ValueError):
                parser.parse(input_text)

# -----------------------------------------------------------------------------
# TEST CASE: TestTypeBuilder4Variant
# -----------------------------------------------------------------------------
class TestTypeBuilder4Variant(ParseTypeTestCase):

    TYPE_CONVERTERS = [ parse_number, parse_yesno ]

    def check_parse_variant_number_or_yesno(self, parse_variant,
                                            with_ignorecase=True):
        schema = "Variant: {variant:YesNo_or_Number}"
        parser = parse.Parser(schema, dict(YesNo_or_Number=parse_variant))

        # -- TYPE 1: YesNo
        self.assert_match(parser, "Variant: yes", "variant", True)
        self.assert_match(parser, "Variant: no",  "variant", False)
        # -- IGNORECASE problem => re_opts
        if with_ignorecase:
            self.assert_match(parser, "Variant: YES", "variant", True)

        # -- TYPE 2: Number
        self.assert_match(parser, "Variant: 0",  "variant",  0)
        self.assert_match(parser, "Variant: 1",  "variant",  1)
        self.assert_match(parser, "Variant: 12", "variant", 12)
        self.assert_match(parser, "Variant: 42", "variant", 42)

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Variant: __YES__")
        self.assert_mismatch(parser, "Variant: yes ")
        self.assert_mismatch(parser, "Variant: yes ZZZ")
        self.assert_mismatch(parser, "Variant: -1")

        # -- PERFORM TESTS:
        self.ensure_can_parse_all_enum_values(parser,
                    parse_yesno, "Variant: %s", "variant")

    def test_make_variant__uncompiled(self):
        type_converters = [parse_yesno, parse_number]
        parse_variant1 = TypeBuilder.make_variant(type_converters)
        self.check_parse_variant_number_or_yesno(parse_variant1)

    def test_make_variant__compiled(self):
        # -- REVERSED ORDER VARIANT:
        type_converters = [parse_number, parse_yesno]
        parse_variant2 = TypeBuilder.make_variant(type_converters,
                                                  compiled=True)
        self.check_parse_variant_number_or_yesno(parse_variant2)


    def test_make_variant__with_re_opts_0(self):
        # -- SKIP: IGNORECASE checks which would raise an error in strict mode.
        type_converters = [parse_number, parse_yesno]
        parse_variant3 = TypeBuilder.make_variant(type_converters, re_opts=0)
        self.check_parse_variant_number_or_yesno(parse_variant3,
                                                 with_ignorecase=False)

    def test_make_variant__with_re_opts_IGNORECASE(self):
        type_converters = [parse_number, parse_yesno]
        parse_variant3 = TypeBuilder.make_variant(type_converters,
                                                  re_opts=re.IGNORECASE)
        self.check_parse_variant_number_or_yesno(parse_variant3)

    def test_make_variant__with_strict(self):
        # -- SKIP: IGNORECASE checks which would raise an error in strict mode.
        type_converters = [parse_number, parse_yesno]
        parse_variant = TypeBuilder.make_variant(type_converters, strict=True)
        self.check_parse_variant_number_or_yesno(parse_variant,
                                                 with_ignorecase=False)

    def test_make_variant__with_strict_raises_error_on_case_mismatch(self):
        # -- NEEDS:
        #  * re_opts=0 (IGNORECASE disabled)
        #  * strict=True, allow that an error is raised
        type_converters = [parse_number, parse_yesno]
        parse_variant = TypeBuilder.make_variant(type_converters,
                                                 strict=True, re_opts=0)
        schema = "Variant: {variant:YesNo_or_Number}"
        parser = parse.Parser(schema, dict(YesNo_or_Number=parse_variant))
        self.assertRaises(AssertionError,  parser.parse, "Variant: YES")

    def test_make_variant__without_strict_may_return_none_on_case_mismatch(self):
        # -- NEEDS:
        #  * re_opts=0 (IGNORECASE disabled)
        #  * strict=False, otherwise an error is raised
        type_converters = [parse_number, parse_yesno]
        parse_variant = TypeBuilder.make_variant(type_converters, re_opts=0,
                                                 strict=False)
        schema = "Variant: {variant:YesNo_or_Number}"
        parser = parse.Parser(schema, dict(YesNo_or_Number=parse_variant))
        result = parser.parse("Variant: No")
        self.assertNotEqual(result, None)
        self.assertEqual(result["variant"], None)

    def test_make_variant__with_strict_and_compiled_raises_error_on_case_mismatch(self):
        # XXX re_opts=0 seems to work differently.
        # -- NEEDS:
        #  * re_opts=0 (IGNORECASE disabled)
        #  * strict=True, allow that an error is raised
        type_converters = [parse_number, parse_yesno]
        # -- ENSURE: coverage for cornercase.
        parse_number.matcher = re.compile(parse_number.pattern)

        parse_variant = TypeBuilder.make_variant(type_converters,
                                        compiled=True, re_opts=0, strict=True)
        schema = "Variant: {variant:YesNo_or_Number}"
        parser = parse.Parser(schema, dict(YesNo_or_Number=parse_variant))
        # XXX self.assertRaises(AssertionError,  parser.parse, "Variant: YES")
        result = parser.parse("Variant: Yes")
        self.assertNotEqual(result, None)
        self.assertEqual(result["variant"], True)

    def test_make_variant__without_strict_and_compiled_may_return_none_on_case_mismatch(self):
        # XXX re_opts=0 seems to work differently.
        # -- NEEDS:
        #  * re_opts=0 (IGNORECASE disabled)
        #  * strict=False, otherwise an error is raised
        type_converters = [parse_number, parse_yesno]
        parse_variant = TypeBuilder.make_variant(type_converters,
                                        compiled=True, re_opts=0, strict=True)
        schema = "Variant: {variant:YesNo_or_Number}"
        parser = parse.Parser(schema, dict(YesNo_or_Number=parse_variant))
        result = parser.parse("Variant: NO")
        self.assertNotEqual(result, None)
        self.assertEqual(result["variant"], False)


    def test_make_variant__with_color_or_person(self):
        type_converters = [parse_color, parse_person_choice]
        parse_variant2 = TypeBuilder.make_variant(type_converters)
        schema = "Variant2: {variant:Color_or_Person}"
        parser = parse.Parser(schema, dict(Color_or_Person=parse_variant2))

        # -- TYPE 1: Color
        self.assert_match(parser, "Variant2: red",  "variant", Color.red)
        self.assert_match(parser, "Variant2: blue", "variant", Color.blue)

        # -- TYPE 2: Person
        self.assert_match(parser, "Variant2: Alice",  "variant", "Alice")
        self.assert_match(parser, "Variant2: Bob",    "variant", "Bob")
        self.assert_match(parser, "Variant2: Charly", "variant", "Charly")

        # -- PARSE MISMATCH:
        self.assert_mismatch(parser, "Variant2: __Alice__")
        self.assert_mismatch(parser, "Variant2: Alice ")
        self.assert_mismatch(parser, "Variant2: Alice2")
        self.assert_mismatch(parser, "Variant2: red2")

        # -- PERFORM TESTS:
        self.ensure_can_parse_all_enum_values(parser,
                    parse_color, "Variant2: %s", "variant")

        self.ensure_can_parse_all_choices(parser,
                    parse_person_choice, "Variant2: %s", "variant")


class TestParserWithManyTypedFields(ParseTypeTestCase):

    parse_variant1 = TypeBuilder.make_variant([parse_number, parse_yesno])
    parse_variant1.name = "Number_or_YesNo"
    parse_variant2 = TypeBuilder.make_variant([parse_color, parse_person_choice])
    parse_variant2.name = "Color_or_PersonChoice"
    TYPE_CONVERTERS = [
        parse_number,
        parse_yesno,
        parse_color,
        parse_person_choice,
        parse_variant1,
        parse_variant2,
    ]

    def test_parse_with_many_named_fields(self):
        type_dict = build_type_dict(self.TYPE_CONVERTERS)
        schema = """\
Number:   {number:Number}
YesNo:    {answer:YesNo}
Color:    {color:Color}
Person:   {person:PersonChoice}
Variant1: {variant1:Number_or_YesNo}
Variant2: {variant2:Color_or_PersonChoice}
"""
        parser = parse.Parser(schema, type_dict)

        text = """\
Number:   12
YesNo:    yes
Color:    red
Person:   Alice
Variant1: 42
Variant2: Bob
"""
        expected = dict(
            number=12,
            answer=True,
            color=Color.red,
            person="Alice",
            variant1=42,
            variant2="Bob"
        )

        result = parser.parse(text)
        self.assertIsNotNone(result)
        self.assertEqual(result.named, expected)

    def test_parse_with_many_unnamed_fields(self):
        type_dict = build_type_dict(self.TYPE_CONVERTERS)
        schema = """\
Number:   {:Number}
YesNo:    {:YesNo}
Color:    {:Color}
Person:   {:PersonChoice}
"""
        # -- OMIT: XFAIL, due to group_index delta counting => Parser problem.
        # Variant2: {:Color_or_PersonChoice}
        # Variant1: {:Number_or_YesNo}
        parser = parse.Parser(schema, type_dict)

        text = """\
Number:   12
YesNo:    yes
Color:    red
Person:   Alice
"""
        # SKIP: Variant2: Bob
        # SKIP: Variant1: 42
        expected = [ 12, True, Color.red, "Alice", ] # -- SKIP: "Bob", 42 ]

        result = parser.parse(text)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))

    def test_parse_with_many_unnamed_fields_with_variants(self):
        type_dict = build_type_dict(self.TYPE_CONVERTERS)
        schema = """\
Number:   {:Number}
YesNo:    {:YesNo}
Color:    {:Color}
Person:   {:PersonChoice}
Variant2: {:Color_or_PersonChoice}
Variant1: {:Number_or_YesNo}
"""
        # -- OMIT: XFAIL, due to group_index delta counting => Parser problem.
        parser = parse.Parser(schema, type_dict)

        text = """\
Number:   12
YesNo:    yes
Color:    red
Person:   Alice
Variant2: Bob
Variant1: 42
"""
        expected = [ 12, True, Color.red, "Alice", "Bob", 42 ]

        result = parser.parse(text)
        self.assertIsNotNone(result)
        self.assertEqual(result.fixed, tuple(expected))


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
