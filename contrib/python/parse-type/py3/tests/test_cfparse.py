# -*- coding: utf-8 -*-
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test suite to test the :mod:`parse_type.cfparse` module.
"""

from __future__ import absolute_import
from .parse_type_test import ParseTypeTestCase, parse_number, unittest
from parse_type.cfparse import Parser
from parse_type.cardinality_field \
    import MissingTypeError, CardinalityFieldTypeBuilder


# -----------------------------------------------------------------------------
# TEST CASE:
# -----------------------------------------------------------------------------
class TestParser(ParseTypeTestCase):
    """
    Test :class:`parse_type.cfparse.Parser`.
    Ensure that:

      * parser can parse fields with CardinalityField part
        even when these special type variants are not provided.

      * parser creates missing type converter variants for CardinalityFields
        as long as the primary type converter for cardinality=1 is provided.
    """
    SPECIAL_FIELD_TYPES_DATA = [
        ("{number1:Number?}", ["Number?"]),
        ("{number2:Number+}", ["Number+"]),
        ("{number3:Number*}", ["Number*"]),
        ("{number1:Number?} {number2:Number+} {number3:Number*}",
                ["Number?", "Number+", "Number*"]),
    ]

    def test_parser__can_parse_normal_fields(self):
        existing_types = dict(Number=parse_number)
        schema = "Number: {number:Number}"
        parser = Parser(schema, existing_types)
        self.assert_match(parser, "Number: 42",  "number", 42)
        self.assert_match(parser, "Number: 123", "number", 123)
        self.assert_mismatch(parser, "Number: ")
        self.assert_mismatch(parser, "Number: XXX")
        self.assert_mismatch(parser, "Number: -123")

    def test_parser__can_parse_cardinality_field_optional(self):
        # -- CARDINALITY: 0..1 = zero_or_one = optional
        existing_types = dict(Number=parse_number)
        self.assertFalse("Number?" in existing_types)

        # -- ENSURE: Missing type variant is created.
        schema = "OptionalNumber: {number:Number?}"
        parser = Parser(schema, existing_types)
        self.assertTrue("Number?" in existing_types)

        # -- ENSURE: Newly created type variant is usable.
        self.assert_match(parser, "OptionalNumber: 42",  "number", 42)
        self.assert_match(parser, "OptionalNumber: 123", "number", 123)
        self.assert_match(parser, "OptionalNumber: ",    "number", None)
        self.assert_mismatch(parser, "OptionalNumber:")
        self.assert_mismatch(parser, "OptionalNumber: XXX")
        self.assert_mismatch(parser, "OptionalNumber: -123")

    def test_parser__can_parse_cardinality_field_many(self):
        # -- CARDINALITY: 1..* = one_or_more = many
        existing_types = dict(Number=parse_number)
        self.assertFalse("Number+" in existing_types)

        # -- ENSURE: Missing type variant is created.
        schema = "List: {numbers:Number+}"
        parser = Parser(schema, existing_types)
        self.assertTrue("Number+" in existing_types)

        # -- ENSURE: Newly created type variant is usable.
        self.assert_match(parser, "List: 42",  "numbers", [42])
        self.assert_match(parser, "List: 1, 2, 3", "numbers", [1, 2, 3])
        self.assert_match(parser, "List: 4,5,6", "numbers", [4, 5, 6])
        self.assert_mismatch(parser, "List: ")
        self.assert_mismatch(parser, "List:")
        self.assert_mismatch(parser, "List: XXX")
        self.assert_mismatch(parser, "List: -123")

    def test_parser__can_parse_cardinality_field_many_with_own_type_builder(self):
        # -- CARDINALITY: 1..* = one_or_more = many
        class MyCardinalityFieldTypeBuilder(CardinalityFieldTypeBuilder):
            listsep = ';'

        type_builder = MyCardinalityFieldTypeBuilder
        existing_types = dict(Number=parse_number)
        self.assertFalse("Number+" in existing_types)

        # -- ENSURE: Missing type variant is created.
        schema = "List: {numbers:Number+}"
        parser = Parser(schema, existing_types, type_builder=type_builder)
        self.assertTrue("Number+" in existing_types)

        # -- ENSURE: Newly created type variant is usable.
        # NOTE: Use other list separator.
        self.assert_match(parser, "List: 42",  "numbers", [42])
        self.assert_match(parser, "List: 1; 2; 3", "numbers", [1, 2, 3])
        self.assert_match(parser, "List: 4;5;6", "numbers", [4, 5, 6])
        self.assert_mismatch(parser, "List: ")
        self.assert_mismatch(parser, "List:")
        self.assert_mismatch(parser, "List: XXX")
        self.assert_mismatch(parser, "List: -123")

    def test_parser__can_parse_cardinality_field_many0(self):
        # -- CARDINALITY: 0..* = zero_or_more = many0
        existing_types = dict(Number=parse_number)
        self.assertFalse("Number*" in existing_types)

        # -- ENSURE: Missing type variant is created.
        schema = "List0: {numbers:Number*}"
        parser = Parser(schema, existing_types)
        self.assertTrue("Number*" in existing_types)

        # -- ENSURE: Newly created type variant is usable.
        self.assert_match(parser, "List0: 42",  "numbers", [42])
        self.assert_match(parser, "List0: 1, 2, 3", "numbers", [1, 2, 3])
        self.assert_match(parser, "List0: ",  "numbers", [])
        self.assert_mismatch(parser, "List0:")
        self.assert_mismatch(parser, "List0: XXX")
        self.assert_mismatch(parser, "List0: -123")


    def test_create_missing_types__without_cardinality_fields_in_schema(self):
        schemas = ["{}", "{:Number}", "{number3}", "{number4:Number}", "XXX"]
        existing_types = {}
        for schema in schemas:
            new_types = Parser.create_missing_types(schema, existing_types)
            self.assertEqual(len(new_types), 0)
            self.assertEqual(new_types, {})

    def test_create_missing_types__raises_error_if_primary_type_is_missing(self):
        # -- HINT: primary type is not provided in type_dict (existing_types)
        existing_types = {}
        for schema, missing_types in self.SPECIAL_FIELD_TYPES_DATA:
            with self.assertRaises(MissingTypeError):
                Parser.create_missing_types(schema, existing_types)

    def test_create_missing_types__if_special_types_are_missing(self):
        existing_types = dict(Number=parse_number)
        for schema, missing_types in self.SPECIAL_FIELD_TYPES_DATA:
            new_types = Parser.create_missing_types(schema, existing_types)
            self.assertSequenceEqual(set(new_types.keys()), set(missing_types))

    def test_create_missing_types__if_special_types_exist(self):
        existing_types = dict(Number=parse_number)
        for schema, missing_types in self.SPECIAL_FIELD_TYPES_DATA:
            # -- FIRST STEP: Prepare
            new_types = Parser.create_missing_types(schema, existing_types)
            self.assertGreater(len(new_types), 0)

            # -- SECOND STEP: Now all needed special types should exist.
            existing_types2 = existing_types.copy()
            existing_types2.update(new_types)
            new_types2 = Parser.create_missing_types(schema, existing_types2)
            self.assertEqual(len(new_types2), 0)


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
