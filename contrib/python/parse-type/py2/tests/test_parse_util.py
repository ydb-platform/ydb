#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test suite to test the :mod:`parse_type.parse_util` module.
"""

from __future__ import absolute_import, print_function
from .parse_type_test import TestCase, unittest
from parse_type.parse_util \
    import Field, FieldParser, FormatSpec, make_format_spec


# -----------------------------------------------------------------------------
# TEST CASE:
# -----------------------------------------------------------------------------
class TestField(TestCase):
    EMPTY_FORMAT_FIELDS = [
        Field(),               #< Empty field.
        Field("name"),         #< Named field without format.
        Field("name", ""),     #< Named field with format=empty-string.
        Field(format=""),      #< Field with format=empty-string.
    ]
    NONEMPTY_FORMAT_FIELDS = [
        Field(format="Number"),    #< Typed field without name".
        Field("name", "Number"),   #< Named and typed field".
    ]
    INVALID_FORMAT_FIELDS = [
        Field(format="<"),     #< Align without type.
        Field(format="_<"),    #< Fill and align without type.
        Field(format="_<10"),  #< Fill, align and width without type.
        Field(format="_<098"), #< Fill, align, zero and width without type.
    ]
    FIELDS = EMPTY_FORMAT_FIELDS + NONEMPTY_FORMAT_FIELDS + INVALID_FORMAT_FIELDS

    def test_is_typed__returns_true_for_nonempty_format(self):
        fields = self.NONEMPTY_FORMAT_FIELDS + self.INVALID_FORMAT_FIELDS
        for field in fields:
            self.assertTrue(field.has_format, "Field: %s" % field)

    def test_is_typed__returns_false_for_empty_format(self):
        fields = self.EMPTY_FORMAT_FIELDS
        for field in fields:
            self.assertFalse(field.has_format, "Field: %s" % field)

    def test_format_spec__returns_none_if_format_is_empty(self):
        for field in self.EMPTY_FORMAT_FIELDS:
            self.assertIsNone(field.format_spec, "Field: %s" % field)

    def test_format_spec__if_format_is_nonempty_and_valid(self):
        for field in self.NONEMPTY_FORMAT_FIELDS:
            self.assertIsNotNone(field.format_spec)
            self.assertIsInstance(field.format_spec, FormatSpec)

    def test_format_spec__raises_error_if_nonempty_format_is_invalid(self):
        for field in self.INVALID_FORMAT_FIELDS:
            with self.assertRaises(ValueError):
                field.format_spec

    def test_format_spec__is_lazy_evaluated(self):
        fields = [Field(), Field("name"),
                  Field("name", "type"), Field(format="type")]
        for field in fields:
            self.assertIsNone(field._format_spec)
            if field.format:
                _ = field.format_spec.type
                self.assertIsNotNone(field.format_spec)
            else:
                self.assertIsNone(field.format_spec)

    def test_set_format_invalidates_format_spec(self):
        field = Field(format="Number")
        self.assertEqual(field.format, "Number")
        self.assertEqual(field.format_spec.type, "Number")
        self.assertEqual(field.format_spec.align, None)

        field.set_format("<ManyNumbers")
        self.assertEqual(field.format, "<ManyNumbers")
        self.assertEqual(field.format_spec.type, "ManyNumbers")
        self.assertEqual(field.format_spec.align, '<')

    def test_to_string_conversion(self):
        test_data = [
            (Field(),               "{}"),
            (Field("name"),         "{name}"),
            (Field(format="type"),  "{:type}"),
            (Field("name", "type"), "{name:type}"),
        ]
        for field, expected_text in test_data:
            text = str(field)
            self.assertEqual(text, expected_text)

    def test_equal__with_field(self):
        for field  in self.FIELDS:
            other = field
            self.assertEqual(field, other)

    def test_equal__with_string(self):
        for field  in self.FIELDS:
            other = str(field)
            self.assertEqual(field, other)

    def test_equal__with_unsupported(self):
        UNSUPPORTED_TYPES =  [None, make_format_spec(), True, False, 10]
        field = Field()
        for other in UNSUPPORTED_TYPES:
            with self.assertRaises(ValueError):
                field == other

    def test_not_equal__with_field(self):
        for field  in self.FIELDS:
            other2 = Field(field.name, "XXX")
            self.assertNotEqual(field.format, other2.format)
            self.assertNotEqual(field, other2)

            other3 = Field("xxx", field.format)
            self.assertNotEqual(field.name, other3.name)
            self.assertNotEqual(field, other3)

    def test_not_equal__with_string(self):
        for field  in self.FIELDS:
            other2 = Field(field.name, "XXX")
            other2_text = str(other2)
            self.assertNotEqual(field.format, other2.format)
            self.assertNotEqual(field, other2_text)

            other3 = Field("xxx", field.format)
            other3_text = str(other3)
            self.assertNotEqual(field.name, other3.name)
            self.assertNotEqual(field, other3_text)

    def test_not_equal__with_unsupported(self):
        UNSUPPORTED_TYPES =  [None, make_format_spec(), True, False, 10]
        field = Field()
        for other in UNSUPPORTED_TYPES:
            with self.assertRaises(ValueError):
                field != other


# -----------------------------------------------------------------------------
# TEST CASE:
# -----------------------------------------------------------------------------
class TestFieldFormatSpec(TestCase):
    """
    Test Field.extract_format_spec().

    FORMAT-SPEC SCHEMA:
    [[fill]align][0][width][.precision][type]
    """

    def assertValidFormatWidth(self, width):
        self.assertIsInstance(width, str)
        for char in width:
            self.assertTrue(char.isdigit())

    def assertValidFormatAlign(self, align):
        self.assertIsInstance(align, str)
        self.assertEqual(len(align), 1)
        self.assertIn(align, Field.ALIGN_CHARS)

    def assertValidFormatPrecision(self, precision):
        self.assertIsInstance(precision, str)
        for char in precision:
            self.assertTrue(char.isdigit())

    def test_extract_format_spec__with_empty_string_raises_error(self):
        with self.assertRaises(ValueError) as cm:
            Field.extract_format_spec("")
        self.assertIn("INVALID-FORMAT", str(cm.exception))

    def test_extract_format_spec__with_type(self):
        format_types = ["d", "w", "Number", "Number?", "Number*", "Number+"]
        for format_type in format_types:
            format_spec = Field.extract_format_spec(format_type)
            expected_spec = make_format_spec(format_type)
            self.assertEqual(format_spec.type, format_type)
            self.assertEqual(format_spec.width, "")
            self.assertEqual(format_spec.zero, False)
            self.assertIsNone(format_spec.align)
            self.assertIsNone(format_spec.fill)
            self.assertEqual(format_spec, expected_spec)

    def test_extract_format_spec_with_width_only_raises_error(self):
        # -- INVALID-FORMAT: Width without type.
        with self.assertRaises(ValueError) as cm:
            Field.extract_format_spec("123")
        self.assertEqual("INVALID-FORMAT: 123 (without type)", str(cm.exception))

    def test_extract_format_spec__with_zero_only_raises_error(self):
        # -- INVALID-FORMAT: Width without type.
        with self.assertRaises(ValueError) as cm:
            Field.extract_format_spec("0")
        self.assertEqual("INVALID-FORMAT: 0 (without type)", str(cm.exception))

    def test_extract_format_spec__with_align_only_raises_error(self):
        # -- INVALID-FORMAT: Width without type.
        for align in Field.ALIGN_CHARS:
            with self.assertRaises(ValueError) as cm:
                Field.extract_format_spec(align)
            self.assertEqual("INVALID-FORMAT: %s (without type)" % align,
                             str(cm.exception))

    def test_extract_format_spec_with_fill_and_align_only_raises_error(self):
        # -- INVALID-FORMAT: Width without type.
        fill = "_"
        for align in Field.ALIGN_CHARS:
            with self.assertRaises(ValueError) as cm:
                format = fill + align
                Field.extract_format_spec(format)
            self.assertEqual("INVALID-FORMAT: %s (without type)" % format,
                             str(cm.exception))

    def test_extract_format_spec__with_width_and_type(self):
        formats = ["1s", "2d", "6s", "10d", "60f", "123456789s"]
        for format in formats:
            format_spec = Field.extract_format_spec(format)
            expected_type  = format[-1]
            expected_width = format[:-1]
            expected_spec = make_format_spec(type=expected_type,
                                             width=expected_width)
            self.assertEqual(format_spec, expected_spec)
            self.assertValidFormatWidth(format_spec.width)

    def test_extract_format_spec__with_precision_and_type(self):
        formats = [".2d", ".6s", ".6f"]
        for format in formats:
            format_spec = Field.extract_format_spec(format)
            expected_type  = format[-1]
            expected_precision = format[1:-1]
            expected_spec = make_format_spec(type=expected_type,
                                             precision=expected_precision)
            self.assertEqual(format_spec, expected_spec)
            self.assertValidFormatPrecision(format_spec.precision)

    def test_extract_format_spec__with_zero_and_type(self):
        formats = ["0s", "0d", "0Number", "0Number+"]
        for format in formats:
            format_spec = Field.extract_format_spec(format)
            expected_type  = format[1:]
            expected_spec = make_format_spec(type=expected_type, zero=True)
            self.assertEqual(format_spec, expected_spec)

    def test_extract_format_spec__with_align_and_type(self):
        # -- ALIGN_CHARS = "<>=^"
        formats = ["<s", ">d", "=Number", "^Number+"]
        for format in formats:
            format_spec = Field.extract_format_spec(format)
            expected_align = format[0]
            expected_type  = format[1:]
            expected_spec = make_format_spec(type=expected_type,
                                             align=expected_align)
            self.assertEqual(format_spec, expected_spec)
            self.assertValidFormatAlign(format_spec.align)

    def test_extract_format_spec__with_fill_align_and_type(self):
        # -- ALIGN_CHARS = "<>=^"
        formats = ["X<s", "_>d", "0=Number", " ^Number+"]
        for format in formats:
            format_spec = Field.extract_format_spec(format)
            expected_fill  = format[0]
            expected_align = format[1]
            expected_type  = format[2:]
            expected_spec = make_format_spec(type=expected_type,
                                    align=expected_align, fill=expected_fill)
            self.assertEqual(format_spec, expected_spec)
            self.assertValidFormatAlign(format_spec.align)

    # -- ALIGN_CHARS = "<>=^"
    FORMAT_AND_FORMAT_SPEC_DATA = [
            ("^010Number+", make_format_spec(type="Number+", width="10",
                                        zero=True, align="^", fill=None)),
            ("X<010Number+", make_format_spec(type="Number+", width="10",
                                        zero=True, align="<", fill="X")),
            ("_>0098Number?", make_format_spec(type="Number?", width="098",
                                        zero=True, align=">", fill="_")),
            ("*=129Number*", make_format_spec(type="Number*", width="129",
                                        zero=False, align="=", fill="*")),
            ("X129Number?", make_format_spec(type="X129Number?", width="",
                                        zero=False, align=None, fill=None)),
            (".3Number", make_format_spec(type="Number", width="",
                                           zero=False, align=None, fill=None,
                                           precision="3")),
            ("6.2Number", make_format_spec(type="Number", width="6",
                                      zero=False, align=None, fill=None,
                                      precision="2")),
    ]

    def test_extract_format_spec__with_all(self):
        for format, expected_spec in self.FORMAT_AND_FORMAT_SPEC_DATA:
            format_spec = Field.extract_format_spec(format)
            self.assertEqual(format_spec, expected_spec)
            self.assertValidFormatWidth(format_spec.width)
            if format_spec.align is not None:
                self.assertValidFormatAlign(format_spec.align)

    def test_make_format(self):
        for expected_format, format_spec in self.FORMAT_AND_FORMAT_SPEC_DATA:
            format = Field.make_format(format_spec)
            self.assertEqual(format, expected_format)
            format_spec2 = Field.extract_format_spec(format)
            self.assertEqual(format_spec2, format_spec)


# -----------------------------------------------------------------------------
# TEST CASE:
# -----------------------------------------------------------------------------
class TestFieldParser(TestCase):
    INVALID_FIELDS = ["", "{", "}", "xxx", "name:type", ":type"]
    VALID_FIELD_DATA = [
        ("{}",          Field()),
        ("{name}",      Field("name")),
        ("{:type}",     Field(format="type")),
        ("{name:type}", Field("name", "type"))
    ]

    #def assertFieldEqual(self, actual, expected):
    #    message = "FAILED: %s == %s" %  (actual, expected)
    #    self.assertIsInstance(actual, Field)
    #    self.assertIsInstance(expected, Field)
    #    self.assertEqual(actual, expected, message)
    #    # self.assertEqual(actual.name,   expected.name, message)
    #    # self.assertEqual(actual.format, expected.format, message)

    def test_parse__raises_error_with_missing_or_partial_braces(self):
        for field_text in self.INVALID_FIELDS:
            with self.assertRaises(ValueError):
                FieldParser.parse(field_text)

    def test_parse__with_valid_fields(self):
        for field_text, expected_field in self.VALID_FIELD_DATA:
            field = FieldParser.parse(field_text)
            self.assertEqual(field, expected_field)

    def test_extract_fields__without_field(self):
        prefix = "XXX ___"
        suffix = "XXX {{escaped_field}} {{escaped_field:xxx_type}} XXX"
        field_texts = [prefix, suffix, prefix + suffix, suffix + prefix]

        for field_text in field_texts:
            fields = list(FieldParser.extract_fields(field_text))
            self.assertEqual(len(fields), 0)

    def test_extract_fields__with_one_field(self):
        prefix = "XXX ___"
        suffix = "XXX {{escaped_field}} {{escaped_field:xxx_type}} XXX"

        for field_text, expected_field in self.VALID_FIELD_DATA:
            fields = list(FieldParser.extract_fields(field_text))
            self.assertEqual(len(fields), 1)
            self.assertSequenceEqual(fields, [expected_field])

            field_text2 = prefix + field_text + suffix
            fields2 = list(FieldParser.extract_fields(field_text2))
            self.assertEqual(len(fields2), 1)
            self.assertSequenceEqual(fields, fields2)

    def test_extract_fields__with_many_fields(self):
        MANY_FIELDS_DATA = [
            ("{}xxx{name2}",     [Field(), Field("name2")]),
            ("{name1}yyy{:type2}", [Field("name1"), Field(format="type2")]),
            ("{:type1}xxx{name2}{name3:type3}",
            [Field(format="type1"), Field("name2"), Field("name3", "type3")]),
        ]
        prefix = "XXX ___"
        suffix = "XXX {{escaped_field}} {{escaped_field:xxx_type}} XXX"

        for field_text, expected_fields in MANY_FIELDS_DATA:
            fields = list(FieldParser.extract_fields(field_text))
            self.assertEqual(len(fields), len(expected_fields))
            self.assertSequenceEqual(fields, expected_fields)

            field_text2 = prefix + field_text + suffix
            fields2 = list(FieldParser.extract_fields(field_text2))
            self.assertEqual(len(fields2), len(expected_fields))
            self.assertSequenceEqual(fields2, expected_fields)


    def test_extract_types(self):
        MANY_TYPES_DATA = [
            ("{}xxx{name2}",                    []),
            ("{name1}yyy{:type2}",              ["type2"]),
            ("{:type1}xxx{name2}{name3:type3}", ["type1", "type3"]),
        ]

        for field_text, expected_types in MANY_TYPES_DATA:
            type_names = list(FieldParser.extract_types(field_text))
            self.assertEqual(len(type_names), len(expected_types))
            self.assertSequenceEqual(type_names, expected_types)


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
