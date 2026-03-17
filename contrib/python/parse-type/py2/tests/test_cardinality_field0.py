#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test experiment for parse.
Add cardinality format field after type:

    "... {person:Person?} ..."   -- CARDINALITY: Zero or one,  0..1 (optional)
    "... {persons:Person*} ..."  -- CARDINALITY: Zero or more, 0..N (many0)
    "... {persons:Person+} ..."  -- CARDINALITY: One or more,  1..N (many)


REQUIRES:
    parse >= 1.5.3.1 ('pattern' attribute support and further extensions)

STATUS:
    IDEA, working prototype with patched parse module, but not accepted.
"""

from __future__ import absolute_import
from .parse_type_test import ParseTypeTestCase
from parse_type import TypeBuilder, build_type_dict
import parse
import unittest

ENABLED = False
if ENABLED:
    # -------------------------------------------------------------------------
    # TEST CASE: TestParseTypeWithCardinalityField
    # -------------------------------------------------------------------------
    class TestParseTypeWithCardinalityField(ParseTypeTestCase):
        """
        Test cardinality field part in parse type expressions, ala:

            "... {person:Person?} ..."   -- OPTIONAL: cardinality is zero or one.
            "... {persons:Person*} ..."  -- MANY0: cardinality is zero or more.
            "... {persons:Person+} ..."  -- MANY:  cardinality is one or more.

        NOTE:
          * TypeBuilder has a similar and slightly more flexible feature.
          * Cardinality field part works currently only for user-defined types.
        """

        def test_without_cardinality_field(self):
            # -- IMPLCIT CARDINALITY: one
            # -- SETUP:
            parse_person = TypeBuilder.make_choice(["Alice", "Bob", "Charly"])
            parse_person.name = "Person"      # For testing only.
            extra_types = build_type_dict([ parse_person ])
            schema = "One: {person:Person}"
            parser = parse.Parser(schema, extra_types)

            # -- PERFORM TESTS:
            self.assert_match(parser, "One: Alice", "person", "Alice")
            self.assert_match(parser, "One: Bob",   "person", "Bob")

            # -- PARSE MISMATCH:
            self.assert_mismatch(parser, "One: ", "person")        # Missing.
            self.assert_mismatch(parser, "One: BAlice", "person")  # Similar1.
            self.assert_mismatch(parser, "One: Boby", "person")    # Similar2.
            self.assert_mismatch(parser, "One: a",    "person")    # INVALID ...

        def test_cardinality_field_with_zero_or_one(self):
            # -- SETUP:
            parse_person = TypeBuilder.make_choice(["Alice", "Bob", "Charly"])
            parse_person.name = "Person"      # For testing only.
            extra_types = build_type_dict([ parse_person ])
            schema = "Optional: {person:Person?}"
            parser = parse.Parser(schema, extra_types)

            # -- PERFORM TESTS:
            self.assert_match(parser, "Optional: ",      "person", None)
            self.assert_match(parser, "Optional: Alice", "person", "Alice")
            self.assert_match(parser, "Optional: Bob",   "person", "Bob")

            # -- PARSE MISMATCH:
            self.assert_mismatch(parser, "Optional: Anna", "person")  # Similar1.
            self.assert_mismatch(parser, "Optional: Boby", "person")  # Similar2.
            self.assert_mismatch(parser, "Optional: a",    "person")  # INVALID ...

        def test_cardinality_field_with_one_or_more(self):
            # -- SETUP:
            parse_person = TypeBuilder.make_choice(["Alice", "Bob", "Charly"])
            parse_person.name = "Person"      # For testing only.
            extra_types = build_type_dict([ parse_person ])
            schema = "List: {persons:Person+}"
            parser = parse.Parser(schema, extra_types)

            # -- PERFORM TESTS:
            self.assert_match(parser, "List: Alice", "persons", [ "Alice" ])
            self.assert_match(parser, "List: Bob",   "persons", [ "Bob" ])
            self.assert_match(parser, "List: Bob, Alice",
                                      "persons", [ "Bob", "Alice" ])

            # -- PARSE MISMATCH:
            self.assert_mismatch(parser, "List: ",       "persons")  # Zero items.
            self.assert_mismatch(parser, "List: BAlice", "persons")  # Unknown1.
            self.assert_mismatch(parser, "List: Boby",   "persons")  # Unknown2.
            self.assert_mismatch(parser, "List: Alice,", "persons")  # Trailing,
            self.assert_mismatch(parser, "List: a, b",   "persons")  # List of...

        def test_cardinality_field_with_zero_or_more(self):
            # -- SETUP:
            parse_person = TypeBuilder.make_choice(["Alice", "Bob", "Charly"])
            parse_person.name = "Person"      # For testing only.
            extra_types = build_type_dict([ parse_person ])
            schema = "List: {persons:Person*}"
            parser = parse.Parser(schema, extra_types)

            # -- PERFORM TESTS:
            self.assert_match(parser, "List: ", "persons", [ ])
            self.assert_match(parser, "List: Alice", "persons", [ "Alice" ])
            self.assert_match(parser, "List: Bob",   "persons", [ "Bob" ])
            self.assert_match(parser, "List: Bob, Alice",
                "persons", [ "Bob", "Alice" ])

            # -- PARSE MISMATCH:
            self.assert_mismatch(parser, "List:", "persons")         # Too short.
            self.assert_mismatch(parser, "List: BAlice", "persons")  # Unknown1.
            self.assert_mismatch(parser, "List: Boby",   "persons")  # Unknown2.
            self.assert_mismatch(parser, "List: Alice,", "persons")  # Trailing,
            self.assert_mismatch(parser, "List: a, b",   "persons")  # List of...

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
