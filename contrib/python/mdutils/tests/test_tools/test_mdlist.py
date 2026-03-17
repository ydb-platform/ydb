# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

from unittest import TestCase
from mdutils.tools.MDList import MDList


class TestMDList(TestCase):

    def setUp(self):
        self.basic_list = "- Item 1\n" \
                          "- Item 2\n" \
                          "- Item 3\n"
        self.complex_list = self.basic_list + \
                            "- Item 4\n" \
                            "    - Item 4.1\n" \
                            "    - Item 4.2\n" \
                            "        - Item 4.2.1\n" \
                            "        - Item 4.2.2\n" \
                            "    - Item 4.3\n" \
                            "        - Item 4.3.1\n" \
                            "- Item 5\n"
        self.basic_items = ["Item 1", "Item 2", "Item 3"]
        self.complex_items = self.basic_items + ["Item 4",
                                                 ["Item 4.1", "Item 4.2",
                                                  ["Item 4.2.1", "Item 4.2.2"],
                                                  "Item 4.3", ["Item 4.3.1"]],
                                                 "Item 5"]

    def test_basic_list(self):
        expected_list = self.basic_list
        mdlist = MDList(items=self.basic_items)
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)

    def test_complex_list(self):
        expected_list = self.complex_list
        mdlist = MDList(items=self.complex_items)
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)

    def test_another_marker(self):
        marker = "*"
        expected_list = self.complex_list.replace("-", marker)
        mdlist = MDList(items=self.complex_items, marked_with=marker)
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)

    def test_ordered_list(self):
        marker = "1"
        expected_list = "1. Item 1\n" \
                        "2. Item 2\n" \
                        "3. Item 3\n" \
                        "4. Item 4\n" \
                        "    1. Item 4.1\n" \
                        "    2. Item 4.2\n" \
                        "        1. Item 4.2.1\n" \
                        "        2. Item 4.2.2\n" \
                        "    3. Item 4.3\n" \
                        "        1. Item 4.3.1\n" \
                        "5. Item 5\n"
        mdlist = MDList(items=self.complex_items, marked_with=marker)
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)

    def test_unordered_with_mixed_ordered_list(self):
        expected_list = self.basic_list + \
                        "- Item 4\n" \
                        "    1. Item 4.1\n" \
                        "    2. Item 4.2\n" \
                        "- Item 5\n"
        items = self.basic_items + ["Item 4", ["1. Item 4.1", "2. Item 4.2"], "Item 5"]
        mdlist = MDList(items)
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)

    def test_ordered_with_mixed_unordered_list(self):
        expected_list = "1. Item 1\n" \
                        "2. Item 2\n" \
                        "3. Item 3\n" \
                        "4. Item 4\n" \
                        "    + Item 4.1\n" \
                        "    + Item 4.2\n" \
                        "        + Item 4.2.1\n" \
                        "        + Item 4.2.2\n" \
                        "    + Item 4.3\n" \
                        "        1. Item 4.3.1\n" \
                        "5. Item 5\n"

        items = ["Item 1", "Item 2", "Item 3", "Item 4",
                 ["+ Item 4.1", "+ Item 4.2",
                  ["+ Item 4.2.1", "+ Item 4.2.2"],
                  "+ Item 4.3", ["Item 4.3.1"]
                  ],
                 "Item 5"
                 ]

        mdlist = MDList(items, marked_with="1")
        actual_list = mdlist.get_md()

        self.assertEqual(expected_list, actual_list)
