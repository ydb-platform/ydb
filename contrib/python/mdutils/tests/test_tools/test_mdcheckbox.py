# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

from unittest import TestCase
from mdutils.tools.MDList import MDCheckbox


class TestMDCheckbox(TestCase):

    def setUp(self):
        self.basic_list = "- [ ] Item 1\n" \
                          "- [ ] Item 2\n" \
                          "- [ ] Item 3\n"
        self.complex_list = self.basic_list + \
                            "- [ ] Item 4\n" \
                            "    - [ ] Item 4.1\n" \
                            "    - [ ] Item 4.2\n" \
                            "        - [ ] Item 4.2.1\n" \
                            "        - [ ] Item 4.2.2\n" \
                            "    - [ ] Item 4.3\n" \
                            "        - [ ] Item 4.3.1\n" \
                            "- [ ] Item 5\n"
        self.basic_items = ["Item 1", "Item 2", "Item 3"]
        self.complex_items = self.basic_items + ["Item 4",
                                                 ["Item 4.1", "Item 4.2",
                                                  ["Item 4.2.1", "Item 4.2.2"],
                                                  "Item 4.3", ["Item 4.3.1"]],
                                                 "Item 5"]

    def test_basic_checkbox(self):
        checkbox = MDCheckbox(self.basic_items)
        expected_checkbox_list = self.basic_list

        self.assertEqual(expected_checkbox_list, checkbox.get_md())

    def test_basic_checkbox_checked(self):
        checkbox = MDCheckbox(self.basic_items, checked=True)
        expected_checkbox_list = self.basic_list.replace('[ ]', '[x]')

        self.assertEqual(expected_checkbox_list, checkbox.get_md())

    def test_complex_checkbox(self):
        checkbox = MDCheckbox(self.complex_items)
        expected_checkbox_list = self.complex_list

        self.assertEqual(expected_checkbox_list, checkbox.get_md())

    def test_complex_checkbox_checked(self):
        checkbox = MDCheckbox(self.complex_items, checked=True)
        expected_checkbox_list = self.complex_list.replace('[ ]', '[x]')

        self.assertEqual(expected_checkbox_list, checkbox.get_md())

    def test_checkbox_with_some_items_checked(self):
        expected_checkbox_list = self.basic_list + \
                       "- [ ] Item 4\n" \
                       "    - [ ] Item 4.1\n" \
                       "    - [x] Item 4.2\n" \
                       "        - [x] Item 4.2.1\n" \
                       "        - [x] Item 4.2.2\n" \
                       "    - [ ] Item 4.3\n" \
                       "        - [x] Item 4.3.1\n" \
                       "- [ ] Item 5\n"
        complex_items = self.basic_items + ["Item 4",
                                            ["Item 4.1", "x Item 4.2",
                                             ["x Item 4.2.1", "x Item 4.2.2"],
                                             "Item 4.3", ["x Item 4.3.1"]],
                                            "Item 5"]
        checkbox = MDCheckbox(complex_items)

        self.assertEqual(expected_checkbox_list, checkbox.get_md())
