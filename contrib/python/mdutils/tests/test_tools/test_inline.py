# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

from unittest import TestCase
from mdutils.tools.Link import Inline


class TestLink(TestCase):
    def setUp(self):
        self.link = "https://github.com/didix21/mdutils"
        self.text = "mdutils library"
        self.reference_tag = "mdutils"
        self.tooltip = "tooltip"

    def test_inline_link(self):
        expected_link = "[" + self.text + "](" + self.link + ")"
        actual_link = Inline.new_link(link=self.link, text=self.text)

        self.assertEqual(expected_link, actual_link)

    def test_text_is_not_defined(self):
        expected_link = "<" + self.link + ">"
        actual_link = Inline.new_link(link=self.link)

        self.assertEqual(expected_link, actual_link)

    def test_link_is_not_defined(self):
        try:
            Inline.new_link()
        except TypeError:
            return

        self.fail()

    def test_link_tooltip(self):
        expected_link = (
            "[" + self.text + "](" + self.link + " '{}'".format(self.tooltip) + ")"
        )
        actual_link = Inline.new_link(
            link=self.link, text=self.text, tooltip=self.tooltip
        )

        self.assertEqual(expected_link, actual_link)

    def test_link_without_text_tooltip(self):
        expected_link = (
            "[" + self.link + "](" + self.link + " '{}'".format(self.tooltip) + ")"
        )
        actual_link = Inline.new_link(link=self.link, tooltip=self.tooltip)

        self.assertEqual(expected_link, actual_link)
