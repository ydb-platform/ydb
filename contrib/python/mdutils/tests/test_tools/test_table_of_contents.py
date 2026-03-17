# Python
#
# This module implements tests for TableOfContents class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.tools.TableOfContents import TableOfContents

__author__ = "didix21"
__project__ = "MdUtils"


class TestTableOfContents(TestCase):
    def test_create_table_of_contents(self):
        array_of_contents = [
            "Results Tests",
            [],
            "Test Details",
            ["Test 1", [], "Test 2", [], "Test 3", [], "Test 4", []],
        ]
        expects = (
            "\n* [Results Tests](#results-tests)\n"
            "* [Test Details](#test-details)\n\t"
            "* [Test 1](#test-1)\n\t"
            "* [Test 2](#test-2)\n\t"
            "* [Test 3](#test-3)\n\t"
            "* [Test 4](#test-4)\n"
        )

        table_of_contents = TableOfContents()
        self.assertEqual(
            table_of_contents.create_table_of_contents(array_of_contents, depth=2),
            expects,
        )

    def test_table_of_contents_with_colon(self):
        array_of_contents = ["My header: 1"]
        expects = "\n* [My header: 1](#my-header-1)\n"

        self.assertEqual(
            TableOfContents().create_table_of_contents(array_of_contents), expects
        )

    def test_table_of_contents_with_dot(self):
        array_of_contents = ["My.header 1.1"]
        expects = "\n* [My.header 1.1](#myheader-11)\n"

        self.assertEqual(
            TableOfContents().create_table_of_contents(array_of_contents), expects
        )

    def test_table_of_contents_with_back_slash(self):
        array_of_contents = ["My\header 1"]
        expects = "\n* [My\header 1](#myheader-1)\n"

        self.assertEqual(
            TableOfContents().create_table_of_contents(array_of_contents), expects
        )

    def test_table_of_contents_with_hyphen(self):
        array_of_contents = ["My-header-1 pop"]
        expects = "\n* [My-header-1 pop](#my-header-1-pop)\n"

        self.assertEqual(
            TableOfContents().create_table_of_contents(array_of_contents), expects
        )
