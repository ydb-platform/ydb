"""Tests for the `logging_tree.node` module."""

import logging.handlers
import unittest
from logging_tree.nodes import tree
from logging_tree.tests.case import LoggingTestCase

class AnyPlaceHolder(object):
    def __eq__(self, other):
        return isinstance(other, logging.PlaceHolder)

any_placeholder = AnyPlaceHolder()

class NodeTests(LoggingTestCase):

    def test_default_tree(self):
        self.assertEqual(tree(), ('', logging.root, []))

    def test_one_level_tree(self):
        a = logging.getLogger('a')
        b = logging.getLogger('b')
        self.assertEqual(tree(), (
                '', logging.root, [
                    ('a', a, []),
                    ('b', b, []),
                    ]))

    def test_two_level_tree(self):
        a = logging.getLogger('a')
        b = logging.getLogger('a.b')
        self.assertEqual(tree(), (
                '', logging.root, [
                    ('a', a, [
                            ('a.b', b, []),
                            ]),
                    ]))

    def test_two_level_tree_with_placeholder(self):
        b = logging.getLogger('a.b')
        self.assertEqual(tree(), (
                '', logging.root, [
                    ('a', any_placeholder, [
                            ('a.b', b, []),
                            ]),
                    ]))


if __name__ == '__main__':  # for Python <= 2.4
    unittest.main()
