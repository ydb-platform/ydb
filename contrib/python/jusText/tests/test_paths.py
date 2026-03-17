# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

from justext.core import PathInfo

import unittest


class TestPathInfo(unittest.TestCase):
    def test_empty_path(self):
        path = PathInfo()

        self.assertEqual(path.dom, "")
        self.assertEqual(path.xpath, "/")

    def test_path_with_root_only(self):
        path = PathInfo().append("html")

        self.assertEqual(path.dom, "html")
        self.assertEqual(path.xpath, "/html[1]")

    def test_path_with_more_elements(self):
        path = PathInfo().append("html").append("body").append("header").append("h1")

        self.assertEqual(path.dom, "html.body.header.h1")
        self.assertEqual(path.xpath, "/html[1]/body[1]/header[1]/h1[1]")

    def test_contains_multiple_tags_with_the_same_name(self):
        path = PathInfo().append("html").append("body").append("div")

        self.assertEqual(path.dom, "html.body.div")
        self.assertEqual(path.xpath, "/html[1]/body[1]/div[1]")

        path.pop().append("div")

        self.assertEqual(path.dom, "html.body.div")
        self.assertEqual(path.xpath, "/html[1]/body[1]/div[2]")

    def test_more_elements_in_tag(self):
        path = PathInfo().append("html").append("body").append("div")

        self.assertEqual(path.dom, "html.body.div")
        self.assertEqual(path.xpath, "/html[1]/body[1]/div[1]")

        path.pop()

        self.assertEqual(path.dom, "html.body")
        self.assertEqual(path.xpath, "/html[1]/body[1]")

        path.append("span")

        self.assertEqual(path.dom, "html.body.span")
        self.assertEqual(path.xpath, "/html[1]/body[1]/span[1]")

        path.pop()

        self.assertEqual(path.dom, "html.body")
        self.assertEqual(path.xpath, "/html[1]/body[1]")

        path.append("pre")

        self.assertEqual(path.dom, "html.body.pre")
        self.assertEqual(path.xpath, "/html[1]/body[1]/pre[1]")

    def test_removing_element(self):
        path = PathInfo().append("html").append("body")
        path.append("div").append("a").pop()

        self.assertEqual(path.dom, "html.body.div")
        self.assertEqual(path.xpath, "/html[1]/body[1]/div[1]")

        path.pop()

        self.assertEqual(path.dom, "html.body")
        self.assertEqual(path.xpath, "/html[1]/body[1]")

    def test_pop_on_empty_path_raises_exception(self):
        path = PathInfo()
        self.assertRaises(IndexError, path.pop)
