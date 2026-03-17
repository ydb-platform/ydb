# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.tools.Html import Html, SizeBadFormat, HtmlSize


class TestHtml(TestCase):
    def setUp(self):
        self.text = "my text"
        self.path = "./my_path"

    def test_paragraph(self):
        expected_paragraph = '<p align="{}">\n    {}\n</p>'.format("left", self.text)
        actual_paragraph = Html.paragraph(text=self.text, align="left")

        self.assertEqual(expected_paragraph, actual_paragraph)

    def test_paragraph_when_align_is_not_defined(self):
        expected_paragraph = "<p>\n    {}\n</p>".format(self.text)
        actual_paragraph = Html.paragraph(text=self.text)

        self.assertEqual(expected_paragraph, actual_paragraph)

    def test_paragraph_when_invalid_align_is_passed(self):
        try:
            Html.paragraph(text=self.text, align="")
        except KeyError:
            return

        self.fail()

    def test_image_path(self):
        expected_image = '<img src="{}" />'.format(self.path)
        actual_image = Html.image(path=self.path)

        self.assertEqual(expected_image, actual_image)

    def test_image_size_width(self):
        size = "200"
        expected_image = '<img src="{}" width="{}"/>'.format(self.path, size)
        actual_image = Html.image(path=self.path, size=size)

        self.assertEqual(expected_image, actual_image)

    def test_image_size_height(self):
        expected_image = '<img src="{}" height="200"/>'.format(self.path)
        actual_image = Html.image(path=self.path, size="x200")

        self.assertEqual(expected_image, actual_image)

    def test_image_size_width_height(self):
        size = "200"
        expected_image = '<img src="{}" width="{}" height="{}"/>'.format(
            self.path, size, size
        )
        actual_image = Html.image(path=self.path, size="200x200")

        self.assertEqual(expected_image, actual_image)

    def test_image_align_center(self):
        html_image = '<img src="{}" />'.format(self.path)
        expected_image = '<p align="{}">\n    {}\n</p>'.format("center", html_image)
        actual_image = Html.image(path=self.path, align="center")

        self.assertEqual(expected_image, actual_image)

    def test_image_align_center_width_height(self):
        size = "200"
        html_image = '<img src="{}" width="{}" height="{}"/>'.format(
            self.path, size, size
        )
        expected_image = '<p align="{}">\n    {}\n</p>'.format("center", html_image)
        actual_image = Html.image(path=self.path, size="200x200", align="center")

        self.assertEqual(expected_image, actual_image)


class TestHtmlSize(TestCase):
    def test_raise_exception(self):
        try:
            HtmlSize.size_to_width_and_height(size="dd")
        except SizeBadFormat:
            return

        self.fail()

    def test_size_to_width_height_when_providing_number(self):
        expected = 'width="200"'
        actual = HtmlSize.size_to_width_and_height(size="200")

        self.assertEqual(expected, actual)

    def test_size_to_width_height_when_providing_x_int(self):
        expected = 'height="200"'
        actual = HtmlSize.size_to_width_and_height(size="x200")

        self.assertEqual(expected, actual)

    def test_size_to_width_height_when_providing_x_str_int(self):
        try:
            HtmlSize.size_to_width_and_height(size="xD200")
        except SizeBadFormat:
            return

        self.fail()

    def test_size_to_width_height_when_providing_int_x_int(self):
        expected = 'width="200" height="300"'
        actual = HtmlSize.size_to_width_and_height(size="200x300")

        self.assertEqual(expected, actual)

    def test_size_to_width_height_when_providing_int_whitespace_X_int(self):
        expected = 'width="200" height="300"'
        actual = HtmlSize.size_to_width_and_height(size="200 X300")

        self.assertEqual(expected, actual)

    def test_size_to_width_height_when_providing_x_str_int(self):
        try:
            HtmlSize.size_to_width_and_height(size="200dx200")
        except SizeBadFormat:
            return

        self.fail()

    def test_size_to_width_height_when_providing_x_str_int(self):
        try:
            HtmlSize.size_to_width_and_height(size="fx200")
        except SizeBadFormat:
            return

        self.fail()
