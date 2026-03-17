# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

__author__ = "didix21"
__project__ = "MdUtils"

from unittest import TestCase
from mdutils.tools.Image import Image
from mdutils.tools.Link import Reference


class TestLink(TestCase):
    def setUp(self):
        self.text = "image"
        self.path = "../some_image.png"
        self.reference_tag = "im"

    def test_new_inline_image(self):
        expected_image = "![{}]({})".format(self.text, self.path)
        actual_image = Image.new_inline_image(text=self.text, path=self.path)

        self.assertEqual(expected_image, actual_image)

    def test_new_reference_image(self):
        link = "https://github.com"
        link_text = "github"
        reference = Reference()
        reference.new_link(link=link, text=link_text)
        image = Image(reference)

        expected_image = "![{}][{}]".format(self.text, self.reference_tag)
        actual_image = image.new_reference_image(
            text=self.text, path=self.path, reference_tag=self.reference_tag
        )

        expected_image_references = {self.reference_tag: self.path, link_text: link}
        actual_image_references = image.reference.get_references()

        self.assertEqual(expected_image, actual_image)
        self.assertEqual(expected_image_references, actual_image_references)

    def test_new_reference_when_reference_tag_is_not_defined(self):
        reference = Reference()
        image = Image(reference)

        expected_image = "![{}]".format(self.text)
        actual_image = image.new_reference_image(text=self.text, path=self.path)

        expected_image_references = {self.text: self.path}
        actual_image_references = image.reference.get_references()

        expected_image_references_markdown = "\n\n\n[{}]: {}\n".format(
            self.text, self.path
        )
        actual_image_references_markdown = image.reference.get_references_as_markdown()

        self.assertEqual(expected_image, actual_image)
        self.assertEqual(expected_image_references, actual_image_references)
        self.assertEqual(
            expected_image_references_markdown, actual_image_references_markdown
        )

    def test_inline_inline_tooltip(self):
        tooltip = "mytooltip"
        expected_image = "![{}]({} '{}')".format(self.text, self.path, tooltip)
        actual_image = Image.new_inline_image(
            text=self.text, path=self.path, tooltip=tooltip
        )

        self.assertEqual(expected_image, actual_image)

    def test_reference_image_tooltip(self):
        tooltip = "mytooltip"
        reference = Reference()
        image = Image(reference)

        expected_image = "![{}]".format(self.text)
        actual_image = image.new_reference_image(
            text=self.text, path=self.path, tooltip=tooltip
        )

        expected_image_references = {self.text: "{} '{}'".format(self.path, tooltip)}
        actual_image_references = image.reference.get_references()

        expected_image_references_markdown = "\n\n\n[{}]: {} '{}'\n".format(
            self.text, self.path, tooltip
        )
        actual_image_references_markdown = image.reference.get_references_as_markdown()

        self.assertEqual(expected_image, actual_image)
        self.assertEqual(expected_image_references, actual_image_references)
        self.assertEqual(
            expected_image_references_markdown, actual_image_references_markdown
        )
