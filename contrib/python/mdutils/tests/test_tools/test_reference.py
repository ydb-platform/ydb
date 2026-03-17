# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

from unittest import TestCase
from mdutils.tools.Link import Reference


class TestReference(TestCase):
    def setUp(self):
        self.link = "https://github.com/didix21/mdutils"
        self.text = "mdutils library"
        self.reference_tag = "mdutils"
        self.expected_reference_markdown = (
            "[" + self.text + "][" + self.reference_tag + "]"
        )
        self.expected_references = {self.reference_tag: self.link}

    def test_new_reference(self):
        reference = Reference()
        actual_link = reference.new_link(self.link, self.text, self.reference_tag)

        self.assertEqual(self.expected_reference_markdown, actual_link)
        self.assertEqual(self.expected_references, reference.get_references())

    def test_new_reference_without_defining_reference_tag(self):
        reference = Reference()
        expected_link = "[" + self.text + "]"
        expected_references = {self.text: self.link}
        actual_link = reference.new_link(self.link, self.text)

        self.assertEqual(expected_link, actual_link)
        self.assertEqual(expected_references, reference.get_references())

    def test_reference_link_if_already_exists_in_global_references(self):
        reference = Reference()

        reference.new_link(self.link, self.text, self.reference_tag)
        expected_references = {self.reference_tag: "https://blablabla.com"}

        actual_references = reference.get_references()

        self.assertEqual({self.reference_tag: self.link}, actual_references)
        self.assertIsNot(expected_references, actual_references)

    def test_add_multiple_link_references(self):
        reference = Reference()
        link = "https://blablabla.com"
        text = "bla bla bla"
        reference_tag = "blablabla"

        reference.new_link(self.link, self.text, self.reference_tag)
        reference.new_link(link, text, reference_tag)
        self.expected_references.update({reference_tag: link})

        self.assertEqual(self.expected_references, reference.get_references())

    def test_get_references_in_markdown_format_check_they_are_sorted(self):
        reference = Reference()
        expected_references_markdown = (
            "\n\n\n"
            "[1]: http://slashdot.org\n"
            "[arbitrary case-insensitive reference text]: https://www.mozilla.org\n"
            "[link text itself]: http://www.reddit.com\n"
        )

        references_tags = [
            "arbitrary case-insensitive reference text",
            "1",
            "link text itself",
        ]
        references_links = [
            "https://www.mozilla.org",
            "http://slashdot.org",
            "http://www.reddit.com",
        ]

        for i in range(3):
            reference.new_link(references_links[i], "text", references_tags[i])

        actual_reference_markdown = reference.get_references_as_markdown()

        self.assertEqual(expected_references_markdown, actual_reference_markdown)

    def test_get_reference_when_references_are_empty(self):
        reference = Reference()
        expected_references_markdown = ""
        actual_references_markdown = reference.get_references_as_markdown()

        self.assertEqual(expected_references_markdown, actual_references_markdown)

    def test_tooltip_get_reference(self):
        reference = Reference()
        references_tags = [
            "arbitrary case-insensitive reference text",
            "1",
            "link text itself",
        ]
        references_links = [
            "https://www.mozilla.org",
            "http://slashdot.org",
            "http://www.reddit.com",
        ]
        expected_references_markdown = (
            "\n\n\n"
            "[1]: http://slashdot.org\n"
            "[arbitrary case-insensitive reference text]: https://www.mozilla.org\n"
            "[link text itself]: http://www.reddit.com 'my tooltip'\n"
            "[my link text]: https://my.link.text.com 'my second tooltip'\n"
        )

        for i in range(2):
            reference.new_link(references_links[i], "text", references_tags[i])

        reference.new_link(
            references_links[2], "text", references_tags[2], tooltip="my tooltip"
        )
        reference.new_link(
            "https://my.link.text.com", "my link text", tooltip="my second tooltip"
        )

        actual_reference_markdown = reference.get_references_as_markdown()

        self.assertEqual(expected_references_markdown, actual_reference_markdown)
