# Python
#
# This module implements tests for TextUtils class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.tools.TextUtils import TextUtils

__author__ = "didix21"
__project__ = "MdUtils"


class TestTextUtils(TestCase):
    def test_bold(self):
        expects = "**Bold Example**"
        self.assertEqual(expects, TextUtils.bold("Bold Example"))

    def test_italics(self):
        expects = "*Italics Example*"
        self.assertEqual(expects, TextUtils.italics("Italics Example"))

    def test_inline_code(self):
        expects = "``Inline Code Example``"
        self.assertEqual(expects, TextUtils.inline_code("Inline Code Example"))

    def test_center_text(self):
        expects = "<center>Text Center Alignment Example</center>"
        self.assertEqual(
            expects, TextUtils.center_text("Text Center Alignment Example")
        )

    def test_text_color(self):
        expects = '<font color="red">Red Color Example</font>'
        self.assertEqual(expects, TextUtils.text_color("Red Color Example", "red"))

    def test_text_external_link(self):
        text = "Text Example"
        link = "https://link.example.org"
        expects = "[" + text + "](" + link + ")"
        self.assertEqual(expects, TextUtils.text_external_link(text, link))

    def test_insert_code(self):
        code = (
            "mdFile.new_header(level=1, title='Atx Header 1')\n"
            "mdFile.new_header(level=2, title='Atx Header 2')\n"
            "mdFile.new_header(level=3, title='Atx Header 3')\n"
            "mdFile.new_header(level=4, title='Atx Header 4')\n"
            "mdFile.new_header(level=5, title='Atx Header 5')\n"
            "mdFile.new_header(level=6, title='Atx Header 6')\n"
        )
        expects = "```\n" + code + "\n```"
        self.assertEqual(TextUtils.insert_code(code), expects)
        language = "python"
        expects = "```" + language + "\n" + code + "\n```"
        self.assertEqual(expects, TextUtils.insert_code(code, language))

    def test_text_format(self):
        color = "red"
        text = "Text Format"
        text_color_center_c = TextUtils.inline_code(text)
        text_color = TextUtils.text_color(text_color_center_c, color)
        text_color_center = TextUtils.center_text(text_color)
        text_color_center_cb = TextUtils.bold(text_color_center)
        text_color_center_cbi = TextUtils.italics(text_color_center_cb)

        expects = text_color_center_cbi
        actual = TextUtils.text_format(
            text, bold_italics_code="bci", color="red", align="center"
        )

        self.assertEqual(expects, actual)

    def test_add_tooltip(self):
        expects = "https://link.com 'tooltip'"

        self.assertEqual(
            expects, TextUtils.add_tooltip(link="https://link.com", tip="tooltip")
        )
