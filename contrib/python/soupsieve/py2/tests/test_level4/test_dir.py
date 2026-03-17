# -*- coding: utf-8 -*-
"""Test direction selectors."""
from __future__ import unicode_literals
from .. import util
import soupsieve as sv


class TestDir(util.TestCase):
    """Test direction selectors."""

    MARKUP = """
    <html id="0">
    <head></head>
    <body>
    <div id="1" dir="rtl">
      <span id="2">test1</span>
      <div id="3" dir="ltr">test2
        <div id="4" dir="auto"><!-- comment -->עִבְרִית<span id="5" dir="auto">()</span></div>
        <div id="6" dir="auto"><script></script><b> </b><span id="7"><!-- comment -->עִבְרִית</span></div>
        <span id="8" dir="auto">test3</span>
      </div>
      <input id="9" type="tel" value="333-444-5555" pattern="[0-9]{3}-[0-9]{3}-[0-9]{4}">
      <input id="10" type="tel" dir="auto" value="333-444-5555" pattern="[0-9]{3}-[0-9]{3}-[0-9]{4}">
      <input id="11" type="email" dir="auto" value="test@mail.com">
      <input id="12" type="search" dir="auto" value="()">
      <input id="13" type="url" dir="auto" value="https://test.com">
      <input id="14" type="search" dir="auto" value="עִבְרִית">
      <input id="15" type="search" dir="auto" value="">
      <input id="16" type="search" dir="auto">
      <textarea id="17" dir="auto">עִבְרִית</textarea>
      <textarea id="18" dir="auto"></textarea>
    </div>
    </body>
    </html>
    """

    def test_dir_rtl(self):
        """Test general direction right to left."""

        self.assert_selector(
            self.MARKUP,
            "div:dir(rtl)",
            ["1", "4", "6"],
            flags=util.HTML
        )

    def test_dir_ltr(self):
        """Test general direction left to right."""

        self.assert_selector(
            self.MARKUP,
            "div:dir(ltr)",
            ["3"],
            flags=util.HTML
        )

    def test_dir_conflict(self):
        """Test conflicting direction."""

        self.assert_selector(
            self.MARKUP,
            "div:dir(ltr):dir(rtl)",
            [],
            flags=util.HTML
        )

    def test_dir_xml(self):
        """Test direction with XML (not supported)."""

        self.assert_selector(
            self.MARKUP,
            "div:dir(ltr)",
            [],
            flags=util.XML
        )

    def test_dir_bidi_detect(self):
        """Test bidirectional detection."""

        self.assert_selector(
            self.MARKUP,
            "span:dir(rtl)",
            ['2', '5', '7'],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "span:dir(ltr)",
            ['8'],
            flags=util.HTML
        )

    def test_dir_on_input(self):
        """Test input direction rules."""

        self.assert_selector(
            self.MARKUP,
            ":is(input, textarea):dir(ltr)",
            ['9', '10', '11', '12', '13'],
            flags=util.HTML5
        )

    def test_dir_on_root(self):
        """Test that the root is assumed left to right if not explicitly defined."""

        self.assert_selector(
            self.MARKUP,
            "html:dir(ltr)",
            ['0'],
            flags=util.HTML
        )

    def test_dir_auto_root(self):
        """Test that the root is assumed left to right if auto used."""

        markup = """
        <html id="0" dir="auto">
        <head></head>
        <body>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            "html:dir(ltr)",
            ['0'],
            flags=util.HTML
        )

    def test_dir_on_input_root(self):
        """Test input direction when input is the root."""

        markup = """<input id="1" type="text" dir="auto">"""
        # Input is root
        for parser in util.available_parsers('html.parser', 'lxml', 'html5lib'):
            soup = self.soup(markup, parser)
            fragment = soup.input.extract()
            self.assertTrue(sv.match(":root:dir(ltr)", fragment, flags=sv.DEBUG))

    def test_iframe(self):
        """Test direction in `iframe`."""

        markup = """
        <html>
        <head></head>
        <body>
        <div id="1" dir="auto">
        <iframe>
        <html>
        <body>
        <div id="2" dir="auto">
        <!-- comment -->עִבְרִית
        <span id="5" dir="auto">()</span></div>
        </div>
        </body>
        </html>
        </iframe>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            "div:dir(ltr)",
            ['1'],
            flags=util.PYHTML
        )

        self.assert_selector(
            markup,
            "div:dir(rtl)",
            ['2'],
            flags=util.PYHTML
        )

    def test_xml_in_html(self):
        """Test cases for when we have XML in HTML."""

        markup = """
        <html>
        <head></head>
        <body>
        <div id="1" dir="auto">
        <math>
        <!-- comment -->עִבְרִית
        </math>
        other text
        </div>
        </body>
        </html>
        """

        self.assert_selector(
            markup,
            "div:dir(ltr)",
            ['1'],
            flags=util.HTML5
        )

        self.assert_selector(
            markup,
            "div:dir(rtl)",
            [],
            flags=util.HTML5
        )

        self.assert_selector(
            markup,
            "math:dir(rtl)",
            [],
            flags=util.HTML5
        )
