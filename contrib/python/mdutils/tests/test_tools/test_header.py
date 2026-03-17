# Python
#
# This module implements tests for Header class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.tools.Header import Header, AtxHeaderLevel, SetextHeaderLevel, HeaderStyle


class TestHeader(TestCase):

    def test_atx_without_header_id(self):
        title = "Text Title Atx"

        for level in AtxHeaderLevel:
            result = f"\n{'#' * level.value} {title}\n"
            self.assertEqual(Header.atx(level, title), result)

    def test_atx_with_header_id(self):
        title = "Text Title Atx"
        header_id = "myheader"

        for level in AtxHeaderLevel:
            result = f"\n{'#' * level.value} {title} {{#myheader}}\n"
            self.assertEqual(Header.atx(level, title, header_id), result)

    def test_setext_level_1(self):
        title = "Text Title Setext 1"
        result = "\n" + title + "\n" + "".join(["=" for _ in title]) + "\n"
        self.assertEqual(Header.setext(SetextHeaderLevel.TITLE, title), result)

    def test_setext_level_2(self):
        title = "Text Title Setext 2"
        result = "\n" + title + "\n" + "".join(["-" for _ in title]) + "\n"
        self.assertEqual(Header.setext(SetextHeaderLevel.HEADING, title), result)

    def test_header(self):
        atx_headers = [
            Header.atx(AtxHeaderLevel.TITLE, "Atx Example"),
            Header.atx(AtxHeaderLevel.HEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.SUBHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.SUBSUBHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.MINORHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.LEASTHEADING, "Atx Example"),
        ]

        setext_headers = [
            Header.setext(SetextHeaderLevel.TITLE, "Setext Example"),
            Header.setext(SetextHeaderLevel.HEADING, "Setext Example"),
        ]

        for level in AtxHeaderLevel:
            title = "Atx Example"
            header = str(Header(level.value, title, style=HeaderStyle.ATX))
            self.assertEqual(header, atx_headers[level.value - 1])

        for level in SetextHeaderLevel:
            title = "Setext Example"  
            header = str(Header(level.value, title, style=HeaderStyle.SETEXT))
            self.assertEqual(header, setext_headers[level.value - 1])

    def test_choose_header(self):
        atx_headers = [
            Header.atx(AtxHeaderLevel.TITLE, "Atx Example"),
            Header.atx(AtxHeaderLevel.HEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.SUBHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.SUBSUBHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.MINORHEADING, "Atx Example"),
            Header.atx(AtxHeaderLevel.LEASTHEADING, "Atx Example"),
        ]

        setext_headers = [
            Header.setext(SetextHeaderLevel.TITLE, "Setext Example"),
            Header.setext(SetextHeaderLevel.HEADING, "Setext Example"),
        ]

        for level in AtxHeaderLevel:
            title = "Atx Example"
            header = Header.choose_header(level.value, title, style="atx")
            self.assertEqual(header, atx_headers[level.value - 1])

        for level in SetextHeaderLevel:
            title = "Setext Example"
            header = Header.choose_header(level.value, title, style="setext")
            self.assertEqual(header, setext_headers[level.value - 1])
