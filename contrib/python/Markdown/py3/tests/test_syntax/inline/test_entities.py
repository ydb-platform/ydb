"""
Python Markdown

A Python implementation of John Gruber's Markdown.

Documentation: https://python-markdown.github.io/
GitHub: https://github.com/Python-Markdown/markdown/
PyPI: https://pypi.org/project/Markdown/

Started by Manfred Stienstra (http://www.dwerg.net/).
Maintained for a few years by Yuri Takhteyev (http://www.freewisdom.org).
Currently maintained by Waylan Limberg (https://github.com/waylan),
Dmitry Shachnev (https://github.com/mitya57) and Isaac Muse (https://github.com/facelessuser).

Copyright 2007-2023 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).
"""

from markdown.test_tools import TestCase


class TestEntities(TestCase):

    def test_named_entities(self):
        self.assertMarkdownRenders("&amp;", "<p>&amp;</p>")
        self.assertMarkdownRenders("&sup2;", "<p>&sup2;</p>")
        self.assertMarkdownRenders("&Aacute;", "<p>&Aacute;</p>")

    def test_decimal_entities(self):
        self.assertMarkdownRenders("&#38;", "<p>&#38;</p>")
        self.assertMarkdownRenders("&#178;", "<p>&#178;</p>")

    def test_hexadecimal_entities(self):
        self.assertMarkdownRenders("&#x00026;", "<p>&#x00026;</p>")
        self.assertMarkdownRenders("&#xB2;", "<p>&#xB2;</p>")

    def test_false_entities(self):
        self.assertMarkdownRenders("&not an entity;", "<p>&amp;not an entity;</p>")
        self.assertMarkdownRenders("&#B2;", "<p>&amp;#B2;</p>")
        self.assertMarkdownRenders("&#xnothex;", "<p>&amp;#xnothex;</p>")
