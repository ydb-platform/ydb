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


class TestLegacyEm(TestCase):
    def test_legacy_emphasis(self):
        self.assertMarkdownRenders(
            '_connected_words_',
            '<p><em>connected</em>words_</p>',
            extensions=['legacy_em']
        )

    def test_legacy_strong(self):
        self.assertMarkdownRenders(
            '__connected__words__',
            '<p><strong>connected</strong>words__</p>',
            extensions=['legacy_em']
        )

    def test_complex_emphasis_underscore(self):
        self.assertMarkdownRenders(
            'This is text __bold _italic bold___ with more text',
            '<p>This is text <strong>bold <em>italic bold</em></strong> with more text</p>',
            extensions=['legacy_em']
        )

    def test_complex_emphasis_underscore_mid_word(self):
        self.assertMarkdownRenders(
            'This is text __bold_italic bold___ with more text',
            '<p>This is text <strong>bold<em>italic bold</em></strong> with more text</p>',
            extensions=['legacy_em']
        )

    def test_complex_multple_underscore_type(self):

        self.assertMarkdownRenders(
            'traced ___along___ bla __blocked__ if other ___or___',
            '<p>traced <strong><em>along</em></strong> bla <strong>blocked</strong> if other <strong><em>or</em></strong></p>'  # noqa: E501
        )

    def test_complex_multple_underscore_type_variant2(self):

        self.assertMarkdownRenders(
            'on the __1-4 row__ of the AP Combat Table ___and___ receive',
            '<p>on the <strong>1-4 row</strong> of the AP Combat Table <strong><em>and</em></strong> receive</p>'
        )
