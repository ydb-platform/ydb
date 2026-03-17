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

Copyright 2007-2019 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).
"""

from markdown.test_tools import TestCase


class TestNotEmphasis(TestCase):

    def test_standalone_asterisk(self):
        self.assertMarkdownRenders(
            '*',
            '<p>*</p>'
        )

    def test_standalone_understore(self):
        self.assertMarkdownRenders(
            '_',
            '<p>_</p>'
        )

    def test_standalone_asterisks_consecutive(self):
        self.assertMarkdownRenders(
            'Foo * * * *',
            '<p>Foo * * * *</p>'
        )

    def test_standalone_understore_consecutive(self):
        self.assertMarkdownRenders(
            'Foo _ _ _ _',
            '<p>Foo _ _ _ _</p>'
        )

    def test_standalone_asterisks_pairs(self):
        self.assertMarkdownRenders(
            'Foo ** ** ** **',
            '<p>Foo ** ** ** **</p>'
        )

    def test_standalone_understore_pairs(self):
        self.assertMarkdownRenders(
            'Foo __ __ __ __',
            '<p>Foo __ __ __ __</p>'
        )

    def test_standalone_asterisks_triples(self):
        self.assertMarkdownRenders(
            'Foo *** *** *** ***',
            '<p>Foo *** *** *** ***</p>'
        )

    def test_standalone_understore_triples(self):
        self.assertMarkdownRenders(
            'Foo ___ ___ ___ ___',
            '<p>Foo ___ ___ ___ ___</p>'
        )

    def test_standalone_asterisk_in_text(self):
        self.assertMarkdownRenders(
            'foo * bar',
            '<p>foo * bar</p>'
        )

    def test_standalone_understore_in_text(self):
        self.assertMarkdownRenders(
            'foo _ bar',
            '<p>foo _ bar</p>'
        )

    def test_standalone_asterisks_in_text(self):
        self.assertMarkdownRenders(
            'foo * bar * baz',
            '<p>foo * bar * baz</p>'
        )

    def test_standalone_understores_in_text(self):
        self.assertMarkdownRenders(
            'foo _ bar _ baz',
            '<p>foo _ bar _ baz</p>'
        )

    def test_standalone_asterisks_with_newlines(self):
        self.assertMarkdownRenders(
            'foo\n* bar *\nbaz',
            '<p>foo\n* bar *\nbaz</p>'
        )

    def test_standalone_understores_with_newlines(self):
        self.assertMarkdownRenders(
            'foo\n_ bar _\nbaz',
            '<p>foo\n_ bar _\nbaz</p>'
        )

    def test_standalone_underscore_at_begin(self):
        self.assertMarkdownRenders(
            '_ foo_ bar',
            '<p>_ foo_ bar</p>'
        )

    def test_standalone_asterisk_at_end(self):
        self.assertMarkdownRenders(
            'foo *bar *',
            '<p>foo *bar *</p>'
        )

    def test_standalone_understores_at_begin_end(self):
        self.assertMarkdownRenders(
            '_ bar _',
            '<p>_ bar _</p>'
        )

    def test_complex_emphasis_asterisk(self):
        self.assertMarkdownRenders(
            'This is text **bold *italic bold*** with more text',
            '<p>This is text <strong>bold <em>italic bold</em></strong> with more text</p>'
        )

    def test_complex_emphasis_asterisk_mid_word(self):
        self.assertMarkdownRenders(
            'This is text **bold*italic bold*** with more text',
            '<p>This is text <strong>bold<em>italic bold</em></strong> with more text</p>'
        )

    def test_complex_emphasis_smart_underscore(self):
        self.assertMarkdownRenders(
            'This is text __bold _italic bold___ with more text',
            '<p>This is text <strong>bold <em>italic bold</em></strong> with more text</p>'
        )

    def test_complex_emphasis_smart_underscore_mid_word(self):
        self.assertMarkdownRenders(
            'This is text __bold_italic bold___ with more text',
            '<p>This is text __bold_italic bold___ with more text</p>'
        )

    def test_nested_emphasis(self):

        self.assertMarkdownRenders(
            'This text is **bold *italic* *italic* bold**',
            '<p>This text is <strong>bold <em>italic</em> <em>italic</em> bold</strong></p>'
        )

    def test_complex_multple_emphasis_type(self):

        self.assertMarkdownRenders(
            'traced ***along*** bla **blocked** if other ***or***',
            '<p>traced <strong><em>along</em></strong> bla <strong>blocked</strong> if other <strong><em>or</em></strong></p>'  # noqa: E501
        )

    def test_complex_multple_emphasis_type_variant2(self):

        self.assertMarkdownRenders(
            'on the **1-4 row** of the AP Combat Table ***and*** receive',
            '<p>on the <strong>1-4 row</strong> of the AP Combat Table <strong><em>and</em></strong> receive</p>'
        )

    def test_link_emphasis_outer(self):

        self.assertMarkdownRenders(
            '**[text](url)**',
            '<p><strong><a href="url">text</a></strong></p>'
        )

    def test_link_emphasis_inner(self):

        self.assertMarkdownRenders(
            '[**text**](url)',
            '<p><a href="url"><strong>text</strong></a></p>'
        )

    def test_link_emphasis_inner_outer(self):

        self.assertMarkdownRenders(
            '**[**text**](url)**',
            '<p><strong><a href="url"><strong>text</strong></a></strong></p>'
        )
