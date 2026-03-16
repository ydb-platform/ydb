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


class TestParagraphBlocks(TestCase):

    def test_simple_paragraph(self):
        self.assertMarkdownRenders(
            'A simple paragraph.',

            '<p>A simple paragraph.</p>'
        )

    def test_blank_line_before_paragraph(self):
        self.assertMarkdownRenders(
            '\nA paragraph preceded by a blank line.',

            '<p>A paragraph preceded by a blank line.</p>'
        )

    def test_multiline_paragraph(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is a paragraph
                on multiple lines
                with hard returns.
                """
            ),
            self.dedent(
                """
                <p>This is a paragraph
                on multiple lines
                with hard returns.</p>
                """
            )
        )

    def test_paragraph_long_line(self):
        self.assertMarkdownRenders(
            'A very long long long long long long long long long long long long long long long long long long long '
            'long long long long long long long long long long long long long paragraph on 1 line.',

            '<p>A very long long long long long long long long long long long long long long long long long long '
            'long long long long long long long long long long long long long long paragraph on 1 line.</p>'
        )

    def test_2_paragraphs_long_line(self):
        self.assertMarkdownRenders(
            'A very long long long long long long long long long long long long long long long long long long long '
            'long long long long long long long long long long long long long paragraph on 1 line.\n\n'

            'A new long long long long long long long long long long long long long long long '
            'long paragraph on 1 line.',

            '<p>A very long long long long long long long long long long long long long long long long long long '
            'long long long long long long long long long long long long long long paragraph on 1 line.</p>\n'
            '<p>A new long long long long long long long long long long long long long long long '
            'long paragraph on 1 line.</p>'
        )

    def test_consecutive_paragraphs(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Paragraph 1.

                Paragraph 2.
                """
            ),
            self.dedent(
                """
                <p>Paragraph 1.</p>
                <p>Paragraph 2.</p>
                """
            )
        )

    def test_consecutive_paragraphs_tab(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Paragraph followed by a line with a tab only.
                \t
                Paragraph after a line with a tab only.
                """
            ),
            self.dedent(
                """
                <p>Paragraph followed by a line with a tab only.</p>
                <p>Paragraph after a line with a tab only.</p>
                """
            )
        )

    def test_consecutive_paragraphs_space(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Paragraph followed by a line with a space only.

                Paragraph after a line with a space only.
                """
            ),
            self.dedent(
                """
                <p>Paragraph followed by a line with a space only.</p>
                <p>Paragraph after a line with a space only.</p>
                """
            )
        )

    def test_consecutive_multiline_paragraphs(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Paragraph 1, line 1.
                Paragraph 1, line 2.

                Paragraph 2, line 1.
                Paragraph 2, line 2.
                """
            ),
            self.dedent(
                """
                <p>Paragraph 1, line 1.
                Paragraph 1, line 2.</p>
                <p>Paragraph 2, line 1.
                Paragraph 2, line 2.</p>
                """
            )
        )

    def test_paragraph_leading_space(self):
        self.assertMarkdownRenders(
            ' A paragraph with 1 leading space.',

            '<p>A paragraph with 1 leading space.</p>'
        )

    def test_paragraph_2_leading_spaces(self):
        self.assertMarkdownRenders(
            '  A paragraph with 2 leading spaces.',

            '<p>A paragraph with 2 leading spaces.</p>'
        )

    def test_paragraph_3_leading_spaces(self):
        self.assertMarkdownRenders(
            '   A paragraph with 3 leading spaces.',

            '<p>A paragraph with 3 leading spaces.</p>'
        )

    def test_paragraph_trailing_leading_space(self):
        self.assertMarkdownRenders(
            ' A paragraph with 1 trailing and 1 leading space. ',

            '<p>A paragraph with 1 trailing and 1 leading space. </p>'
        )

    def test_paragraph_trailing_tab(self):
        self.assertMarkdownRenders(
            'A paragraph with 1 trailing tab.\t',

            '<p>A paragraph with 1 trailing tab.    </p>'
        )

    def test_paragraphs_CR(self):
        self.assertMarkdownRenders(
            'Paragraph 1, line 1.\rParagraph 1, line 2.\r\rParagraph 2, line 1.\rParagraph 2, line 2.\r',

            self.dedent(
                """
                <p>Paragraph 1, line 1.
                Paragraph 1, line 2.</p>
                <p>Paragraph 2, line 1.
                Paragraph 2, line 2.</p>
                """
            )
        )

    def test_paragraphs_LF(self):
        self.assertMarkdownRenders(
            'Paragraph 1, line 1.\nParagraph 1, line 2.\n\nParagraph 2, line 1.\nParagraph 2, line 2.\n',

            self.dedent(
                """
                <p>Paragraph 1, line 1.
                Paragraph 1, line 2.</p>
                <p>Paragraph 2, line 1.
                Paragraph 2, line 2.</p>
                """
            )
        )

    def test_paragraphs_CR_LF(self):
        self.assertMarkdownRenders(
            'Paragraph 1, line 1.\r\nParagraph 1, line 2.\r\n\r\nParagraph 2, line 1.\r\nParagraph 2, line 2.\r\n',

            self.dedent(
                """
                <p>Paragraph 1, line 1.
                Paragraph 1, line 2.</p>
                <p>Paragraph 2, line 1.
                Paragraph 2, line 2.</p>
                """
            )
        )

    def test_paragraphs_no_list(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Paragraph:
                * no list

                Paragraph
                 * no list

                Paragraph:
                  * no list

                Paragraph:
                   * no list
                """
            ),
            '<p>Paragraph:\n'
            '* no list</p>\n'
            '<p>Paragraph\n'
            ' * no list</p>\n'
            '<p>Paragraph:\n'
            '  * no list</p>\n'
            '<p>Paragraph:\n'
            '   * no list</p>',
        )
