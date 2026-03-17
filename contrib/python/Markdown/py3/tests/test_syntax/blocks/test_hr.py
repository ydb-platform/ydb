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


class TestHorizontalRules(TestCase):

    def test_hr_asterisks(self):
        self.assertMarkdownRenders(
            '***',

            '<hr />'
        )

    def test_hr_asterisks_spaces(self):
        self.assertMarkdownRenders(
            '* * *',

            '<hr />'
        )

    def test_hr_asterisks_long(self):
        self.assertMarkdownRenders(
            '*******',

            '<hr />'
        )

    def test_hr_asterisks_spaces_long(self):
        self.assertMarkdownRenders(
            '* * * * * * *',

            '<hr />'
        )

    def test_hr_asterisks_1_indent(self):
        self.assertMarkdownRenders(
            ' ***',

            '<hr />'
        )

    def test_hr_asterisks_spaces_1_indent(self):
        self.assertMarkdownRenders(
            ' * * *',

            '<hr />'
        )

    def test_hr_asterisks_2_indent(self):
        self.assertMarkdownRenders(
            '  ***',

            '<hr />'
        )

    def test_hr_asterisks_spaces_2_indent(self):
        self.assertMarkdownRenders(
            '  * * *',

            '<hr />'
        )

    def test_hr_asterisks_3_indent(self):
        self.assertMarkdownRenders(
            '   ***',

            '<hr />'
        )

    def test_hr_asterisks_spaces_3_indent(self):
        self.assertMarkdownRenders(
            '   * * *',

            '<hr />'
        )

    def test_hr_asterisks_trailing_space(self):
        self.assertMarkdownRenders(
            '*** ',

            '<hr />'
        )

    def test_hr_asterisks_spaces_trailing_space(self):
        self.assertMarkdownRenders(
            '* * * ',

            '<hr />'
        )

    def test_hr_hyphens(self):
        self.assertMarkdownRenders(
            '---',

            '<hr />'
        )

    def test_hr_hyphens_spaces(self):
        self.assertMarkdownRenders(
            '- - -',

            '<hr />'
        )

    def test_hr_hyphens_long(self):
        self.assertMarkdownRenders(
            '-------',

            '<hr />'
        )

    def test_hr_hyphens_spaces_long(self):
        self.assertMarkdownRenders(
            '- - - - - - -',

            '<hr />'
        )

    def test_hr_hyphens_1_indent(self):
        self.assertMarkdownRenders(
            ' ---',

            '<hr />'
        )

    def test_hr_hyphens_spaces_1_indent(self):
        self.assertMarkdownRenders(
            ' - - -',

            '<hr />'
        )

    def test_hr_hyphens_2_indent(self):
        self.assertMarkdownRenders(
            '  ---',

            '<hr />'
        )

    def test_hr_hyphens_spaces_2_indent(self):
        self.assertMarkdownRenders(
            '  - - -',

            '<hr />'
        )

    def test_hr_hyphens_3_indent(self):
        self.assertMarkdownRenders(
            '   ---',

            '<hr />'
        )

    def test_hr_hyphens_spaces_3_indent(self):
        self.assertMarkdownRenders(
            '   - - -',

            '<hr />'
        )

    def test_hr_hyphens_trailing_space(self):
        self.assertMarkdownRenders(
            '--- ',

            '<hr />'
        )

    def test_hr_hyphens_spaces_trailing_space(self):
        self.assertMarkdownRenders(
            '- - - ',

            '<hr />'
        )

    def test_hr_underscores(self):
        self.assertMarkdownRenders(
            '___',

            '<hr />'
        )

    def test_hr_underscores_spaces(self):
        self.assertMarkdownRenders(
            '_ _ _',

            '<hr />'
        )

    def test_hr_underscores_long(self):
        self.assertMarkdownRenders(
            '_______',

            '<hr />'
        )

    def test_hr_underscores_spaces_long(self):
        self.assertMarkdownRenders(
            '_ _ _ _ _ _ _',

            '<hr />'
        )

    def test_hr_underscores_1_indent(self):
        self.assertMarkdownRenders(
            ' ___',

            '<hr />'
        )

    def test_hr_underscores_spaces_1_indent(self):
        self.assertMarkdownRenders(
            ' _ _ _',

            '<hr />'
        )

    def test_hr_underscores_2_indent(self):
        self.assertMarkdownRenders(
            '  ___',

            '<hr />'
        )

    def test_hr_underscores_spaces_2_indent(self):
        self.assertMarkdownRenders(
            '  _ _ _',

            '<hr />'
        )

    def test_hr_underscores_3_indent(self):
        self.assertMarkdownRenders(
            '   ___',

            '<hr />'
        )

    def test_hr_underscores_spaces_3_indent(self):
        self.assertMarkdownRenders(
            '   _ _ _',

            '<hr />'
        )

    def test_hr_underscores_trailing_space(self):
        self.assertMarkdownRenders(
            '___ ',

            '<hr />'
        )

    def test_hr_underscores_spaces_trailing_space(self):
        self.assertMarkdownRenders(
            '_ _ _ ',

            '<hr />'
        )

    def test_hr_before_paragraph(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ***
                An HR followed by a paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <hr />
                <p>An HR followed by a paragraph with no blank line.</p>
                """
            )
        )

    def test_hr_after_paragraph(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                A paragraph followed by an HR with no blank line.
                ***
                """
            ),
            self.dedent(
                """
                <p>A paragraph followed by an HR with no blank line.</p>
                <hr />
                """
            )
        )

    def test_hr_after_emstrong(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ***text***
                ***
                """
            ),
            self.dedent(
                """
                <p><strong><em>text</em></strong></p>
                <hr />
                """
            )
        )

    def test_not_hr_2_asterisks(self):
        self.assertMarkdownRenders(
            '**',

            '<p>**</p>'
        )

    def test_not_hr_2_asterisks_spaces(self):
        self.assertMarkdownRenders(
            '* *',

            self.dedent(
                """
                <ul>
                <li>*</li>
                </ul>
                """
            )
        )

    def test_not_hr_2_hyphens(self):
        self.assertMarkdownRenders(
            '--',

            '<p>--</p>'
        )

    def test_not_hr_2_hyphens_spaces(self):
        self.assertMarkdownRenders(
            '- -',

            self.dedent(
                """
                <ul>
                <li>-</li>
                </ul>
                """
            )
        )

    def test_not_hr_2_underscores(self):
        self.assertMarkdownRenders(
            '__',

            '<p>__</p>'
        )

    def test_not_hr_2_underscores_spaces(self):
        self.assertMarkdownRenders(
            '_ _',

            '<p>_ _</p>'
        )

    def test_2_consecutive_hr(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                - - -
                - - -
                """
            ),
            self.dedent(
                """
                <hr />
                <hr />
                """
            )
        )

    def test_not_hr_end_in_char(self):
        self.assertMarkdownRenders(
            '--------------------------------------c',

            '<p>--------------------------------------c</p>'
        )
