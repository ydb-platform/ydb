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


class TestCodeBlocks(TestCase):

    def test_spaced_codeblock(self):
        self.assertMarkdownRenders(
            '    # A code block.',

            self.dedent(
                """
                <pre><code># A code block.
                </code></pre>
                """
            )
        )

    def test_tabbed_codeblock(self):
        self.assertMarkdownRenders(
            '\t# A code block.',

            self.dedent(
                """
                <pre><code># A code block.
                </code></pre>
                """
            )
        )

    def test_multiline_codeblock(self):
        self.assertMarkdownRenders(
            '    # Line 1\n    # Line 2\n',

            self.dedent(
                """
                <pre><code># Line 1
                # Line 2
                </code></pre>
                """
            )
        )

    def test_codeblock_with_blankline(self):
        self.assertMarkdownRenders(
            '    # Line 1\n\n    # Line 2\n',

            self.dedent(
                """
                <pre><code># Line 1

                # Line 2
                </code></pre>
                """
            )
        )

    def test_codeblock_escape(self):
        self.assertMarkdownRenders(
            '    <foo & bar>',

            self.dedent(
                """
                <pre><code>&lt;foo &amp; bar&gt;
                </code></pre>
                """
            )
        )

    def test_codeblock_second_line(self):
        self.assertMarkdownRenders(
            '\n    Code on the second line',
            self.dedent(
                """
                <pre><code>Code on the second line
                </code></pre>
                """
            )
        )
