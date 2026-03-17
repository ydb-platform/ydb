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


class TestInlineLinks(TestCase):

    def test_nested_square_brackets(self):
        self.assertMarkdownRenders(
            """[Text[[[[[[[]]]]]]][]](http://link.com) more text""",
            """<p><a href="http://link.com">Text[[[[[[[]]]]]]][]</a> more text</p>"""
        )

    def test_nested_round_brackets(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/(((((((()))))))())) more text""",
            """<p><a href="http://link.com/(((((((()))))))())">Text</a> more text</p>"""
        )

    def test_nested_escaped_brackets(self):
        self.assertMarkdownRenders(
            R"""[Text](/url\(test\) "title").""",
            """<p><a href="/url(test)" title="title">Text</a>.</p>"""
        )

    def test_nested_escaped_brackets_and_angles(self):
        self.assertMarkdownRenders(
            R"""[Text](</url\(test\)> "title").""",
            """<p><a href="/url(test)" title="title">Text</a>.</p>"""
        )

    def test_nested_unescaped_brackets(self):
        self.assertMarkdownRenders(
            R"""[Text](/url(test) "title").""",
            """<p><a href="/url(test)" title="title">Text</a>.</p>"""
        )

    def test_nested_unescaped_brackets_and_angles(self):
        self.assertMarkdownRenders(
            R"""[Text](</url(test)> "title").""",
            """<p><a href="/url(test)" title="title">Text</a>.</p>"""
        )

    def test_uneven_brackets_with_titles1(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/("title") more text""",
            """<p><a href="http://link.com/(" title="title">Text</a> more text</p>"""
        )

    def test_uneven_brackets_with_titles2(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/('"title") more text""",
            """<p><a href="http://link.com/('" title="title">Text</a> more text</p>"""
        )

    def test_uneven_brackets_with_titles3(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/("title)") more text""",
            """<p><a href="http://link.com/(" title="title)">Text</a> more text</p>"""
        )

    def test_uneven_brackets_with_titles4(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/( "title") more text""",
            """<p><a href="http://link.com/(" title="title">Text</a> more text</p>"""
        )

    def test_uneven_brackets_with_titles5(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/( "title)") more text""",
            """<p><a href="http://link.com/(" title="title)">Text</a> more text</p>"""
        )

    def test_mixed_title_quotes1(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/'"title") more text""",
            """<p><a href="http://link.com/'" title="title">Text</a> more text</p>"""
        )

    def test_mixed_title_quotes2(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/"'title') more text""",
            """<p><a href="http://link.com/&quot;" title="title">Text</a> more text</p>"""
        )

    def test_mixed_title_quotes3(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/with spaces'"and quotes" 'and title') more text""",
            """<p><a href="http://link.com/with spaces" title="&quot;and quotes&quot; 'and title">"""
            """Text</a> more text</p>"""
        )

    def test_mixed_title_quotes4(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/with spaces'"and quotes" 'and title") more text""",
            """<p><a href="http://link.com/with spaces'" title="and quotes&quot; 'and title">Text</a> more text</p>"""
        )

    def test_mixed_title_quotes5(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/with spaces '"and quotes" 'and title') more text""",
            """<p><a href="http://link.com/with spaces" title="&quot;and quotes&quot; 'and title">"""
            """Text</a> more text</p>"""
        )

    def test_mixed_title_quotes6(self):
        self.assertMarkdownRenders(
            """[Text](http://link.com/with spaces "and quotes" 'and title') more text""",
            """<p><a href="http://link.com/with spaces &quot;and quotes&quot;" title="and title">"""
            """Text</a> more text</p>"""
        )

    def test_single_quote(self):
        self.assertMarkdownRenders(
            """[test](link"notitle)""",
            """<p><a href="link&quot;notitle">test</a></p>"""
        )

    def test_angle_with_mixed_title_quotes(self):
        self.assertMarkdownRenders(
            """[Text](<http://link.com/with spaces '"and quotes"> 'and title') more text""",
            """<p><a href="http://link.com/with spaces '&quot;and quotes&quot;" title="and title">"""
            """Text</a> more text</p>"""
        )

    def test_amp_in_url(self):
        """Test amp in URLs."""

        self.assertMarkdownRenders(
            '[link](http://www.freewisdom.org/this&that)',
            '<p><a href="http://www.freewisdom.org/this&amp;that">link</a></p>'
        )
        self.assertMarkdownRenders(
            '[title](http://example.com/?a=1&amp;b=2)',
            '<p><a href="http://example.com/?a=1&amp;b=2">title</a></p>'
        )
        self.assertMarkdownRenders(
            '[title](http://example.com/?a=1&#x26;b=2)',
            '<p><a href="http://example.com/?a=1&#x26;b=2">title</a></p>'
        )

    def test_angles_and_nonsense_url(self):
        self.assertMarkdownRenders(
            '[test nonsense](<?}]*+|&)>).',
            '<p><a href="?}]*+|&amp;)">test nonsense</a>.</p>'
        )


class TestReferenceLinks(TestCase):

    def test_ref_link(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com
                """
            ),
            """<p><a href="http://example.com">Text</a></p>"""
        )

    def test_ref_link_angle_brackets(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: <http://example.com>
                """
            ),
            """<p><a href="http://example.com">Text</a></p>"""
        )

    def test_ref_link_no_space(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]:http://example.com
                """
            ),
            """<p><a href="http://example.com">Text</a></p>"""
        )

    def test_ref_link_angle_brackets_no_space(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]:<http://example.com>
                """
            ),
            """<p><a href="http://example.com">Text</a></p>"""
        )

    def test_ref_link_angle_brackets_title(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: <http://example.com> "title"
                """
            ),
            """<p><a href="http://example.com" title="title">Text</a></p>"""
        )

    def test_ref_link_title(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com "title"
                """
            ),
            """<p><a href="http://example.com" title="title">Text</a></p>"""
        )

    def test_ref_link_angle_brackets_title_no_space(self):
        # TODO: Maybe reevaluate this?
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: <http://example.com>"title"
                """
            ),
            """<p><a href="http://example.com&gt;&quot;title&quot;">Text</a></p>"""
        )

    def test_ref_link_title_no_space(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com"title"
                """
            ),
            """<p><a href="http://example.com&quot;title&quot;">Text</a></p>"""
        )

    def test_ref_link_single_quoted_title(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com 'title'
                """
            ),
            """<p><a href="http://example.com" title="title">Text</a></p>"""
        )

    def test_ref_link_title_nested_quote(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com "title'"
                """
            ),
            """<p><a href="http://example.com" title="title'">Text</a></p>"""
        )

    def test_ref_link_single_quoted_title_nested_quote(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com 'title"'
                """
            ),
            """<p><a href="http://example.com" title="title&quot;">Text</a></p>"""
        )

    def test_ref_link_override(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]: http://example.com 'ignore'
                [Text]: https://example.com 'override'
                """
            ),
            """<p><a href="https://example.com" title="override">Text</a></p>"""
        )

    def test_ref_link_title_no_blank_lines(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]
                [Text]: http://example.com "title"
                [Text]
                """
            ),
            self.dedent(
                """
                <p><a href="http://example.com" title="title">Text</a></p>
                <p><a href="http://example.com" title="title">Text</a></p>
                """
            )
        )

    def test_ref_link_multi_line(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]

                [Text]:
                    http://example.com
                    "title"
                """
            ),
            """<p><a href="http://example.com" title="title">Text</a></p>"""
        )

    def test_reference_newlines(self):
        """Test reference id whitespace cleanup."""

        self.assertMarkdownRenders(
            self.dedent(
                """
                Two things:

                 - I would like to tell you about the [code of
                   conduct][] we are using in this project.
                 - Only one in fact.

                [code of conduct]: https://github.com/Python-Markdown/markdown/blob/master/CODE_OF_CONDUCT.md
                """
            ),
            '<p>Two things:</p>\n<ul>\n<li>I would like to tell you about the '
            '<a href="https://github.com/Python-Markdown/markdown/blob/master/CODE_OF_CONDUCT.md">code of\n'
            '   conduct</a> we are using in this project.</li>\n<li>Only one in fact.</li>\n</ul>'
        )

    def test_reference_across_blocks(self):
        """Test references across blocks."""

        self.assertMarkdownRenders(
            self.dedent(
                """
                I would like to tell you about the [code of

                conduct][] we are using in this project.

                [code of conduct]: https://github.com/Python-Markdown/markdown/blob/master/CODE_OF_CONDUCT.md
                """
            ),
            '<p>I would like to tell you about the [code of</p>\n'
            '<p>conduct][] we are using in this project.</p>'
        )

    def test_ref_link_nested_left_bracket(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text[]

                [Text[]: http://example.com
                """
            ),
            self.dedent(
                """
                <p>[Text[]</p>
                <p>[Text[]: http://example.com</p>
                """
            )
        )

    def test_ref_link_nested_right_bracket(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text]]

                [Text]]: http://example.com
                """
            ),
            self.dedent(
                """
                <p>[Text]]</p>
                <p>[Text]]: http://example.com</p>
                """
            )
        )

    def test_ref_round_brackets(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                [Text][1].

                [Text][2].

                  [1]: /url(test) "title"
                  [2]: </url(test)> "title"
                """
            ),
            self.dedent(
                """
                <p><a href="/url(test)" title="title">Text</a>.</p>
                <p><a href="/url(test)" title="title">Text</a>.</p>
                """
            )
        )
