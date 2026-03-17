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


class TestAdmonition(TestCase):

    def test_with_lists(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                - List

                    !!! note "Admontion"

                        - Paragraph

                            Paragraph
                '''
            ),
            self.dedent(
                '''
                <ul>
                <li>
                <p>List</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <ul>
                <li>
                <p>Paragraph</p>
                <p>Paragraph</p>
                </li>
                </ul>
                </div>
                </li>
                </ul>
                '''
            ),
            extensions=['admonition']
        )

    def test_with_big_lists(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                - List

                    !!! note "Admontion"

                        - Paragraph

                            Paragraph

                        - Paragraph

                            paragraph
                '''
            ),
            self.dedent(
                '''
                <ul>
                <li>
                <p>List</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <ul>
                <li>
                <p>Paragraph</p>
                <p>Paragraph</p>
                </li>
                <li>
                <p>Paragraph</p>
                <p>paragraph</p>
                </li>
                </ul>
                </div>
                </li>
                </ul>
                '''
            ),
            extensions=['admonition']
        )

    def test_with_complex_lists(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                - List

                    !!! note "Admontion"

                        - Paragraph

                            !!! note "Admontion"

                                1. Paragraph

                                    Paragraph
                '''
            ),
            self.dedent(
                '''
                <ul>
                <li>
                <p>List</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <ul>
                <li>
                <p>Paragraph</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <ol>
                <li>
                <p>Paragraph</p>
                <p>Paragraph</p>
                </li>
                </ol>
                </div>
                </li>
                </ul>
                </div>
                </li>
                </ul>
                '''
            ),
            extensions=['admonition']
        )

    def test_definition_list(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                - List

                    !!! note "Admontion"

                        Term

                        :   Definition

                            More text

                        :   Another
                            definition

                            Even more text
                '''
            ),
            self.dedent(
                '''
                <ul>
                <li>
                <p>List</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <dl>
                <dt>Term</dt>
                <dd>
                <p>Definition</p>
                <p>More text</p>
                </dd>
                <dd>
                <p>Another
                definition</p>
                <p>Even more text</p>
                </dd>
                </dl>
                </div>
                </li>
                </ul>
                '''
            ),
            extensions=['admonition', 'def_list']
        )

    def test_with_preceding_text(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                foo
                **foo**
                !!! note "Admonition"
                '''
            ),
            self.dedent(
                '''
                <p>foo
                <strong>foo</strong></p>
                <div class="admonition note">
                <p class="admonition-title">Admonition</p>
                </div>
                '''
            ),
            extensions=['admonition']
        )

    def test_admontion_detabbing(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                !!! note "Admonition"
                    - Parent 1

                        - Child 1
                        - Child 2
                '''
            ),
            self.dedent(
                '''
                <div class="admonition note">
                <p class="admonition-title">Admonition</p>
                <ul>
                <li>
                <p>Parent 1</p>
                <ul>
                <li>Child 1</li>
                <li>Child 2</li>
                </ul>
                </li>
                </ul>
                </div>
                '''
            ),
            extensions=['admonition']
        )

    def test_admonition_first_indented(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                !!! danger "This is not"
                        one long admonition title
                '''
            ),
            self.dedent(
                '''
                <div class="admonition danger">
                <p class="admonition-title">This is not</p>
                <pre><code>one long admonition title
                </code></pre>
                </div>
                '''
            ),
            extensions=['admonition']
        )
