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


class TestDefList(TestCase):

    def test_def_list_with_ol(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''

                term

                :   this is a definition for term. it has
                    multiple lines in the first paragraph.

                    1.  first thing

                        first thing details in a second paragraph.

                    1.  second thing

                        second thing details in a second paragraph.

                    1.  third thing

                        third thing details in a second paragraph.
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>term</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <ol>
                <li>
                <p>first thing</p>
                <p>first thing details in a second paragraph.</p>
                </li>
                <li>
                <p>second thing</p>
                <p>second thing details in a second paragraph.</p>
                </li>
                <li>
                <p>third thing</p>
                <p>third thing details in a second paragraph.</p>
                </li>
                </ol>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list']
        )

    def test_def_list_with_ul(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''

                term

                :   this is a definition for term. it has
                    multiple lines in the first paragraph.

                    -   first thing

                        first thing details in a second paragraph.

                    -   second thing

                        second thing details in a second paragraph.

                    -   third thing

                        third thing details in a second paragraph.
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>term</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <ul>
                <li>
                <p>first thing</p>
                <p>first thing details in a second paragraph.</p>
                </li>
                <li>
                <p>second thing</p>
                <p>second thing details in a second paragraph.</p>
                </li>
                <li>
                <p>third thing</p>
                <p>third thing details in a second paragraph.</p>
                </li>
                </ul>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list']
        )

    def test_def_list_with_nesting(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''

                term

                :   this is a definition for term. it has
                    multiple lines in the first paragraph.

                    1.  first thing

                        first thing details in a second paragraph.

                        -   first nested thing

                            second nested thing details
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>term</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <ol>
                <li>
                <p>first thing</p>
                <p>first thing details in a second paragraph.</p>
                <ul>
                <li>
                <p>first nested thing</p>
                <p>second nested thing details</p>
                </li>
                </ul>
                </li>
                </ol>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list']
        )

    def test_def_list_with_nesting_self(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''

                term

                :   this is a definition for term. it has
                    multiple lines in the first paragraph.

                    inception

                    :   this is a definition for term. it has
                        multiple lines in the first paragraph.

                        - bullet point

                          another paragraph
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>term</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <dl>
                <dt>inception</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <ul>
                <li>bullet point</li>
                </ul>
                <p>another paragraph</p>
                </dd>
                </dl>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list']
        )

    def test_def_list_unreasonable_nesting(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''

                turducken

                :   this is a definition for term. it has
                    multiple lines in the first paragraph.

                    1.  ordered list

                        - nested list

                            term

                            :   definition

                                -   item 1 paragraph 1

                                    item 1 paragraph 2
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>turducken</dt>
                <dd>
                <p>this is a definition for term. it has
                multiple lines in the first paragraph.</p>
                <ol>
                <li>
                <p>ordered list</p>
                <ul>
                <li>
                <p>nested list</p>
                <dl>
                <dt>term</dt>
                <dd>
                <p>definition</p>
                <ul>
                <li>
                <p>item 1 paragraph 1</p>
                <p>item 1 paragraph 2</p>
                </li>
                </ul>
                </dd>
                </dl>
                </li>
                </ul>
                </li>
                </ol>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list']
        )

    def test_def_list_nested_admontions(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                term

                :   definition

                    !!! note "Admontion"

                        term

                        :   definition

                            1.  list

                                continue
                '''
            ),
            self.dedent(
                '''
                <dl>
                <dt>term</dt>
                <dd>
                <p>definition</p>
                <div class="admonition note">
                <p class="admonition-title">Admontion</p>
                <dl>
                <dt>term</dt>
                <dd>
                <p>definition</p>
                <ol>
                <li>
                <p>list</p>
                <p>continue</p>
                </li>
                </ol>
                </dd>
                </dl>
                </div>
                </dd>
                </dl>
                '''
            ),
            extensions=['def_list', 'admonition']
        )
