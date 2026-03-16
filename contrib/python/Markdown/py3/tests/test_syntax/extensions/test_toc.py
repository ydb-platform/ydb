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
from markdown import Markdown
from markdown.extensions.toc import TocExtension, strip_tags, unique
from markdown.extensions.nl2br import Nl2BrExtension


class TestTOC(TestCase):
    maxDiff = None
    default_kwargs = {
        'extensions': [TocExtension()]
    }

    def testTOCMarker(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]

                # Header 1

                ## Header 2
                '''
            ),
            '<div class="toc">\n'
              '<ul>\n'                                             # noqa
                '<li><a href="#header-1">Header 1</a>'             # noqa
                  '<ul>\n'                                         # noqa
                    '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                  '</ul>\n'                                        # noqa
                '</li>\n'                                          # noqa
              '</ul>\n'                                            # noqa
            '</div>\n'
            '<h1 id="header-1">Header 1</h1>\n'
            '<h2 id="header-2">Header 2</h2>'
        )

    def testNoTOCMarker(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header 2
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1">Header 1</h1>
                <h2 id="header-2">Header 2</h2>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-1">Header 1</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'
                )
            }
        )

    def testAlternateTOCMarker(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                {{marker}}

                # Header 1

                ## Header 2
                '''
            ),
            '<div class="toc">\n'
              '<ul>\n'                                             # noqa
                '<li><a href="#header-1">Header 1</a>'             # noqa
                  '<ul>\n'                                         # noqa
                    '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                  '</ul>\n'                                        # noqa
                '</li>\n'                                          # noqa
              '</ul>\n'                                            # noqa
            '</div>\n'
            '<h1 id="header-1">Header 1</h1>\n'
            '<h2 id="header-2">Header 2</h2>',
            extensions=[TocExtension(marker='{{marker}}')]
        )

    def testDisabledTOCMarker(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]

                # Header 1

                ## Header 2
                '''
            ),
            self.dedent(
                '''
                <p>[TOC]</p>
                <h1 id="header-1">Header 1</h1>
                <h2 id="header-2">Header 2</h2>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-1">Header 1</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'
                )
            },
            extensions=[TocExtension(marker='')]
        )

    def testTOCReset(self):
        md = Markdown(extensions=[TocExtension()])
        self.assertEqual(md.toc, '')
        self.assertEqual(md.toc_tokens, [])
        md.convert('# Header 1')
        self.assertEqual('<div class="toc">', md.toc[:17])
        self.assertEqual(len(md.toc_tokens), 1)
        md.reset()
        self.assertEqual(md.toc, '')
        self.assertEqual(md.toc_tokens, [])

    def testUniqueIds(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                #Header
                #Header
                #Header
                '''
            ),
            self.dedent(
                '''
                <h1 id="header">Header</h1>
                <h1 id="header_1">Header</h1>
                <h1 id="header_2">Header</h1>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                       # noqa
                        '<li><a href="#header">Header</a></li>\n'    # noqa
                        '<li><a href="#header_1">Header</a></li>\n'  # noqa
                        '<li><a href="#header_2">Header</a></li>\n'  # noqa
                      '</ul>\n'                                      # noqa
                    '</div>\n'
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': 'header',
                        'name': 'Header',
                        'html': 'Header',
                        'data-toc-label': '',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header_1',
                        'name': 'Header',
                        'html': 'Header',
                        'data-toc-label': '',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header_2',
                        'name': 'Header',
                        'html': 'Header',
                        'data-toc-label': '',
                        'children': []
                    },
                ]
            }
        )

    def testHtmlEntitiesInTOC(self):
        self.assertMarkdownRenders(
            '# Foo &amp; bar',
            '<h1 id="foo-bar">Foo &amp; bar</h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#foo-bar">Foo &amp; bar</a></li>\n'  # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'
                ),
                'toc_tokens': [{
                    'level': 1,
                    'id': 'foo-bar',
                    'name': 'Foo &amp; bar',
                    'html': 'Foo &amp; bar',
                    'data-toc-label': '',
                    'children': []
                }]
            }
        )

    def testHtmlSpecialCharsInTOC(self):
        self.assertMarkdownRenders(
            '# Foo > & bar',
            '<h1 id="foo-bar">Foo &gt; &amp; bar</h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                  # noqa
                        '<li><a href="#foo-bar">Foo &gt; &amp; bar</a></li>\n'  # noqa
                      '</ul>\n'                                                 # noqa
                    '</div>\n'
                ),
                'toc_tokens': [{
                    'level': 1,
                    'id': 'foo-bar',
                    'name': 'Foo &gt; &amp; bar',
                    'html': 'Foo &gt; &amp; bar',
                    'data-toc-label': '',
                    'children': []
                }]
            }
        )

    def testRawHtmlInTOC(self):
        self.assertMarkdownRenders(
            '# Foo <b>Bar</b> Baz.',
            '<h1 id="foo-bar-baz">Foo <b>Bar</b> Baz.</h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                # noqa
                        '<li><a href="#foo-bar-baz">Foo Bar Baz.</a></li>\n'  # noqa
                      '</ul>\n'                                               # noqa
                    '</div>\n'
                ),
                'toc_tokens': [{
                    'level': 1,
                    'id': 'foo-bar-baz',
                    'name': 'Foo Bar Baz.',
                    'html': 'Foo <b>Bar</b> Baz.',
                    'data-toc-label': '',
                    'children': []
                }]
            }
        )

    def testTOCBaseLevel(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Some Header
                ## Next Level
                ### Too High
                '''
            ),
            self.dedent(
                '''
                <h5 id="some-header">Some Header</h5>
                <h6 id="next-level">Next Level</h6>
                <h6 id="too-high">Too High</h6>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                 # noqa
                        '<li><a href="#some-header">Some Header</a>'           # noqa
                          '<ul>\n'                                             # noqa
                            '<li><a href="#next-level">Next Level</a></li>\n'  # noqa
                            '<li><a href="#too-high">Too High</a></li>\n'      # noqa
                          '</ul>\n'                                            # noqa
                        '</li>\n'                                              # noqa
                      '</ul>\n'                                                # noqa
                    '</div>\n'
                ),
                'toc_tokens': [{
                    'level': 5,
                    'id': 'some-header',
                    'name': 'Some Header',
                    'html': 'Some Header',
                    'data-toc-label': '',
                    'children': [
                        {
                            'level': 6,
                            'id': 'next-level',
                            'name': 'Next Level',
                            'html': 'Next Level',
                            'data-toc-label': '',
                            'children': []
                        },
                        {
                            'level': 6,
                            'id': 'too-high',
                            'name': 'Too High',
                            'html': 'Too High',
                            'data-toc-label': '',
                            'children': []
                        }
                    ]
                }]
            },
            extensions=[TocExtension(baselevel=5)]
        )

    def testHeaderInlineMarkup(self):
        self.assertMarkdownRenders(
            '#Some *Header* with [markup](http://example.com).',
            '<h1 id="some-header-with-markup">Some <em>Header</em> with '
            '<a href="http://example.com">markup</a>.</h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                     # noqa
                        '<li><a href="#some-header-with-markup">'  # noqa
                          'Some Header with markup.</a></li>\n'    # noqa
                      '</ul>\n'                                    # noqa
                    '</div>\n'
                ),
                'toc_tokens': [{
                    'level': 1,
                    'id': 'some-header-with-markup',
                    'name': 'Some Header with markup.',
                    'html': 'Some <em>Header</em> with <a href="http://example.com">markup</a>.',
                    'data-toc-label': '',
                    'children': []
                }]
            }
        )

    def testTOCTitle(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header 2
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1">Header 1</h1>
                <h2 id="header-2">Header 2</h2>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc"><span class="toctitle">Table of Contents</span>'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-1">Header 1</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'
                )
            },
            extensions=[TocExtension(title='Table of Contents')]
        )

    def testTOCUniqueFunc(self):
        ids = {'foo'}
        self.assertEqual(unique('foo', ids), 'foo_1')
        self.assertEqual(ids, {'foo', 'foo_1'})

    def testTocInHeaders(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                #[TOC]
                '''
            ),
            '<div class="toc">\n'                       # noqa
              '<ul>\n'                                  # noqa
                '<li><a href="#toc">[TOC]</a></li>\n'   # noqa
              '</ul>\n'                                 # noqa
            '</div>\n'                                  # noqa
            '<h1 id="toc">[TOC]</h1>'                   # noqa
        )

        self.assertMarkdownRenders(
            self.dedent(
                '''
                #[TOC]
                [TOC]
                '''
            ),
            '<h1 id="toc">[TOC]</h1>\n'                 # noqa
            '<div class="toc">\n'                       # noqa
              '<ul>\n'                                  # noqa
                '<li><a href="#toc">[TOC]</a></li>\n'   # noqa
              '</ul>\n'                                 # noqa
            '</div>'                                    # noqa
        )

        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                # *[TOC]*
                '''
            ),
            '<div class="toc">\n'                       # noqa
              '<ul>\n'                                  # noqa
                '<li><a href="#toc">[TOC]</a></li>\n'   # noqa
              '</ul>\n'                                 # noqa
            '</div>\n'                                  # noqa
            '<h1 id="toc"><em>[TOC]</em></h1>'          # noqa
        )

    def testTOCPermalink(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Hd 1

                ## Hd 2
                '''
            ),
            '<h1 id="hd-1">'
                'Hd 1'                                            # noqa
                '<a class="headerlink" href="#hd-1" title="PL">'  # noqa
                    '&para;'                                      # noqa
                '</a>'                                            # noqa
            '</h1>\n'
            '<h2 id="hd-2">'
                'Hd 2'                                            # noqa
                '<a class="headerlink" href="#hd-2" title="PL">'  # noqa
                    '&para;'                                      # noqa
                '</a>'                                            # noqa
            '</h2>',
            extensions=[TocExtension(permalink=True, permalink_title="PL")]
        )

    def testTOCPermalinkLeading(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Hd 1
                ## Hd 2
                '''
            ),
            '<h1 id="hd-1">'
                '<a class="headerlink" href="#hd-1" title="PL">'  # noqa
                    '&para;'                                      # noqa
                '</a>'                                            # noqa
                'Hd 1'                                            # noqa
            '</h1>\n'
            '<h2 id="hd-2">'
                '<a class="headerlink" href="#hd-2" title="PL">'  # noqa
                    '&para;'                                      # noqa
                '</a>'                                            # noqa
                'Hd 2'                                            # noqa
            '</h2>',
            extensions=[TocExtension(permalink=True, permalink_title="PL", permalink_leading=True)]
        )

    def testTOCInlineMarkupPermalink(self):
        self.assertMarkdownRenders(
            '# Code `in` hd',
            '<h1 id="code-in-hd">'
                'Code <code>in</code> hd'                               # noqa
                '<a class="headerlink" href="#code-in-hd" title="PL">'  # noqa
                    '&para;'                                            # noqa
                '</a>'                                                  # noqa
            '</h1>',
            extensions=[TocExtension(permalink=True, permalink_title="PL")]
        )

    def testTOCInlineMarkupPermalinkLeading(self):
        self.assertMarkdownRenders(
            '# Code `in` hd',
            '<h1 id="code-in-hd">'
                '<a class="headerlink" href="#code-in-hd" title="PL">'  # noqa
                    '&para;'                                            # noqa
                '</a>'                                                  # noqa
                'Code <code>in</code> hd'                               # noqa
            '</h1>',
            extensions=[TocExtension(permalink=True, permalink_title="PL", permalink_leading=True)]
        )

    def testAnchorLink(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header *2*
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1"><a class="toclink" href="#header-1">Header 1</a></h1>
                <h2 id="header-2"><a class="toclink" href="#header-2">Header <em>2</em></a></h2>
                '''
            ),
            extensions=[TocExtension(anchorlink=True)]
        )

    def testAnchorLinkWithSingleInlineCode(self):
        self.assertMarkdownRenders(
            '# This is `code`.',
            '<h1 id="this-is-code">'                        # noqa
                '<a class="toclink" href="#this-is-code">'  # noqa
                    'This is <code>code</code>.'            # noqa
                '</a>'                                      # noqa
            '</h1>',                                        # noqa
            extensions=[TocExtension(anchorlink=True)]
        )

    def testAnchorLinkWithDoubleInlineCode(self):
        self.assertMarkdownRenders(
            '# This is `code` and `this` too.',
            '<h1 id="this-is-code-and-this-too">'                           # noqa
                '<a class="toclink" href="#this-is-code-and-this-too">'     # noqa
                    'This is <code>code</code> and <code>this</code> too.'  # noqa
                '</a>'                                                      # noqa
            '</h1>',                                                        # noqa
            extensions=[TocExtension(anchorlink=True)]
        )

    def testPermalink(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                            # noqa
                'Header'                                                                  # noqa
                '<a class="headerlink" href="#header" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                      # noqa
            extensions=[TocExtension(permalink=True)]
        )

    def testPermalinkWithSingleInlineCode(self):
        self.assertMarkdownRenders(
            '# This is `code`.',
            '<h1 id="this-is-code">'                                                            # noqa
                'This is <code>code</code>.'                                                    # noqa
                '<a class="headerlink" href="#this-is-code" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                            # noqa
            extensions=[TocExtension(permalink=True)]
        )

    def testPermalinkWithDoubleInlineCode(self):
        self.assertMarkdownRenders(
            '# This is `code` and `this` too.',
            '<h1 id="this-is-code-and-this-too">'                                                            # noqa
                'This is <code>code</code> and <code>this</code> too.'                                       # noqa
                '<a class="headerlink" href="#this-is-code-and-this-too" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                                         # noqa
            extensions=[TocExtension(permalink=True)]
        )

    def testMinMaxLevel(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1 not in TOC

                ## Header 2 not in TOC

                ### Header 3

                #### Header 4

                ##### Header 5 not in TOC
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1-not-in-toc">Header 1 not in TOC</h1>
                <h2 id="header-2-not-in-toc">Header 2 not in TOC</h2>
                <h3 id="header-3">Header 3</h3>
                <h4 id="header-4">Header 4</h4>
                <h5 id="header-5-not-in-toc">Header 5 not in TOC</h5>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-3">Header 3</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-4">Header 4</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'                                             # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 3,
                        'id': 'header-3',
                        'name': 'Header 3',
                        'html': 'Header 3',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 4,
                                'id': 'header-4',
                                'name': 'Header 4',
                                'html': 'Header 4',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth='3-4')]
        )

    def testMaxLevel(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header 2

                ### Header 3 not in TOC
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1">Header 1</h1>
                <h2 id="header-2">Header 2</h2>
                <h3 id="header-3-not-in-toc">Header 3 not in TOC</h3>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-1">Header 1</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-2">Header 2</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'                                             # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': 'header-1',
                        'name': 'Header 1',
                        'html': 'Header 1',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 2,
                                'id': 'header-2',
                                'name': 'Header 2',
                                'html': 'Header 2',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth=2)]
        )

    def testMinMaxLevelwithAnchorLink(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1 not in TOC

                ## Header 2 not in TOC

                ### Header 3

                #### Header 4

                ##### Header 5 not in TOC
                '''
            ),
            '<h1 id="header-1-not-in-toc">'                                                      # noqa
                '<a class="toclink" href="#header-1-not-in-toc">Header 1 not in TOC</a></h1>\n'  # noqa
            '<h2 id="header-2-not-in-toc">'                                                      # noqa
                '<a class="toclink" href="#header-2-not-in-toc">Header 2 not in TOC</a></h2>\n'  # noqa
            '<h3 id="header-3">'                                                                 # noqa
                '<a class="toclink" href="#header-3">Header 3</a></h3>\n'                        # noqa
            '<h4 id="header-4">'                                                                 # noqa
                '<a class="toclink" href="#header-4">Header 4</a></h4>\n'                        # noqa
            '<h5 id="header-5-not-in-toc">'                                                      # noqa
                '<a class="toclink" href="#header-5-not-in-toc">Header 5 not in TOC</a></h5>',   # noqa
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-3">Header 3</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-4">Header 4</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'                                             # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 3,
                        'id': 'header-3',
                        'name': 'Header 3',
                        'html': 'Header 3',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 4,
                                'id': 'header-4',
                                'name': 'Header 4',
                                'html': 'Header 4',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth='3-4', anchorlink=True)]
        )

    def testMinMaxLevelwithPermalink(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1 not in TOC

                ## Header 2 not in TOC

                ### Header 3

                #### Header 4

                ##### Header 5 not in TOC
                '''
            ),
            '<h1 id="header-1-not-in-toc">Header 1 not in TOC'                                                # noqa
                '<a class="headerlink" href="#header-1-not-in-toc" title="Permanent link">&para;</a></h1>\n'  # noqa
            '<h2 id="header-2-not-in-toc">Header 2 not in TOC'                                                # noqa
                '<a class="headerlink" href="#header-2-not-in-toc" title="Permanent link">&para;</a></h2>\n'  # noqa
            '<h3 id="header-3">Header 3'                                                                      # noqa
                '<a class="headerlink" href="#header-3" title="Permanent link">&para;</a></h3>\n'             # noqa
            '<h4 id="header-4">Header 4'                                                                      # noqa
                '<a class="headerlink" href="#header-4" title="Permanent link">&para;</a></h4>\n'             # noqa
            '<h5 id="header-5-not-in-toc">Header 5 not in TOC'                                                # noqa
                '<a class="headerlink" href="#header-5-not-in-toc" title="Permanent link">&para;</a></h5>',   # noqa
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                             # noqa
                        '<li><a href="#header-3">Header 3</a>'             # noqa
                          '<ul>\n'                                         # noqa
                            '<li><a href="#header-4">Header 4</a></li>\n'  # noqa
                          '</ul>\n'                                        # noqa
                        '</li>\n'                                          # noqa
                      '</ul>\n'                                            # noqa
                    '</div>\n'                                             # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 3,
                        'id': 'header-3',
                        'name': 'Header 3',
                        'html': 'Header 3',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 4,
                                'id': 'header-4',
                                'name': 'Header 4',
                                'html': 'Header 4',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth='3-4', permalink=True)]
        )

    def testMinMaxLevelwithBaseLevel(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # First Header

                ## Second Level

                ### Third Level

                #### Forth Level
                '''
            ),
            self.dedent(
                '''
                <h3 id="first-header">First Header</h3>
                <h4 id="second-level">Second Level</h4>
                <h5 id="third-level">Third Level</h5>
                <h6 id="forth-level">Forth Level</h6>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                  # noqa
                        '<li><a href="#second-level">Second Level</a>'          # noqa
                          '<ul>\n'                                              # noqa
                            '<li><a href="#third-level">Third Level</a></li>\n' # noqa
                          '</ul>\n'                                             # noqa
                        '</li>\n'                                               # noqa
                      '</ul>\n'                                                 # noqa
                    '</div>\n'                                                  # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 4,
                        'id': 'second-level',
                        'name': 'Second Level',
                        'html': 'Second Level',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 5,
                                'id': 'third-level',
                                'name': 'Third Level',
                                'html': 'Third Level',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth='4-5', baselevel=3)]
        )

    def testMaxLevelwithBaseLevel(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Some Header

                ## Next Level

                ### Too High
                '''
            ),
            self.dedent(
                '''
                <h2 id="some-header">Some Header</h2>
                <h3 id="next-level">Next Level</h3>
                <h4 id="too-high">Too High</h4>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                 # noqa
                        '<li><a href="#some-header">Some Header</a>'           # noqa
                          '<ul>\n'                                             # noqa
                            '<li><a href="#next-level">Next Level</a></li>\n'  # noqa
                          '</ul>\n'                                            # noqa
                        '</li>\n'                                              # noqa
                      '</ul>\n'                                                # noqa
                    '</div>\n'                                                 # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 2,
                        'id': 'some-header',
                        'name': 'Some Header',
                        'html': 'Some Header',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 3,
                                'id': 'next-level',
                                'name': 'Next Level',
                                'html': 'Next Level',
                                'data-toc-label': '',
                                'children': []
                            }
                        ]
                    }
                ]
            },
            extensions=[TocExtension(toc_depth=3, baselevel=2)]
        )

    def test_escaped_code(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]

                # `<test>`
                '''
            ),
            self.dedent(
                '''
                <div class="toc">
                <ul>
                <li><a href="#test">&lt;test&gt;</a></li>
                </ul>
                </div>
                <h1 id="test"><code>&lt;test&gt;</code></h1>
                '''
            ),
            extensions=['toc']
        )

    def test_escaped_char_in_id(self):
        self.assertMarkdownRenders(
            r'# escaped\_character',
            '<h1 id="escaped_character">escaped_character</h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                           # noqa
                        '<li><a href="#escaped_character">escaped_character</a></li>\n'  # noqa
                      '</ul>\n'                                                          # noqa
                    '</div>\n'                                                           # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': 'escaped_character',
                        'name': 'escaped_character',
                        'html': 'escaped_character',
                        'data-toc-label': '',
                        'children': []
                    }
                ]
            },
            extensions=['toc']
        )

    def test_escaped_char_in_attr_list(self):
        self.assertMarkdownRenders(
            r'# `*Foo*` { id="\*Foo\*" }',
            '<h1 id="*Foo*"><code>*Foo*</code></h1>',
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                           # noqa
                        '<li><a href="#*Foo*">*Foo*</a></li>\n'                          # noqa
                      '</ul>\n'                                                          # noqa
                    '</div>\n'                                                           # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': '*Foo*',
                        'name': '*Foo*',
                        'html': '<code>*Foo*</code>',
                        'data-toc-label': '',
                        'children': []
                    }
                ]
            },
            extensions=['toc', 'attr_list']
        )

    def testAutoLinkEmail(self):
        self.assertMarkdownRenders(
            '## <foo@example.org>',
            '<h2 id="fooexampleorg"><a href="&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#102;&#111;&#111;&#64;&#101;'
            '&#120;&#97;&#109;&#112;&#108;&#101;&#46;&#111;&#114;&#103;">&#102;&#111;&#111;&#64;&#101;&#120;&#97;'
            '&#109;&#112;&#108;&#101;&#46;&#111;&#114;&#103;</a></h2>',
            expected_attrs={
                'toc_tokens': [
                    {
                        'level': 2,
                        'id': 'fooexampleorg',
                        'name': '&#102;&#111;&#111;&#64;&#101;&#120;&#97;&#109;'
                                '&#112;&#108;&#101;&#46;&#111;&#114;&#103;',
                        'html': '<a href="&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#102;&#111;&#111;&#64;&#101;'
                                '&#120;&#97;&#109;&#112;&#108;&#101;&#46;&#111;&#114;&#103;">&#102;&#111;&#111;'
                                '&#64;&#101;&#120;&#97;&#109;&#112;&#108;&#101;&#46;&#111;&#114;&#103;</a>',
                        'data-toc-label': '',
                        'children': []
                    }
                ]
            },
            extensions=['toc']
        )

    def testAnchorLinkWithCustomClass(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header *2*
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1"><a class="custom" href="#header-1">Header 1</a></h1>
                <h2 id="header-2"><a class="custom" href="#header-2">Header <em>2</em></a></h2>
                '''
            ),
            extensions=[TocExtension(anchorlink=True, anchorlink_class="custom")]
        )

    def testAnchorLinkWithCustomClasses(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header *2*
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1"><a class="custom1 custom2" href="#header-1">Header 1</a></h1>
                <h2 id="header-2"><a class="custom1 custom2" href="#header-2">Header <em>2</em></a></h2>
                '''
            ),
            extensions=[TocExtension(anchorlink=True, anchorlink_class="custom1 custom2")]
        )

    def testPermalinkWithEmptyText(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                      # noqa
                'Header'                                                            # noqa
                '<a class="headerlink" href="#header" title="Permanent link"></a>'  # noqa
            '</h1>',                                                                # noqa
            extensions=[TocExtension(permalink="")]
        )

    def testPermalinkWithCustomClass(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                        # noqa
                'Header'                                                              # noqa
                '<a class="custom" href="#header" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                  # noqa
            extensions=[TocExtension(permalink=True, permalink_class="custom")]
        )

    def testPermalinkWithCustomClasses(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                                 # noqa
                'Header'                                                                       # noqa
                '<a class="custom1 custom2" href="#header" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                           # noqa
            extensions=[TocExtension(permalink=True, permalink_class="custom1 custom2")]
        )

    def testPermalinkWithCustomTitle(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                    # noqa
                'Header'                                                          # noqa
                '<a class="headerlink" href="#header" title="custom">&para;</a>'  # noqa
            '</h1>',                                                              # noqa
            extensions=[TocExtension(permalink=True, permalink_title="custom")]
        )

    def testPermalinkWithEmptyTitle(self):
        self.assertMarkdownRenders(
            '# Header',
            '<h1 id="header">'                                                    # noqa
                'Header'                                                          # noqa
                '<a class="headerlink" href="#header">&para;</a>'                 # noqa
            '</h1>',                                                              # noqa
            extensions=[TocExtension(permalink=True, permalink_title="")]
        )

    def testPermalinkWithUnicodeInID(self):
        from markdown.extensions.toc import slugify_unicode
        self.assertMarkdownRenders(
            '# Unicode ヘッダー',
            '<h1 id="unicode-ヘッダー">'                                                            # noqa
                'Unicode ヘッダー'                                                                  # noqa
                '<a class="headerlink" href="#unicode-ヘッダー" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                               # noqa
            extensions=[TocExtension(permalink=True, slugify=slugify_unicode)]
        )

    def testPermalinkWithUnicodeTitle(self):
        from markdown.extensions.toc import slugify_unicode
        self.assertMarkdownRenders(
            '# Unicode ヘッダー',
            '<h1 id="unicode-ヘッダー">'                                                        # noqa
                'Unicode ヘッダー'                                                              # noqa
                '<a class="headerlink" href="#unicode-ヘッダー" title="パーマリンク">&para;</a>'  # noqa
            '</h1>',                                                                           # noqa
            extensions=[TocExtension(permalink=True, permalink_title="パーマリンク", slugify=slugify_unicode)]
        )

    def testPermalinkWithExtendedLatinInID(self):
        self.assertMarkdownRenders(
            '# Théâtre',
            '<h1 id="theatre">'                                                            # noqa
                'Théâtre'                                                                  # noqa
                '<a class="headerlink" href="#theatre" title="Permanent link">&para;</a>'  # noqa
            '</h1>',                                                                       # noqa
            extensions=[TocExtension(permalink=True)]
        )

    def testNl2brCompatibility(self):
        self.assertMarkdownRenders(
            '[TOC]\ntext',
            '<p>[TOC]<br />\ntext</p>',
            extensions=[TocExtension(), Nl2BrExtension()]
        )

    def testTOCWithCustomClass(self):

        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                # Header
                '''
            ),
            self.dedent(
                '''
                <div class="custom">
                <ul>
                <li><a href="#header">Header</a></li>
                </ul>
                </div>
                <h1 id="header">Header</h1>
                '''
            ),
            extensions=[TocExtension(toc_class="custom")]
        )

    def testTOCWithCustomClasses(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                # Header
                '''
            ),
            self.dedent(
                '''
                <div class="custom1 custom2">
                <ul>
                <li><a href="#header">Header</a></li>
                </ul>
                </div>
                <h1 id="header">Header</h1>
                '''
            ),
            extensions=[TocExtension(toc_class="custom1 custom2")]
        )

    def testTOCWithEmptyTitleClass(self):

        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                # Header
                '''
            ),
            self.dedent(
                '''
                <div class="toc"><span>ToC</span><ul>
                <li><a href="#header">Header</a></li>
                </ul>
                </div>
                <h1 id="header">Header</h1>
                '''
            ),
            extensions=[TocExtension(title_class="", title='ToC')]
        )

    def testTOCWithCustomTitleClass(self):

        self.assertMarkdownRenders(
            self.dedent(
                '''
                [TOC]
                # Header
                '''
            ),
            self.dedent(
                '''
                <div class="toc"><span class="tocname">ToC</span><ul>
                <li><a href="#header">Header</a></li>
                </ul>
                </div>
                <h1 id="header">Header</h1>
                '''
            ),
            extensions=[TocExtension(title_class="tocname", title='ToC')]
        )

    def testTocWithAttrList(self):

        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1

                ## Header 2 { #foo }

                ## Header 3 { data-toc-label="Foo Bar" }

                # Header 4 { data-toc-label="Foo > &amp; < Baz" }

                # Header 5 { data-toc-label="Foo <b>Quux</b>" }
                '''
            ),
            self.dedent(
                '''
                <h1 id="header-1">Header 1</h1>
                <h2 id="foo">Header 2</h2>
                <h2 id="header-3">Header 3</h2>
                <h1 id="header-4">Header 4</h1>
                <h1 id="header-5">Header 5</h1>
                '''
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                        # noqa
                        '<li><a href="#header-1">Header 1</a>'                        # noqa
                          '<ul>\n'                                                    # noqa
                            '<li><a href="#foo">Header 2</a></li>\n'                  # noqa
                            '<li><a href="#header-3">Foo Bar</a></li>\n'              # noqa
                          '</ul>\n'                                                   # noqa
                        '</li>\n'                                                     # noqa
                        '<li><a href="#header-4">Foo &gt; &amp; &lt; Baz</a></li>\n'  # noqa
                        '<li><a href="#header-5">Foo Quux</a></li>\n'                 # noqa
                      '</ul>\n'                                                       # noqa
                    '</div>\n'
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': 'header-1',
                        'name': 'Header 1',
                        'html': 'Header 1',
                        'data-toc-label': '',
                        'children': [
                            {
                                'level': 2,
                                'id': 'foo',
                                'name': 'Header 2',
                                'html': 'Header 2',
                                'data-toc-label': '',
                                'children': []
                            },
                            {
                                'level': 2,
                                'id': 'header-3',
                                'name': 'Foo Bar',
                                'html': 'Header 3',
                                'data-toc-label': 'Foo Bar',
                                'children': []
                            }
                        ]
                    },
                    {
                        'level': 1,
                        'id': 'header-4',
                        'name': 'Foo &gt; &amp; &lt; Baz',
                        'html': 'Header 4',
                        'data-toc-label': 'Foo > &amp; < Baz',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header-5',
                        'name': 'Foo Quux',
                        'html': 'Header 5',
                        'data-toc-label': 'Foo <b>Quux</b>',
                        'children': []
                    },
                ]
            },
            extensions=[TocExtension(), 'attr_list']
        )

    def testHeadingRemoveFootnoteRef(self):

        self.assertMarkdownRenders(
            self.dedent(
                '''
                # Header 1[^1]
                # Header[^1] 2
                # Header *subelement*[^1] 3
                # Header[^1] double[^1] 4

                [^1]: footnote
                '''
            ),
            (
                '<h1 id="header-1">Header 1<sup id="fnref:1"><a class="footnote-ref" href="#fn:1">1</a></sup></h1>\n'
                '<h1 id="header-2">Header<sup id="fnref2:1"><a class="footnote-ref" href="#fn:1">1</a></sup> 2</h1>\n'
                '<h1 id="header-subelement-3">'
                'Header <em>subelement</em><sup id="fnref3:1"><a class="footnote-ref" href="#fn:1">1</a></sup> 3'
                '</h1>\n'
                '<h1 id="header-double-4">'
                'Header<sup id="fnref4:1"><a class="footnote-ref" href="#fn:1">1</a></sup> double'
                '<sup id="fnref5:1"><a class="footnote-ref" href="#fn:1">1</a></sup> 4'
                '</h1>\n'
                '<div class="footnote">\n'
                '<hr />\n'
                '<ol>\n'
                '<li id="fn:1">\n'
                '<p>'
                'footnote&#160;'
                '<a class="footnote-backref" href="#fnref:1" title="Jump back to footnote 1 in the text">&#8617;</a>'
                '<a class="footnote-backref" href="#fnref2:1" title="Jump back to footnote 1 in the text">&#8617;</a>'
                '<a class="footnote-backref" href="#fnref3:1" title="Jump back to footnote 1 in the text">&#8617;</a>'
                '<a class="footnote-backref" href="#fnref4:1" title="Jump back to footnote 1 in the text">&#8617;</a>'
                '<a class="footnote-backref" href="#fnref5:1" title="Jump back to footnote 1 in the text">&#8617;</a>'
                '</p>\n'
                '</li>\n'
                '</ol>\n'
                '</div>'
            ),
            expected_attrs={
                'toc': (
                    '<div class="toc">\n'
                      '<ul>\n'                                                               # noqa
                        '<li><a href="#header-1">Header 1</a></li>\n'                        # noqa
                        '<li><a href="#header-2">Header 2</a></li>\n'                        # noqa
                        '<li><a href="#header-subelement-3">Header subelement 3</a></li>\n'  # noqa
                        '<li><a href="#header-double-4">Header double 4</a></li>\n'          # noqa
                      '</ul>\n'                                                              # noqa
                    '</div>\n'                                                               # noqa
                ),
                'toc_tokens': [
                    {
                        'level': 1,
                        'id': 'header-1',
                        'name': 'Header 1',
                        'html': 'Header 1',
                        'data-toc-label': '',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header-2',
                        'name': 'Header 2',
                        'html': 'Header 2',
                        'data-toc-label': '',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header-subelement-3',
                        'name': 'Header subelement 3',
                        'html': 'Header <em>subelement</em> 3',
                        'data-toc-label': '',
                        'children': []
                    },
                    {
                        'level': 1,
                        'id': 'header-double-4',
                        'name': 'Header double 4',
                        'html': 'Header double 4',
                        'data-toc-label': '',
                        'children': []
                    }
                ]
            },
            extensions=[TocExtension(), 'footnotes']
        )


class testStripTags(TestCase):

    def testStripElement(self):
        self.assertEqual(
            strip_tags('foo <em>bar</em>'),
            'foo bar'
        )

    def testStripOpenElement(self):
        self.assertEqual(
            strip_tags('foo <em>bar'),
            'foo bar'
        )

    def testStripEmptyElement(self):
        self.assertEqual(
            strip_tags('foo <br />bar'),
            'foo bar'
        )

    def testDontStripOpenBracket(self):
        self.assertEqual(
            strip_tags('foo < bar'),
            'foo < bar'
        )

    def testDontStripCloseBracket(self):
        self.assertEqual(
            strip_tags('foo > bar'),
            'foo > bar'
        )

    def testStripCollapseWhitespace(self):
        self.assertEqual(
            strip_tags('foo <em>\tbar\t</em>'),
            'foo bar'
        )

    def testStripElementWithNewlines(self):
        self.assertEqual(
            strip_tags('foo <meta content="tag\nwith\nnewlines"> bar'),
            'foo bar'
        )

    def testStripComment(self):
        self.assertEqual(
            strip_tags('foo <!-- comment --> bar'),
            'foo bar'
        )

    def testStripCommentWithInnerTags(self):
        self.assertEqual(
            strip_tags('foo <!-- comment with <em> --> bar'),
            'foo bar'
        )

    def testStripCommentInElement(self):
        self.assertEqual(
            strip_tags('<em>foo <!-- comment --> bar<em>'),
            'foo bar'
        )

    def testDontStripHTMLEntities(self):
        self.assertEqual(
            strip_tags('foo &lt; &amp; &lt; bar'),
            'foo &lt; &amp; &lt; bar'
        )
