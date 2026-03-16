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

Python-Markdown Extension Regression Tests
==========================================

A collection of regression tests to confirm that the included extensions
continue to work as advertised. This used to be accomplished by `doctests`.
"""

import unittest
import markdown


class TestExtensionClass(unittest.TestCase):
    """ Test markdown.extensions.Extension. """

    def setUp(self):
        class TestExtension(markdown.extensions.Extension):
            config = {
                'foo': ['bar', 'Description of foo'],
                'bar': ['baz', 'Description of bar']
            }

        self.ext = TestExtension()
        self.ExtKlass = TestExtension

    def testGetConfig(self):
        self.assertEqual(self.ext.getConfig('foo'), 'bar')

    def testGetConfigDefault(self):
        self.assertEqual(self.ext.getConfig('baz'), '')
        self.assertEqual(self.ext.getConfig('baz', default='missing'), 'missing')

    def testGetConfigs(self):
        self.assertEqual(self.ext.getConfigs(), {'foo': 'bar', 'bar': 'baz'})

    def testGetConfigInfo(self):
        self.assertEqual(
            dict(self.ext.getConfigInfo()),
            dict([
                ('foo', 'Description of foo'),
                ('bar', 'Description of bar')
            ])
        )

    def testSetConfig(self):
        self.ext.setConfig('foo', 'baz')
        self.assertEqual(self.ext.getConfigs(), {'foo': 'baz', 'bar': 'baz'})

    def testSetConfigWithBadKey(self):
        # `self.ext.setConfig('bad', 'baz)` => `KeyError`
        self.assertRaises(KeyError, self.ext.setConfig, 'bad', 'baz')

    def testConfigAsKwargsOnInit(self):
        ext = self.ExtKlass(foo='baz', bar='blah')
        self.assertEqual(ext.getConfigs(), {'foo': 'baz', 'bar': 'blah'})


class TestMetaData(unittest.TestCase):
    """ Test `MetaData` extension. """

    def setUp(self):
        self.md = markdown.Markdown(extensions=['meta'])

    def testBasicMetaData(self):
        """ Test basic metadata. """

        text = '''Title: A Test Doc.
Author: Waylan Limberg
        John Doe
Blank_Data:

The body. This is paragraph one.'''
        self.assertEqual(
            self.md.convert(text),
            '<p>The body. This is paragraph one.</p>'
        )
        self.assertEqual(
            self.md.Meta, {
                'author': ['Waylan Limberg', 'John Doe'],
                'blank_data': [''],
                'title': ['A Test Doc.']
            }
        )

    def testYamlMetaData(self):
        """ Test metadata specified as simple YAML. """

        text = '''---
Title: A Test Doc.
Author: [Waylan Limberg, John Doe]
Blank_Data:
---

The body. This is paragraph one.'''
        self.assertEqual(
            self.md.convert(text),
            '<p>The body. This is paragraph one.</p>'
        )
        self.assertEqual(
            self.md.Meta, {
                'author': ['[Waylan Limberg, John Doe]'],
                'blank_data': [''],
                'title': ['A Test Doc.']
            }
        )

    def testMissingMetaData(self):
        """ Test document without Meta Data. """

        text = '    Some Code - not extra lines of meta data.'
        self.assertEqual(
            self.md.convert(text),
            '<pre><code>Some Code - not extra lines of meta data.\n'
            '</code></pre>'
        )
        self.assertEqual(self.md.Meta, {})

    def testMetaDataWithoutNewline(self):
        """ Test document with only metadata and no newline at end."""
        text = 'title: No newline'
        self.assertEqual(self.md.convert(text), '')
        self.assertEqual(self.md.Meta, {'title': ['No newline']})

    def testMetaDataReset(self):
        """ Test that reset call remove Meta entirely """

        text = '''Title: A Test Doc.
Author: Waylan Limberg
        John Doe
Blank_Data:

The body. This is paragraph one.'''
        self.md.convert(text)

        self.md.reset()
        self.assertEqual(self.md.Meta, {})


class TestWikiLinks(unittest.TestCase):
    """ Test `Wikilinks` Extension. """

    def setUp(self):
        self.md = markdown.Markdown(extensions=['wikilinks'])
        self.text = "Some text with a [[WikiLink]]."

    def testBasicWikilinks(self):
        """ Test `[[wikilinks]]`. """

        self.assertEqual(
            self.md.convert(self.text),
            '<p>Some text with a '
            '<a class="wikilink" href="/WikiLink/">WikiLink</a>.</p>'
        )

    def testWikilinkWhitespace(self):
        """ Test whitespace in `wikilinks`. """
        self.assertEqual(
            self.md.convert('[[ foo bar_baz ]]'),
            '<p><a class="wikilink" href="/foo_bar_baz/">foo bar_baz</a></p>'
        )
        self.assertEqual(
            self.md.convert('foo [[ ]] bar'),
            '<p>foo  bar</p>'
        )

    def testSimpleSettings(self):
        """ Test Simple Settings. """

        self.assertEqual(markdown.markdown(
            self.text, extensions=[
                markdown.extensions.wikilinks.WikiLinkExtension(
                    base_url='/wiki/',
                    end_url='.html',
                    html_class='foo')
                ]
            ),
            '<p>Some text with a '
            '<a class="foo" href="/wiki/WikiLink.html">WikiLink</a>.</p>')

    def testComplexSettings(self):
        """ Test Complex Settings. """

        md = markdown.Markdown(
            extensions=['wikilinks'],
            extension_configs={
                'wikilinks': [
                    ('base_url', 'http://example.com/'),
                    ('end_url', '.html'),
                    ('html_class', '')
                ]
            },
            safe_mode=True
        )
        self.assertEqual(
            md.convert(self.text),
            '<p>Some text with a '
            '<a href="http://example.com/WikiLink.html">WikiLink</a>.</p>'
        )

    def testWikilinksMetaData(self):
        """ test `MetaData` with `Wikilinks` Extension. """

        text = """wiki_base_url: http://example.com/
wiki_end_url:   .html
wiki_html_class:

Some text with a [[WikiLink]]."""
        md = markdown.Markdown(extensions=['meta', 'wikilinks'])
        self.assertEqual(
            md.convert(text),
            '<p>Some text with a '
            '<a href="http://example.com/WikiLink.html">WikiLink</a>.</p>'
        )

        # `MetaData` should not carry over to next document:
        self.assertEqual(
            md.convert("No [[MetaData]] here."),
            '<p>No <a class="wikilink" href="/MetaData/">MetaData</a> '
            'here.</p>'
        )

    def testURLCallback(self):
        """ Test used of a custom URL builder. """

        from markdown.extensions.wikilinks import WikiLinkExtension

        def my_url_builder(label, base, end):
            return '/bar/'

        md = markdown.Markdown(extensions=[WikiLinkExtension(build_url=my_url_builder)])
        self.assertEqual(
            md.convert('[[foo]]'),
            '<p><a class="wikilink" href="/bar/">foo</a></p>'
        )


class TestAdmonition(unittest.TestCase):
    """ Test Admonition Extension. """

    def setUp(self):
        self.md = markdown.Markdown(extensions=['admonition'])

    def testRE(self):
        RE = self.md.parser.blockprocessors['admonition'].RE
        tests = [
            ('!!! note', ('note', None)),
            ('!!! note "Please Note"', ('note', 'Please Note')),
            ('!!! note ""', ('note', '')),
        ]
        for test, expected in tests:
            self.assertEqual(RE.match(test).groups(), expected)


class TestSmarty(unittest.TestCase):
    def setUp(self):
        config = {
            'smarty': [
                ('smart_angled_quotes', True),
                ('substitutions', {
                    'ndash': '\u2013',
                    'mdash': '\u2014',
                    'ellipsis': '\u2026',
                    'left-single-quote': '&sbquo;',  # `sb` is not a typo!
                    'right-single-quote': '&lsquo;',
                    'left-double-quote': '&bdquo;',
                    'right-double-quote': '&ldquo;',
                    'left-angle-quote': '[',
                    'right-angle-quote': ']',
                }),
            ]
        }
        self.md = markdown.Markdown(
            extensions=['smarty'],
            extension_configs=config
        )

    def testCustomSubstitutions(self):
        text = """<< The "Unicode char of the year 2014"
is the 'mdash': ---
Must not be confused with 'ndash'  (--) ... >>
"""
        correct = """<p>[ The &bdquo;Unicode char of the year 2014&ldquo;
is the &sbquo;mdash&lsquo;: \u2014
Must not be confused with &sbquo;ndash&lsquo;  (\u2013) \u2026 ]</p>"""
        self.assertEqual(self.md.convert(text), correct)
