# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2021 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at http://babel.edgewall.org/wiki/License.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at http://babel.edgewall.org/log/.

import codecs
import sys
import unittest

from babel.messages import extract
from babel._compat import BytesIO, StringIO


class ExtractPythonTestCase(unittest.TestCase):

    def test_nested_calls(self):
        buf = BytesIO(b"""\
msg1 = _(i18n_arg.replace(r'\"', '"'))
msg2 = ungettext(i18n_arg.replace(r'\"', '"'), multi_arg.replace(r'\"', '"'), 2)
msg3 = ungettext("Babel", multi_arg.replace(r'\"', '"'), 2)
msg4 = ungettext(i18n_arg.replace(r'\"', '"'), "Babels", 2)
msg5 = ungettext('bunny', 'bunnies', random.randint(1, 2))
msg6 = ungettext(arg0, 'bunnies', random.randint(1, 2))
msg7 = _(hello.there)
msg8 = gettext('Rabbit')
msg9 = dgettext('wiki', model.addPage())
msg10 = dngettext(getDomain(), 'Page', 'Pages', 3)
""")
        messages = list(extract.extract_python(buf,
                                               extract.DEFAULT_KEYWORDS.keys(),
                                               [], {}))
        self.assertEqual([
            (1, '_', None, []),
            (2, 'ungettext', (None, None, None), []),
            (3, 'ungettext', (u'Babel', None, None), []),
            (4, 'ungettext', (None, u'Babels', None), []),
            (5, 'ungettext', (u'bunny', u'bunnies', None), []),
            (6, 'ungettext', (None, u'bunnies', None), []),
            (7, '_', None, []),
            (8, 'gettext', u'Rabbit', []),
            (9, 'dgettext', (u'wiki', None), []),
            (10, 'dngettext', (None, u'Page', u'Pages', None), [])],
            messages)

    def test_extract_default_encoding_ascii(self):
        buf = BytesIO(b'_("a")')
        messages = list(extract.extract_python(
            buf, list(extract.DEFAULT_KEYWORDS), [], {},
        ))
        # Should work great in both py2 and py3
        self.assertEqual([(1, '_', 'a', [])], messages)

    def test_extract_default_encoding_utf8(self):
        buf = BytesIO(u'_("☃")'.encode('UTF-8'))
        messages = list(extract.extract_python(
            buf, list(extract.DEFAULT_KEYWORDS), [], {},
        ))
        self.assertEqual([(1, '_', u'☃', [])], messages)

    def test_nested_comments(self):
        buf = BytesIO(b"""\
msg = ngettext('pylon',  # TRANSLATORS: shouldn't be
               'pylons', # TRANSLATORS: seeing this
               count)
""")
        messages = list(extract.extract_python(buf, ('ngettext',),
                                               ['TRANSLATORS:'], {}))
        self.assertEqual([(1, 'ngettext', (u'pylon', u'pylons', None), [])],
                         messages)

    def test_comments_with_calls_that_spawn_multiple_lines(self):
        buf = BytesIO(b"""\
# NOTE: This Comment SHOULD Be Extracted
add_notice(req, ngettext("Catalog deleted.",
                         "Catalogs deleted.", len(selected)))

# NOTE: This Comment SHOULD Be Extracted
add_notice(req, _("Locale deleted."))


# NOTE: This Comment SHOULD Be Extracted
add_notice(req, ngettext("Foo deleted.", "Foos deleted.", len(selected)))

# NOTE: This Comment SHOULD Be Extracted
# NOTE: And This One Too
add_notice(req, ngettext("Bar deleted.",
                         "Bars deleted.", len(selected)))
""")
        messages = list(extract.extract_python(buf, ('ngettext', '_'), ['NOTE:'],

                                               {'strip_comment_tags': False}))
        self.assertEqual((6, '_', 'Locale deleted.',
                          [u'NOTE: This Comment SHOULD Be Extracted']),
                         messages[1])
        self.assertEqual((10, 'ngettext', (u'Foo deleted.', u'Foos deleted.',
                                           None),
                          [u'NOTE: This Comment SHOULD Be Extracted']),
                         messages[2])
        self.assertEqual((3, 'ngettext',
                          (u'Catalog deleted.',
                           u'Catalogs deleted.', None),
                          [u'NOTE: This Comment SHOULD Be Extracted']),
                         messages[0])
        self.assertEqual((15, 'ngettext', (u'Bar deleted.', u'Bars deleted.',
                                           None),
                          [u'NOTE: This Comment SHOULD Be Extracted',
                           u'NOTE: And This One Too']),
                         messages[3])

    def test_declarations(self):
        buf = BytesIO(b"""\
class gettext(object):
    pass
def render_body(context,x,y=_('Page arg 1'),z=_('Page arg 2'),**pageargs):
    pass
def ngettext(y='arg 1',z='arg 2',**pageargs):
    pass
class Meta:
    verbose_name = _('log entry')
""")
        messages = list(extract.extract_python(buf,
                                               extract.DEFAULT_KEYWORDS.keys(),
                                               [], {}))
        self.assertEqual([(3, '_', u'Page arg 1', []),
                          (3, '_', u'Page arg 2', []),
                          (8, '_', u'log entry', [])],
                         messages)

    def test_multiline(self):
        buf = BytesIO(b"""\
msg1 = ngettext('pylon',
                'pylons', count)
msg2 = ngettext('elvis',
                'elvises',
                 count)
""")
        messages = list(extract.extract_python(buf, ('ngettext',), [], {}))
        self.assertEqual([(1, 'ngettext', (u'pylon', u'pylons', None), []),
                          (3, 'ngettext', (u'elvis', u'elvises', None), [])],
                         messages)

    def test_npgettext(self):
        buf = BytesIO(b"""\
msg1 = npgettext('Strings','pylon',
                'pylons', count)
msg2 = npgettext('Strings','elvis',
                'elvises',
                 count)
""")
        messages = list(extract.extract_python(buf, ('npgettext',), [], {}))
        self.assertEqual([(1, 'npgettext', (u'Strings', u'pylon', u'pylons', None), []),
                          (3, 'npgettext', (u'Strings', u'elvis', u'elvises', None), [])],
                         messages)
        buf = BytesIO(b"""\
msg = npgettext('Strings', 'pylon',  # TRANSLATORS: shouldn't be
                'pylons', # TRANSLATORS: seeing this
                count)
""")
        messages = list(extract.extract_python(buf, ('npgettext',),
                                               ['TRANSLATORS:'], {}))
        self.assertEqual([(1, 'npgettext', (u'Strings', u'pylon', u'pylons', None), [])],
                         messages)

    def test_triple_quoted_strings(self):
        buf = BytesIO(b"""\
msg1 = _('''pylons''')
msg2 = ngettext(r'''elvis''', \"\"\"elvises\"\"\", count)
msg2 = ngettext(\"\"\"elvis\"\"\", 'elvises', count)
""")
        messages = list(extract.extract_python(buf,
                                               extract.DEFAULT_KEYWORDS.keys(),
                                               [], {}))
        self.assertEqual([(1, '_', u'pylons', []),
                          (2, 'ngettext', (u'elvis', u'elvises', None), []),
                          (3, 'ngettext', (u'elvis', u'elvises', None), [])],
                         messages)

    def test_multiline_strings(self):
        buf = BytesIO(b"""\
_('''This module provides internationalization and localization
support for your Python programs by providing an interface to the GNU
gettext message catalog library.''')
""")
        messages = list(extract.extract_python(buf,
                                               extract.DEFAULT_KEYWORDS.keys(),
                                               [], {}))
        self.assertEqual(
            [(1, '_',
              u'This module provides internationalization and localization\n'
              'support for your Python programs by providing an interface to '
              'the GNU\ngettext message catalog library.', [])],
            messages)

    def test_concatenated_strings(self):
        buf = BytesIO(b"""\
foobar = _('foo' 'bar')
""")
        messages = list(extract.extract_python(buf,
                                               extract.DEFAULT_KEYWORDS.keys(),
                                               [], {}))
        self.assertEqual(u'foobar', messages[0][2])

    def test_unicode_string_arg(self):
        buf = BytesIO(b"msg = _(u'Foo Bar')")
        messages = list(extract.extract_python(buf, ('_',), [], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])

    def test_comment_tag(self):
        buf = BytesIO(b"""
# NOTE: A translation comment
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u'NOTE: A translation comment'], messages[0][3])

    def test_comment_tag_multiline(self):
        buf = BytesIO(b"""
# NOTE: A translation comment
# with a second line
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u'NOTE: A translation comment', u'with a second line'],
                         messages[0][3])

    def test_translator_comments_with_previous_non_translator_comments(self):
        buf = BytesIO(b"""
# This shouldn't be in the output
# because it didn't start with a comment tag
# NOTE: A translation comment
# with a second line
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u'NOTE: A translation comment', u'with a second line'],
                         messages[0][3])

    def test_comment_tags_not_on_start_of_comment(self):
        buf = BytesIO(b"""
# This shouldn't be in the output
# because it didn't start with a comment tag
# do NOTE: this will not be a translation comment
# NOTE: This one will be
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u'NOTE: This one will be'], messages[0][3])

    def test_multiple_comment_tags(self):
        buf = BytesIO(b"""
# NOTE1: A translation comment for tag1
# with a second line
msg = _(u'Foo Bar1')

# NOTE2: A translation comment for tag2
msg = _(u'Foo Bar2')
""")
        messages = list(extract.extract_python(buf, ('_',),
                                               ['NOTE1:', 'NOTE2:'], {}))
        self.assertEqual(u'Foo Bar1', messages[0][2])
        self.assertEqual([u'NOTE1: A translation comment for tag1',
                          u'with a second line'], messages[0][3])
        self.assertEqual(u'Foo Bar2', messages[1][2])
        self.assertEqual([u'NOTE2: A translation comment for tag2'], messages[1][3])

    def test_two_succeeding_comments(self):
        buf = BytesIO(b"""
# NOTE: one
# NOTE: two
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u'NOTE: one', u'NOTE: two'], messages[0][3])

    def test_invalid_translator_comments(self):
        buf = BytesIO(b"""
# NOTE: this shouldn't apply to any messages
hello = 'there'

msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([], messages[0][3])

    def test_invalid_translator_comments2(self):
        buf = BytesIO(b"""
# NOTE: Hi!
hithere = _('Hi there!')

# NOTE: you should not be seeing this in the .po
rows = [[v for v in range(0,10)] for row in range(0,10)]

# this (NOTE:) should not show up either
hello = _('Hello')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Hi there!', messages[0][2])
        self.assertEqual([u'NOTE: Hi!'], messages[0][3])
        self.assertEqual(u'Hello', messages[1][2])
        self.assertEqual([], messages[1][3])

    def test_invalid_translator_comments3(self):
        buf = BytesIO(b"""
# NOTE: Hi,

# there!
hithere = _('Hi there!')
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Hi there!', messages[0][2])
        self.assertEqual([], messages[0][3])

    def test_comment_tag_with_leading_space(self):
        buf = BytesIO(b"""
  #: A translation comment
  #: with leading spaces
msg = _(u'Foo Bar')
""")
        messages = list(extract.extract_python(buf, ('_',), [':'], {}))
        self.assertEqual(u'Foo Bar', messages[0][2])
        self.assertEqual([u': A translation comment', u': with leading spaces'],
                         messages[0][3])

    def test_different_signatures(self):
        buf = BytesIO(b"""
foo = _('foo', 'bar')
n = ngettext('hello', 'there', n=3)
n = ngettext(n=3, 'hello', 'there')
n = ngettext(n=3, *messages)
n = ngettext()
n = ngettext('foo')
""")
        messages = list(extract.extract_python(buf, ('_', 'ngettext'), [], {}))
        self.assertEqual((u'foo', u'bar'), messages[0][2])
        self.assertEqual((u'hello', u'there', None), messages[1][2])
        self.assertEqual((None, u'hello', u'there'), messages[2][2])
        self.assertEqual((None, None), messages[3][2])
        self.assertEqual(None, messages[4][2])
        self.assertEqual('foo', messages[5][2])

    def test_utf8_message(self):
        buf = BytesIO(u"""
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'],
                                               {'encoding': 'utf-8'}))
        self.assertEqual(u'Bonjour à tous', messages[0][2])
        self.assertEqual([u'NOTE: hello'], messages[0][3])

    def test_utf8_message_with_magic_comment(self):
        buf = BytesIO(u"""# -*- coding: utf-8 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Bonjour à tous', messages[0][2])
        self.assertEqual([u'NOTE: hello'], messages[0][3])

    def test_utf8_message_with_utf8_bom(self):
        buf = BytesIO(codecs.BOM_UTF8 + u"""
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Bonjour à tous', messages[0][2])
        self.assertEqual([u'NOTE: hello'], messages[0][3])

    def test_utf8_message_with_utf8_bom_and_magic_comment(self):
        buf = BytesIO(codecs.BOM_UTF8 + u"""# -*- coding: utf-8 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Bonjour à tous', messages[0][2])
        self.assertEqual([u'NOTE: hello'], messages[0][3])

    def test_utf8_bom_with_latin_magic_comment_fails(self):
        buf = BytesIO(codecs.BOM_UTF8 + u"""# -*- coding: latin-1 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
        self.assertRaises(SyntaxError, list,
                          extract.extract_python(buf, ('_',), ['NOTE:'], {}))

    def test_utf8_raw_strings_match_unicode_strings(self):
        buf = BytesIO(codecs.BOM_UTF8 + u"""
msg = _('Bonjour à tous')
msgu = _(u'Bonjour à tous')
""".encode('utf-8'))
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual(u'Bonjour à tous', messages[0][2])
        self.assertEqual(messages[0][2], messages[1][2])

    def test_extract_strip_comment_tags(self):
        buf = BytesIO(b"""\
#: This is a comment with a very simple
#: prefix specified
_('Servus')

# NOTE: This is a multiline comment with
# a prefix too
_('Babatschi')""")
        messages = list(extract.extract('python', buf, comment_tags=['NOTE:', ':'],
                                        strip_comment_tags=True))
        self.assertEqual(u'Servus', messages[0][1])
        self.assertEqual([u'This is a comment with a very simple',
                          u'prefix specified'], messages[0][2])
        self.assertEqual(u'Babatschi', messages[1][1])
        self.assertEqual([u'This is a multiline comment with',
                          u'a prefix too'], messages[1][2])

    def test_nested_messages(self):
        buf = BytesIO(b"""
# NOTE: First
_(u'Hello, {name}!', name=_(u'Foo Bar'))

# NOTE: Second
_(u'Hello, {name1} and {name2}!', name1=_(u'Heungsub'),
  name2=_(u'Armin'))

# NOTE: Third
_(u'Hello, {0} and {1}!', _(u'Heungsub'),
  _(u'Armin'))
""")
        messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
        self.assertEqual((u'Hello, {name}!', None), messages[0][2])
        self.assertEqual([u'NOTE: First'], messages[0][3])
        self.assertEqual(u'Foo Bar', messages[1][2])
        self.assertEqual([], messages[1][3])
        self.assertEqual((u'Hello, {name1} and {name2}!', None), messages[2][2])
        self.assertEqual([u'NOTE: Second'], messages[2][3])
        self.assertEqual(u'Heungsub', messages[3][2])
        self.assertEqual([], messages[3][3])
        self.assertEqual(u'Armin', messages[4][2])
        self.assertEqual([], messages[4][3])
        self.assertEqual((u'Hello, {0} and {1}!', None), messages[5][2])
        self.assertEqual([u'NOTE: Third'], messages[5][3])
        self.assertEqual(u'Heungsub', messages[6][2])
        self.assertEqual([], messages[6][3])
        self.assertEqual(u'Armin', messages[7][2])
        self.assertEqual([], messages[7][3])


class ExtractTestCase(unittest.TestCase):

    def test_invalid_filter(self):
        buf = BytesIO(b"""\
msg1 = _(i18n_arg.replace(r'\"', '"'))
msg2 = ungettext(i18n_arg.replace(r'\"', '"'), multi_arg.replace(r'\"', '"'), 2)
msg3 = ungettext("Babel", multi_arg.replace(r'\"', '"'), 2)
msg4 = ungettext(i18n_arg.replace(r'\"', '"'), "Babels", 2)
msg5 = ungettext('bunny', 'bunnies', random.randint(1, 2))
msg6 = ungettext(arg0, 'bunnies', random.randint(1, 2))
msg7 = _(hello.there)
msg8 = gettext('Rabbit')
msg9 = dgettext('wiki', model.addPage())
msg10 = dngettext(domain, 'Page', 'Pages', 3)
""")
        messages = \
            list(extract.extract('python', buf, extract.DEFAULT_KEYWORDS, [],
                                 {}))
        self.assertEqual([(5, (u'bunny', u'bunnies'), [], None),
                          (8, u'Rabbit', [], None),
                          (10, (u'Page', u'Pages'), [], None)], messages)

    def test_invalid_extract_method(self):
        buf = BytesIO(b'')
        self.assertRaises(ValueError, list, extract.extract('spam', buf))

    def test_different_signatures(self):
        buf = BytesIO(b"""
foo = _('foo', 'bar')
n = ngettext('hello', 'there', n=3)
n = ngettext(n=3, 'hello', 'there')
n = ngettext(n=3, *messages)
n = ngettext()
n = ngettext('foo')
""")
        messages = \
            list(extract.extract('python', buf, extract.DEFAULT_KEYWORDS, [],
                                 {}))
        self.assertEqual(len(messages), 2)
        self.assertEqual(u'foo', messages[0][1])
        self.assertEqual((u'hello', u'there'), messages[1][1])

    def test_empty_string_msgid(self):
        buf = BytesIO(b"""\
msg = _('')
""")
        stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            messages = \
                list(extract.extract('python', buf, extract.DEFAULT_KEYWORDS,
                                     [], {}))
            self.assertEqual([], messages)
            assert 'warning: Empty msgid.' in sys.stderr.getvalue()
        finally:
            sys.stderr = stderr

    def test_warn_if_empty_string_msgid_found_in_context_aware_extraction_method(self):
        buf = BytesIO(b"\nmsg = pgettext('ctxt', '')\n")
        stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            messages = extract.extract('python', buf)
            self.assertEqual([], list(messages))
            assert 'warning: Empty msgid.' in sys.stderr.getvalue()
        finally:
            sys.stderr = stderr

    def test_extract_allows_callable(self):
        def arbitrary_extractor(fileobj, keywords, comment_tags, options):
            return [(1, None, (), ())]
        for x in extract.extract(arbitrary_extractor, BytesIO(b"")):
            assert x[0] == 1

    def test_future(self):
        buf = BytesIO(br"""
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
nbsp = _('\xa0')
""")
        messages = list(extract.extract('python', buf,
                                        extract.DEFAULT_KEYWORDS, [], {}))
        assert messages[0][1] == u'\xa0'
