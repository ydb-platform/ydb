#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

import codecs
from io import BytesIO

import pytest

from babel.messages import extract


def test_nested_calls():
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
msg11 = ngettext(
"bunny",
"bunnies",
len(bunnies)
)
""")
    messages = list(extract.extract_python(buf,
                                           extract.DEFAULT_KEYWORDS.keys(),
                                           [], {}))
    assert messages == [
        (1, '_', None, []),
        (2, 'ungettext', (None, None, None), []),
        (3, 'ungettext', ('Babel', None, None), []),
        (4, 'ungettext', (None, 'Babels', None), []),
        (5, 'ungettext', ('bunny', 'bunnies', None), []),
        (6, 'ungettext', (None, 'bunnies', None), []),
        (7, '_', None, []),
        (8, 'gettext', 'Rabbit', []),
        (9, 'dgettext', ('wiki', None), []),
        (10, 'dngettext', (None, 'Page', 'Pages', None), []),
        (12, 'ngettext', ('bunny', 'bunnies', None), []),
    ]


def test_extract_default_encoding_ascii():
    buf = BytesIO(b'_("a")')
    messages = list(extract.extract_python(
        buf, list(extract.DEFAULT_KEYWORDS), [], {},
    ))
    # Should work great in both py2 and py3
    assert messages == [(1, '_', 'a', [])]


def test_extract_default_encoding_utf8():
    buf = BytesIO('_("☃")'.encode('UTF-8'))
    messages = list(extract.extract_python(
        buf, list(extract.DEFAULT_KEYWORDS), [], {},
    ))
    assert messages == [(1, '_', '☃', [])]


def test_nested_comments():
    buf = BytesIO(b"""\
msg = ngettext('pylon',  # TRANSLATORS: shouldn't be
           'pylons', # TRANSLATORS: seeing this
           count)
""")
    messages = list(extract.extract_python(buf, ('ngettext',),
                                           ['TRANSLATORS:'], {}))
    assert messages == [(1, 'ngettext', ('pylon', 'pylons', None), [])]


def test_comments_with_calls_that_spawn_multiple_lines():
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
    assert messages[0] == (2, 'ngettext', ('Catalog deleted.', 'Catalogs deleted.', None), ['NOTE: This Comment SHOULD Be Extracted'])
    assert messages[1] == (6, '_', 'Locale deleted.', ['NOTE: This Comment SHOULD Be Extracted'])
    assert messages[2] == (10, 'ngettext', ('Foo deleted.', 'Foos deleted.', None), ['NOTE: This Comment SHOULD Be Extracted'])
    assert messages[3] == (14, 'ngettext', ('Bar deleted.', 'Bars deleted.', None), ['NOTE: This Comment SHOULD Be Extracted', 'NOTE: And This One Too'])


def test_declarations():
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
    assert messages == [
        (3, '_', 'Page arg 1', []),
        (3, '_', 'Page arg 2', []),
        (8, '_', 'log entry', []),
    ]


def test_multiline():
    buf = BytesIO(b"""\
msg1 = ngettext('pylon',
            'pylons', count)
msg2 = ngettext('elvis',
            'elvises',
             count)
""")
    messages = list(extract.extract_python(buf, ('ngettext',), [], {}))
    assert messages == [
        (1, 'ngettext', ('pylon', 'pylons', None), []),
        (3, 'ngettext', ('elvis', 'elvises', None), []),
    ]


def test_dpgettext():
    buf = BytesIO(b"""\
msg1 = dpgettext('dev', 'Strings',
             'pylon')
msg2 = dpgettext('dev', 'Strings', 'elvis')
""")
    messages = list(extract.extract_python(buf, ('dpgettext',), [], {}))
    assert messages == [
        (1, 'dpgettext', ('dev', 'Strings', 'pylon'), []),
        (3, 'dpgettext', ('dev', 'Strings', 'elvis'), []),
    ]
    buf = BytesIO(b"""\
msg = dpgettext('dev', 'Strings', 'pylon',  # TRANSLATORS: shouldn't be
            )                # TRANSLATORS: seeing this
""")
    messages = list(extract.extract_python(buf, ('dpgettext',),['TRANSLATORS:'], {}))
    assert messages == [
        (1, 'dpgettext', ('dev', 'Strings', 'pylon', None), []),
    ]


def test_npgettext():
    buf = BytesIO(b"""\
msg1 = npgettext('Strings','pylon',
            'pylons', count)
msg2 = npgettext('Strings','elvis',
            'elvises',
             count)
""")
    messages = list(extract.extract_python(buf, ('npgettext',), [], {}))
    assert messages == [
        (1, 'npgettext', ('Strings', 'pylon', 'pylons', None), []),
        (3, 'npgettext', ('Strings', 'elvis', 'elvises', None), []),
    ]
    buf = BytesIO(b"""\
msg = npgettext('Strings', 'pylon',  # TRANSLATORS: shouldn't be
            'pylons', # TRANSLATORS: seeing this
            count)
""")
    messages = list(extract.extract_python(buf, ('npgettext',),
                                           ['TRANSLATORS:'], {}))
    assert messages == [
        (1, 'npgettext', ('Strings', 'pylon', 'pylons', None), []),
    ]


def test_dnpgettext():
    buf = BytesIO(b"""\
msg1 = dnpgettext('dev', 'Strings','pylon',
            'pylons', count)
msg2 = dnpgettext('dev', 'Strings','elvis',
            'elvises',
             count)
""")
    messages = list(extract.extract_python(buf, ('dnpgettext',), [], {}))
    assert messages == [
        (1, 'dnpgettext', ('dev', 'Strings', 'pylon', 'pylons', None), []),
        (3, 'dnpgettext', ('dev', 'Strings', 'elvis', 'elvises', None), []),
    ]
    buf = BytesIO(b"""\
msg = dnpgettext('dev', 'Strings', 'pylon',  # TRANSLATORS: shouldn't be
             'pylons', # TRANSLATORS: seeing this
             count)
""")
    messages = list(extract.extract_python(buf, ('dnpgettext',),['TRANSLATORS:'], {}))
    assert messages == [
        (1, 'dnpgettext', ('dev', 'Strings', 'pylon', 'pylons', None), []),
    ]


def test_triple_quoted_strings():
    buf = BytesIO(b"""\
msg1 = _('''pylons''')
msg2 = ngettext(r'''elvis''', \"\"\"elvises\"\"\", count)
msg2 = ngettext(\"\"\"elvis\"\"\", 'elvises', count)
""")
    messages = list(extract.extract_python(buf,
                                           extract.DEFAULT_KEYWORDS.keys(),
                                           [], {}))
    assert messages == [
        (1, '_', 'pylons', []),
        (2, 'ngettext', ('elvis', 'elvises', None), []),
        (3, 'ngettext', ('elvis', 'elvises', None), []),
    ]


def test_multiline_strings():
    buf = BytesIO(b"""\
_('''This module provides internationalization and localization
support for your Python programs by providing an interface to the GNU
gettext message catalog library.''')
""")
    messages = list(extract.extract_python(buf,
                                           extract.DEFAULT_KEYWORDS.keys(),
                                           [], {}))
    assert messages == [
        (1, '_',
        'This module provides internationalization and localization\n'
        'support for your Python programs by providing an interface to '
        'the GNU\ngettext message catalog library.', []),
    ]


def test_concatenated_strings():
    buf = BytesIO(b"""\
foobar = _('foo' 'bar')
""")
    messages = list(extract.extract_python(buf,
                                           extract.DEFAULT_KEYWORDS.keys(),
                                           [], {}))
    assert messages[0][2] == 'foobar'


def test_unicode_string_arg():
    buf = BytesIO(b"msg = _('Foo Bar')")
    messages = list(extract.extract_python(buf, ('_',), [], {}))
    assert messages[0][2] == 'Foo Bar'


def test_comment_tag():
    buf = BytesIO(b"""
# NOTE: A translation comment
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == ['NOTE: A translation comment']


def test_comment_tag_multiline():
    buf = BytesIO(b"""
# NOTE: A translation comment
# with a second line
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == ['NOTE: A translation comment', 'with a second line']


def test_translator_comments_with_previous_non_translator_comments():
    buf = BytesIO(b"""
# This shouldn't be in the output
# because it didn't start with a comment tag
# NOTE: A translation comment
# with a second line
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == ['NOTE: A translation comment', 'with a second line']


def test_comment_tags_not_on_start_of_comment():
    buf = BytesIO(b"""
# This shouldn't be in the output
# because it didn't start with a comment tag
# do NOTE: this will not be a translation comment
# NOTE: This one will be
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == ['NOTE: This one will be']


def test_multiple_comment_tags():
    buf = BytesIO(b"""
# NOTE1: A translation comment for tag1
# with a second line
msg = _('Foo Bar1')

# NOTE2: A translation comment for tag2
msg = _('Foo Bar2')
""")
    messages = list(extract.extract_python(buf, ('_',),
                                           ['NOTE1:', 'NOTE2:'], {}))
    assert messages[0][2] == 'Foo Bar1'
    assert messages[0][3] == ['NOTE1: A translation comment for tag1', 'with a second line']
    assert messages[1][2] == 'Foo Bar2'
    assert messages[1][3] == ['NOTE2: A translation comment for tag2']


def test_two_succeeding_comments():
    buf = BytesIO(b"""
# NOTE: one
# NOTE: two
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == ['NOTE: one', 'NOTE: two']


def test_invalid_translator_comments():
    buf = BytesIO(b"""
# NOTE: this shouldn't apply to any messages
hello = 'there'

msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == []


def test_invalid_translator_comments2():
    buf = BytesIO(b"""
# NOTE: Hi!
hithere = _('Hi there!')

# NOTE: you should not be seeing this in the .po
rows = [[v for v in range(0,10)] for row in range(0,10)]

# this (NOTE:) should not show up either
hello = _('Hello')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Hi there!'
    assert messages[0][3] == ['NOTE: Hi!']
    assert messages[1][2] == 'Hello'
    assert messages[1][3] == []


def test_invalid_translator_comments3():
    buf = BytesIO(b"""
# NOTE: Hi,

# there!
hithere = _('Hi there!')
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Hi there!'
    assert messages[0][3] == []


def test_comment_tag_with_leading_space():
    buf = BytesIO(b"""
#: A translation comment
#: with leading spaces
msg = _('Foo Bar')
""")
    messages = list(extract.extract_python(buf, ('_',), [':'], {}))
    assert messages[0][2] == 'Foo Bar'
    assert messages[0][3] == [': A translation comment', ': with leading spaces']


def test_different_signatures():
    buf = BytesIO(b"""
foo = _('foo', 'bar')
n = ngettext('hello', 'there', n=3)
n = ngettext(n=3, 'hello', 'there')
n = ngettext(n=3, *messages)
n = ngettext()
n = ngettext('foo')
""")
    messages = list(extract.extract_python(buf, ('_', 'ngettext'), [], {}))
    assert messages[0][2] == ('foo', 'bar')
    assert messages[1][2] == ('hello', 'there', None)
    assert messages[2][2] == (None, 'hello', 'there')
    assert messages[3][2] == (None, None)
    assert messages[4][2] is None
    assert messages[5][2] == 'foo'


def test_utf8_message():
    buf = BytesIO("""
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'],
                                           {'encoding': 'utf-8'}))
    assert messages[0][2] == 'Bonjour à tous'
    assert messages[0][3] == ['NOTE: hello']


def test_utf8_message_with_magic_comment():
    buf = BytesIO("""# -*- coding: utf-8 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Bonjour à tous'
    assert messages[0][3] == ['NOTE: hello']


def test_utf8_message_with_utf8_bom():
    buf = BytesIO(codecs.BOM_UTF8 + """
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Bonjour à tous'
    assert messages[0][3] == ['NOTE: hello']


def test_utf8_message_with_utf8_bom_and_magic_comment():
    buf = BytesIO(codecs.BOM_UTF8 + """# -*- coding: utf-8 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Bonjour à tous'
    assert messages[0][3] == ['NOTE: hello']


def test_utf8_bom_with_latin_magic_comment_fails():
    buf = BytesIO(codecs.BOM_UTF8 + """# -*- coding: latin-1 -*-
# NOTE: hello
msg = _('Bonjour à tous')
""".encode('utf-8'))
    with pytest.raises(SyntaxError):
        list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))


def test_utf8_raw_strings_match_unicode_strings():
    buf = BytesIO(codecs.BOM_UTF8 + """
msg = _('Bonjour à tous')
msgu = _('Bonjour à tous')
""".encode('utf-8'))
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == 'Bonjour à tous'
    assert messages[0][2] == messages[1][2]


def test_extract_strip_comment_tags():
    buf = BytesIO(b"""\
#: This is a comment with a very simple
#: prefix specified
_('Servus')

# NOTE: This is a multiline comment with
# a prefix too
_('Babatschi')""")
    messages = list(extract.extract('python', buf, comment_tags=['NOTE:', ':'],
                                    strip_comment_tags=True))
    assert messages[0][1] == 'Servus'
    assert messages[0][2] == ['This is a comment with a very simple', 'prefix specified']
    assert messages[1][1] == 'Babatschi'
    assert messages[1][2] == ['This is a multiline comment with', 'a prefix too']


def test_nested_messages():
    buf = BytesIO(b"""
# NOTE: First
_('Hello, {name}!', name=_('Foo Bar'))

# NOTE: Second
_('Hello, {name1} and {name2}!', name1=_('Heungsub'),
name2=_('Armin'))

# NOTE: Third
_('Hello, {0} and {1}!', _('Heungsub'),
_('Armin'))
""")
    messages = list(extract.extract_python(buf, ('_',), ['NOTE:'], {}))
    assert messages[0][2] == ('Hello, {name}!', None)
    assert messages[0][3] == ['NOTE: First']
    assert messages[1][2] == 'Foo Bar'
    assert messages[1][3] == []
    assert messages[2][2] == ('Hello, {name1} and {name2}!', None)
    assert messages[2][3] == ['NOTE: Second']
    assert messages[3][2] == 'Heungsub'
    assert messages[3][3] == []
    assert messages[4][2] == 'Armin'
    assert messages[4][3] == []
    assert messages[5][2] == ('Hello, {0} and {1}!', None)
    assert messages[5][3] == ['NOTE: Third']
    assert messages[6][2] == 'Heungsub'
    assert messages[6][3] == []
    assert messages[7][2] == 'Armin'
    assert messages[7][3] == []
