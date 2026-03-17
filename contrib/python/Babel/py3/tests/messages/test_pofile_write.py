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

from datetime import datetime
from io import BytesIO

from babel.messages import Catalog, Message, pofile


def test_join_locations():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('utils.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 utils.py:3
msgid "foo"
msgstr ""'''


def test_write_po_file_with_specified_charset():
    catalog = Catalog(charset='iso-8859-1')
    catalog.add('foo', 'äöü', locations=[('main.py', 1)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=False)
    po_file = buf.getvalue().strip()
    assert b'"Content-Type: text/plain; charset=iso-8859-1\\n"' in po_file
    assert 'msgstr "äöü"'.encode('iso-8859-1') in po_file


def test_duplicate_comments():
    catalog = Catalog()
    catalog.add('foo', auto_comments=['A comment'])
    catalog.add('foo', auto_comments=['A comment'])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#. A comment
msgid "foo"
msgstr ""'''


def test_wrap_long_lines():
    text = """Here's some text where
white space and line breaks matter, and should

not be removed

"""
    catalog = Catalog()
    catalog.add(text, locations=[('main.py', 1)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, no_location=True, omit_header=True,
                    width=42)
    assert buf.getvalue().strip() == b'''msgid ""
"Here's some text where\\n"
"white space and line breaks matter, and"
" should\\n"
"\\n"
"not be removed\\n"
"\\n"
msgstr ""'''


def test_wrap_long_lines_with_long_word():
    text = """Here's some text that
includesareallylongwordthatmightbutshouldnt throw us into an infinite loop
"""
    catalog = Catalog()
    catalog.add(text, locations=[('main.py', 1)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, no_location=True, omit_header=True,
                    width=32)
    assert buf.getvalue().strip() == b'''msgid ""
"Here's some text that\\n"
"includesareallylongwordthatmightbutshouldnt"
" throw us into an infinite "
"loop\\n"
msgstr ""'''


def test_wrap_long_lines_in_header():
    """
    Verify that long lines in the header comment are wrapped correctly.
    """
    catalog = Catalog(project='AReallyReallyLongNameForAProject',
                      revision_date=datetime(2007, 4, 1))
    buf = BytesIO()
    pofile.write_po(buf, catalog)
    assert b'\n'.join(buf.getvalue().splitlines()[:7]) == b'''\
# Translations template for AReallyReallyLongNameForAProject.
# Copyright (C) 2007 ORGANIZATION
# This file is distributed under the same license as the
# AReallyReallyLongNameForAProject project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
#, fuzzy'''


def test_wrap_locations_with_hyphens():
    catalog = Catalog()
    catalog.add('foo', locations=[
        ('doupy/templates/base/navmenu.inc.html.py', 60),
    ])
    catalog.add('foo', locations=[
        ('doupy/templates/job-offers/helpers.html', 22),
    ])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#: doupy/templates/base/navmenu.inc.html.py:60
#: doupy/templates/job-offers/helpers.html:22
msgid "foo"
msgstr ""'''


def test_no_wrap_and_width_behaviour_on_comments():
    catalog = Catalog()
    catalog.add("Pretty dam long message id, which must really be big "
                "to test this wrap behaviour, if not it won't work.",
                locations=[("fake.py", n) for n in range(1, 30)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, width=None, omit_header=True)
    assert buf.getvalue().lower() == b"""\
#: fake.py:1 fake.py:2 fake.py:3 fake.py:4 fake.py:5 fake.py:6 fake.py:7
#: fake.py:8 fake.py:9 fake.py:10 fake.py:11 fake.py:12 fake.py:13 fake.py:14
#: fake.py:15 fake.py:16 fake.py:17 fake.py:18 fake.py:19 fake.py:20 fake.py:21
#: fake.py:22 fake.py:23 fake.py:24 fake.py:25 fake.py:26 fake.py:27 fake.py:28
#: fake.py:29
msgid "pretty dam long message id, which must really be big to test this wrap behaviour, if not it won't work."
msgstr ""

"""
    buf = BytesIO()
    pofile.write_po(buf, catalog, width=100, omit_header=True)
    assert buf.getvalue().lower() == b"""\
#: fake.py:1 fake.py:2 fake.py:3 fake.py:4 fake.py:5 fake.py:6 fake.py:7 fake.py:8 fake.py:9 fake.py:10
#: fake.py:11 fake.py:12 fake.py:13 fake.py:14 fake.py:15 fake.py:16 fake.py:17 fake.py:18 fake.py:19
#: fake.py:20 fake.py:21 fake.py:22 fake.py:23 fake.py:24 fake.py:25 fake.py:26 fake.py:27 fake.py:28
#: fake.py:29
msgid ""
"pretty dam long message id, which must really be big to test this wrap behaviour, if not it won't"
" work."
msgstr ""

"""


def test_pot_with_translator_comments():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)],
                auto_comments=['Comment About `foo`'])
    catalog.add('bar', locations=[('utils.py', 3)],
                user_comments=['Comment About `bar` with',
                               'multiple lines.'])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#. Comment About `foo`
#: main.py:1
msgid "foo"
msgstr ""

# Comment About `bar` with
# multiple lines.
#: utils.py:3
msgid "bar"
msgstr ""'''


def test_po_with_obsolete_message():
    catalog = Catalog()
    catalog.add('foo', 'Voh', locations=[('main.py', 1)])
    catalog.obsolete['bar'] = Message('bar', 'Bahr',
                                      locations=[('utils.py', 3)],
                                      user_comments=['User comment'])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#: main.py:1
msgid "foo"
msgstr "Voh"

# User comment
#~ msgid "bar"
#~ msgstr "Bahr"'''


def test_po_with_multiline_obsolete_message():
    catalog = Catalog()
    catalog.add('foo', 'Voh', locations=[('main.py', 1)])
    msgid = r"""Here's a message that covers
multiple lines, and should still be handled
correctly.
"""
    msgstr = r"""Here's a message that covers
multiple lines, and should still be handled
correctly.
"""
    catalog.obsolete[msgid] = Message(msgid, msgstr,
                                      locations=[('utils.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True)
    assert buf.getvalue().strip() == b'''#: main.py:1
msgid "foo"
msgstr "Voh"

#~ msgid ""
#~ "Here's a message that covers\\n"
#~ "multiple lines, and should still be handled\\n"
#~ "correctly.\\n"
#~ msgstr ""
#~ "Here's a message that covers\\n"
#~ "multiple lines, and should still be handled\\n"
#~ "correctly.\\n"'''


def test_po_with_obsolete_message_ignored():
    catalog = Catalog()
    catalog.add('foo', 'Voh', locations=[('main.py', 1)])
    catalog.obsolete['bar'] = Message('bar', 'Bahr',
                                      locations=[('utils.py', 3)],
                                      user_comments=['User comment'])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, ignore_obsolete=True)
    assert buf.getvalue().strip() == b'''#: main.py:1
msgid "foo"
msgstr "Voh"'''


def test_po_with_previous_msgid():
    catalog = Catalog()
    catalog.add('foo', 'Voh', locations=[('main.py', 1)],
                previous_id='fo')
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_previous=True)
    assert buf.getvalue().strip() == b'''#: main.py:1
#| msgid "fo"
msgid "foo"
msgstr "Voh"'''


def test_po_with_previous_msgid_plural():
    catalog = Catalog()
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'),
                locations=[('main.py', 1)], previous_id=('fo', 'fos'))
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_previous=True)
    assert buf.getvalue().strip() == b'''#: main.py:1
#| msgid "fo"
#| msgid_plural "fos"
msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Voeh"'''


def test_sorted_po():
    catalog = Catalog()
    catalog.add('bar', locations=[('utils.py', 3)],
                user_comments=['Comment About `bar` with',
                               'multiple lines.'])
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'),
                locations=[('main.py', 1)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, sort_output=True)
    value = buf.getvalue().strip()
    assert b'''\
# Comment About `bar` with
# multiple lines.
#: utils.py:3
msgid "bar"
msgstr ""

#: main.py:1
msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Voeh"''' in value
    assert value.find(b'msgid ""') < value.find(b'msgid "bar"') < value.find(b'msgid "foo"')


def test_sorted_po_context():
    catalog = Catalog()
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'),
                locations=[('main.py', 1)],
                context='there')
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'),
                locations=[('main.py', 1)])
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'),
                locations=[('main.py', 1)],
                context='here')
    buf = BytesIO()
    pofile.write_po(buf, catalog, sort_output=True)
    value = buf.getvalue().strip()
    # We expect the foo without ctx, followed by "here" foo and "there" foo
    assert b'''\
#: main.py:1
msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Voeh"

#: main.py:1
msgctxt "here"
msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Voeh"

#: main.py:1
msgctxt "there"
msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Voeh"''' in value


def test_file_sorted_po():
    catalog = Catalog()
    catalog.add('bar', locations=[('utils.py', 3)])
    catalog.add(('foo', 'foos'), ('Voh', 'Voeh'), locations=[('main.py', 1)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, sort_by_file=True)
    value = buf.getvalue().strip()
    assert value.find(b'main.py') < value.find(b'utils.py')


def test_file_with_no_lineno():
    catalog = Catalog()
    catalog.add('bar', locations=[('utils.py', None)],
                user_comments=['Comment About `bar` with',
                               'multiple lines.'])
    buf = BytesIO()
    pofile.write_po(buf, catalog, sort_output=True)
    value = buf.getvalue().strip()
    assert b'''\
# Comment About `bar` with
# multiple lines.
#: utils.py
msgid "bar"
msgstr ""''' in value


def test_silent_location_fallback():
    buf = BytesIO(b'''\
#: broken_file.py
msgid "missing line number"
msgstr ""

#: broken_file.py:broken_line_number
msgid "broken line number"
msgstr ""''')
    catalog = pofile.read_po(buf)
    assert catalog['missing line number'].locations == [('broken_file.py', None)]
    assert catalog['broken line number'].locations == []


def test_include_lineno():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('utils.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 utils.py:3
msgid "foo"
msgstr ""'''


def test_no_include_lineno():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('main.py', 2)])
    catalog.add('foo', locations=[('utils.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=False)
    assert buf.getvalue().strip() == b'''#: main.py utils.py
msgid "foo"
msgstr ""'''


def test_white_space_in_location():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('utils b.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 \xe2\x81\xa8utils b.py\xe2\x81\xa9:3
msgid "foo"
msgstr ""'''


def test_white_space_in_location_already_enclosed():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('\u2068utils b.py\u2069', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 \xe2\x81\xa8utils b.py\xe2\x81\xa9:3
msgid "foo"
msgstr ""'''


def test_tab_in_location():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('utils\tb.py', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 \xe2\x81\xa8utils        b.py\xe2\x81\xa9:3
msgid "foo"
msgstr ""'''


def test_tab_in_location_already_enclosed():
    catalog = Catalog()
    catalog.add('foo', locations=[('main.py', 1)])
    catalog.add('foo', locations=[('\u2068utils\tb.py\u2069', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    assert buf.getvalue().strip() == b'''#: main.py:1 \xe2\x81\xa8utils        b.py\xe2\x81\xa9:3
msgid "foo"
msgstr ""'''


def test_wrap_with_enclosed_file_locations():
    # Ensure that file names containing white space are not wrapped regardless of the --width parameter
    catalog = Catalog()
    catalog.add('foo', locations=[('\u2068test utils.py\u2069', 1)])
    catalog.add('foo', locations=[('\u2068test utils.py\u2069', 3)])
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True, width=1)
    assert buf.getvalue().strip() == b'''#: \xe2\x81\xa8test utils.py\xe2\x81\xa9:1
#: \xe2\x81\xa8test utils.py\xe2\x81\xa9:3
msgid "foo"
msgstr ""'''
