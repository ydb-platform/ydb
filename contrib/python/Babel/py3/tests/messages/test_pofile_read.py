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
from io import BytesIO, StringIO

import pytest

from babel import Locale
from babel.messages import Catalog, pofile
from babel.util import FixedOffsetTimezone


def test_preserve_locale():
    buf = StringIO(r'''msgid "foo"
msgstr "Voh"''')
    catalog = pofile.read_po(buf, locale='en_US')
    assert Locale('en', 'US') == catalog.locale


def test_locale_gets_overridden_by_file():
    buf = StringIO(r'''
msgid ""
msgstr ""
"Language: en_US\n"''')
    catalog = pofile.read_po(buf, locale='de')
    assert Locale('en', 'US') == catalog.locale
    buf = StringIO(r'''
msgid ""
msgstr ""
"Language: ko-KR\n"''')
    catalog = pofile.read_po(buf, locale='de')
    assert Locale('ko', 'KR') == catalog.locale


def test_preserve_domain():
    buf = StringIO(r'''msgid "foo"
msgstr "Voh"''')
    catalog = pofile.read_po(buf, domain='mydomain')
    assert catalog.domain == 'mydomain'


def test_applies_specified_encoding_during_read():
    buf = BytesIO('''
msgid ""
msgstr ""
"Project-Id-Version:  3.15\\n"
"Report-Msgid-Bugs-To: Fliegender Zirkus <fliegender@zirkus.de>\\n"
"POT-Creation-Date: 2007-09-27 11:19+0700\\n"
"PO-Revision-Date: 2007-09-27 21:42-0700\\n"
"Last-Translator: John <cleese@bavaria.de>\\n"
"Language-Team: German Lang <de@babel.org>\\n"
"Plural-Forms: nplurals=2; plural=(n != 1);\\n"
"MIME-Version: 1.0\\n"
"Content-Type: text/plain; charset=iso-8859-1\\n"
"Content-Transfer-Encoding: 8bit\\n"
"Generated-By: Babel 1.0dev-r313\\n"

msgid "foo"
msgstr "bär"'''.encode('iso-8859-1'))
    catalog = pofile.read_po(buf, locale='de_DE')
    assert catalog.get('foo').string == 'bär'


def test_encoding_header_read():
    buf = BytesIO(b'msgid ""\nmsgstr ""\n"Content-Type: text/plain; charset=mac_roman\\n"\n')
    catalog = pofile.read_po(buf, locale='xx_XX')
    assert catalog.charset == 'mac_roman'


def test_plural_forms_header_parsed():
    buf = BytesIO(b'msgid ""\nmsgstr ""\n"Plural-Forms: nplurals=42; plural=(n % 11);\\n"\n')
    catalog = pofile.read_po(buf, locale='xx_XX')
    assert catalog.plural_expr == '(n % 11)'
    assert catalog.num_plurals == 42


def test_read_multiline():
    buf = StringIO(r'''msgid ""
"Here's some text that\n"
"includesareallylongwordthatmightbutshouldnt"
" throw us into an infinite "
"loop\n"
msgstr ""''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 1
    message = list(catalog)[1]
    assert message.id == (
        "Here's some text that\nincludesareallylongwordthat"
        "mightbutshouldnt throw us into an infinite loop\n"
    )


def test_fuzzy_header():
    buf = StringIO(r'''
# Translations template for AReallyReallyLongNameForAProject.
# Copyright (C) 2007 ORGANIZATION
# This file is distributed under the same license as the
# AReallyReallyLongNameForAProject project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
#, fuzzy
''')
    catalog = pofile.read_po(buf)
    assert len(list(catalog)) == 1
    assert list(catalog)[0].fuzzy


def test_not_fuzzy_header():
    buf = StringIO(r'''
# Translations template for AReallyReallyLongNameForAProject.
# Copyright (C) 2007 ORGANIZATION
# This file is distributed under the same license as the
# AReallyReallyLongNameForAProject project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
''')
    catalog = pofile.read_po(buf)
    assert len(list(catalog)) == 1
    assert not list(catalog)[0].fuzzy


def test_header_entry():
    buf = StringIO(r'''
# SOME DESCRIPTIVE TITLE.
# Copyright (C) 2007 THE PACKAGE'S COPYRIGHT HOLDER
# This file is distributed under the same license as the PACKAGE package.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version:  3.15\n"
"Report-Msgid-Bugs-To: Fliegender Zirkus <fliegender@zirkus.de>\n"
"POT-Creation-Date: 2007-09-27 11:19+0700\n"
"PO-Revision-Date: 2007-09-27 21:42-0700\n"
"Last-Translator: John <cleese@bavaria.de>\n"
"Language: de\n"
"Language-Team: German Lang <de@babel.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1);\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=iso-8859-2\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel 1.0dev-r313\n"
''')
    catalog = pofile.read_po(buf)
    assert len(list(catalog)) == 1
    assert catalog.version == '3.15'
    assert catalog.msgid_bugs_address == 'Fliegender Zirkus <fliegender@zirkus.de>'
    assert datetime(2007, 9, 27, 11, 19, tzinfo=FixedOffsetTimezone(7 * 60)) == catalog.creation_date
    assert catalog.last_translator == 'John <cleese@bavaria.de>'
    assert Locale('de') == catalog.locale
    assert catalog.language_team == 'German Lang <de@babel.org>'
    assert catalog.charset == 'iso-8859-2'
    assert list(catalog)[0].fuzzy


def test_obsolete_message():
    buf = StringIO(r'''# This is an obsolete message
#~ msgid "foo"
#~ msgstr "Voh"

# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 1
    assert len(catalog.obsolete) == 1
    message = catalog.obsolete['foo']
    assert message.id == 'foo'
    assert message.string == 'Voh'
    assert message.user_comments == ['This is an obsolete message']


def test_obsolete_message_ignored():
    buf = StringIO(r'''# This is an obsolete message
#~ msgid "foo"
#~ msgstr "Voh"

# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf, ignore_obsolete=True)
    assert len(catalog) == 1
    assert len(catalog.obsolete) == 0


def test_multi_line_obsolete_message():
    buf = StringIO(r'''# This is an obsolete message
#~ msgid ""
#~ "foo"
#~ "foo"
#~ msgstr ""
#~ "Voh"
#~ "Vooooh"

# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog.obsolete) == 1
    message = catalog.obsolete['foofoo']
    assert message.id == 'foofoo'
    assert message.string == 'VohVooooh'
    assert message.user_comments == ['This is an obsolete message']


def test_unit_following_multi_line_obsolete_message():
    buf = StringIO(r'''# This is an obsolete message
#~ msgid ""
#~ "foo"
#~ "fooooooo"
#~ msgstr ""
#~ "Voh"
#~ "Vooooh"

# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 1
    message = catalog['bar']
    assert message.id == 'bar'
    assert message.string == 'Bahr'
    assert message.user_comments == ['This message is not obsolete']


def test_unit_before_obsolete_is_not_obsoleted():
    buf = StringIO(r'''
# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"

# This is an obsolete message
#~ msgid ""
#~ "foo"
#~ "fooooooo"
#~ msgstr ""
#~ "Voh"
#~ "Vooooh"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 1
    message = catalog['bar']
    assert message.id == 'bar'
    assert message.string == 'Bahr'
    assert message.user_comments == ['This message is not obsolete']


def test_with_context():
    buf = BytesIO(b'''# Some string in the menu
#: main.py:1
msgctxt "Menu"
msgid "foo"
msgstr "Voh"

# Another string in the menu
#: main.py:2
msgctxt "Menu"
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf, ignore_obsolete=True)
    assert len(catalog) == 2
    message = catalog.get('foo', context='Menu')
    assert message.context == 'Menu'
    message = catalog.get('bar', context='Menu')
    assert message.context == 'Menu'

    # And verify it pass through write_po
    out_buf = BytesIO()
    pofile.write_po(out_buf, catalog, omit_header=True)
    assert out_buf.getvalue().strip() == buf.getvalue().strip()


def test_obsolete_message_with_context():
    buf = StringIO('''
# This message is not obsolete
msgid "baz"
msgstr "Bazczch"

# This is an obsolete message
#~ msgctxt "other"
#~ msgid "foo"
#~ msgstr "Voh"

# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 2
    assert len(catalog.obsolete) == 1
    message = catalog.obsolete[("foo", "other")]
    assert message.context == 'other'
    assert message.string == 'Voh'


def test_obsolete_messages_with_context():
    buf = StringIO('''
# This is an obsolete message
#~ msgctxt "apple"
#~ msgid "foo"
#~ msgstr "Foo"

# This is an obsolete message with the same id but different context
#~ msgctxt "orange"
#~ msgid "foo"
#~ msgstr "Bar"
''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 0
    assert len(catalog.obsolete) == 2
    assert 'foo' not in catalog.obsolete

    apple_msg = catalog.obsolete[('foo', 'apple')]
    assert apple_msg.id == 'foo'
    assert apple_msg.string == 'Foo'
    assert apple_msg.user_comments == ['This is an obsolete message']

    orange_msg = catalog.obsolete[('foo', 'orange')]
    assert orange_msg.id == 'foo'
    assert orange_msg.string == 'Bar'
    assert orange_msg.user_comments == ['This is an obsolete message with the same id but different context']


def test_obsolete_messages_roundtrip():
    buf = StringIO('''\
# This message is not obsolete
#: main.py:1
msgid "bar"
msgstr "Bahr"

# This is an obsolete message
#~ msgid "foo"
#~ msgstr "Voh"

# This is an obsolete message
#~ msgctxt "apple"
#~ msgid "foo"
#~ msgstr "Foo"

# This is an obsolete message with the same id but different context
#~ msgctxt "orange"
#~ msgid "foo"
#~ msgstr "Bar"

''')
    generated_po_file = ''.join(pofile.generate_po(pofile.read_po(buf), omit_header=True))
    assert buf.getvalue() == generated_po_file


def test_multiline_context():
    buf = StringIO('''
msgctxt "a really long "
"message context "
"why?"
msgid "mid"
msgstr "mst"
    ''')
    catalog = pofile.read_po(buf)
    assert len(catalog) == 1
    message = catalog.get('mid', context="a really long message context why?")
    assert message is not None
    assert message.context == 'a really long message context why?'


def test_with_context_two():
    buf = BytesIO(b'''msgctxt "Menu"
msgid "foo"
msgstr "Voh"

msgctxt "Mannu"
msgid "bar"
msgstr "Bahr"
''')
    catalog = pofile.read_po(buf, ignore_obsolete=True)
    assert len(catalog) == 2
    message = catalog.get('foo', context='Menu')
    assert message.context == 'Menu'
    message = catalog.get('bar', context='Mannu')
    assert message.context == 'Mannu'

    # And verify it pass through write_po
    out_buf = BytesIO()
    pofile.write_po(out_buf, catalog, omit_header=True)
    assert out_buf.getvalue().strip() == buf.getvalue().strip(), out_buf.getvalue()


def test_single_plural_form():
    buf = StringIO(r'''msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"''')
    catalog = pofile.read_po(buf, locale='ja_JP')
    assert len(catalog) == 1
    assert catalog.num_plurals == 1
    message = catalog['foo']
    assert len(message.string) == 1


def test_singular_plural_form():
    buf = StringIO(r'''msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Vohs"''')
    catalog = pofile.read_po(buf, locale='nl_NL')
    assert len(catalog) == 1
    assert catalog.num_plurals == 2
    message = catalog['foo']
    assert len(message.string) == 2


def test_more_than_two_plural_forms():
    buf = StringIO(r'''msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh"
msgstr[1] "Vohs"
msgstr[2] "Vohss"''')
    catalog = pofile.read_po(buf, locale='lv_LV')
    assert len(catalog) == 1
    assert catalog.num_plurals == 3
    message = catalog['foo']
    assert len(message.string) == 3
    assert message.string[2] == 'Vohss'


def test_plural_with_square_brackets():
    buf = StringIO(r'''msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh [text]"
msgstr[1] "Vohs [text]"''')
    catalog = pofile.read_po(buf, locale='nb_NO')
    assert len(catalog) == 1
    assert catalog.num_plurals == 2
    message = catalog['foo']
    assert len(message.string) == 2


def test_obsolete_plural_with_square_brackets():
    buf = StringIO('''\
#~ msgid "foo"
#~ msgid_plural "foos"
#~ msgstr[0] "Voh [text]"
#~ msgstr[1] "Vohs [text]"
''')
    catalog = pofile.read_po(buf, locale='nb_NO')
    assert len(catalog) == 0
    assert len(catalog.obsolete) == 1
    assert catalog.num_plurals == 2
    message = catalog.obsolete['foo']
    assert len(message.string) == 2
    assert message.string[0] == 'Voh [text]'
    assert message.string[1] == 'Vohs [text]'


def test_missing_plural():
    buf = StringIO('''\
msgid ""
msgstr ""
"Plural-Forms: nplurals=3; plural=(n < 2) ? n : 2;\n"

msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh [text]"
msgstr[1] "Vohs [text]"
''')
    catalog = pofile.read_po(buf, locale='nb_NO')
    assert len(catalog) == 1
    assert catalog.num_plurals == 3
    message = catalog['foo']
    assert len(message.string) == 3
    assert message.string[0] == 'Voh [text]'
    assert message.string[1] == 'Vohs [text]'
    assert message.string[2] == ''


def test_missing_plural_in_the_middle():
    buf = StringIO('''\
msgid ""
msgstr ""
"Plural-Forms: nplurals=3; plural=(n < 2) ? n : 2;\n"

msgid "foo"
msgid_plural "foos"
msgstr[0] "Voh [text]"
msgstr[2] "Vohs [text]"
''')
    catalog = pofile.read_po(buf, locale='nb_NO')
    assert len(catalog) == 1
    assert catalog.num_plurals == 3
    message = catalog['foo']
    assert len(message.string) == 3
    assert message.string[0] == 'Voh [text]'
    assert message.string[1] == ''
    assert message.string[2] == 'Vohs [text]'


def test_with_location():
    buf = StringIO('''\
#: main.py:1 \u2068filename with whitespace.py\u2069:123
msgid "foo"
msgstr "bar"
''')
    catalog = pofile.read_po(buf, locale='de_DE')
    assert len(catalog) == 1
    message = catalog['foo']
    assert message.string == 'bar'
    assert message.locations == [("main.py", 1), ("filename with whitespace.py", 123)]


def test_abort_invalid_po_file():
    invalid_po = '''
        msgctxt ""
        "{\"checksum\": 2148532640, \"cxt\": \"collector_thankyou\", \"id\": "
        "270005359}"
        msgid ""
        "Thank you very much for your time.\n"
        "If you have any questions regarding this survey, please contact Fulano "
        "at nadie@blah.com"
        msgstr "Merci de prendre le temps de remplir le sondage.
        Pour toute question, veuillez communiquer avec Fulano  à nadie@blah.com
        "
    '''
    invalid_po_2 = '''
        msgctxt ""
        "{\"checksum\": 2148532640, \"cxt\": \"collector_thankyou\", \"id\": "
        "270005359}"
        msgid ""
        "Thank you very much for your time.\n"
        "If you have any questions regarding this survey, please contact Fulano "
        "at fulano@blah.com."
        msgstr "Merci de prendre le temps de remplir le sondage.
        Pour toute question, veuillez communiquer avec Fulano a fulano@blah.com
        "
        '''
    # Catalog not created, throws Unicode Error
    buf = StringIO(invalid_po)
    output = pofile.read_po(buf, locale='fr', abort_invalid=False)
    assert isinstance(output, Catalog)

    # Catalog not created, throws PoFileError
    buf = StringIO(invalid_po_2)
    with pytest.raises(pofile.PoFileError):
        pofile.read_po(buf, locale='fr', abort_invalid=True)

    # Catalog is created with warning, no abort
    buf = StringIO(invalid_po_2)
    output = pofile.read_po(buf, locale='fr', abort_invalid=False)
    assert isinstance(output, Catalog)

    # Catalog not created, aborted with PoFileError
    buf = StringIO(invalid_po_2)
    with pytest.raises(pofile.PoFileError):
        pofile.read_po(buf, locale='fr', abort_invalid=True)


def test_invalid_pofile_with_abort_flag():
    parser = pofile.PoFileParser(None, abort_invalid=True)
    lineno = 10
    line = 'Algo esta mal'
    msg = 'invalid file'
    with pytest.raises(pofile.PoFileError):
        parser._invalid_pofile(line, lineno, msg)
