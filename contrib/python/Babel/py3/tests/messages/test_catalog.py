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

import copy
import datetime
import pickle
from io import StringIO

from babel.dates import UTC, format_datetime
from babel.messages import catalog, pofile
from babel.util import FixedOffsetTimezone


def test_message_python_format():
    assert catalog.PYTHON_FORMAT.search('foo %d bar')
    assert catalog.PYTHON_FORMAT.search('foo %s bar')
    assert catalog.PYTHON_FORMAT.search('foo %r bar')
    assert catalog.PYTHON_FORMAT.search('foo %(name).1f')
    assert catalog.PYTHON_FORMAT.search('foo %(name)3.3f')
    assert catalog.PYTHON_FORMAT.search('foo %(name)3f')
    assert catalog.PYTHON_FORMAT.search('foo %(name)06d')
    assert catalog.PYTHON_FORMAT.search('foo %(name)Li')
    assert catalog.PYTHON_FORMAT.search('foo %(name)#d')
    assert catalog.PYTHON_FORMAT.search('foo %(name)-4.4hs')
    assert catalog.PYTHON_FORMAT.search('foo %(name)*.3f')
    assert catalog.PYTHON_FORMAT.search('foo %(name).*f')
    assert catalog.PYTHON_FORMAT.search('foo %(name)3.*f')
    assert catalog.PYTHON_FORMAT.search('foo %(name)*.*f')
    assert catalog.PYTHON_FORMAT.search('foo %()s')


def test_message_python_brace_format():
    assert not catalog._has_python_brace_format('')
    assert not catalog._has_python_brace_format('foo')
    assert not catalog._has_python_brace_format('{')
    assert not catalog._has_python_brace_format('}')
    assert not catalog._has_python_brace_format('{} {')
    assert not catalog._has_python_brace_format('{{}}')
    assert catalog._has_python_brace_format('{}')
    assert catalog._has_python_brace_format('foo {name}')
    assert catalog._has_python_brace_format('foo {name!s}')
    assert catalog._has_python_brace_format('foo {name!r}')
    assert catalog._has_python_brace_format('foo {name!a}')
    assert catalog._has_python_brace_format('foo {name!r:10}')
    assert catalog._has_python_brace_format('foo {name!r:10.2}')
    assert catalog._has_python_brace_format('foo {name!r:10.2f}')
    assert catalog._has_python_brace_format('foo {name!r:10.2f} {name!r:10.2f}')
    assert catalog._has_python_brace_format('foo {name!r:10.2f=}')


def test_message_translator_comments():
    mess = catalog.Message('foo', user_comments=['Comment About `foo`'])
    assert mess.user_comments == ['Comment About `foo`']
    mess = catalog.Message('foo',
                           auto_comments=['Comment 1 About `foo`',
                                          'Comment 2 About `foo`'])
    assert mess.auto_comments == ['Comment 1 About `foo`', 'Comment 2 About `foo`']


def test_message_clone_message_object():
    msg = catalog.Message('foo', locations=[('foo.py', 42)])
    clone = msg.clone()
    clone.locations.append(('bar.py', 42))
    assert msg.locations == [('foo.py', 42)]
    msg.flags.add('fuzzy')
    assert not clone.fuzzy and msg.fuzzy


def test_catalog_add_returns_message_instance():
    cat = catalog.Catalog()
    message = cat.add('foo')
    assert message.id == 'foo'


def test_catalog_two_messages_with_same_singular():
    cat = catalog.Catalog()
    cat.add('foo')
    cat.add(('foo', 'foos'))
    assert len(cat) == 1


def test_catalog_duplicate_auto_comment():
    cat = catalog.Catalog()
    cat.add('foo', auto_comments=['A comment'])
    cat.add('foo', auto_comments=['A comment', 'Another comment'])
    assert cat['foo'].auto_comments == ['A comment', 'Another comment']


def test_catalog_duplicate_user_comment():
    cat = catalog.Catalog()
    cat.add('foo', user_comments=['A comment'])
    cat.add('foo', user_comments=['A comment', 'Another comment'])
    assert cat['foo'].user_comments == ['A comment', 'Another comment']


def test_catalog_duplicate_location():
    cat = catalog.Catalog()
    cat.add('foo', locations=[('foo.py', 1)])
    cat.add('foo', locations=[('foo.py', 1)])
    assert cat['foo'].locations == [('foo.py', 1)]


def test_catalog_update_message_changed_to_plural():
    cat = catalog.Catalog()
    cat.add('foo', 'Voh')
    tmpl = catalog.Catalog()
    tmpl.add(('foo', 'foos'))
    cat.update(tmpl)
    assert cat['foo'].string == ('Voh', '')
    assert cat['foo'].fuzzy


def test_catalog_update_message_changed_to_simple():
    cat = catalog.Catalog()
    cat.add('foo' 'foos', ('Voh', 'Vöhs'))
    tmpl = catalog.Catalog()
    tmpl.add('foo')
    cat.update(tmpl)
    assert cat['foo'].string == 'Voh'
    assert cat['foo'].fuzzy


def test_catalog_update_message_updates_comments():
    cat = catalog.Catalog()
    cat['foo'] = catalog.Message('foo', locations=[('main.py', 5)])
    assert cat['foo'].auto_comments == []
    assert cat['foo'].user_comments == []
    # Update cat['foo'] with a new location and a comment
    cat['foo'] = catalog.Message('foo', locations=[('main.py', 7)],
                                 user_comments=['Foo Bar comment 1'])
    assert cat['foo'].user_comments == ['Foo Bar comment 1']
    # now add yet another location with another comment
    cat['foo'] = catalog.Message('foo', locations=[('main.py', 9)],
                                 auto_comments=['Foo Bar comment 2'])
    assert cat['foo'].auto_comments == ['Foo Bar comment 2']


def test_catalog_update_fuzzy_matching_with_case_change():
    cat = catalog.Catalog()
    cat.add('FOO', 'Voh')
    cat.add('bar', 'Bahr')
    tmpl = catalog.Catalog()
    tmpl.add('foo')
    cat.update(tmpl)
    assert len(cat.obsolete) == 1
    assert 'FOO' not in cat

    assert cat['foo'].string == 'Voh'
    assert cat['foo'].fuzzy is True


def test_catalog_update_fuzzy_matching_with_char_change():
    cat = catalog.Catalog()
    cat.add('fo', 'Voh')
    cat.add('bar', 'Bahr')
    tmpl = catalog.Catalog()
    tmpl.add('foo')
    cat.update(tmpl)
    assert len(cat.obsolete) == 1
    assert 'fo' not in cat

    assert cat['foo'].string == 'Voh'
    assert cat['foo'].fuzzy is True


def test_catalog_update_fuzzy_matching_no_msgstr():
    cat = catalog.Catalog()
    cat.add('fo', '')
    tmpl = catalog.Catalog()
    tmpl.add('fo')
    tmpl.add('foo')
    cat.update(tmpl)
    assert 'fo' in cat
    assert 'foo' in cat

    assert cat['fo'].string == ''
    assert cat['fo'].fuzzy is False
    assert cat['foo'].string is None
    assert cat['foo'].fuzzy is False


def test_catalog_update_fuzzy_matching_with_new_context():
    cat = catalog.Catalog()
    cat.add('foo', 'Voh')
    cat.add('bar', 'Bahr')
    tmpl = catalog.Catalog()
    tmpl.add('Foo', context='Menu')
    cat.update(tmpl)
    assert len(cat.obsolete) == 1
    assert 'foo' not in cat

    message = cat.get('Foo', 'Menu')
    assert message.string == 'Voh'
    assert message.fuzzy is True
    assert message.context == 'Menu'


def test_catalog_update_fuzzy_matching_with_changed_context():
    cat = catalog.Catalog()
    cat.add('foo', 'Voh', context='Menu|File')
    cat.add('bar', 'Bahr', context='Menu|File')
    tmpl = catalog.Catalog()
    tmpl.add('Foo', context='Menu|Edit')
    cat.update(tmpl)
    assert len(cat.obsolete) == 1
    assert cat.get('Foo', 'Menu|File') is None

    message = cat.get('Foo', 'Menu|Edit')
    assert message.string == 'Voh'
    assert message.fuzzy is True
    assert message.context == 'Menu|Edit'


def test_catalog_update_fuzzy_matching_no_cascading():
    cat = catalog.Catalog()
    cat.add('fo', 'Voh')
    cat.add('foo', 'Vohe')
    tmpl = catalog.Catalog()
    tmpl.add('fo')
    tmpl.add('foo')
    tmpl.add('fooo')
    cat.update(tmpl)
    assert 'fo' in cat
    assert 'foo' in cat

    assert cat['fo'].string == 'Voh'
    assert cat['fo'].fuzzy is False
    assert cat['foo'].string == 'Vohe'
    assert cat['foo'].fuzzy is False
    assert cat['fooo'].string == 'Vohe'
    assert cat['fooo'].fuzzy is True


def test_catalog_update_fuzzy_matching_long_string():
    lipsum = "\
Lorem Ipsum is simply dummy text of the printing and typesetting \
industry. Lorem Ipsum has been the industry's standard dummy text ever \
since the 1500s, when an unknown printer took a galley of type and \
scrambled it to make a type specimen book. It has survived not only \
five centuries, but also the leap into electronic typesetting, \
remaining essentially unchanged. It was popularised in the 1960s with \
the release of Letraset sheets containing Lorem Ipsum passages, and \
more recently with desktop publishing software like Aldus PageMaker \
including versions of Lorem Ipsum."
    cat = catalog.Catalog()
    cat.add("ZZZZZZ " + lipsum, "foo")
    tmpl = catalog.Catalog()
    tmpl.add(lipsum + " ZZZZZZ")
    cat.update(tmpl)
    assert cat[lipsum + " ZZZZZZ"].fuzzy is True
    assert len(cat.obsolete) == 0


def test_catalog_update_without_fuzzy_matching():
    cat = catalog.Catalog()
    cat.add('fo', 'Voh')
    cat.add('bar', 'Bahr')
    tmpl = catalog.Catalog()
    tmpl.add('foo')
    cat.update(tmpl, no_fuzzy_matching=True)
    assert len(cat.obsolete) == 2


def test_catalog_fuzzy_matching_regarding_plurals():
    cat = catalog.Catalog()
    cat.add(('foo', 'foh'), ('foo', 'foh'))
    ru = copy.copy(cat)
    ru.locale = 'ru_RU'
    ru.update(cat)
    assert ru['foo'].fuzzy is True
    ru = copy.copy(cat)
    ru.locale = 'ru_RU'
    ru['foo'].string = ('foh', 'fohh', 'fohhh')
    ru.update(cat)
    assert ru['foo'].fuzzy is False


def test_catalog_update_no_template_mutation():
    tmpl = catalog.Catalog()
    tmpl.add('foo')
    cat1 = catalog.Catalog()
    cat1.add('foo', 'Voh')
    cat1.update(tmpl)
    cat2 = catalog.Catalog()
    cat2.update(tmpl)

    assert cat2['foo'].string is None
    assert cat2['foo'].fuzzy is False


def test_catalog_update_po_updates_pot_creation_date():
    template = catalog.Catalog()
    localized_catalog = copy.deepcopy(template)
    localized_catalog.locale = 'de_DE'
    assert template.mime_headers != localized_catalog.mime_headers
    assert template.creation_date == localized_catalog.creation_date
    template.creation_date = datetime.datetime.now() - \
        datetime.timedelta(minutes=5)
    localized_catalog.update(template)
    assert template.creation_date == localized_catalog.creation_date


def test_catalog_update_po_ignores_pot_creation_date():
    template = catalog.Catalog()
    localized_catalog = copy.deepcopy(template)
    localized_catalog.locale = 'de_DE'
    assert template.mime_headers != localized_catalog.mime_headers
    assert template.creation_date == localized_catalog.creation_date
    template.creation_date = datetime.datetime.now() - \
        datetime.timedelta(minutes=5)
    localized_catalog.update(template, update_creation_date=False)
    assert template.creation_date != localized_catalog.creation_date


def test_catalog_update_po_keeps_po_revision_date():
    template = catalog.Catalog()
    localized_catalog = copy.deepcopy(template)
    localized_catalog.locale = 'de_DE'
    fake_rev_date = datetime.datetime.now() - datetime.timedelta(days=5)
    localized_catalog.revision_date = fake_rev_date
    assert template.mime_headers != localized_catalog.mime_headers
    assert template.creation_date == localized_catalog.creation_date
    template.creation_date = datetime.datetime.now() - \
        datetime.timedelta(minutes=5)
    localized_catalog.update(template)
    assert localized_catalog.revision_date == fake_rev_date


def test_catalog_stores_datetime_correctly():
    localized = catalog.Catalog()
    localized.locale = 'de_DE'
    localized[''] = catalog.Message('',
                                    "POT-Creation-Date: 2009-03-09 15:47-0700\n" +
                                    "PO-Revision-Date: 2009-03-09 15:47-0700\n")
    for key, value in localized.mime_headers:
        if key in ('POT-Creation-Date', 'PO-Revision-Date'):
            assert value == '2009-03-09 15:47-0700'


def test_catalog_mime_headers_contain_same_information_as_attributes():
    cat = catalog.Catalog()
    cat[''] = catalog.Message('',
                              "Last-Translator: Foo Bar <foo.bar@example.com>\n" +
                              "Language-Team: de <de@example.com>\n" +
                              "POT-Creation-Date: 2009-03-01 11:20+0200\n" +
                              "PO-Revision-Date: 2009-03-09 15:47-0700\n")
    assert cat.locale is None
    mime_headers = dict(cat.mime_headers)

    assert cat.last_translator == 'Foo Bar <foo.bar@example.com>'
    assert mime_headers['Last-Translator'] == 'Foo Bar <foo.bar@example.com>'

    assert cat.language_team == 'de <de@example.com>'
    assert mime_headers['Language-Team'] == 'de <de@example.com>'

    dt = datetime.datetime(2009, 3, 9, 15, 47, tzinfo=FixedOffsetTimezone(-7 * 60))
    assert cat.revision_date == dt
    formatted_dt = format_datetime(dt, 'yyyy-MM-dd HH:mmZ', locale='en')
    assert mime_headers['PO-Revision-Date'] == formatted_dt


def test_message_fuzzy():
    assert not catalog.Message('foo').fuzzy
    msg = catalog.Message('foo', 'foo', flags=['fuzzy'])
    assert msg.fuzzy
    assert msg.id == 'foo'


def test_message_pluralizable():
    assert not catalog.Message('foo').pluralizable
    assert catalog.Message(('foo', 'bar')).pluralizable


def test_message_python_format_2():
    assert not catalog.Message('foo').python_format
    assert not catalog.Message(('foo', 'foo')).python_format
    assert catalog.Message('foo %(name)s bar').python_format
    assert catalog.Message(('foo %(name)s', 'foo %(name)s')).python_format


def test_message_python_brace_format_2():
    assert not catalog.Message('foo').python_brace_format
    assert not catalog.Message(('foo', 'foo')).python_brace_format
    assert catalog.Message('foo {name} bar').python_brace_format
    assert catalog.Message(('foo {name}', 'foo {name}')).python_brace_format


def test_catalog():
    cat = catalog.Catalog(project='Foobar', version='1.0',
                          copyright_holder='Foo Company')
    assert cat.header_comment == (
        '# Translations template for Foobar.\n'
        '# Copyright (C) %(year)d Foo Company\n'
        '# This file is distributed under the same '
        'license as the Foobar project.\n'
        '# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)d.\n'
        '#') % {'year': datetime.date.today().year}

    cat = catalog.Catalog(project='Foobar', version='1.0',
                          copyright_holder='Foo Company')
    cat.header_comment = (
        '# The POT for my really cool PROJECT project.\n'
        '# Copyright (C) 1990-2003 ORGANIZATION\n'
        '# This file is distributed under the same license as the PROJECT\n'
        '# project.\n'
        '#\n')
    assert cat.header_comment == (
        '# The POT for my really cool Foobar project.\n'
        '# Copyright (C) 1990-2003 Foo Company\n'
        '# This file is distributed under the same license as the Foobar\n'
        '# project.\n'
        '#\n')


def test_catalog_mime_headers():
    created = datetime.datetime(1990, 4, 1, 15, 30, tzinfo=UTC)
    cat = catalog.Catalog(project='Foobar', version='1.0',
                          creation_date=created)
    assert cat.mime_headers == [
        ('Project-Id-Version', 'Foobar 1.0'),
        ('Report-Msgid-Bugs-To', 'EMAIL@ADDRESS'),
        ('POT-Creation-Date', '1990-04-01 15:30+0000'),
        ('PO-Revision-Date', 'YEAR-MO-DA HO:MI+ZONE'),
        ('Last-Translator', 'FULL NAME <EMAIL@ADDRESS>'),
        ('Language-Team', 'LANGUAGE <LL@li.org>'),
        ('MIME-Version', '1.0'),
        ('Content-Type', 'text/plain; charset=utf-8'),
        ('Content-Transfer-Encoding', '8bit'),
        ('Generated-By', f'Babel {catalog.VERSION}\n'),
    ]


def test_catalog_mime_headers_set_locale():
    created = datetime.datetime(1990, 4, 1, 15, 30, tzinfo=UTC)
    revised = datetime.datetime(1990, 8, 3, 12, 0, tzinfo=UTC)
    cat = catalog.Catalog(locale='de_DE', project='Foobar', version='1.0',
                          creation_date=created, revision_date=revised,
                          last_translator='John Doe <jd@example.com>',
                          language_team='de_DE <de@example.com>')
    assert cat.mime_headers == [
        ('Project-Id-Version', 'Foobar 1.0'),
        ('Report-Msgid-Bugs-To', 'EMAIL@ADDRESS'),
        ('POT-Creation-Date', '1990-04-01 15:30+0000'),
        ('PO-Revision-Date', '1990-08-03 12:00+0000'),
        ('Last-Translator', 'John Doe <jd@example.com>'),
        ('Language', 'de_DE'),
        ('Language-Team', 'de_DE <de@example.com>'),
        ('Plural-Forms', 'nplurals=2; plural=(n != 1);'),
        ('MIME-Version', '1.0'),
        ('Content-Type', 'text/plain; charset=utf-8'),
        ('Content-Transfer-Encoding', '8bit'),
        ('Generated-By', f'Babel {catalog.VERSION}\n'),
    ]


def test_catalog_mime_headers_type_coercion():
    """
    Test that mime headers' keys and values are coerced to strings
    """
    cat = catalog.Catalog(locale='de_DE', project='Foobar', version='1.0')
    # This is a strange interface in that it doesn't actually overwrite all
    # of the MIME headers, but just sets the ones that are passed in (and known).
    cat.mime_headers = {b'REPORT-MSGID-BUGS-TO': 8}.items()
    assert dict(cat.mime_headers)['Report-Msgid-Bugs-To'] == '8'


def test_catalog_num_plurals():
    assert catalog.Catalog(locale='en').num_plurals == 2
    assert catalog.Catalog(locale='ga').num_plurals == 5


def test_catalog_plural_expr():
    assert catalog.Catalog(locale='en').plural_expr == '(n != 1)'
    assert (catalog.Catalog(locale='ga').plural_expr
            == '(n==1 ? 0 : n==2 ? 1 : n>=3 && n<=6 ? 2 : n>=7 && n<=10 ? 3 : 4)')


def test_catalog_plural_forms():
    assert (catalog.Catalog(locale='en').plural_forms
            == 'nplurals=2; plural=(n != 1);')
    assert (catalog.Catalog(locale='pt_BR').plural_forms
            == 'nplurals=2; plural=(n > 1);')


def test_catalog_setitem():
    cat = catalog.Catalog()
    cat['foo'] = catalog.Message('foo')
    assert cat['foo'].id == 'foo'

    cat = catalog.Catalog()
    cat['foo'] = catalog.Message('foo', locations=[('main.py', 1)])
    assert cat['foo'].locations == [('main.py', 1)]
    cat['foo'] = catalog.Message('foo', locations=[('utils.py', 5)])
    assert cat['foo'].locations == [('main.py', 1), ('utils.py', 5)]


def test_catalog_add():
    cat = catalog.Catalog()
    foo = cat.add('foo')
    assert foo.id == 'foo'
    assert cat['foo'] is foo


def test_catalog_update():
    template = catalog.Catalog(header_comment="# A Custom Header")
    template.add('green', locations=[('main.py', 99)])
    template.add('blue', locations=[('main.py', 100)])
    template.add(('salad', 'salads'), locations=[('util.py', 42)])
    cat = catalog.Catalog(locale='de_DE')
    cat.add('blue', 'blau', locations=[('main.py', 98)])
    cat.add('head', 'Kopf', locations=[('util.py', 33)])
    cat.add(('salad', 'salads'), ('Salat', 'Salate'),
            locations=[('util.py', 38)])

    cat.update(template)
    assert len(cat) == 3

    msg1 = cat['green']
    assert not msg1.string
    assert msg1.locations == [('main.py', 99)]

    msg2 = cat['blue']
    assert msg2.string == 'blau'
    assert msg2.locations == [('main.py', 100)]

    msg3 = cat['salad']
    assert msg3.string == ('Salat', 'Salate')
    assert msg3.locations == [('util.py', 42)]

    assert 'head' not in cat
    assert list(cat.obsolete.values())[0].id == 'head'

    cat.update(template, update_header_comment=True)
    assert cat.header_comment == template.header_comment  # Header comment also gets updated


def test_datetime_parsing():
    val1 = catalog._parse_datetime_header('2006-06-28 23:24+0200')
    assert val1.timetuple()[:5] == (2006, 6, 28, 23, 24)
    assert val1.utctimetuple()[:5] == (2006, 6, 28, 21, 24)
    assert val1.tzinfo.tzname(None) == 'Etc/GMT+120'
    assert val1 == datetime.datetime(2006, 6, 28, 21, 24, tzinfo=UTC)

    val2 = catalog._parse_datetime_header('2006-06-28 23:24')
    assert val2.year == 2006
    assert val2.month == 6
    assert val2.day == 28
    assert val2.tzinfo is None


def test_update_catalog_comments():
    # Based on https://web.archive.org/web/20100710131029/http://babel.edgewall.org/attachment/ticket/163/cat-update-comments.py

    catalog = pofile.read_po(StringIO('''
    # A user comment
    #. An auto comment
    #: main.py:1
    #, fuzzy, python-format
    msgid "foo %(name)s"
    msgstr "foo %(name)s"
    '''))

    assert all(message.user_comments and message.auto_comments for message in catalog if message.id)

    # NOTE: in the POT file, there are no comments
    template = pofile.read_po(StringIO('''
    #: main.py:1
    #, fuzzy, python-format
    msgid "bar %(name)s"
    msgstr ""
    '''))

    catalog.update(template)

    # Auto comments will be obliterated here
    assert all(message.user_comments for message in catalog if message.id)


def test_catalog_tz_pickleable():
    """
    Test that catalogs with timezoned times are pickleable.
    This would previously fail with `FixedOffsetTimezone.__init__() missing 1 required positional argument: 'offset'`
    when trying to load the pickled data.
    """
    pickle.loads(pickle.dumps(pofile.read_po(StringIO(r"""
msgid ""
msgstr ""
"POT-Creation-Date: 2007-04-01 15:30+0200\n"
    """))))
