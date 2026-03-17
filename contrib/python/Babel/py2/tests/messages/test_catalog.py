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

import copy
import datetime
import unittest

from babel._compat import StringIO
from babel.dates import format_datetime, UTC
from babel.messages import catalog, pofile
from babel.util import FixedOffsetTimezone


class MessageTestCase(unittest.TestCase):

    def test_python_format(self):
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

    def test_translator_comments(self):
        mess = catalog.Message('foo', user_comments=['Comment About `foo`'])
        self.assertEqual(mess.user_comments, ['Comment About `foo`'])
        mess = catalog.Message('foo',
                               auto_comments=['Comment 1 About `foo`',
                                              'Comment 2 About `foo`'])
        self.assertEqual(mess.auto_comments, ['Comment 1 About `foo`',
                                              'Comment 2 About `foo`'])

    def test_clone_message_object(self):
        msg = catalog.Message('foo', locations=[('foo.py', 42)])
        clone = msg.clone()
        clone.locations.append(('bar.py', 42))
        self.assertEqual(msg.locations, [('foo.py', 42)])
        msg.flags.add('fuzzy')
        assert not clone.fuzzy and msg.fuzzy


class CatalogTestCase(unittest.TestCase):

    def test_add_returns_message_instance(self):
        cat = catalog.Catalog()
        message = cat.add('foo')
        self.assertEqual('foo', message.id)

    def test_two_messages_with_same_singular(self):
        cat = catalog.Catalog()
        cat.add('foo')
        cat.add(('foo', 'foos'))
        self.assertEqual(1, len(cat))

    def test_duplicate_auto_comment(self):
        cat = catalog.Catalog()
        cat.add('foo', auto_comments=['A comment'])
        cat.add('foo', auto_comments=['A comment', 'Another comment'])
        self.assertEqual(['A comment', 'Another comment'],
                         cat['foo'].auto_comments)

    def test_duplicate_user_comment(self):
        cat = catalog.Catalog()
        cat.add('foo', user_comments=['A comment'])
        cat.add('foo', user_comments=['A comment', 'Another comment'])
        self.assertEqual(['A comment', 'Another comment'],
                         cat['foo'].user_comments)

    def test_duplicate_location(self):
        cat = catalog.Catalog()
        cat.add('foo', locations=[('foo.py', 1)])
        cat.add('foo', locations=[('foo.py', 1)])
        self.assertEqual([('foo.py', 1)], cat['foo'].locations)

    def test_update_message_changed_to_plural(self):
        cat = catalog.Catalog()
        cat.add(u'foo', u'Voh')
        tmpl = catalog.Catalog()
        tmpl.add((u'foo', u'foos'))
        cat.update(tmpl)
        self.assertEqual((u'Voh', ''), cat['foo'].string)
        assert cat['foo'].fuzzy

    def test_update_message_changed_to_simple(self):
        cat = catalog.Catalog()
        cat.add(u'foo' u'foos', (u'Voh', u'Vöhs'))
        tmpl = catalog.Catalog()
        tmpl.add(u'foo')
        cat.update(tmpl)
        self.assertEqual(u'Voh', cat['foo'].string)
        assert cat['foo'].fuzzy

    def test_update_message_updates_comments(self):
        cat = catalog.Catalog()
        cat[u'foo'] = catalog.Message('foo', locations=[('main.py', 5)])
        self.assertEqual(cat[u'foo'].auto_comments, [])
        self.assertEqual(cat[u'foo'].user_comments, [])
        # Update cat[u'foo'] with a new location and a comment
        cat[u'foo'] = catalog.Message('foo', locations=[('main.py', 7)],
                                      user_comments=['Foo Bar comment 1'])
        self.assertEqual(cat[u'foo'].user_comments, ['Foo Bar comment 1'])
        # now add yet another location with another comment
        cat[u'foo'] = catalog.Message('foo', locations=[('main.py', 9)],
                                      auto_comments=['Foo Bar comment 2'])
        self.assertEqual(cat[u'foo'].auto_comments, ['Foo Bar comment 2'])

    def test_update_fuzzy_matching_with_case_change(self):
        cat = catalog.Catalog()
        cat.add('foo', 'Voh')
        cat.add('bar', 'Bahr')
        tmpl = catalog.Catalog()
        tmpl.add('Foo')
        cat.update(tmpl)
        self.assertEqual(1, len(cat.obsolete))
        assert 'foo' not in cat

        self.assertEqual('Voh', cat['Foo'].string)
        self.assertEqual(True, cat['Foo'].fuzzy)

    def test_update_fuzzy_matching_with_char_change(self):
        cat = catalog.Catalog()
        cat.add('fo', 'Voh')
        cat.add('bar', 'Bahr')
        tmpl = catalog.Catalog()
        tmpl.add('foo')
        cat.update(tmpl)
        self.assertEqual(1, len(cat.obsolete))
        assert 'fo' not in cat

        self.assertEqual('Voh', cat['foo'].string)
        self.assertEqual(True, cat['foo'].fuzzy)

    def test_update_fuzzy_matching_no_msgstr(self):
        cat = catalog.Catalog()
        cat.add('fo', '')
        tmpl = catalog.Catalog()
        tmpl.add('fo')
        tmpl.add('foo')
        cat.update(tmpl)
        assert 'fo' in cat
        assert 'foo' in cat

        self.assertEqual('', cat['fo'].string)
        self.assertEqual(False, cat['fo'].fuzzy)
        self.assertEqual(None, cat['foo'].string)
        self.assertEqual(False, cat['foo'].fuzzy)

    def test_update_fuzzy_matching_with_new_context(self):
        cat = catalog.Catalog()
        cat.add('foo', 'Voh')
        cat.add('bar', 'Bahr')
        tmpl = catalog.Catalog()
        tmpl.add('Foo', context='Menu')
        cat.update(tmpl)
        self.assertEqual(1, len(cat.obsolete))
        assert 'foo' not in cat

        message = cat.get('Foo', 'Menu')
        self.assertEqual('Voh', message.string)
        self.assertEqual(True, message.fuzzy)
        self.assertEqual('Menu', message.context)

    def test_update_fuzzy_matching_with_changed_context(self):
        cat = catalog.Catalog()
        cat.add('foo', 'Voh', context='Menu|File')
        cat.add('bar', 'Bahr', context='Menu|File')
        tmpl = catalog.Catalog()
        tmpl.add('Foo', context='Menu|Edit')
        cat.update(tmpl)
        self.assertEqual(1, len(cat.obsolete))
        assert cat.get('Foo', 'Menu|File') is None

        message = cat.get('Foo', 'Menu|Edit')
        self.assertEqual('Voh', message.string)
        self.assertEqual(True, message.fuzzy)
        self.assertEqual('Menu|Edit', message.context)

    def test_update_fuzzy_matching_no_cascading(self):
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

        self.assertEqual('Voh', cat['fo'].string)
        self.assertEqual(False, cat['fo'].fuzzy)
        self.assertEqual('Vohe', cat['foo'].string)
        self.assertEqual(False, cat['foo'].fuzzy)
        self.assertEqual('Vohe', cat['fooo'].string)
        self.assertEqual(True, cat['fooo'].fuzzy)

    def test_update_without_fuzzy_matching(self):
        cat = catalog.Catalog()
        cat.add('fo', 'Voh')
        cat.add('bar', 'Bahr')
        tmpl = catalog.Catalog()
        tmpl.add('foo')
        cat.update(tmpl, no_fuzzy_matching=True)
        self.assertEqual(2, len(cat.obsolete))

    def test_fuzzy_matching_regarding_plurals(self):
        cat = catalog.Catalog()
        cat.add(('foo', 'foh'), ('foo', 'foh'))
        ru = copy.copy(cat)
        ru.locale = 'ru_RU'
        ru.update(cat)
        self.assertEqual(True, ru['foo'].fuzzy)
        ru = copy.copy(cat)
        ru.locale = 'ru_RU'
        ru['foo'].string = ('foh', 'fohh', 'fohhh')
        ru.update(cat)
        self.assertEqual(False, ru['foo'].fuzzy)

    def test_update_no_template_mutation(self):
        tmpl = catalog.Catalog()
        tmpl.add('foo')
        cat1 = catalog.Catalog()
        cat1.add('foo', 'Voh')
        cat1.update(tmpl)
        cat2 = catalog.Catalog()
        cat2.update(tmpl)

        self.assertEqual(None, cat2['foo'].string)
        self.assertEqual(False, cat2['foo'].fuzzy)

    def test_update_po_updates_pot_creation_date(self):
        template = catalog.Catalog()
        localized_catalog = copy.deepcopy(template)
        localized_catalog.locale = 'de_DE'
        self.assertNotEqual(template.mime_headers,
                            localized_catalog.mime_headers)
        self.assertEqual(template.creation_date,
                         localized_catalog.creation_date)
        template.creation_date = datetime.datetime.now() - \
            datetime.timedelta(minutes=5)
        localized_catalog.update(template)
        self.assertEqual(template.creation_date,
                         localized_catalog.creation_date)

    def test_update_po_keeps_po_revision_date(self):
        template = catalog.Catalog()
        localized_catalog = copy.deepcopy(template)
        localized_catalog.locale = 'de_DE'
        fake_rev_date = datetime.datetime.now() - datetime.timedelta(days=5)
        localized_catalog.revision_date = fake_rev_date
        self.assertNotEqual(template.mime_headers,
                            localized_catalog.mime_headers)
        self.assertEqual(template.creation_date,
                         localized_catalog.creation_date)
        template.creation_date = datetime.datetime.now() - \
            datetime.timedelta(minutes=5)
        localized_catalog.update(template)
        self.assertEqual(localized_catalog.revision_date, fake_rev_date)

    def test_stores_datetime_correctly(self):
        localized = catalog.Catalog()
        localized.locale = 'de_DE'
        localized[''] = catalog.Message('',
                                        "POT-Creation-Date: 2009-03-09 15:47-0700\n" +
                                        "PO-Revision-Date: 2009-03-09 15:47-0700\n")
        for key, value in localized.mime_headers:
            if key in ('POT-Creation-Date', 'PO-Revision-Date'):
                self.assertEqual(value, '2009-03-09 15:47-0700')

    def test_mime_headers_contain_same_information_as_attributes(self):
        cat = catalog.Catalog()
        cat[''] = catalog.Message('',
                                  "Last-Translator: Foo Bar <foo.bar@example.com>\n" +
                                  "Language-Team: de <de@example.com>\n" +
                                  "POT-Creation-Date: 2009-03-01 11:20+0200\n" +
                                  "PO-Revision-Date: 2009-03-09 15:47-0700\n")
        self.assertEqual(None, cat.locale)
        mime_headers = dict(cat.mime_headers)

        self.assertEqual('Foo Bar <foo.bar@example.com>', cat.last_translator)
        self.assertEqual('Foo Bar <foo.bar@example.com>',
                         mime_headers['Last-Translator'])

        self.assertEqual('de <de@example.com>', cat.language_team)
        self.assertEqual('de <de@example.com>', mime_headers['Language-Team'])

        dt = datetime.datetime(2009, 3, 9, 15, 47, tzinfo=FixedOffsetTimezone(-7 * 60))
        self.assertEqual(dt, cat.revision_date)
        formatted_dt = format_datetime(dt, 'yyyy-MM-dd HH:mmZ', locale='en')
        self.assertEqual(formatted_dt, mime_headers['PO-Revision-Date'])


def test_message_fuzzy():
    assert not catalog.Message('foo').fuzzy
    msg = catalog.Message('foo', 'foo', flags=['fuzzy'])
    assert msg.fuzzy
    assert msg.id == 'foo'


def test_message_pluralizable():
    assert not catalog.Message('foo').pluralizable
    assert catalog.Message(('foo', 'bar')).pluralizable


def test_message_python_format():
    assert catalog.Message('foo %(name)s bar').python_format
    assert catalog.Message(('foo %(name)s', 'foo %(name)s')).python_format


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
        ('Generated-By', 'Babel %s\n' % catalog.VERSION),
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
        ('Plural-Forms', 'nplurals=2; plural=(n != 1)'),
        ('MIME-Version', '1.0'),
        ('Content-Type', 'text/plain; charset=utf-8'),
        ('Content-Transfer-Encoding', '8bit'),
        ('Generated-By', 'Babel %s\n' % catalog.VERSION),
    ]


def test_catalog_num_plurals():
    assert catalog.Catalog(locale='en').num_plurals == 2
    assert catalog.Catalog(locale='ga').num_plurals == 5


def test_catalog_plural_expr():
    assert catalog.Catalog(locale='en').plural_expr == '(n != 1)'
    assert (catalog.Catalog(locale='ga').plural_expr
            == '(n==1 ? 0 : n==2 ? 1 : n>=3 && n<=6 ? 2 : n>=7 && n<=10 ? 3 : 4)')


def test_catalog_plural_forms():
    assert (catalog.Catalog(locale='en').plural_forms
            == 'nplurals=2; plural=(n != 1)')
    assert (catalog.Catalog(locale='pt_BR').plural_forms
            == 'nplurals=2; plural=(n > 1)')


def test_catalog_setitem():
    cat = catalog.Catalog()
    cat[u'foo'] = catalog.Message(u'foo')
    assert cat[u'foo'].id == 'foo'

    cat = catalog.Catalog()
    cat[u'foo'] = catalog.Message(u'foo', locations=[('main.py', 1)])
    assert cat[u'foo'].locations == [('main.py', 1)]
    cat[u'foo'] = catalog.Message(u'foo', locations=[('utils.py', 5)])
    assert cat[u'foo'].locations == [('main.py', 1), ('utils.py', 5)]


def test_catalog_add():
    cat = catalog.Catalog()
    foo = cat.add(u'foo')
    assert foo.id == 'foo'
    assert cat[u'foo'] is foo


def test_catalog_update():
    template = catalog.Catalog(header_comment="# A Custom Header")
    template.add('green', locations=[('main.py', 99)])
    template.add('blue', locations=[('main.py', 100)])
    template.add(('salad', 'salads'), locations=[('util.py', 42)])
    cat = catalog.Catalog(locale='de_DE')
    cat.add('blue', u'blau', locations=[('main.py', 98)])
    cat.add('head', u'Kopf', locations=[('util.py', 33)])
    cat.add(('salad', 'salads'), (u'Salat', u'Salate'),
            locations=[('util.py', 38)])

    cat.update(template)
    assert len(cat) == 3

    msg1 = cat['green']
    msg1.string
    assert msg1.locations == [('main.py', 99)]

    msg2 = cat['blue']
    assert msg2.string == u'blau'
    assert msg2.locations == [('main.py', 100)]

    msg3 = cat['salad']
    assert msg3.string == (u'Salat', u'Salate')
    assert msg3.locations == [('util.py', 42)]

    assert 'head' not in cat
    assert list(cat.obsolete.values())[0].id == 'head'

    cat.update(template, update_header_comment=True)
    assert cat.header_comment == template.header_comment  # Header comment also gets updated


def test_datetime_parsing():
    val1 = catalog._parse_datetime_header('2006-06-28 23:24+0200')
    assert val1.year == 2006
    assert val1.month == 6
    assert val1.day == 28
    assert val1.tzinfo.zone == 'Etc/GMT+120'

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
