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

import os
import unittest

from babel.messages import mofile, Catalog
from babel._compat import BytesIO, text_type
from babel.support import Translations


class ReadMoTestCase(unittest.TestCase):

    def setUp(self):
        self.datadir = os.path.join(os.path.dirname(__file__), 'data')

    def _test_basics(self):
        mo_path = os.path.join(self.datadir, 'project', 'i18n', 'de',
                               'LC_MESSAGES', 'messages.mo')
        with open(mo_path, 'rb') as mo_file:
            catalog = mofile.read_mo(mo_file)
            self.assertEqual(2, len(catalog))
            self.assertEqual('TestProject', catalog.project)
            self.assertEqual('0.1', catalog.version)
            self.assertEqual('Stange', catalog['bar'].string)
            self.assertEqual(['Fuhstange', 'Fuhstangen'],
                             catalog['foobar'].string)


class WriteMoTestCase(unittest.TestCase):

    def test_sorting(self):
        # Ensure the header is sorted to the first entry so that its charset
        # can be applied to all subsequent messages by GNUTranslations
        # (ensuring all messages are safely converted to unicode)
        catalog = Catalog(locale='en_US')
        catalog.add(u'', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
        catalog.add(u'foo', 'Voh')
        catalog.add((u'There is', u'There are'), (u'Es gibt', u'Es gibt'))
        catalog.add(u'Fizz', '')
        catalog.add(('Fuzz', 'Fuzzes'), ('', ''))
        buf = BytesIO()
        mofile.write_mo(buf, catalog)
        buf.seek(0)
        translations = Translations(fp=buf)
        self.assertEqual(u'Voh', translations.ugettext('foo'))
        assert isinstance(translations.ugettext('foo'), text_type)
        self.assertEqual(u'Es gibt', translations.ungettext('There is', 'There are', 1))
        assert isinstance(translations.ungettext('There is', 'There are', 1), text_type)
        self.assertEqual(u'Fizz', translations.ugettext('Fizz'))
        assert isinstance(translations.ugettext('Fizz'), text_type)
        self.assertEqual(u'Fuzz', translations.ugettext('Fuzz'))
        assert isinstance(translations.ugettext('Fuzz'), text_type)
        self.assertEqual(u'Fuzzes', translations.ugettext('Fuzzes'))
        assert isinstance(translations.ugettext('Fuzzes'), text_type)

    def test_more_plural_forms(self):
        catalog2 = Catalog(locale='ru_RU')
        catalog2.add(('Fuzz', 'Fuzzes'), ('', '', ''))
        buf = BytesIO()
        mofile.write_mo(buf, catalog2)

    def test_empty_translation_with_fallback(self):
        catalog1 = Catalog(locale='fr_FR')
        catalog1.add(u'', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
        catalog1.add(u'Fuzz', '')
        buf1 = BytesIO()
        mofile.write_mo(buf1, catalog1)
        buf1.seek(0)
        catalog2 = Catalog(locale='fr')
        catalog2.add(u'', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
        catalog2.add(u'Fuzz', 'Flou')
        buf2 = BytesIO()
        mofile.write_mo(buf2, catalog2)
        buf2.seek(0)

        translations = Translations(fp=buf1)
        translations.add_fallback(Translations(fp=buf2))

        self.assertEqual(u'Flou', translations.ugettext('Fuzz'))
