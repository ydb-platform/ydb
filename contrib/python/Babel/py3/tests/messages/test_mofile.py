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

import os
from io import BytesIO

from babel.messages import Catalog, mofile
from babel.support import Translations

import yatest.common as yc
data_dir = os.path.join(os.path.dirname(yc.source_path(__file__)), 'data')


def _test_basics():
    mo_path = os.path.join(data_dir, 'project', 'i18n', 'de',
                           'LC_MESSAGES', 'messages.mo')
    with open(mo_path, 'rb') as mo_file:
        catalog = mofile.read_mo(mo_file)
        assert len(catalog) == 2
        assert catalog.project == 'TestProject'
        assert catalog.version == '0.1'
        assert catalog['bar'].string == 'Stange'
        assert catalog['foobar'].string == ['Fuhstange', 'Fuhstangen']



def test_sorting():
    # Ensure the header is sorted to the first entry so that its charset
    # can be applied to all subsequent messages by GNUTranslations
    # (ensuring all messages are safely converted to unicode)
    catalog = Catalog(locale='en_US')
    catalog.add('', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
    catalog.add('foo', 'Voh')
    catalog.add(('There is', 'There are'), ('Es gibt', 'Es gibt'))
    catalog.add('Fizz', '')
    catalog.add(('Fuzz', 'Fuzzes'), ('', ''))
    buf = BytesIO()
    mofile.write_mo(buf, catalog)
    buf.seek(0)
    translations = Translations(fp=buf)
    assert translations.ugettext('foo') == 'Voh'
    assert translations.ungettext('There is', 'There are', 1) == 'Es gibt'
    assert translations.ugettext('Fizz') == 'Fizz'
    assert translations.ugettext('Fuzz') == 'Fuzz'
    assert translations.ugettext('Fuzzes') == 'Fuzzes'


def test_more_plural_forms():
    catalog2 = Catalog(locale='ru_RU')
    catalog2.add(('Fuzz', 'Fuzzes'), ('', '', ''))
    buf = BytesIO()
    mofile.write_mo(buf, catalog2)


def test_empty_translation_with_fallback():
    catalog1 = Catalog(locale='fr_FR')
    catalog1.add('', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
    catalog1.add('Fuzz', '')
    buf1 = BytesIO()
    mofile.write_mo(buf1, catalog1)
    buf1.seek(0)
    catalog2 = Catalog(locale='fr')
    catalog2.add('', '''\
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n''')
    catalog2.add('Fuzz', 'Flou')
    buf2 = BytesIO()
    mofile.write_mo(buf2, catalog2)
    buf2.seek(0)

    translations = Translations(fp=buf1)
    translations.add_fallback(Translations(fp=buf2))

    assert translations.ugettext('Fuzz') == 'Flou'
