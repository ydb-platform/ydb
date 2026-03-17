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
import pytest

from babel import Locale
from babel.messages import plurals


@pytest.mark.parametrize(('locale', 'num_plurals', 'plural_expr'), [
    (Locale('en'), 2, '(n != 1)'),
    (Locale('en', 'US'), 2, '(n != 1)'),
    (Locale('zh'), 1, '0'),
    (Locale('zh', script='Hans'), 1, '0'),
    (Locale('zh', script='Hant'), 1, '0'),
    (Locale('zh', 'CN', 'Hans'), 1, '0'),
    (Locale('zh', 'TW', 'Hant'), 1, '0'),
])
def test_get_plural_selection(locale, num_plurals, plural_expr):
    assert plurals.get_plural(locale) == (num_plurals, plural_expr)


def test_get_plural_accpets_strings():
    assert plurals.get_plural(locale='ga') == (5, '(n==1 ? 0 : n==2 ? 1 : n>=3 && n<=6 ? 2 : n>=7 && n<=10 ? 3 : 4)')


def test_get_plural_falls_back_to_default():
    assert plurals.get_plural('ii') == (2, '(n != 1)')


def test_get_plural():
    # See https://localization-guide.readthedocs.io/en/latest/l10n/pluralforms.html for more details.
    assert plurals.get_plural(locale='en') == (2, '(n != 1)')
    assert plurals.get_plural(locale='ga') == (5, '(n==1 ? 0 : n==2 ? 1 : n>=3 && n<=6 ? 2 : n>=7 && n<=10 ? 3 : 4)')

    plural_ja = plurals.get_plural("ja")
    assert str(plural_ja) == 'nplurals=1; plural=0;'
    assert plural_ja.num_plurals == 1
    assert plural_ja.plural_expr == '0'
    assert plural_ja.plural_forms == 'nplurals=1; plural=0;'

    plural_en_US = plurals.get_plural('en_US')
    assert str(plural_en_US) == 'nplurals=2; plural=(n != 1);'
    assert plural_en_US.num_plurals == 2
    assert plural_en_US.plural_expr == '(n != 1)'

    plural_fr_FR = plurals.get_plural('fr_FR')
    assert str(plural_fr_FR) == 'nplurals=2; plural=(n > 1);'
    assert plural_fr_FR.num_plurals == 2
    assert plural_fr_FR.plural_expr == '(n > 1)'

    plural_pl_PL = plurals.get_plural('pl_PL')
    assert str(plural_pl_PL) == 'nplurals=3; plural=(n==1 ? 0 : n%10>=2 && n%10<=4 && (n%100<10 || n%100>=20) ? 1 : 2);'
    assert plural_pl_PL.num_plurals == 3
    assert plural_pl_PL.plural_expr == '(n==1 ? 0 : n%10>=2 && n%10<=4 && (n%100<10 || n%100>=20) ? 1 : 2)'
