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

import __future__
import unittest

import pytest

from babel import util
from babel._compat import BytesIO
from babel.util import parse_future_flags


class _FF:
    division         = __future__.division.compiler_flag
    print_function   = __future__.print_function.compiler_flag
    with_statement   = __future__.with_statement.compiler_flag
    unicode_literals = __future__.unicode_literals.compiler_flag

def test_distinct():
    assert list(util.distinct([1, 2, 1, 3, 4, 4])) == [1, 2, 3, 4]
    assert list(util.distinct('foobar')) == ['f', 'o', 'b', 'a', 'r']


def test_pathmatch():
    assert util.pathmatch('**.py', 'bar.py')
    assert util.pathmatch('**.py', 'foo/bar/baz.py')
    assert not util.pathmatch('**.py', 'templates/index.html')
    assert util.pathmatch('**/templates/*.html', 'templates/index.html')
    assert not util.pathmatch('**/templates/*.html', 'templates/foo/bar.html')
    assert util.pathmatch('^foo/**.py', 'foo/bar/baz/blah.py')
    assert not util.pathmatch('^foo/**.py', 'blah/foo/bar/baz.py')
    assert util.pathmatch('./foo/**.py', 'foo/bar/baz/blah.py')
    assert util.pathmatch('./blah.py', 'blah.py')
    assert not util.pathmatch('./foo/**.py', 'blah/foo/bar/baz.py')


class FixedOffsetTimezoneTestCase(unittest.TestCase):

    def test_zone_negative_offset(self):
        self.assertEqual('Etc/GMT-60', util.FixedOffsetTimezone(-60).zone)

    def test_zone_zero_offset(self):
        self.assertEqual('Etc/GMT+0', util.FixedOffsetTimezone(0).zone)

    def test_zone_positive_offset(self):
        self.assertEqual('Etc/GMT+330', util.FixedOffsetTimezone(330).zone)


parse_encoding = lambda s: util.parse_encoding(BytesIO(s.encode('utf-8')))


def test_parse_encoding_defined():
    assert parse_encoding(u'# coding: utf-8') == 'utf-8'


def test_parse_encoding_undefined():
    assert parse_encoding(u'') is None


def test_parse_encoding_non_ascii():
    assert parse_encoding(u'K\xf6ln') is None


@pytest.mark.parametrize('source, result', [
    ('''
from __future__ import print_function,
    division, with_statement,
    unicode_literals
''', _FF.print_function | _FF.division | _FF.with_statement | _FF.unicode_literals),
    ('''
from __future__ import print_function, division
print('hello')
''', _FF.print_function | _FF.division),
    ('''
from __future__ import print_function, division, unknown,,,,,
print 'hello'
''', _FF.print_function | _FF.division),
    ('''
from __future__ import (
    print_function,
    division)
''', _FF.print_function | _FF.division),
    ('''
from __future__ import \\
    print_function, \\
    division
''', _FF.print_function | _FF.division),
])
def test_parse_future(source, result):
    fp = BytesIO(source.encode('latin-1'))
    flags = parse_future_flags(fp)
    assert flags == result
