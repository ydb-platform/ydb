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
import unittest
import pytest

from babel import plural, localedata
from babel._compat import decimal

EPSILON = decimal.Decimal("0.0001")


def test_plural_rule():
    rule = plural.PluralRule({'one': 'n is 1'})
    assert rule(1) == 'one'
    assert rule(2) == 'other'

    rule = plural.PluralRule({'one': 'n is 1'})
    assert rule.rules == {'one': 'n is 1'}


def test_plural_rule_operands_i():
    rule = plural.PluralRule({'one': 'i is 1'})
    assert rule(1.2) == 'one'
    assert rule(2) == 'other'


def test_plural_rule_operands_v():
    rule = plural.PluralRule({'one': 'v is 2'})
    assert rule(decimal.Decimal('1.20')) == 'one'
    assert rule(decimal.Decimal('1.2')) == 'other'
    assert rule(2) == 'other'


def test_plural_rule_operands_w():
    rule = plural.PluralRule({'one': 'w is 2'})
    assert rule(decimal.Decimal('1.23')) == 'one'
    assert rule(decimal.Decimal('1.20')) == 'other'
    assert rule(1.2) == 'other'


def test_plural_rule_operands_f():
    rule = plural.PluralRule({'one': 'f is 20'})
    assert rule(decimal.Decimal('1.23')) == 'other'
    assert rule(decimal.Decimal('1.20')) == 'one'
    assert rule(1.2) == 'other'


def test_plural_rule_operands_t():
    rule = plural.PluralRule({'one': 't = 5'})
    assert rule(decimal.Decimal('1.53')) == 'other'
    assert rule(decimal.Decimal('1.50')) == 'one'
    assert rule(1.5) == 'one'


def test_plural_other_is_ignored():
    rule = plural.PluralRule({'one': 'n is 1', 'other': '@integer 2'})
    assert rule(1) == 'one'


def test_to_javascript():
    assert (plural.to_javascript({'one': 'n is 1'})
            == "(function(n) { return (n == 1) ? 'one' : 'other'; })")


def test_to_python():
    func = plural.to_python({'one': 'n is 1', 'few': 'n in 2..4'})
    assert func(1) == 'one'
    assert func(3) == 'few'

    func = plural.to_python({'one': 'n in 1,11', 'few': 'n in 3..10,13..19'})
    assert func(11) == 'one'
    assert func(15) == 'few'


def test_to_gettext():
    assert (plural.to_gettext({'one': 'n is 1', 'two': 'n is 2'})
            == 'nplurals=3; plural=((n == 1) ? 0 : (n == 2) ? 1 : 2)')


def test_in_range_list():
    assert plural.in_range_list(1, [(1, 3)])
    assert plural.in_range_list(3, [(1, 3)])
    assert plural.in_range_list(3, [(1, 3), (5, 8)])
    assert not plural.in_range_list(1.2, [(1, 4)])
    assert not plural.in_range_list(10, [(1, 4)])
    assert not plural.in_range_list(10, [(1, 4), (6, 8)])


def test_within_range_list():
    assert plural.within_range_list(1, [(1, 3)])
    assert plural.within_range_list(1.0, [(1, 3)])
    assert plural.within_range_list(1.2, [(1, 4)])
    assert plural.within_range_list(8.8, [(1, 4), (7, 15)])
    assert not plural.within_range_list(10, [(1, 4)])
    assert not plural.within_range_list(10.5, [(1, 4), (20, 30)])


def test_cldr_modulo():
    assert plural.cldr_modulo(-3, 5) == -3
    assert plural.cldr_modulo(-3, -5) == -3
    assert plural.cldr_modulo(3, 5) == 3


def test_plural_within_rules():
    p = plural.PluralRule({'one': 'n is 1', 'few': 'n within 2,4,7..9'})
    assert repr(p) == "<PluralRule 'one: n is 1, few: n within 2,4,7..9'>"
    assert plural.to_javascript(p) == (
        "(function(n) { "
        "return ((n == 2) || (n == 4) || (n >= 7 && n <= 9))"
        " ? 'few' : (n == 1) ? 'one' : 'other'; })")
    assert plural.to_gettext(p) == (
        'nplurals=3; plural=(((n == 2) || (n == 4) || (n >= 7 && n <= 9))'
        ' ? 1 : (n == 1) ? 0 : 2)')
    assert p(0) == 'other'
    assert p(1) == 'one'
    assert p(2) == 'few'
    assert p(3) == 'other'
    assert p(4) == 'few'
    assert p(5) == 'other'
    assert p(6) == 'other'
    assert p(7) == 'few'
    assert p(8) == 'few'
    assert p(9) == 'few'


def test_locales_with_no_plural_rules_have_default():
    from babel import Locale
    pf = Locale.parse('ii').plural_form
    assert pf(1) == 'other'
    assert pf(2) == 'other'
    assert pf(15) == 'other'


WELL_FORMED_TOKEN_TESTS = (
    ('', []),
    ('n = 1', [('value', '1'), ('symbol', '='), ('word', 'n'), ]),
    ('n = 1 @integer 1', [('value', '1'), ('symbol', '='), ('word', 'n'), ]),
    ('n is 1', [('value', '1'), ('word', 'is'), ('word', 'n'), ]),
    ('n % 100 = 3..10', [('value', '10'), ('ellipsis', '..'), ('value', '3'),
                         ('symbol', '='), ('value', '100'), ('symbol', '%'),
                         ('word', 'n'), ]),
)


@pytest.mark.parametrize('rule_text,tokens', WELL_FORMED_TOKEN_TESTS)
def test_tokenize_well_formed(rule_text, tokens):
    assert plural.tokenize_rule(rule_text) == tokens


MALFORMED_TOKEN_TESTS = (
    'a = 1', 'n ! 2',
)


@pytest.mark.parametrize('rule_text', MALFORMED_TOKEN_TESTS)
def test_tokenize_malformed(rule_text):
    with pytest.raises(plural.RuleError):
        plural.tokenize_rule(rule_text)


class TestNextTokenTestCase(unittest.TestCase):

    def test_empty(self):
        assert not plural.test_next_token([], '')

    def test_type_ok_and_no_value(self):
        assert plural.test_next_token([('word', 'and')], 'word')

    def test_type_ok_and_not_value(self):
        assert not plural.test_next_token([('word', 'and')], 'word', 'or')

    def test_type_ok_and_value_ok(self):
        assert plural.test_next_token([('word', 'and')], 'word', 'and')

    def test_type_not_ok_and_value_ok(self):
        assert not plural.test_next_token([('abc', 'and')], 'word', 'and')


def make_range_list(*values):
    ranges = []
    for v in values:
        if isinstance(v, int):
            val_node = plural.value_node(v)
            ranges.append((val_node, val_node))
        else:
            assert isinstance(v, tuple)
            ranges.append((plural.value_node(v[0]),
                           plural.value_node(v[1])))
    return plural.range_list_node(ranges)


class PluralRuleParserTestCase(unittest.TestCase):

    def setUp(self):
        self.n = plural.ident_node('n')

    def n_eq(self, v):
        return 'relation', ('in', self.n, make_range_list(v))

    def test_error_when_unexpected_end(self):
        with pytest.raises(plural.RuleError):
            plural._Parser('n =')

    def test_eq_relation(self):
        assert plural._Parser('n = 1').ast == self.n_eq(1)

    def test_in_range_relation(self):
        assert plural._Parser('n = 2..4').ast == \
            ('relation', ('in', self.n, make_range_list((2, 4))))

    def test_negate(self):
        assert plural._Parser('n != 1').ast == plural.negate(self.n_eq(1))

    def test_or(self):
        assert plural._Parser('n = 1 or n = 2').ast ==\
            ('or', (self.n_eq(1), self.n_eq(2)))

    def test_and(self):
        assert plural._Parser('n = 1 and n = 2').ast ==\
            ('and', (self.n_eq(1), self.n_eq(2)))

    def test_or_and(self):
        assert plural._Parser('n = 0 or n != 1 and n % 100 = 1..19').ast == \
            ('or', (self.n_eq(0),
                    ('and', (plural.negate(self.n_eq(1)),
                             ('relation', ('in',
                                           ('mod', (self.n,
                                                    plural.value_node(100))),
                                           (make_range_list((1, 19)))))))
                    ))


EXTRACT_OPERANDS_TESTS = (
    (1, 1, 1, 0, 0, 0, 0),
    (decimal.Decimal('1.0'), '1.0', 1, 1, 0, 0, 0),
    (decimal.Decimal('1.00'), '1.00', 1, 2, 0, 0, 0),
    (decimal.Decimal('1.3'), '1.3', 1, 1, 1, 3, 3),
    (decimal.Decimal('1.30'), '1.30', 1, 2, 1, 30, 3),
    (decimal.Decimal('1.03'), '1.03', 1, 2, 2, 3, 3),
    (decimal.Decimal('1.230'), '1.230', 1, 3, 2, 230, 23),
    (-1, 1, 1, 0, 0, 0, 0),
    (1.3, '1.3', 1, 1, 1, 3, 3),
)


@pytest.mark.parametrize('source,n,i,v,w,f,t', EXTRACT_OPERANDS_TESTS)
def test_extract_operands(source, n, i, v, w, f, t):
    e_n, e_i, e_v, e_w, e_f, e_t = plural.extract_operands(source)
    assert abs(e_n - decimal.Decimal(n)) <= EPSILON  # float-decimal conversion inaccuracy
    assert e_i == i
    assert e_v == v
    assert e_w == w
    assert e_f == f
    assert e_t == t


@pytest.mark.parametrize('locale', ('ru', 'pl'))
def test_gettext_compilation(locale):
    # Test that new plural form elements introduced in recent CLDR versions
    # are compiled "down" to `n` when emitting Gettext rules.
    ru_rules = localedata.load(locale)['plural_form'].rules
    chars = 'ivwft'
    # Test that these rules are valid for this test; i.e. that they contain at least one
    # of the gettext-unsupported characters.
    assert any((" " + ch + " ") in rule for ch in chars for rule in ru_rules.values())
    # Then test that the generated value indeed does not contain these.
    ru_rules_gettext = plural.to_gettext(ru_rules)
    assert not any(ch in ru_rules_gettext for ch in chars)
