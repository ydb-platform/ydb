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

import pytest

from babel import plural

N_NODE = plural.ident_node('n')


def make_range_list(*values):
    ranges = []
    for v in values:
        if isinstance(v, int):
            val_node = plural.value_node(v)
            ranges.append((val_node, val_node))
        else:
            assert isinstance(v, tuple)
            ranges.append((plural.value_node(v[0]), plural.value_node(v[1])))
    return plural.range_list_node(ranges)


def n_eq(v):
    return 'relation', ('in', N_NODE, make_range_list(v))


def test_error_when_unexpected_end():
    with pytest.raises(plural.RuleError):
        plural._Parser('n =')


def test_eq_relation():
    assert plural._Parser('n = 1').ast == n_eq(1)


def test_in_range_relation():
    assert plural._Parser('n = 2..4').ast == (
        'relation',
        ('in', N_NODE, make_range_list((2, 4))),
    )


def test_negate():
    assert plural._Parser('n != 1').ast == plural.negate(n_eq(1))


def test_or():
    assert plural._Parser('n = 1 or n = 2').ast == ('or', (n_eq(1), n_eq(2)))


def test_and():
    assert plural._Parser('n = 1 and n = 2').ast == ('and', (n_eq(1), n_eq(2)))


def test_or_and():
    assert plural._Parser('n = 0 or n != 1 and n % 100 = 1..19').ast == (
        'or',
        (
            n_eq(0),
            (
                'and',
                (
                    plural.negate(n_eq(1)),
                    (
                        'relation',
                        (
                            'in',
                            ('mod', (N_NODE, plural.value_node(100))),
                            (make_range_list((1, 19))),
                        ),
                    ),
                ),
            ),
        ),
    )
