# -*- coding: utf-8 -*-
from __future__ import absolute_import

import pytest
import pycrfsuite


def test_basic():
    seq = pycrfsuite.ItemSequence([])
    assert len(seq) == 0
    assert seq.items() == []


def test_lists():
    seq = pycrfsuite.ItemSequence([['foo', 'bar'], ['bar', 'baz']])
    assert len(seq) == 2
    assert seq.items() == [{'foo': 1.0, 'bar': 1.0}, {'bar': 1.0, 'baz': 1.0}]
    assert pycrfsuite.ItemSequence(seq.items()).items() == seq.items()


def test_dicts():
    seq = pycrfsuite.ItemSequence([
        {'foo': True, 'bar': {'foo': -1, 'baz': False}},
    ])
    assert len(seq) == 1
    assert seq.items() == [{'foo': 1.0, 'bar:foo': -1, 'bar:baz': 0.0}]


def test_unicode():
    seq = pycrfsuite.ItemSequence([
        {'foo': u'привет', u'ключ': 1.0, u'привет': u'мир'},
    ])
    assert seq.items() == [{u'foo:привет': 1.0, u'ключ': 1.0, u'привет:мир': 1.0}]


@pytest.mark.xfail()
def test_bad():
    with pytest.raises(ValueError):
        seq = pycrfsuite.ItemSequence('foo')
        print(seq.items())

    with pytest.raises(ValueError):
        seq = pycrfsuite.ItemSequence([[{'foo': 'bar'}]])
        print(seq.items())


def test_nested():
    seq = pycrfsuite.ItemSequence([
        {
            "foo": {
                "bar": "baz",
                "spam": 0.5,
                "egg": ["x", "y"],
                "ham": {"x": -0.5, "y": -0.1}
            },
        },
        {
            "foo": {
                "bar": "ham",
                "spam": -0.5,
                "ham": set(["x", "y"])
            },
        },
    ])
    assert len(seq) == 2
    assert seq.items() == [
        {
            'foo:bar:baz': 1.0,
            'foo:spam': 0.5,
            'foo:egg:x': 1.0,
            'foo:egg:y': 1.0,
            'foo:ham:x': -0.5,
            'foo:ham:y': -0.1,
        },
        {
            'foo:bar:ham': 1.0,
            'foo:spam': -0.5,
            'foo:ham:x': 1.0,
            'foo:ham:y': 1.0,
        }
    ]
    assert pycrfsuite.ItemSequence(seq.items()).items() == seq.items()
