# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import itertools
from functools import partial

import pytest
import six

from cyson import (
    YsonEntity, InputStream,
    list_fragments, key_switched_list_fragments,
    Reader, UnicodeReader
)


def filter_control_records(list):
    return [
        _ for _ in list
        if not isinstance(_[2], YsonEntity)
    ]


def canonize(val, as_unicode):
    _canonize = partial(canonize, as_unicode=as_unicode)

    if isinstance(val, six.binary_type) and as_unicode:
        return val.decode('utf8')
    elif isinstance(val, six.text_type) and not as_unicode:
        return val.encode('utf8')
    elif isinstance(val, (list, tuple)):
        return [_canonize(elem) for elem in val]
    elif isinstance(val, dict):
        return {_canonize(k): _canonize(v) for k, v in val.items()}
    return val


@pytest.mark.parametrize(
    'reader, as_unicode', [
        [Reader, False],
        [UnicodeReader, True],
    ],
)
@pytest.mark.parametrize(
    'keep_control_records', [True, False]
)
def test_row_index(keep_control_records, reader, as_unicode):
    _ = partial(canonize, as_unicode=as_unicode)

    data = b"""
        <row_index=0>#;
        {a=1;b=2};
        {a=2;b=3};
        {a=3;b=4};
        <row_index=10000>#;
        {a=-1;b=-1};
        {a=-2;b=-2};
    """

    iter = list_fragments(
        stream=InputStream.from_string(data),
        Reader=reader,
        process_attributes=True,
        keep_control_records=keep_control_records,
    )
    records = [(iter.range_index, iter.row_index, __) for __ in iter]

    etalon = [
        (None, -1, YsonEntity(attributes={b'row_index': 0})),
        (None, 0, _({b'a': 1, b'b': 2})),
        (None, 1, _({b'a': 2, b'b': 3})),
        (None, 2, _({b'a': 3, b'b': 4})),
        (None, 9999, YsonEntity(attributes={b'row_index': 10000})),
        (None, 10000, _({b'a': -1, b'b': -1})),
        (None, 10001, _({b'a': -2, b'b': -2})),
    ]

    if not keep_control_records:
        etalon = filter_control_records(etalon)

    assert records == etalon


@pytest.mark.parametrize(
    'reader, as_unicode', [
        [Reader, False],
        [UnicodeReader, True],
    ]
)
@pytest.mark.parametrize(
    'keep_control_records', [True, False],
)
@pytest.mark.parametrize(
    'parameter_name',
    ['process_attributes', 'process_table_index']
)
def test_range_index(parameter_name, keep_control_records, reader, as_unicode):
    _ = partial(canonize, as_unicode=as_unicode)

    data = b"""
        <range_index=2; row_index=0>#;
        {a=1;b=2};
        {a=2;b=3};
        {a=3;b=4};
        <range_index=0; row_index=10000>#;
        {a=-1;b=-1};
        {a=-2;b=-2};
    """

    iter = list_fragments(
        stream=InputStream.from_string(data),
        Reader=reader,
        **{parameter_name: True, 'keep_control_records': keep_control_records}
    )
    records = [(iter.range_index, iter.row_index, __) for __ in iter]

    etalon = [
        (2, -1, YsonEntity(attributes={b'range_index': 2, b'row_index': 0})),
        (2, 0, _({b'a': 1, b'b': 2})),
        (2, 1, _({b'a': 2, b'b': 3})),
        (2, 2, _({b'a': 3, b'b': 4})),
        (0, 9999, YsonEntity(attributes={b'range_index': 0, b'row_index': 10000})),
        (0, 10000, _({b'a': -1, b'b': -1})),
        (0, 10001, _({b'a': -2, b'b': -2})),
    ]

    if not keep_control_records:
        etalon = filter_control_records(etalon)

    assert records == etalon


@pytest.mark.parametrize(
    'reader, as_unicode', [
        [Reader, False],
        [UnicodeReader, True],
    ]
)
def test_key_switch_first(reader, as_unicode):
    _ = partial(canonize, as_unicode=as_unicode)

    data = b"""
        <key_switch=True>#;
        {k=1;a=1;b=2};
        {k=1;a=2;b=3};
        {k=1;a=3;b=4};
        <key_switch=True>#;
        {k=2;a=-1;b=-1};
        {k=2;a=-2;b=-2};
    """

    iter = key_switched_list_fragments(
        stream=InputStream.from_string(data),
        Reader=reader,
    )
    records = [list(__) for __ in iter]

    assert records == [
        [
            _({b'k': 1, b'a': 1, b'b': 2}),
            _({b'k': 1, b'a': 2, b'b': 3}),
            _({b'k': 1, b'a': 3, b'b': 4}),
        ],
        [
            _({b'k': 2, b'a': -1, b'b': -1}),
            _({b'k': 2, b'a': -2, b'b': -2}),
        ]
    ]


@pytest.mark.parametrize(
    'reader, as_unicode', [
        [Reader, False],
        [UnicodeReader, True],
    ]
)
def test_key_switch_nofirst(reader, as_unicode):
    _ = partial(canonize, as_unicode=as_unicode)

    data = b"""
        {k=1;a=1;b=2};
        {k=1;a=2;b=3};
        {k=1;a=3;b=4};
        <key_switch=True>#;
        {k=2;a=-1;b=-1};
        {k=2;a=-2;b=-2};
    """

    iter = key_switched_list_fragments(
        stream=InputStream.from_string(data),
        Reader=reader
    )
    records = [list(__) for __ in iter]

    assert records == [
        [
            _({b'k': 1, b'a': 1, b'b': 2}),
            _({b'k': 1, b'a': 2, b'b': 3}),
            _({b'k': 1, b'a': 3, b'b': 4}),
        ],
        [
            _({b'k': 2, b'a': -1, b'b': -1}),
            _({b'k': 2, b'a': -2, b'b': -2}),
        ]
    ]


@pytest.mark.parametrize(
    'reader, as_unicode', [
        [Reader, False],
        [UnicodeReader, True],
    ]
)
def test_key_switch_exhaust_unused_records(reader, as_unicode):
    _ = partial(canonize, as_unicode=as_unicode)

    data = b"""
        {k=1;a=1;b=2};
        {k=1;a=2;b=3};
        {k=1;a=3;b=4};
        <key_switch=True>#;
        {k=2;a=-1;b=-1};
        {k=2;a=-2;b=-2};
    """

    iter = key_switched_list_fragments(
        stream=InputStream.from_string(data),
        Reader=reader,
    )

    records = []

    for group in iter:
        records.append(
            list(itertools.islice(group, 2))
        )

    assert records == [
        [
            _({b'k': 1, b'a': 1, b'b': 2}),
            _({b'k': 1, b'a': 2, b'b': 3}),
        ],
        [
            _({b'k': 2, b'a': -1, b'b': -1}),
            _({b'k': 2, b'a': -2, b'b': -2}),
        ]
    ]


@pytest.mark.parametrize('reader', [Reader, UnicodeReader])
def test_key_switch_empty(reader):
    assert list(
        key_switched_list_fragments(
            stream=InputStream.from_string(""),
            Reader=reader,
        )
    ) == []
