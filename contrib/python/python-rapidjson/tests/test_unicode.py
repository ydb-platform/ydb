# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Unicode tests
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2016, 2017, 2018, 2020 Lele Gaifax
#

import json

import pytest

import rapidjson


@pytest.mark.parametrize('u', [
    '\N{GREEK SMALL LETTER ALPHA}\N{GREEK CAPITAL LETTER OMEGA}',
    '\U0010ffff',
    'asdf \U0010ffff \U0001ffff qwert \uffff \u10ff \u00ff \u0080 \u7fff \b\n\r',
])
def test_unicode(u, dumps, loads):
    s = u.encode('utf-8')
    ju = dumps(u)
    js = dumps(s)
    assert ju == js
    assert ju.lower() == json.dumps(u).lower()
    assert dumps(u, ensure_ascii=False) == json.dumps(u, ensure_ascii=False)


# @pytest.mark.parametrize('o', [
#     "\ud80d",
#     {"foo": "\ud80d"},
#     {"\ud80d": "foo"},
# ])
# def test_dump_surrogate(o, dumps):
#     with pytest.raises(UnicodeEncodeError, match="surrogates not allowed"):
#         dumps(o)
#
#
# @pytest.mark.parametrize('j', [
#     '"\\ud80d"',
#     '{"foo": "\\ud80d"}',
#     '{"\\ud80d": "foo"}',
# ])
# def test_load_surrogate(j, loads):
#     with pytest.raises(ValueError, match="surrogate pair in string is invalid"):
#         loads(j)


@pytest.mark.parametrize('j', [
    '"\\udc00"',
    '"\\udfff"',
])
def test_unicode_decode_error(j, loads):
    with pytest.raises(rapidjson.JSONDecodeError,
                       match="The surrogate pair in string is invalid."):
        loads(j)


def test_non_utf8_bytes(dumps):
    value = b'\xff\xf0'
    with pytest.raises(UnicodeDecodeError, match="'utf-8' codec can't decode byte"):
        dumps(value)
