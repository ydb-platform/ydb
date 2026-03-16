# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Tests on RawJSON
# :Author:    Silvio Tomatis <silviot@gmail.com>
# :License:   MIT License
# :Copyright: © 2018 Silvio Tomatis
# :Copyright: © 2018, 2020 Lele Gaifax
#

import pytest

import rapidjson as rj


def test_instantiation_positional():
    value = 'a string'
    raw = rj.RawJSON(value)
    assert raw.value == value


def test_instantiation_named():
    value = 'a string'
    raw = rj.RawJSON(value=value)
    assert raw.value == value


def test_only_bytes_allowed():
    with pytest.raises(TypeError):
        rj.RawJSON(b'A bytes string')
    with pytest.raises(TypeError):
        rj.RawJSON({})
    with pytest.raises(TypeError):
        rj.RawJSON(None)


def test_mix_preserialized():
    assert rj.dumps({'a': rj.RawJSON('{1 : 2}')}) == '{"a":{1 : 2}}'
