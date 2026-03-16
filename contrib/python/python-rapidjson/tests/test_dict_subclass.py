# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Test dict subclasses
# :Created:   dom 17 mar 2019 12:38:24 CET
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   MIT License
# :Copyright: © 2019, 2021 Lele Gaifax
#

from collections import OrderedDict
from rapidjson import Decoder


class OrderedDecoder(Decoder):
    def start_object(self):
        return OrderedDict()


def test_ordered_dict():
    od = OrderedDecoder()
    result = od('{"foo": "bar", "bar": "foo"}')
    assert isinstance(result, OrderedDict)
    assert list(result.keys()) == ["foo", "bar"]


class ObjectsAsKeyValuePairsDecoder(Decoder):
    def start_object(self):
        return []


def test_objects_as_key_value_pairs():
    kvp = ObjectsAsKeyValuePairsDecoder()
    result = kvp('{"a": 1, "b": {"b1": 1, "b2": 2}}')
    assert result == [('a', 1), ('b', [('b1', 1), ('b2', 2)])]


class JoinDuplicatedKeysDecoder(ObjectsAsKeyValuePairsDecoder):
    def end_object(self, ordered_pairs):
        # Adapted from https://stackoverflow.com/a/38307621
        d = {}
        for k, v in ordered_pairs:
            if k in d:
                if type(d[k]) == list:
                    d[k].append(v)
                else:
                    newlist = []
                    newlist.append(d[k])
                    newlist.append(v)
                    d[k] = newlist
            else:
                d[k] = v
        return d


def test_join_duplicated_keys():
    jdk = JoinDuplicatedKeysDecoder()
    result = jdk('{"a": 1, "b": {"b1": 1, "b2": 2}, "b": {"b1": 3, "b2": 2,"b4": 8}}')
    assert result == {'a': 1, 'b': [{'b1': 1, 'b2': 2}, {'b1': 3, 'b2': 2, 'b4': 8}]}
