# coding: utf-8

from __future__ import print_function

import pytest  # NOQA

import json


def load(s, typ=float):
    import srsly.ruamel_yaml

    x = '{"low": %s }' % (s)
    print("input: [%s]" % (s), repr(x))
    # just to check it is loadable json
    res = json.loads(x)
    assert isinstance(res["low"], typ)
    ret_val = srsly.ruamel_yaml.load(x, srsly.ruamel_yaml.RoundTripLoader)
    print(ret_val)
    return ret_val["low"]


class TestJSONNumbers:
    # based on http://stackoverflow.com/a/30462009/1307905
    # yaml number regex: http://yaml.org/spec/1.2/spec.html#id2804092
    #
    # -? [1-9] ( \. [0-9]* [1-9] )? ( e [-+] [1-9] [0-9]* )?
    #
    # which is not a superset of the JSON numbers
    def test_json_number_float(self):
        for x in (
            y.split("#")[0].strip()
            for y in """
        1.0  # should fail on YAML spec on 1-9 allowed as single digit
        -1.0
        1e-06
        3.1e-5
        3.1e+5
        3.1e5  # should fail on YAML spec: no +- after e
        """.splitlines()
        ):
            if not x:
                continue
            res = load(x)
            assert isinstance(res, float)

    def test_json_number_int(self):
        for x in (
            y.split("#")[0].strip()
            for y in """
        42
        """.splitlines()
        ):
            if not x:
                continue
            res = load(x, int)
            assert isinstance(res, int)
