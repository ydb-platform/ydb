# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Compliance tests
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2017, 2020 Lele Gaifax
#

# from http://json.org/JSON_checker/test/pass2.json
JSON = r'''
[[[[[[[[[[[[[[[[[[["Not too deep"]]]]]]]]]]]]]]]]]]]
'''

def test_parse(dumps, loads):
    # test in/out equivalence and parsing
    res = loads(JSON)
    out = dumps(res)
    assert res == loads(out)
