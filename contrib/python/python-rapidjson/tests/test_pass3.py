# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Compliance tests
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2017, 2020 Lele Gaifax
#

# from http://json.org/JSON_checker/test/pass3.json
JSON = r'''
{
    "JSON Test Pattern pass3": {
        "The outermost value": "must be an object or array.",
        "In this test": "It is an object."
    }
}
'''

def test_parse(dumps, loads):
    # test in/out equivalence and parsing
    res = loads(JSON)
    out = dumps(res)
    assert res == loads(out)
