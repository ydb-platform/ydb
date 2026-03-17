from unittest import TestCase
import hjson as json

# from http://json.org/JSON_checker/test/pass2.json
JSON = r'''
[[[[[[[[[[[[[[[[[[["Not too deep"]]]]]]]]]]]]]]]]]]]
'''

class TestPass2(TestCase):
    def test_parse(self):
        # test in/out equivalence and parsing
        res = json.loads(JSON)
        out = json.dumpsJSON(res)
        self.assertEqual(res, json.loads(out))
