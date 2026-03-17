from unittest import TestCase
import hjson as json

def default_iterable(obj):
    return list(obj)

class TestCheckCircular(TestCase):
    def test_circular_dict(self):
        dct = {}
        dct['a'] = dct
        self.assertRaises(ValueError, json.dumpsJSON, dct)

    def test_circular_list(self):
        lst = []
        lst.append(lst)
        self.assertRaises(ValueError, json.dumpsJSON, lst)

    def test_circular_composite(self):
        dct2 = {}
        dct2['a'] = []
        dct2['a'].append(dct2)
        self.assertRaises(ValueError, json.dumpsJSON, dct2)

    def test_circular_default(self):
        json.dumpsJSON([set()], default=default_iterable)
        self.assertRaises(TypeError, json.dumpsJSON, [set()])

    def test_circular_off_default(self):
        json.dumpsJSON([set()], default=default_iterable, check_circular=False)
        self.assertRaises(TypeError, json.dumpsJSON, [set()], check_circular=False)
