from unittest import TestCase

import hjson as json

class TestDefault(TestCase):
    def test_default(self):
        self.assertEqual(
            json.dumpsJSON(type, default=repr),
            json.dumpsJSON(repr(type)))
