import unittest

import marshmallow

import marshmallow_dataclass


# Regression test for https://github.com/lovasoa/marshmallow_dataclass/issues/75
class TestPostLoad(unittest.TestCase):
    @marshmallow_dataclass.dataclass
    class Named:
        first_name: str
        last_name: str

        @marshmallow.post_load
        def a(self, data, **_kwargs):
            data["first_name"] = data["first_name"].capitalize()
            return data

        @marshmallow.post_load
        def z(self, data, **_kwargs):
            data["last_name"] = data["last_name"].capitalize()
            return data

    def test_post_load_method_naming_does_not_affect_data(self):
        actual = self.Named.Schema().load(
            {"first_name": "matt", "last_name": "groening"}
        )
        expected = self.Named(first_name="Matt", last_name="Groening")
        self.assertEqual(actual, expected)

    def test_load_many(self):
        actual = self.Named.Schema().load(
            [
                {"first_name": "matt", "last_name": "groening"},
                {"first_name": "bart", "last_name": "simpson"},
            ],
            many=True,
        )
        expected = [
            self.Named(first_name="Matt", last_name="Groening"),
            self.Named(first_name="Bart", last_name="Simpson"),
        ]
        self.assertEqual(actual, expected)
