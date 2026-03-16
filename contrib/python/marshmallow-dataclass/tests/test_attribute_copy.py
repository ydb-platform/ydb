import dataclasses
import unittest

import marshmallow.validate

from marshmallow_dataclass import class_schema


class TestAttributesCopy(unittest.TestCase):
    def test_meta_class_copied(self):
        @dataclasses.dataclass
        class Anything:
            class Meta:
                pass

        schema = class_schema(Anything)
        self.assertEqual(schema.Meta, Anything.Meta)

    def test_validates_schema_copied(self):
        @dataclasses.dataclass
        class Anything:
            @marshmallow.validates_schema
            def validates_schema(self, *args, **kwargs):
                pass

        schema = class_schema(Anything)
        self.assertIn("validates_schema", dir(schema))

    def test_custom_method_not_copied(self):
        @dataclasses.dataclass
        class Anything:
            def custom_method(self):
                pass

        schema = class_schema(Anything)
        self.assertNotIn("custom_method", dir(schema))

    def test_custom_property_not_copied(self):
        @dataclasses.dataclass
        class Anything:
            @property
            def custom_property(self):
                return 42

        schema = class_schema(Anything)
        self.assertNotIn("custom_property", dir(schema))


if __name__ == "__main__":
    unittest.main()
