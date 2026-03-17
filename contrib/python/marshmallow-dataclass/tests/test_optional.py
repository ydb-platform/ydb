import unittest
from dataclasses import field
from typing import Optional

import marshmallow

from marshmallow_dataclass import dataclass


class TestOptionalField(unittest.TestCase):
    def test_optional_field(self):
        @dataclass
        class OptionalValue:
            value: Optional[str] = "value"

        schema = OptionalValue.Schema()

        self.assertEqual(schema.load({"value": None}), OptionalValue(value=None))
        self.assertEqual(schema.load({"value": "hello"}), OptionalValue(value="hello"))
        self.assertEqual(schema.load({}), OptionalValue())

    def test_optional_field_not_none(self):
        @dataclass
        class OptionalValueNotNone:
            value: Optional[str] = field(
                default="value", metadata={"allow_none": False}
            )

        schema = OptionalValueNotNone.Schema()

        self.assertEqual(schema.load({}), OptionalValueNotNone())
        self.assertEqual(
            schema.load({"value": "hello"}), OptionalValueNotNone(value="hello")
        )
        with self.assertRaises(marshmallow.exceptions.ValidationError) as exc_cm:
            schema.load({"value": None})
        self.assertEqual(
            exc_cm.exception.messages, {"value": ["Field may not be null."]}
        )

    def test_required_optional_field(self):
        @dataclass
        class RequiredOptionalValue:
            value: Optional[str] = field(default="default", metadata={"required": True})

        schema = RequiredOptionalValue.Schema()

        self.assertEqual(schema.load({"value": None}), RequiredOptionalValue(None))
        self.assertEqual(
            schema.load({"value": "hello"}), RequiredOptionalValue(value="hello")
        )
        with self.assertRaises(marshmallow.exceptions.ValidationError) as exc_cm:
            schema.load({})
        self.assertEqual(
            exc_cm.exception.messages, {"value": ["Missing data for required field."]}
        )
