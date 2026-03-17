import typing as T
from dataclasses import field

import marshmallow as mm
import marshmallow_dataclass as mmdc
import collections.abc
import unittest


# Test from https://github.com/lovasoa/marshmallow_dataclass/issues/125
class TestPostDump(unittest.TestCase):
    def setUp(self) -> None:
        class BaseSchema(mm.Schema):
            SKIP_VALUES = {None}

            @mm.post_dump
            def remove_skip_values(self, data, many):
                return {
                    key: value
                    for key, value in data.items()
                    if not isinstance(value, collections.abc.Hashable)
                    or value not in self.SKIP_VALUES
                }

        @mmdc.add_schema(base_schema=BaseSchema)
        @mmdc.dataclass
        class Item:
            key: str
            val: str
            Schema: T.ClassVar[T.Type[mm.Schema]] = mm.Schema

        @mmdc.add_schema(base_schema=BaseSchema)
        @mmdc.dataclass
        class Items:
            other: T.Optional[str]
            items: T.List[Item] = field(default_factory=list)
            Schema: T.ClassVar[T.Type[mm.Schema]] = mm.Schema

        self.BaseSchema = BaseSchema
        self.Item = Item
        self.Items = Items
        self.data = {"items": [{"key": "any key", "val": "any value"}]}

    def test_class_schema(self):
        items_schema = mmdc.class_schema(self.Items, base_schema=self.BaseSchema)()
        items = items_schema.load(self.data)
        self.assertEqual(
            {"items": [{"key": "any key", "val": "any value"}]},
            items_schema.dump(items),
        )

    def test_inside_schema(self):
        items = self.Items.Schema().load(self.data)
        self.assertEqual(
            {"items": [{"key": "any key", "val": "any value"}]},
            self.Items.Schema().dump(items),
        )
