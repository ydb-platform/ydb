import copy
import inspect
import sys
import typing
import unittest
from enum import Enum
from typing import Dict, Optional, Union, Any, List, Tuple

try:
    from typing import Final, Literal  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Final, Literal  # type: ignore[assignment]

from marshmallow import fields, Schema, validate

from marshmallow_dataclass import (
    field_for_schema,
    dataclass,
    union_field,
    collection_field,
)


class TestFieldForSchema(unittest.TestCase):
    maxDiff = None

    def assertFieldsEqual(self, a: fields.Field, b: fields.Field):
        self.assertEqual(a.__class__, b.__class__, "field class")

        def attrs(x):
            return sorted(
                (k, f"{v!r} ({v.__mro__!r})" if inspect.isclass(v) else repr(v))
                for k, v in x.__dict__.items()
                if not k.startswith("_")
            )

        self.assertEqual(attrs(a), attrs(b))

    def test_int(self):
        self.assertFieldsEqual(
            field_for_schema(int, default=9, metadata=dict(required=False)),
            fields.Integer(dump_default=9, load_default=9, required=False),
        )

    def test_any(self):
        self.assertFieldsEqual(
            field_for_schema(Any), fields.Raw(required=True, allow_none=True)
        )

    def test_dict_from_typing(self):
        self.assertFieldsEqual(
            field_for_schema(Dict[str, float]),
            fields.Dict(
                keys=fields.String(required=True),
                values=fields.Float(required=True),
                required=True,
            ),
        )

    def test_dict_from_typing_wo_args(self):
        self.assertFieldsEqual(
            field_for_schema(Dict),
            fields.Dict(
                keys=fields.Raw(required=True, allow_none=True),
                values=fields.Raw(required=True, allow_none=True),
                required=True,
            ),
        )

    def test_builtin_dict(self):
        self.assertFieldsEqual(
            field_for_schema(dict),
            fields.Dict(
                keys=fields.Raw(required=True, allow_none=True),
                values=fields.Raw(required=True, allow_none=True),
                required=True,
            ),
        )

    def test_list_from_typing(self):
        self.assertFieldsEqual(
            field_for_schema(List[int]),
            fields.List(fields.Integer(required=True), required=True),
        )

    def test_list_from_typing_wo_args(self):
        self.assertFieldsEqual(
            field_for_schema(List),
            fields.List(
                fields.Raw(required=True, allow_none=True),
                required=True,
            ),
        )

    def test_builtin_list(self):
        self.assertFieldsEqual(
            field_for_schema(list, metadata=dict(required=False)),
            fields.List(fields.Raw(required=True, allow_none=True), required=False),
        )

    def test_explicit_field(self):
        explicit_field = fields.Url(required=True)
        self.assertFieldsEqual(
            field_for_schema(str, metadata={"marshmallow_field": explicit_field}),
            explicit_field,
        )

    def test_str(self):
        self.assertFieldsEqual(field_for_schema(str), fields.String(required=True))

    def test_optional_str(self):
        self.assertFieldsEqual(
            field_for_schema(Optional[str]),
            fields.String(
                allow_none=True, required=False, dump_default=None, load_default=None
            ),
        )

    def test_enum(self):
        class Color(Enum):
            RED: 1
            GREEN: 2
            BLUE: 3

        if hasattr(fields, "Enum"):
            self.assertFieldsEqual(
                field_for_schema(Color),
                fields.Enum(enum=Color, required=True),
            )
        else:
            import marshmallow_enum

            self.assertFieldsEqual(
                field_for_schema(Color),
                marshmallow_enum.EnumField(enum=Color, required=True),
            )

    def test_literal(self):
        self.assertFieldsEqual(
            field_for_schema(Literal["a"]),
            fields.Raw(required=True, validate=validate.Equal("a")),
        )

    def test_literal_multiple_types(self):
        self.assertFieldsEqual(
            field_for_schema(Literal["a", 1, 1.23, True]),
            fields.Raw(required=True, validate=validate.OneOf(("a", 1, 1.23, True))),
        )

    def test_final(self):
        self.assertFieldsEqual(
            field_for_schema(Final[str]), fields.String(required=True)
        )

    def test_final_without_type(self):
        self.assertFieldsEqual(
            field_for_schema(Final), fields.Raw(required=True, allow_none=True)
        )

    def test_union(self):
        self.assertFieldsEqual(
            field_for_schema(Union[int, str]),
            union_field.Union(
                [
                    (int, fields.Integer(required=True)),
                    (str, fields.String(required=True)),
                ],
                required=True,
            ),
        )

    def test_union_multiple_types_with_none(self):
        self.assertFieldsEqual(
            field_for_schema(Union[int, str, None]),
            union_field.Union(
                [
                    (int, fields.Integer(required=True)),
                    (str, fields.String(required=True)),
                ],
                required=False,
                dump_default=None,
                load_default=None,
            ),
        )

    @unittest.expectedFailure
    def test_optional_multiple_types(self):
        # excercise bug (see #247)
        Optional[Union[str, int]]

        self.assertFieldsEqual(
            field_for_schema(Optional[Union[int, str]]),
            union_field.Union(
                [
                    (int, fields.Integer(required=True)),
                    (str, fields.String(required=True)),
                ],
                required=False,
                dump_default=None,
                load_default=None,
            ),
        )

    def test_optional_multiple_types_ignoring_union_field_order(self):
        # see https://github.com/lovasoa/marshmallow_dataclass/pull/246#issuecomment-1722204048
        result = field_for_schema(Optional[Union[int, str]])
        expected = union_field.Union(
            [
                (int, fields.Integer(required=True)),
                (str, fields.String(required=True)),
            ],
            required=False,
            dump_default=None,
            load_default=None,
        )

        def sort_union_fields(field):
            rv = copy.copy(field)
            rv.union_fields = sorted(field.union_fields, key=repr)
            return rv

        self.assertFieldsEqual(sort_union_fields(result), sort_union_fields(expected))

    def test_newtype(self):
        self.assertFieldsEqual(
            field_for_schema(typing.NewType("UserId", int), default=0),
            fields.Integer(
                required=False,
                dump_default=0,
                load_default=0,
                metadata={"description": "UserId"},
            ),
        )

    def test_marshmallow_dataclass(self):
        class NewSchema(Schema):
            pass

        @dataclass(base_schema=NewSchema)
        class NewDataclass:
            pass

        self.assertFieldsEqual(
            field_for_schema(NewDataclass, metadata=dict(required=False)),
            fields.Nested(NewDataclass.Schema),
        )

    def test_override_container_type_with_type_mapping(self):
        type_mapping = [
            (List, fields.List, List[int]),
            (Dict, fields.Dict, Dict[str, int]),
            (Tuple, fields.Tuple, Tuple[int, str, bytes]),
        ]
        for base_type, marshmallow_field, schema in type_mapping:

            class MyType(marshmallow_field):
                ...

            self.assertIsInstance(field_for_schema(schema), marshmallow_field)

            class BaseSchema(Schema):
                TYPE_MAPPING = {base_type: MyType}

            self.assertIsInstance(
                field_for_schema(schema, base_schema=BaseSchema), MyType
            )

    def test_mapping(self):
        self.assertFieldsEqual(
            field_for_schema(typing.Mapping),
            fields.Dict(
                keys=fields.Raw(required=True, allow_none=True),
                values=fields.Raw(required=True, allow_none=True),
                required=True,
            ),
        )

    def test_sequence(self):
        self.maxDiff = 2000
        self.assertFieldsEqual(
            field_for_schema(typing.Sequence[int]),
            collection_field.Sequence(fields.Integer(required=True), required=True),
        )

    def test_sequence_wo_args(self):
        self.assertFieldsEqual(
            field_for_schema(typing.Sequence),
            collection_field.Sequence(
                cls_or_instance=fields.Raw(required=True, allow_none=True),
                required=True,
            ),
        )

    def test_homogeneous_tuple_from_typing(self):
        self.assertFieldsEqual(
            field_for_schema(Tuple[str, ...]),
            collection_field.Sequence(fields.String(required=True), required=True),
        )

    @unittest.skipIf(sys.version_info < (3, 9), "PEP 585 unsupported")
    def test_homogeneous_tuple(self):
        self.assertFieldsEqual(
            field_for_schema(tuple[float, ...]),
            collection_field.Sequence(fields.Float(required=True), required=True),
        )

    def test_set_from_typing(self):
        self.assertFieldsEqual(
            field_for_schema(typing.Set[str]),
            collection_field.Set(
                fields.String(required=True),
                frozen=False,
                required=True,
            ),
        )

    def test_set_from_typing_wo_args(self):
        self.assertFieldsEqual(
            field_for_schema(typing.Set),
            collection_field.Set(
                cls_or_instance=fields.Raw(required=True, allow_none=True),
                frozen=False,
                required=True,
            ),
        )

    def test_builtin_set(self):
        self.assertFieldsEqual(
            field_for_schema(set),
            collection_field.Set(
                cls_or_instance=fields.Raw(required=True, allow_none=True),
                frozen=False,
                required=True,
            ),
        )

    def test_frozenset_from_typing(self):
        self.assertFieldsEqual(
            field_for_schema(typing.FrozenSet[int]),
            collection_field.Set(
                fields.Integer(required=True),
                frozen=True,
                required=True,
            ),
        )

    def test_frozenset_from_typing_wo_args(self):
        self.assertFieldsEqual(
            field_for_schema(typing.FrozenSet),
            collection_field.Set(
                cls_or_instance=fields.Raw(required=True, allow_none=True),
                frozen=True,
                required=True,
            ),
        )

    def test_builtin_frozenset(self):
        self.assertFieldsEqual(
            field_for_schema(frozenset),
            collection_field.Set(
                cls_or_instance=fields.Raw(required=True, allow_none=True),
                frozen=True,
                required=True,
            ),
        )


if __name__ == "__main__":
    unittest.main()
