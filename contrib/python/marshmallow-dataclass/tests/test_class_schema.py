import inspect
import typing
import unittest
from typing import Any, cast, TYPE_CHECKING
from uuid import UUID

try:
    from typing import Final, Literal  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Final, Literal  # type: ignore[assignment]

import dataclasses
from marshmallow import Schema, ValidationError
from marshmallow.fields import Field, UUID as UUIDField, List as ListField, Integer
from marshmallow.validate import Validator

from marshmallow_dataclass import class_schema, NewType


class TestClassSchema(unittest.TestCase):
    def test_simple_unique_schemas(self):
        @dataclasses.dataclass
        class Simple:
            one: str = dataclasses.field()
            two: str = dataclasses.field()

        @dataclasses.dataclass
        class ComplexNested:
            three: int = dataclasses.field()
            four: Simple = dataclasses.field()

        self.assertIs(class_schema(ComplexNested), class_schema(ComplexNested))
        self.assertIs(class_schema(Simple), class_schema(Simple))
        self.assertIs(
            class_schema(Simple),
            class_schema(ComplexNested)._declared_fields["four"].nested,
        )

        complex_set = {
            class_schema(ComplexNested),
            class_schema(ComplexNested, base_schema=None),
            class_schema(ComplexNested, clazz_frame=None),
            class_schema(ComplexNested, None),
            class_schema(ComplexNested, None, None),
        }
        simple_set = {
            class_schema(Simple),
            class_schema(Simple, base_schema=None),
            class_schema(Simple, clazz_frame=None),
            class_schema(Simple, None),
            class_schema(Simple, None, None),
        }
        self.assertEqual(len(complex_set), 1)
        self.assertEqual(len(simple_set), 1)

    def test_nested_schema_with_passed_frame(self):
        @dataclasses.dataclass
        class Simple:
            one: str = dataclasses.field()
            two: str = dataclasses.field()

        @dataclasses.dataclass
        class ComplexNested:
            three: int = dataclasses.field()
            four: Simple = dataclasses.field()

        frame = inspect.stack()[0][0]

        self.assertIs(
            class_schema(ComplexNested, clazz_frame=frame),
            class_schema(ComplexNested, clazz_frame=frame),
        )
        self.assertIs(
            class_schema(Simple, clazz_frame=frame),
            class_schema(Simple, clazz_frame=frame),
        )
        self.assertIs(
            class_schema(Simple, clazz_frame=frame),
            class_schema(ComplexNested, clazz_frame=frame)
            ._declared_fields["four"]
            .nested,
        )

        complex_set = {
            class_schema(ComplexNested, clazz_frame=frame),
            class_schema(ComplexNested, base_schema=None, clazz_frame=frame),
            class_schema(ComplexNested, None, clazz_frame=frame),
            class_schema(ComplexNested, None, frame),
        }
        simple_set = {
            class_schema(Simple, clazz_frame=frame),
            class_schema(Simple, base_schema=None, clazz_frame=frame),
            class_schema(Simple, None, clazz_frame=frame),
            class_schema(Simple, clazz_frame=frame),
            class_schema(Simple, None, frame),
        }
        self.assertEqual(len(complex_set), 1)
        self.assertEqual(len(simple_set), 1)

    def test_use_type_mapping_from_base_schema(self):
        class CustomType:
            pass

        class CustomField(Field):
            pass

        class CustomListField(ListField):
            pass

        class BaseSchema(Schema):
            TYPE_MAPPING = {CustomType: CustomField, typing.List: CustomListField}

        @dataclasses.dataclass
        class WithCustomField:
            custom: CustomType
            custom_list: typing.List[float]
            uuid: UUID
            n: int

        schema = class_schema(WithCustomField, base_schema=BaseSchema)()
        self.assertIsInstance(schema.fields["custom"], CustomField)
        self.assertIsInstance(schema.fields["custom_list"], CustomListField)
        self.assertIsInstance(schema.fields["uuid"], UUIDField)
        self.assertIsInstance(schema.fields["n"], Integer)

    def test_filtering_list_schema(self):
        class FilteringListField(ListField):
            def __init__(
                self,
                cls_or_instance: typing.Union[Field, type],
                min: typing.Any,
                max: typing.Any,
                **kwargs,
            ):
                super().__init__(cls_or_instance, **kwargs)
                self.min = min
                self.max = max

            def _deserialize(
                self, value, attr, data, **kwargs
            ) -> typing.List[typing.Any]:
                loaded = super()._deserialize(value, attr, data, **kwargs)
                return [x for x in loaded if self.min <= x <= self.max]

        class BaseSchema(Schema):
            TYPE_MAPPING = {typing.List: FilteringListField}

        @dataclasses.dataclass
        class WithCustomField:
            constrained_floats: typing.List[float] = dataclasses.field(
                metadata={"max": 2.5, "min": 1}
            )
            constrained_strings: typing.List[str] = dataclasses.field(
                metadata={"max": "x", "min": "a"}
            )

        schema = class_schema(WithCustomField, base_schema=BaseSchema)()
        actual = schema.load(
            {
                "constrained_floats": [0, 1, 2, 3],
                "constrained_strings": ["z", "a", "b", "c", ""],
            }
        )
        self.assertEqual(
            actual,
            WithCustomField(
                constrained_floats=[1.0, 2.0], constrained_strings=["a", "b", "c"]
            ),
        )

    def test_any_none(self):
        # See: https://github.com/lovasoa/marshmallow_dataclass/issues/80
        @dataclasses.dataclass
        class A:
            data: Any

        schema = class_schema(A)()
        self.assertEqual(A(data=None), schema.load({"data": None}))
        self.assertEqual(schema.dump(A(data=None)), {"data": None})

    def test_any_none_disallowed(self):
        @dataclasses.dataclass
        class A:
            data: Any = dataclasses.field(metadata={"allow_none": False})

        schema = class_schema(A)()
        self.assertRaises(ValidationError, lambda: schema.load({"data": None}))

    def test_literal(self):
        @dataclasses.dataclass
        class A:
            data: Literal["a"]

        schema = class_schema(A)()
        self.assertEqual(A(data="a"), schema.load({"data": "a"}))
        self.assertEqual(schema.dump(A(data="a")), {"data": "a"})
        for data in ["b", 2, 2.34, False]:
            with self.assertRaises(ValidationError):
                schema.load({"data": data})

    def test_literal_multiple_types(self):
        @dataclasses.dataclass
        class A:
            data: Literal["a", 1, 1.23, True]

        schema = class_schema(A)()
        for data in ["a", 1, 1.23, True]:
            self.assertEqual(A(data=data), schema.load({"data": data}))
            self.assertEqual(schema.dump(A(data=data)), {"data": data})
        for data in ["b", 2, 2.34, False]:
            with self.assertRaises(ValidationError):
                schema.load({"data": data})

    def test_final(self):
        @dataclasses.dataclass
        class A:
            # Mypy currently considers read-only dataclass attributes without a
            # default value an error.
            # See: https://github.com/python/mypy/issues/10688.
            data: Final[str]  # type: ignore[misc]

        schema = class_schema(A)()
        self.assertEqual(A(data="a"), schema.load({"data": "a"}))
        self.assertEqual(schema.dump(A(data="a")), {"data": "a"})
        for data in [2, 2.34, False]:
            with self.assertRaises(ValidationError):
                schema.load({"data": data})

    def test_final_infers_type_from_default(self):
        # @dataclasses.dataclass(frozen=True)
        class A:
            data: Final = "a"

        # @dataclasses.dataclass
        class B:
            data: Final = A()

        # NOTE: This workaround is needed to avoid a Mypy crash.
        # See: https://github.com/python/mypy/issues/10090#issuecomment-865971891
        if not TYPE_CHECKING:
            frozen_dataclass = dataclasses.dataclass(frozen=True)
            A = frozen_dataclass(A)
            B = dataclasses.dataclass(B)

        with self.assertWarns(Warning):
            schema_a = class_schema(A)()
        self.assertEqual(A(data="a"), schema_a.load({}))
        self.assertEqual(A(data="a"), schema_a.load({"data": "a"}))
        self.assertEqual(A(data="b"), schema_a.load({"data": "b"}))
        self.assertEqual(schema_a.dump(A()), {"data": "a"})
        self.assertEqual(schema_a.dump(A(data="a")), {"data": "a"})
        self.assertEqual(schema_a.dump(A(data="b")), {"data": "b"})
        for data in [2, 2.34, False]:
            with self.assertRaises(ValidationError):
                schema_a.load({"data": data})

        with self.assertWarns(Warning):
            schema_b = class_schema(B)()
        self.assertEqual(B(data=A()), schema_b.load({}))
        self.assertEqual(B(data=A()), schema_b.load({"data": {}}))
        self.assertEqual(B(data=A()), schema_b.load({"data": {"data": "a"}}))
        self.assertEqual(B(data=A(data="b")), schema_b.load({"data": {"data": "b"}}))
        self.assertEqual(schema_b.dump(B()), {"data": {"data": "a"}})
        self.assertEqual(schema_b.dump(B(data=A())), {"data": {"data": "a"}})
        self.assertEqual(schema_b.dump(B(data=A(data="a"))), {"data": {"data": "a"}})
        self.assertEqual(schema_b.dump(B(data=A(data="b"))), {"data": {"data": "b"}})
        for data in [2, 2.34, False]:
            with self.assertRaises(ValidationError):
                schema_b.load({"data": data})

    def test_final_infers_type_any_from_field_default_factory(self):
        # @dataclasses.dataclass
        class A:
            data: Final = dataclasses.field(default_factory=lambda: [])

        # NOTE: This workaround is needed to avoid a Mypy crash.
        # See: https://github.com/python/mypy/issues/10090#issuecomment-866686096
        if not TYPE_CHECKING:
            A = dataclasses.dataclass(A)

        with self.assertWarns(Warning):
            schema = class_schema(A)()

        self.assertEqual(A(data=[]), schema.load({}))
        self.assertEqual(A(data=[]), schema.load({"data": []}))
        self.assertEqual(A(data=["a"]), schema.load({"data": ["a"]}))
        self.assertEqual(schema.dump(A()), {"data": []})
        self.assertEqual(schema.dump(A(data=[])), {"data": []})
        self.assertEqual(schema.dump(A(data=["a"])), {"data": ["a"]})

        # The following test cases pass because the field type cannot be
        # inferred from a factory.
        self.assertEqual(A(data=cast(Any, True)), schema.load({"data": True}))
        self.assertEqual(A(data=cast(Any, "a")), schema.load({"data": "a"}))

    def test_validator_stacking(self):
        # See: https://github.com/lovasoa/marshmallow_dataclass/issues/91
        class SimpleValidator(Validator):
            # Marshmallow checks for valid validators at construction time only using `callable`
            def __call__(self):
                pass

        validator_a = SimpleValidator()
        validator_b = SimpleValidator()
        validator_c = SimpleValidator()
        validator_d = SimpleValidator()

        CustomTypeOneValidator = NewType(
            "CustomTypeOneValidator", str, validate=validator_a
        )
        CustomTypeNoneValidator = NewType("CustomTypeNoneValidator", str, validate=None)
        CustomTypeMultiValidator = NewType(
            "CustomTypeNoneValidator", str, validate=[validator_a, validator_b]
        )

        @dataclasses.dataclass
        class A:
            data: CustomTypeNoneValidator = dataclasses.field()

        schema_a = class_schema(A)()
        self.assertListEqual(schema_a.fields["data"].validators, [])

        @dataclasses.dataclass
        class B:
            data: CustomTypeNoneValidator = dataclasses.field(
                metadata={"validate": validator_a}
            )

        schema_b = class_schema(B)()
        self.assertListEqual(schema_b.fields["data"].validators, [validator_a])

        @dataclasses.dataclass
        class C:
            data: CustomTypeNoneValidator = dataclasses.field(
                metadata={"validate": [validator_a, validator_b]}
            )

        schema_c = class_schema(C)()
        self.assertListEqual(
            schema_c.fields["data"].validators, [validator_a, validator_b]
        )

        @dataclasses.dataclass
        class D:
            data: CustomTypeOneValidator = dataclasses.field()

        schema_d = class_schema(D)()
        self.assertListEqual(schema_d.fields["data"].validators, [validator_a])

        @dataclasses.dataclass
        class E:
            data: CustomTypeOneValidator = dataclasses.field(
                metadata={"validate": validator_b}
            )

        schema_e = class_schema(E)()
        self.assertListEqual(
            schema_e.fields["data"].validators, [validator_a, validator_b]
        )

        @dataclasses.dataclass
        class F:
            data: CustomTypeOneValidator = dataclasses.field(
                metadata={"validate": [validator_b, validator_c]}
            )

        schema_f = class_schema(F)()
        self.assertListEqual(
            schema_f.fields["data"].validators, [validator_a, validator_b, validator_c]
        )

        @dataclasses.dataclass
        class G:
            data: CustomTypeMultiValidator = dataclasses.field()

        schema_g = class_schema(G)()
        self.assertListEqual(
            schema_g.fields["data"].validators, [validator_a, validator_b]
        )

        @dataclasses.dataclass
        class H:
            data: CustomTypeMultiValidator = dataclasses.field(
                metadata={"validate": validator_c}
            )

        schema_h = class_schema(H)()
        self.assertListEqual(
            schema_h.fields["data"].validators, [validator_a, validator_b, validator_c]
        )

        @dataclasses.dataclass
        class J:
            data: CustomTypeMultiValidator = dataclasses.field(
                metadata={"validate": [validator_c, validator_d]}
            )

        schema_j = class_schema(J)()
        self.assertListEqual(
            schema_j.fields["data"].validators,
            [validator_a, validator_b, validator_c, validator_d],
        )

    def test_recursive_reference(self):
        @dataclasses.dataclass
        class Tree:
            children: typing.List["Tree"]  # noqa: F821

        schema = class_schema(Tree)()

        self.assertEqual(
            schema.load({"children": [{"children": []}]}),
            Tree(children=[Tree(children=[])]),
        )

    def test_cyclic_reference(self):
        @dataclasses.dataclass
        class First:
            second: typing.Optional["Second"]  # noqa: F821

        @dataclasses.dataclass
        class Second:
            first: typing.Optional["First"]

        first_schema = class_schema(First)()
        second_schema = class_schema(Second)()

        self.assertEqual(
            first_schema.load({"second": {"first": None}}),
            First(second=Second(first=None)),
        )
        self.assertEqual(
            second_schema.dump(Second(first=First(second=Second(first=None)))),
            {"first": {"second": {"first": None}}},
        )

    def test_init_fields(self):
        @dataclasses.dataclass
        class NoMeta:
            no_init: str = dataclasses.field(init=False)

        @dataclasses.dataclass
        class NoInit:
            class Meta:
                pass

            no_init: str = dataclasses.field(init=False)

        @dataclasses.dataclass
        class Init:
            class Meta:
                include_non_init = True

            no_init: str = dataclasses.field(init=False)

        self.assertNotIn("no_init", class_schema(NoMeta)().fields)
        self.assertNotIn("no_init", class_schema(NoInit)().fields)
        self.assertIn("no_init", class_schema(Init)().fields)


if __name__ == "__main__":
    unittest.main()
