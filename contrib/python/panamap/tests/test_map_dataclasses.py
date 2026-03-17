from dataclasses import dataclass
from typing import Optional, List
from unittest import TestCase

from panamap import Mapper, MissingMappingException, FieldMappingException


@dataclass
class A:
    a_value: str
    common_value: int


@dataclass
class B:
    b_value: str
    common_value: int


@dataclass
class NestedA:
    value: int


@dataclass
class OuterA:
    nested: NestedA


@dataclass
class NestedB:
    value: int


@dataclass
class OuterB:
    nested: NestedB


@dataclass
class StringCarrier:
    value: str


@dataclass
class OptionalStringCarrier:
    value: Optional[str]


@dataclass
class ForwardRefCarrierA:
    value: "ForwardReferencedA"


@dataclass
class ForwardRefListCarrierA:
    value: List["ForwardReferencedA"]


@dataclass
class ForwardReferencedA:
    value: str


@dataclass
class ForwardRefListCarrierB:
    value: List["ForwardReferencedB"]


@dataclass
class ForwardReferencedB:
    value: str


class TestMapDataclasses(TestCase):
    def test_map_matching_dataclasses(self):
        mapper = Mapper()
        mapper.mapping(A, B).bidirectional("a_value", "b_value").map_matching().register()

        b = mapper.map(A("123", 456), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, "123")
        self.assertEqual(b.common_value, 456)

        a = mapper.map(B("xyz", 789), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.a_value, "xyz")
        self.assertEqual(a.common_value, 789)

    def test_map_nested_dataclasses(self):
        mapper = Mapper()
        mapper.mapping(OuterA, OuterB).map_matching().register()
        mapper.mapping(NestedA, NestedB).map_matching().register()

        b = mapper.map(OuterA(NestedA(123)), OuterB)

        self.assertEqual(b.__class__, OuterB)
        self.assertEqual(b.nested.__class__, NestedB)
        self.assertEqual(b.nested.value, 123)

    def test_map_optional_value(self):
        mapper = Mapper()
        mapper.mapping(StringCarrier, OptionalStringCarrier).map_matching().register()

        b = mapper.map(StringCarrier("123"), OptionalStringCarrier)

        self.assertEqual(b.value, "123")

        a = mapper.map(OptionalStringCarrier("456"), StringCarrier)

        self.assertEqual(a.value, "456")

        with self.assertRaises(MissingMappingException):
            mapper.map(OptionalStringCarrier(None), StringCarrier)

    def test_forward_ref_resolving(self):
        mapper = Mapper()
        mapper.mapping(ForwardRefCarrierA, dict).map_matching().register()
        mapper.mapping(ForwardReferencedA, dict).map_matching().register()

        a = mapper.map({"value": {"value": "abc"}}, ForwardRefCarrierA)

        self.assertEqual(a.__class__, ForwardRefCarrierA)
        self.assertEqual(a.value.__class__, ForwardReferencedA)
        self.assertEqual(a.value.value, "abc")

    def test_forward_ref_in_list_resolving(self):
        mapper = Mapper()
        mapper.mapping(ForwardRefListCarrierA, ForwardRefListCarrierB).map_matching().register()
        mapper.mapping(ForwardReferencedA, ForwardReferencedB).map_matching().register()

        b = mapper.map(
            ForwardRefListCarrierA([ForwardReferencedA("abc"), ForwardReferencedA("def")]), ForwardRefListCarrierB
        )

        self.assertEqual(b.__class__, ForwardRefListCarrierB)
        self.assertEqual(len(b.value), 2)
        self.assertEqual(b.value[0].__class__, ForwardReferencedB)
        self.assertEqual(b.value[0].value, "abc")
        self.assertEqual(b.value[1].__class__, ForwardReferencedB)
        self.assertEqual(b.value[1].value, "def")

    def test_raises_correct_exception_on_missing_mapping_with_forward_ref(self):
        mapper = Mapper()
        mapper.mapping(ForwardRefListCarrierA, ForwardRefListCarrierB).map_matching().register()
        mapper.mapping(ForwardReferencedB, dict).map_matching().register()

        with self.assertRaises(FieldMappingException):
            mapper.map(
                ForwardRefListCarrierA([ForwardReferencedA("abc"), ForwardReferencedA("def")]), ForwardRefListCarrierB
            )
