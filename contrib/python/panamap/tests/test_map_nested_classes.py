from dataclasses import dataclass
from unittest import TestCase

from panamap import Mapper


@dataclass
class NestedA:
    value: str


@dataclass
class A:
    value: NestedA


@dataclass
class NestedB:
    value: str


@dataclass
class B:
    value: NestedB


class TestMapNestedClasses(TestCase):
    def test_map_nested_classes(self):
        mapper = Mapper()
        mapper.mapping(A, B).map_matching().register()

        mapper.mapping(NestedA, NestedB).map_matching().register()

        b = mapper.map(A(NestedA("abc")), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.value.__class__, NestedB)
        self.assertEqual(b.value.value, "abc")

        a = mapper.map(B(NestedB("xyz")), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.value.__class__, NestedA)
        self.assertEqual(a.value.value, "xyz")
