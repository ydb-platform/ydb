import unittest
from typing import List, Optional

from marshmallow_dataclass import dataclass


@dataclass
class GlobalA:
    b: "GlobalB"


@dataclass
class GlobalB:
    pass


@dataclass
class GlobalSelfRecursion:
    related: "List[GlobalSelfRecursion]"


@dataclass
class GlobalRecursion:
    related: "List[GlobalRecursion]"


@dataclass
class GlobalCyclicA:
    b: "Optional[GlobalCyclicB]"


@dataclass
class GlobalCyclicB:
    a: "Optional[GlobalCyclicA]"


class TestForwardReferences(unittest.TestCase):
    def test_late_evaluated_types(self):
        @dataclass
        class MyData:
            value: int

        self.assertEqual(MyData(1), MyData.Schema().load(dict(value=1)))

    def test_forward_references_for_basic_types(self):
        @dataclass
        class Person:
            name: "str"
            age: "int"

        self.assertEqual(
            Person("Jon", 25), Person.Schema().load(dict(name="Jon", age=25))
        )

    def test_global_forward_references(self):
        self.assertEqual(GlobalA(GlobalB()), GlobalA.Schema().load(dict(b=dict())))

    def test_global_self_recursive_type(self):
        self.assertEqual(
            GlobalSelfRecursion([GlobalSelfRecursion([])]),
            GlobalSelfRecursion.Schema().load(dict(related=[dict(related=[])])),
        )

    def test_global_recursive_type(self):
        self.assertEqual(
            GlobalRecursion([GlobalRecursion([])]),
            GlobalRecursion.Schema().load(dict(related=[dict(related=[])])),
        )

    def test_global_circular_reference(self):
        self.assertEqual(
            GlobalCyclicA(GlobalCyclicB(GlobalCyclicA(None))),
            GlobalCyclicA.Schema().load(dict(b=dict(a=dict(b=None)))),
        )

    def test_local_self_recursive_type(self):
        @dataclass
        class LocalSelfRecursion:
            related: "List[LocalSelfRecursion]"

        self.assertEqual(
            LocalSelfRecursion([LocalSelfRecursion([])]),
            LocalSelfRecursion.Schema().load(dict(related=[dict(related=[])])),
        )

    def test_local_recursive_type(self):
        @dataclass
        class LocalRecursion:
            related: "List[LocalRecursion]"

        self.assertEqual(
            LocalRecursion([LocalRecursion([])]),
            LocalRecursion.Schema().load(dict(related=[dict(related=[])])),
        )

    def test_local_forward_references(self):
        @dataclass
        class LocalA:
            b: "LocalB"

        @dataclass
        class LocalB:
            pass

        self.assertEqual(LocalA(LocalB()), LocalA.Schema().load(dict(b=dict())))

    def test_name_collisions(self):
        """
        This is one example about why you should not make local schemas
        :return:
        """

        def make_another_a():
            @dataclass
            class A:
                d: int

            A.Schema()

        make_another_a()

        @dataclass
        class A:
            c: int

        A.Schema()

        @dataclass
        class B:
            a: "A"

        # with self.assertRaises(marshmallow.exceptions.ValidationError):
        B.Schema().load(dict(a=dict(c=1)))
        # marshmallow.exceptions.ValidationError:
        # {'a': {'d': ['Missing data for required field.'], 'c': ['Unknown field.']}}

    def test_locals_from_decoration_ns(self):
        # Test that locals are picked-up at decoration-time rather
        # than when the decorator is constructed.
        @frozen_dataclass
        class A:
            b: "B"

        @frozen_dataclass
        class B:
            x: int

        assert A.Schema().load({"b": {"x": 42}}) == A(b=B(x=42))


frozen_dataclass = dataclass(frozen=True)
