from dataclasses import dataclass
from typing import List, Tuple
from unittest import TestCase

from panamap import Mapper


@dataclass
class ValueA:
    value: str


@dataclass
class ContainerA:
    value: List[ValueA]


@dataclass
class ValueB:
    value: str


@dataclass
class ContainerB:
    value: List[ValueB]


@dataclass
class FirstTupleValueA:
    value: str


@dataclass
class SecondTupleValueA:
    value: str


@dataclass
class TupleContainerA:
    value: Tuple[FirstTupleValueA, SecondTupleValueA]


@dataclass
class FirstTupleValueB:
    value: str


@dataclass
class SecondTupleValueB:
    value: str


@dataclass
class TupleContainerB:
    value: Tuple[FirstTupleValueB, SecondTupleValueB]


class TestMapIterables(TestCase):
    def test_map_list(self):
        mapper = Mapper()
        mapper.mapping(ContainerA, ContainerB).map_matching().register()
        mapper.mapping(ValueA, ValueB).map_matching().register()

        b = mapper.map(ContainerA([ValueA("abc"), ValueA("def")]), ContainerB)

        self.assertEqual(b.__class__, ContainerB)
        self.assertEqual(len(b.value), 2)
        self.assertEqual(b.value[0].__class__, ValueB)
        self.assertEqual(b.value[0].value, "abc")
        self.assertEqual(b.value[1].__class__, ValueB)
        self.assertEqual(b.value[1].value, "def")

    def test_map_tuple(self):
        mapper = Mapper()
        mapper.mapping(TupleContainerA, TupleContainerB).map_matching().register()
        mapper.mapping(FirstTupleValueA, FirstTupleValueB).map_matching().register()
        mapper.mapping(SecondTupleValueA, SecondTupleValueB).map_matching().register()

        b = mapper.map(TupleContainerA((FirstTupleValueA("abc"), SecondTupleValueA("def"))), TupleContainerB)

        self.assertEqual(b.__class__, TupleContainerB)
        self.assertEqual(len(b.value), 2)
        self.assertEqual(b.value[0].__class__, FirstTupleValueB)
        self.assertEqual(b.value[0].value, "abc")
        self.assertEqual(b.value[1].__class__, SecondTupleValueB)
        self.assertEqual(b.value[1].value, "def")
