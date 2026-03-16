from unittest import TestCase
from dataclasses import dataclass

from panamap import Mapper


class TestMapPrimitiveValues(TestCase):
    def test_map_primitive_values(self):
        mapper = Mapper()

        self.assertEqual(mapper.map(123, str), "123")
        self.assertEqual(mapper.map("456", int), 456)

    def test_map_int_to_str_field(self):
        @dataclass
        class A:
            value: int

        @dataclass
        class B:
            value: str

        mapper = Mapper()
        mapper.mapping(A, B).map_matching().register()

        b = mapper.map(A(123), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.value, "123")
