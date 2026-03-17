from unittest import TestCase

from panamap import Mapper


class A:
    def __init__(self, a_value: int):
        self.a_value = a_value


class B:
    def __init__(self, b_value: int):
        self.b_value = b_value


class C:
    pass


class TestMapWithCustomConverter(TestCase):
    def test_map_with_custom_converter(self):
        def converter(i: int) -> str:
            return f"--{i}--"

        l_to_r_mapper = Mapper()
        l_to_r_mapper.mapping(A, B).l_to_r("a_value", "b_value", converter).register()

        b = l_to_r_mapper.map(A(123), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, "--123--")

    def test_map_non_constructor_field_with_custom_converter(self):
        def converter(i: int) -> str:
            return f"--{i}--"

        l_to_r_mapper = Mapper()
        l_to_r_mapper.mapping(C, B).l_to_r("c_value", "b_value", converter).register()

        c = C()
        c.c_value = 123
        b = l_to_r_mapper.map(c, B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, "--123--")
