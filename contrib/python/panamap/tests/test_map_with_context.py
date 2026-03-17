from unittest import TestCase

from panamap import Mapper


class A:
    def __init__(self, a_value: int):
        self.a_value = a_value


class B:
    def __init__(self, b_value: int, contextual: str):
        self.b_value = b_value
        self.contextual = contextual


class TestMapWithContext(TestCase):
    def test_map_with_context(self):
        mapper = Mapper()
        mapper.mapping(A, B).l_to_r_converter(lambda a, ctx: B(a.a_value, ctx["contextual"])).register()

        b = mapper.map(A(123), B, {"contextual": "contextual value"})

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)
        self.assertEqual(b.contextual, "contextual value")
