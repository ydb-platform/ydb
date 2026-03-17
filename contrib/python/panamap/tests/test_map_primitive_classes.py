from unittest import TestCase

from panamap import Mapper, ImproperlyConfiguredException


class A:
    def __init__(self, a_value: int):
        self.a_value = a_value


class B:
    def __init__(self, b_value: int):
        self.b_value = b_value


class TestMapPrimitiveClasses(TestCase):
    def test_map_primitive_class_l_to_r(self):
        l_to_r_mapper = Mapper()
        l_to_r_mapper.mapping(A, B).l_to_r("a_value", "b_value").register()

        b = l_to_r_mapper.map(A(123), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)

    def test_map_primitive_class_r_to_l(self):
        r_to_l_mapper = Mapper()
        r_to_l_mapper.mapping(A, B).r_to_l("a_value", "b_value").register()

        a = r_to_l_mapper.map(B(456), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.a_value, 456)

    def test_map_primitive_class_bidirectional(self):
        bi_d_mapper = Mapper()
        bi_d_mapper.mapping(A, B).bidirectional("a_value", "b_value").register()

        b = bi_d_mapper.map(A(123), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)

        a = bi_d_mapper.map(B(456), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.a_value, 456)

    def test_map_non_constructor_attributes(self):
        mapper = Mapper()
        mapper.mapping(A, B).l_to_r("a_value", "b_value").l_to_r("additional_value", "additional_value").register()

        a = A(123)
        a.additional_value = 456
        b = mapper.map(a, B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)
        self.assertEqual(b.additional_value, 456)

    def test_map_with_converter(self):
        mapper = Mapper()
        mapper.mapping(A, B).l_to_r_converter(lambda a: B(a.a_value + 10)).register()

        b = mapper.map(A(123), B)
        self.assertEqual(b.b_value, 133)

    def test_raise_when_converter_and_mapping_defined(self):
        with self.assertRaises(ImproperlyConfiguredException):
            mapper = Mapper()
            mapper.mapping(A, B).l_to_r_converter(lambda a: B(a.a_value + 10)).l_to_r("a_value", "b_value").register()
