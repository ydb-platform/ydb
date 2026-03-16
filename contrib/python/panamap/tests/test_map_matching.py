from unittest import TestCase

from panamap import Mapper


class TestMapMatching(TestCase):
    def test_map_matching(self):
        class A:
            def __init__(self, a_value: int, common_value: str):
                self.a_value = a_value
                self.common_value = common_value

        class B:
            def __init__(self, b_value: int, common_value: str):
                self.b_value = b_value
                self.common_value = common_value

        mapper = Mapper()
        mapper.mapping(A, B).bidirectional("a_value", "b_value").map_matching().register()

        b = mapper.map(A(123, "456"), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)
        self.assertEqual(b.common_value, "456")

        a = mapper.map(B(789, "xyz"), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.a_value, 789)
        self.assertEqual(a.common_value, "xyz")

    def test_map_matching_ignore_case(self):
        class A:
            def __init__(self, a_value: int, common_value: str):
                self.a_value = a_value
                self.common_value = common_value

        class B:
            def __init__(self, b_value: int, CommonValue: str):
                self.b_value = b_value
                self.CommonValue = CommonValue

        mapper = Mapper()
        mapper.mapping(A, B).bidirectional("a_value", "b_value").map_matching(ignore_case=True).register()

        b = mapper.map(A(123, "456"), B)

        self.assertEqual(b.__class__, B)
        self.assertEqual(b.b_value, 123)
        self.assertEqual(b.CommonValue, "456")

        a = mapper.map(B(789, "xyz"), A)

        self.assertEqual(a.__class__, A)
        self.assertEqual(a.a_value, 789)
        self.assertEqual(a.common_value, "xyz")
