import unittest


class Test__is_field(unittest.TestCase):

    def _callFUT(self, value):
        from zope.schema.interfaces import _is_field
        return _is_field(value)

    def test_non_fields(self):
        self.assertEqual(self._callFUT(None), False)
        self.assertEqual(self._callFUT(0), False)
        self.assertEqual(self._callFUT(0.0), False)
        self.assertEqual(self._callFUT(True), False)
        self.assertEqual(self._callFUT(b''), False)
        self.assertEqual(self._callFUT(''), False)
        self.assertEqual(self._callFUT(()), False)
        self.assertEqual(self._callFUT([]), False)
        self.assertEqual(self._callFUT({}), False)
        self.assertEqual(self._callFUT(set()), False)
        self.assertEqual(self._callFUT(frozenset()), False)
        self.assertEqual(self._callFUT(object()), False)

    def test_w_normal_fields(self):
        from zope.schema import Bytes
        from zope.schema import Decimal
        from zope.schema import Float
        from zope.schema import Int
        from zope.schema import Text
        self.assertEqual(self._callFUT(Text()), True)
        self.assertEqual(self._callFUT(Bytes()), True)
        self.assertEqual(self._callFUT(Int()), True)
        self.assertEqual(self._callFUT(Float()), True)
        self.assertEqual(self._callFUT(Decimal()), True)

    def test_w_explicitly_provided(self):
        from zope.interface import directlyProvides

        from zope.schema.interfaces import IField

        class Foo:
            pass

        foo = Foo()
        self.assertEqual(self._callFUT(foo), False)
        directlyProvides(foo, IField)
        self.assertEqual(self._callFUT(foo), True)


class Test__fields(unittest.TestCase):

    def _callFUT(self, values):
        from zope.schema.interfaces import _fields
        return _fields(values)

    def test_empty_containers(self):
        self.assertEqual(self._callFUT(()), True)
        self.assertEqual(self._callFUT([]), True)

    def test_w_non_fields(self):
        self.assertEqual(self._callFUT([None]), False)
        self.assertEqual(self._callFUT(['']), False)
        self.assertEqual(self._callFUT([object()]), False)

    def test_w_fields(self):
        from zope.schema import Bytes
        from zope.schema import Decimal
        from zope.schema import Float
        from zope.schema import Int
        from zope.schema import Text
        self.assertEqual(self._callFUT([Text()]), True)
        self.assertEqual(self._callFUT([Bytes()]), True)
        self.assertEqual(self._callFUT([Int()]), True)
        self.assertEqual(self._callFUT([Float()]), True)
        self.assertEqual(self._callFUT([Decimal()]), True)
        self.assertEqual(
            self._callFUT([Text(), Bytes(), Int(), Float(), Decimal()]),
            True
        )

    def test_w_mixed(self):
        from zope.schema import Bytes
        from zope.schema import Decimal
        from zope.schema import Float
        from zope.schema import Int
        from zope.schema import Text
        self.assertEqual(self._callFUT([Text(), 0]), False)
        self.assertEqual(
            self._callFUT([Text(), Bytes(), Int(), Float(), Decimal(), 0]),
            False
        )

    def test_bool_not_required(self):
        """If class Bool is used as a schema itself, it must not be required.
        """
        from zope.schema.interfaces import IBool

        # treat IBool as schema with fields
        field = IBool.get("required")
        self.assertFalse(field.required)

    def test_bool_defaults_to_false(self):
        """If class Bool is used as a schema itself, it must default to False
        """
        from zope.schema.interfaces import IBool

        # treat IBool as schema with fields
        field = IBool.get("default")
        self.assertFalse(field.default)
