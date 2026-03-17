# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.immutable
import dns._immutable_attr

try:
    import dns._immutable_ctx as immutable_ctx
    _have_contextvars = True
except ImportError:
    _have_contextvars = False

    class immutable_ctx:
        pass


class ImmutableTestCase(unittest.TestCase):

    def test_immutable_dict_hash(self):
        d1 = dns.immutable.Dict({'a': 1, 'b': 2})
        d2 = dns.immutable.Dict({'b': 2, 'a': 1})
        d3 = {'b': 2, 'a': 1}
        self.assertEqual(d1, d2)
        self.assertEqual(d2, d3)
        self.assertEqual(hash(d1), hash(d2))

    def test_immutable_dict_hash_cache(self):
        d = dns.immutable.Dict({'a': 1, 'b': 2})
        self.assertEqual(d._hash, None)
        h1 = hash(d)
        self.assertEqual(d._hash, h1)
        h2 = hash(d)
        self.assertEqual(h1, h2)

    def test_constify(self):
        items = (
            (bytearray([1, 2, 3]), b'\x01\x02\x03'),
            ((1, 2, 3), (1, 2, 3)),
            ((1, [2], 3), (1, (2,), 3)),
            ([1, 2, 3], (1, 2, 3)),
            ([1, {'a': [1, 2]}],
             (1, dns.immutable.Dict({'a': (1, 2)}))),
            ('hi', 'hi'),
            (b'hi', b'hi'),
        )
        for input, expected in items:
            self.assertEqual(dns.immutable.constify(input), expected)
        self.assertIsInstance(dns.immutable.constify({'a': 1}),
                              dns.immutable.Dict)


class DecoratorTestCase(unittest.TestCase):

    immutable_module = dns._immutable_attr

    def make_classes(self):
        class A:
            def __init__(self, a, akw=10):
                self.a = a
                self.akw = akw

        class B(A):
            def __init__(self, a, b):
                super().__init__(a, akw=20)
                self.b = b
        B = self.immutable_module.immutable(B)

        # note C is immutable by inheritance
        class C(B):
            def __init__(self, a, b, c):
                super().__init__(a, b)
                self.c = c
        C = self.immutable_module.immutable(C)

        class SA:
            __slots__ = ('a', 'akw')
            def __init__(self, a, akw=10):
                self.a = a
                self.akw = akw

        class SB(A):
            __slots__ = ('b')
            def __init__(self, a, b):
                super().__init__(a, akw=20)
                self.b = b
        SB = self.immutable_module.immutable(SB)

        # note SC is immutable by inheritance and has no slots of its own
        class SC(SB):
            def __init__(self, a, b, c):
                super().__init__(a, b)
                self.c = c
        SC = self.immutable_module.immutable(SC)

        return ((A, B, C), (SA, SB, SC))

    def test_basic(self):
        for A, B, C in self.make_classes():
            a = A(1)
            self.assertEqual(a.a, 1)
            self.assertEqual(a.akw, 10)
            b = B(11, 21)
            self.assertEqual(b.a, 11)
            self.assertEqual(b.akw, 20)
            self.assertEqual(b.b, 21)
            c = C(111, 211, 311)
            self.assertEqual(c.a, 111)
            self.assertEqual(c.akw, 20)
            self.assertEqual(c.b, 211)
            self.assertEqual(c.c, 311)
            # changing A is ok!
            a.a = 11
            self.assertEqual(a.a, 11)
            # changing B is not!
            with self.assertRaises(TypeError):
                b.a = 11
            with self.assertRaises(TypeError):
                del b.a

    def test_constructor_deletes_attribute(self):
        class A:
            def __init__(self, a):
                self.a = a
                self.b = a
                del self.b
        A = self.immutable_module.immutable(A)
        a = A(10)
        self.assertEqual(a.a, 10)
        self.assertFalse(hasattr(a, 'b'))

    def test_no_collateral_damage(self):

        # A and B are immutable but not related.  The magic that lets
        # us write to immutable things while initializing B should not let
        # B mess with A.

        class A:
            def __init__(self, a):
                self.a = a
        A = self.immutable_module.immutable(A)

        class B:
            def __init__(self, a, b):
                self.b = a.a + b
                # rudely attempt to mutate innocent immutable bystander 'a'
                a.a = 1000
        B = self.immutable_module.immutable(B)

        a = A(10)
        self.assertEqual(a.a, 10)
        with self.assertRaises(TypeError):
            B(a, 20)
        self.assertEqual(a.a, 10)


@unittest.skipIf(not _have_contextvars, "contextvars not available")
class CtxDecoratorTestCase(DecoratorTestCase):

    immutable_module = immutable_ctx
