
from unittest import TestCase
from terml.parser import parseTerm as term
from terml.quasiterm import quasiterm

class QuasiTermSubstituteTests(TestCase):

    def test_basic(self):
        x = quasiterm("foo($x, $y)").substitute({"x": 1, "y": term("baz")})
        self.assertEqual(x, term("foo(1, baz)"))
        y = quasiterm("foo($0, ${1})").substitute([1, term("baz")])
        self.assertEqual(y, term("foo(1, baz)"))


    def test_withArgs(self):
        x = quasiterm("$x(3)").substitute({"x": term("foo")})
        self.assertEqual(x, term("foo(3)"))
        x = quasiterm("foo($x)").substitute({"x": term("baz(3)")})
        self.assertEqual(x, term("foo(baz(3))"))
        self.assertRaises(TypeError, quasiterm("$x(3)").substitute,
                          {"x": term("foo(3)")})


class QuasiTermMatchTests(TestCase):

    def test_simple(self):
        self.assertEqual(quasiterm("@foo()").match("hello"),
                         {"foo": term('hello')})
        self.assertEqual(quasiterm("@foo").match("hello"),
                         {"foo": term('"hello"')})
        self.assertEqual(quasiterm("@foo").match(term("hello")),
                         {"foo": term('hello')})
        self.assertRaises(TypeError, quasiterm("hello@foo").match, "hello")
        self.assertEqual(quasiterm(".String.@foo").match(term('"hello"')),
                         {"foo": term('"hello"')})
        self.assertEqual(quasiterm(".String.@foo").match("hello"),
                         {"foo": term('"hello"')})
        self.assertEqual(quasiterm("hello@foo").match(term("hello(3, 4)")),
                         {"foo": term("hello(3, 4)")})
        self.assertEqual(quasiterm("hello@bar()").match(term("hello")),
                         {"bar": term("hello")})
        self.assertEqual(quasiterm("hello@foo()").match("hello"),
                         {"foo": term("hello")})
        self.assertEqual(quasiterm("Foo(@x, Bar(1, @y))").match(
                term("Foo(a, Bar(1, 2))")),
                         {"x": term("a"), "y": term("2")})
        self.assertRaises(TypeError, quasiterm("Foo(@x, Bar(3, @y))").match,
                          term("Foo(a, Bar(1, 2))"))
        self.assertRaises(TypeError, quasiterm("hello@foo()").match,
                          term("hello(3, 4)"))
        self.assertRaises(TypeError, quasiterm("hello@foo").match,
                          "hello")
