import unittest
from ometa.runtime import ParseError
from terml.nodes import Tag, Term, coerceToTerm, TermMaker, termMaker
from terml.parser import TermLParser, character, parseTerm


class TestCase(unittest.TestCase):
    def assertRaises(self, ex, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except ex as e:
            return e
        else:
            assert False, "%r didn't raise %r" % (f, ex)


class TermMakerTests(TestCase):
    def test_make(self):
        m = TermMaker()
        t1 = m.Foo(1, 'a', m.Baz())
        self.assertEqual(t1, parseTerm('Foo(1, "a", Baz)'))


class ParserTest(TestCase):
    """
    Test TermL parser rules.
    """


    def getParser(self, rule):
        def parse(src):
            p = TermLParser(src)
            result, error = p.apply(rule)
            return result
        return parse


    def test_literal(self):
        """
        Literals are parsed to literal terms.
        """
        parse = self.getParser("literal")
        self.assertEqual(parse('"foo bar"'),
                         Term(Tag('.String.'), "foo bar", None, None))
        self.assertEqual(parse("'x'"),
                         Term(Tag('.char.'), 'x', None, None))
        self.assertEqual(parse("0xDECAFC0FFEEBAD"),
                         Term(Tag('.int.'), 0xDECAFC0FFEEBAD, None, None))
        self.assertEqual(parse("0755"),
                         Term(Tag('.int.'), 0o755, None, None))
        self.assertEqual(parse("3.14159E17"),
                         Term(Tag('.float64.'), 3.14159E17, None, None))
        self.assertEqual(parse("1e9"),
                         Term(Tag('.float64.'), 1e9, None, None))
        self.assertEqual(parse("0"), Term(Tag(".int."), 0, None, None))
        self.assertEqual(parse("7"), Term(Tag(".int."), 7, None, None))
        self.assertEqual(parse("-1"), Term(Tag(".int."), -1, None, None))
        self.assertEqual(parse("-3.14"),
                         Term(Tag('.float64.'), -3.14, None, None))
        self.assertEqual(parse("3_000"),
                         Term(Tag('.int.'), 3000, None, None))
        self.assertEqual(parse("0.91"),
                         Term(Tag('.float64.'), 0.91, None, None))
        self.assertEqual(parse("3e-2"),
                         Term(Tag('.float64.'), 3e-2, None, None))
        self.assertEqual(parse("'\\n'"),
                         Term(Tag('.char.'), character("\n"), None, None))
        self.assertEqual(parse('"foo\\nbar"'),
                         Term(Tag('.String.'), "foo\nbar", None, None))
        self.assertEqual(parse("'\\u0061'"),
                         Term(Tag('.char.'), character("a"), None, None))
        self.assertEqual(parse('"z\141p"'),
                         Term(Tag('.String.'), "zap", None, None))
        self.assertEqual(parse('"x\41"'),
                         Term(Tag('.String.'), "x!", None, None))
        self.assertEqual(parse('"foo\\\nbar"'),
                         Term(Tag('.String.'), "foobar", None, None))


    def test_simpleTag(self):
        """
        Tags are parsed properly.
        """

        parse = self.getParser("tag")
        self.assertEqual(parse("foo"), Tag("foo"))
        self.assertEqual(parse('::"foo"'), Tag('::"foo"'))
        self.assertEqual(parse("::foo"), Tag('::foo'))
        self.assertEqual(parse("foo::baz"), Tag('foo::baz'))
        self.assertEqual(parse('foo::"baz"'), Tag('foo::"baz"'))
        self.assertEqual(parse("biz::baz::foo"), Tag('biz::baz::foo'))
        self.assertEqual(parse("foo_yay"), Tag('foo_yay'))
        self.assertEqual(parse("foo$baz32"), Tag('foo$baz32'))
        self.assertEqual(parse("foo-baz.19"), Tag('foo-baz.19'))


    def test_simpleTerm(self):
        """
        Kernel syntax for terms is parsed properly.
        """

        parse = self.getParser("baseTerm")
        self.assertEqual(parse("x"), Term(Tag("x"), None, None, None))
        self.assertEqual(parse("x()"), Term(Tag("x"), None, [], None))
        self.assertEqual(parse("x(1)"), Term(Tag("x"), None,
                                             (Term(Tag(".int."), 1, None, None),),
                                             None))
        self.assertEqual(parse("x(1, 2)"), Term(Tag("x"), None,
                                                (Term(Tag(".int."), 1,
                                                      None, None),
                                                 Term(Tag(".int."), 2,
                                                      None, None)),
                                                None))
        self.assertEqual(parse("1"), Term(Tag(".int."), 1, None, None))
        self.assertEqual(parse('"1"'), Term(Tag(".String."), "1", None, None))
        self.assertRaises(ValueError, parse, "'x'(x)")
        self.assertRaises(ValueError, parse, '3.14(1)')
        self.assertRaises(ValueError, parse, '"foo"(x)')
        self.assertRaises(ValueError, parse, "1(2)")


    def test_fullTerm(self):
        """
        Shortcut syntax for terms is handled.
        """

        self.assertEqual(parseTerm("[x, y, 1]"), parseTerm(".tuple.(x, y, 1)"))
        self.assertEqual(parseTerm("{x, y, 1}"), parseTerm(".bag.(x, y, 1)"))
        self.assertEqual(parseTerm("f {x, y, 1}"), parseTerm("f(.bag.(x, y, 1))"))
        self.assertEqual(parseTerm("a: b"), parseTerm(".attr.(a, b)"))
        self.assertEqual(parseTerm('"a": b'), parseTerm('.attr.("a", b)'))
        self.assertEqual(parseTerm('a: [b]'), parseTerm('.attr.(a, .tuple.(b))'))


    def test_multiline(self):
        """
        Terms spread across multiple lines are parsed correctly.
        """
        single = parseTerm('foo(baz({x: "y", boz: 42}))')
        multi = parseTerm(
                """foo(
                    baz({
                     x: "y",
                     boz: 42}
                   ))""")
        self.assertEqual(multi, single)


    def test_leftovers(self):
        e = self.assertRaises(ParseError, parseTerm, "foo(x) and stuff")
        self.assertEqual(e.position, 7)


    def test_unparse(self):

        def assertRoundtrip(txt):
            self.assertEqual('term(%r)' % (txt,), repr(parseTerm(txt)))
        cases = ["1", "3.25", "f", "f(1)", "f(1, 2)", "f(a, b)",
                  "{a, b}", "[a, b]", "f{1, 2}",  '''{"name": "Robert", attrs: {'c': 3}}''']
        for case in cases:
            assertRoundtrip(case)


    def test_coerce(self):
        self.assertEqual(
            coerceToTerm({3: 4, "a": character('x'), (2, 3): [4, 5]}),
            parseTerm('{"a": \'x\', 3: 4, [2, 3]: [4, 5]}'))


    def test_hash(self):
        t = TermMaker()
        a = t.Arbitrary('foo')
        b = t.Arbitrary('foo')
        self.assertEqual(hash(a), hash(b))
