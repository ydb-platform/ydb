from unittest import TestCase
from terml.nodes import termMaker as t
from ometa.vm_builder import writeBytecode, writeBytecodeRule, writeBytecodeGrammar


class TestVMBuilder(TestCase):
    def test_exactly(self):
        x = t.Exactly("a")
        self.assertEqual(writeBytecode(x),
                         [t.Match("a")])

    def test_apply(self):
        one = t.Action("1")
        x = t.Action("x")
        a = t.Apply("foo", "main", [one, x])
        self.assertEqual(writeBytecode(a),
                         [t.Python('1'),
                          t.Push(),
                          t.Python('x'),
                          t.Push(),
                          t.Call('foo')])

    def test_foreignApply(self):
        one = t.Action("1")
        x = t.Action("x")
        a = t.ForeignApply("thegrammar", "foo", "main", [one, x])
        self.assertEqual(writeBytecode(a),
                         [t.Python('1'),
                          t.Push(),
                          t.Python('x'),
                          t.Push(),
                          t.ForeignCall('thegrammar', 'foo')])

    def test_superApply(self):
        one = t.Action("1")
        x = t.Action("x")
        a = t.Apply("super", "main", [one, x])
        self.assertEqual(writeBytecode(a),
                         [t.Python('1'),
                          t.Push(),
                          t.Python('x'),
                          t.Push(),
                          t.SuperCall('main')])

    def test_many(self):
        xs = t.Many(t.Exactly("x"))
        self.assertEqual(writeBytecode(xs),
                         [t.Choice(3),
                          t.Match("x"),
                          t.Commit(-2)])
        # self.assertEqual(writeBytecode(xs),
        #                  [t.Choice(3),
        #                   t.Match("x"),
        #                   t.PartialCommit(0)])

    def test_many1(self):
        xs = t.Many1(t.Exactly("x"))
        self.assertEqual(writeBytecode(xs),
                         [t.Match('x'),
                          t.Choice(3),
                          t.Match('x'),
                          t.Commit(-2)])

        # self.assertEqual(writeBytecode(xs),
        #                  [t.Match('x'),
        #                   t.Choice(4),
        #                   t.Match('x'),
        #                   t.PartialCommit(1)])

    def test_tripleOr(self):
        xy = t.Or([t.Exactly("x"),
                   t.Exactly("y"),
                   t.Exactly("z")])
        self.assertEqual(writeBytecode(xy),
                         [t.Choice(3),
                          t.Match('x'),
                          t.Commit(5),
                          t.Choice(3),
                          t.Match('y'),
                          t.Commit(2),
                          t.Match('z')])

    def test_doubleOr(self):
        xy = t.Or([t.Exactly("x"),
                   t.Exactly("y")])
        self.assertEqual(writeBytecode(xy),
                         [t.Choice(3),
                          t.Match('x'),
                          t.Commit(2),
                          t.Match('y')])

    def test_singleOr(self):
        x1 = t.Or([t.Exactly("x")])
        x = t.Exactly("x")
        self.assertEqual(writeBytecode(x1),
                         writeBytecode(x))

    def test_optional(self):
        x = t.Optional(t.Exactly("x"))
        self.assertEqual(writeBytecode(x),
                         [t.Choice(3),
                          t.Match('x'),
                          t.Commit(2),
                          t.Python("None")])

    def test_not(self):
        x = t.Not(t.Exactly("x"))
        self.assertEqual(writeBytecode(x),
                         [t.Choice(4),
                          t.Match('x'),
                          t.Commit(1),
                          t.Fail()])

        # self.assertEqual(writeBytecode(x),
        #                  [t.Choice(3),
        #                   t.Match('x'),
        #                   t.FailTwice()])

    def test_lookahead(self):
        x = t.Lookahead(t.Exactly("x"))
        self.assertEqual(writeBytecode(x),
                         [t.Choice(7),
                          t.Choice(4),
                          t.Match('x'),
                          t.Commit(1),
                          t.Fail(),
                          t.Commit(1),
                          t.Fail()])

        # self.assertEqual(writeBytecode(x),
        #                  [t.Choice(5),
        #                   t.Choice(2),
        #                   t.Match('x'),
        #                   t.Commit(1),
        #                   t.Fail()])

    def test_sequence(self):
        x = t.Exactly("x")
        y = t.Exactly("y")
        z = t.And([x, y])
        self.assertEqual(writeBytecode(z),
                         [t.Match('x'),
                          t.Match('y')])

    def test_bind(self):
        x = t.Exactly("x")
        b = t.Bind("var", x)
        self.assertEqual(writeBytecode(b),
                         [t.Match('x'),
                          t.Bind('var')])

    def test_bind_apply(self):
        x = t.Apply("members", "object", [])
        b = t.Bind("m", x)
        self.assertEqual(writeBytecode(b),
                         [t.Call('members'),
                          t.Bind('m')])

    def test_pred(self):
        x = t.Predicate(t.Action("doStuff()"))
        self.assertEqual(writeBytecode(x),
                         [t.Python('doStuff()'),
                          t.Predicate()])

    def test_listpattern(self):
        x = t.List(t.Exactly("x"))
        self.assertEqual(writeBytecode(x),
                         [t.Descend(),
                          t.Match('x'),
                          t.Ascend()])

    def test_rule(self):
        x = t.Rule("foo", t.Exactly("x"))
        k, v = writeBytecodeRule(x)
        self.assertEqual(k, "foo")
        self.assertEqual(v, [t.Match('x')])

    def test_grammar(self):
        r1 = t.Rule("foo", t.Exactly("x"))
        r2 = t.Rule("baz", t.Exactly("y"))
        x = t.Grammar("BuilderTest", False, [r1, r2])
        g = writeBytecodeGrammar(x)
        self.assertEqual(sorted(g.keys()), ['baz', 'foo'])
        self.assertEqual(g['foo'], [t.Match('x')])
        self.assertEqual(g['baz'], [t.Match('y')])

    def test_repeat(self):
        x = t.Repeat(3, 4, t.Exactly('x'))
        self.assertEqual(writeBytecode(x),
                         [t.Python("3"),
                          t.Push(),
                          t.Python("4"),
                          t.Push(),
                          t.RepeatChoice(3),
                          t.Match('x'),
                          t.Commit(-2)])

    def test_consumedby(self):
        x = t.ConsumedBy(t.Exactly('x'))
        self.assertEqual(writeBytecode(x),
                         [t.StartSlice(),
                          t.Match('x'),
                          t.EndSlice()])
