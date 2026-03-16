from textwrap import dedent
import unittest

from ometa.builder import writePython
from terml.nodes import termMaker as t
from terml.parser import parseTerm as term

def dd(txt):
    return dedent(txt).strip()

class PythonWriterTests(unittest.TestCase):
    """
    Tests for generating Python source from an AST.
    """

    def test_exactly(self):
        """
        Test generation of code for the 'exactly' pattern.
        """

        x = t.Exactly("x")
        self.assertEqual(writePython(x ,""),
                         dd("""
                            _G_exactly_1, lastError = self.exactly('x')
                            self.considerError(lastError, None)
                            _G_exactly_1
                            """))



    def test_apply(self):
        """
        Test generation of code for rule application.
        """

        one = t.Action("1")
        x = t.Action("x")
        a = t.Apply("foo", "main", [one, x])
        self.assertEqual(writePython(a, ""),
                         dd("""
                            _G_python_1, lastError = (1), None
                            self.considerError(lastError, None)
                            _G_python_2, lastError = eval('x', self.globals, _locals), None
                            self.considerError(lastError, None)
                            _G_apply_3, lastError = self._apply(self.rule_foo, "foo", [_G_python_1, _G_python_2])
                            self.considerError(lastError, None)
                            _G_apply_3
                            """))

    def test_foreignApply(self):
        """
        Test generation of code for calling foreign grammar's rules.
        """
        one = t.Action("1")
        x = t.Action("x")
        a = t.ForeignApply("thegrammar", "foo", "main", [one, x])
        self.assertEqual(writePython(a, ""),
                         dd("""
                            _G_python_1, lastError = (1), None
                            self.considerError(lastError, None)
                            _G_python_2, lastError = eval('x', self.globals, _locals), None
                            self.considerError(lastError, None)
                            _G_apply_3, lastError = self.foreignApply("thegrammar", "foo", self.globals, _locals, _G_python_1, _G_python_2)
                            self.considerError(lastError, None)
                            _G_apply_3
                            """))

    def test_superApply(self):
        """
        Test generation of code for calling the superclass' implementation of
        the current rule.
        """
        one = t.Action("1")
        x = t.Action("x")
        a = t.Apply("super", "main", [one, x])
        self.assertEqual(writePython(a, ""),
                         dd("""
                            _G_python_1, lastError = (1), None
                            self.considerError(lastError, None)
                            _G_python_2, lastError = eval('x', self.globals, _locals), None
                            self.considerError(lastError, None)
                            _G_apply_3, lastError = self.superApply("main", _G_python_1, _G_python_2)
                            self.considerError(lastError, None)
                            _G_apply_3
                            """))


    def test_many(self):
        """
        Test generation of code for matching zero or more instances of
        a pattern.
        """

        xs = t.Many(t.Exactly("x"))
        self.assertEqual(writePython(xs, ""),
                         dd("""
                            def _G_many_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_many_3, lastError = self.many(_G_many_1)
                            self.considerError(lastError, None)
                            _G_many_3
                            """))


    def test_many1(self):
        """
        Test generation of code for matching one or more instances of
        a pattern.
        """

        xs = t.Many1(t.Exactly("x"))
        self.assertEqual(writePython(xs, ""),
                         dd("""
                            def _G_many1_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_many1_3, lastError = self.many(_G_many1_1, _G_many1_1())
                            self.considerError(lastError, None)
                            _G_many1_3
                            """))



    def test_or(self):
        """
        Test code generation for a sequence of alternatives.
        """

        xy = t.Or([t.Exactly("x"),
                               t.Exactly("y")])
        self.assertEqual(writePython(xy, ""),
                         dd("""
                            def _G_or_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            def _G_or_3():
                                _G_exactly_4, lastError = self.exactly('y')
                                self.considerError(lastError, None)
                                return (_G_exactly_4, self.currentError)
                            _G_or_5, lastError = self._or([_G_or_1, _G_or_3])
                            self.considerError(lastError, None)
                            _G_or_5
                            """))

    def test_singleOr(self):
        """
        Test code generation for a sequence of alternatives.
        """

        x1 = t.Or([t.Exactly("x")])
        x = t.Exactly("x")
        self.assertEqual(writePython(x, ""), writePython(x1, ""))


    def test_optional(self):
        """
        Test code generation for optional terms.
        """
        x = t.Optional(t.Exactly("x"))
        self.assertEqual(writePython(x, ""),
                         dd("""
                            def _G_optional_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            def _G_optional_3():
                                return (None, self.input.nullError())
                            _G_or_4, lastError = self._or([_G_optional_1, _G_optional_3])
                            self.considerError(lastError, None)
                            _G_or_4
                            """))


    def test_not(self):
        """
        Test code generation for negated terms.
        """
        x = t.Not(t.Exactly("x"))
        self.assertEqual(writePython(x ,""),
                         dd("""
                            def _G_not_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_not_3, lastError = self._not(_G_not_1)
                            self.considerError(lastError, None)
                            _G_not_3
                            """))


    def test_lookahead(self):
        """
        Test code generation for lookahead expressions.
        """
        x = t.Lookahead(t.Exactly("x"))
        self.assertEqual(writePython(x, ""),
                         dd("""
                            def _G_lookahead_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_lookahead_3, lastError = self.lookahead(_G_lookahead_1)
                            self.considerError(lastError, None)
                            _G_lookahead_3
                            """))



    def test_sequence(self):
        """
        Test generation of code for sequence patterns.
        """
        x = t.Exactly("x")
        y = t.Exactly("y")
        z = t.And([x, y])
        self.assertEqual(writePython(z, ""),
                         dd("""
                            _G_exactly_1, lastError = self.exactly('x')
                            self.considerError(lastError, None)
                            _G_exactly_2, lastError = self.exactly('y')
                            self.considerError(lastError, None)
                            _G_exactly_2
                            """))


    def test_bind(self):
        """
        Test code generation for variable assignment.
        """
        x = t.Exactly("x")
        b = t.Bind("var", x)
        self.assertEqual(writePython(b, ""),
                         dd("""
                            _G_exactly_1, lastError = self.exactly('x')
                            self.considerError(lastError, None)
                            _locals['var'] = _G_exactly_1
                            _G_exactly_1
                            """))


    def test_pred(self):
        """
        Test code generation for predicate expressions.
        """
        x = t.Predicate(t.Exactly("x"))
        self.assertEqual(writePython(x, ""),
                         dd("""
                            def _G_pred_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_pred_3, lastError = self.pred(_G_pred_1)
                            self.considerError(lastError, None)
                            _G_pred_3
                            """))


    def test_action(self):
        """
        Test code generation for semantic actions.
        """
        x = t.Action("doStuff()")
        self.assertEqual(writePython(x, ""),
                         dd("""
                            _G_python_1, lastError = eval('doStuff()', self.globals, _locals), None
                            self.considerError(lastError, None)
                            _G_python_1
                            """))


    def test_expr(self):
        """
        Test code generation for semantic predicates.
        """
        x = t.Action("returnStuff()")
        self.assertEqual(writePython(x, ""),
                         dd("""
                            _G_python_1, lastError = eval('returnStuff()', self.globals, _locals), None
                            self.considerError(lastError, None)
                            _G_python_1
                            """))

    def test_label(self):
        """
        Test code generation for custom labels.
        """
        xs = t.Label(t.Exactly("x"), 'CustomLabel')
        self.assertEqual(writePython(xs, ""),
                         dd("""
                                def _G_label_1():
                                    _G_exactly_2, lastError = self.exactly('x')
                                    self.considerError(lastError, None)
                                    return (_G_exactly_2, self.currentError)
                                _G_label_3, lastError = self.label(_G_label_1, "CustomLabel")
                                self.considerError(lastError, None)
                                _G_label_3
                                """))

    def test_listpattern(self):
        """
        Test code generation for list patterns.
        """
        x = t.List(t.Exactly("x"))
        self.assertEqual(writePython(x, ""),
                         dd("""
                            def _G_listpattern_1():
                                _G_exactly_2, lastError = self.exactly('x')
                                self.considerError(lastError, None)
                                return (_G_exactly_2, self.currentError)
                            _G_listpattern_3, lastError = self.listpattern(_G_listpattern_1)
                            self.considerError(lastError, None)
                            _G_listpattern_3
                            """))


    def test_markAsTree(self):
        """
        Grammars containing list patterns are marked as taking
        tree-shaped input rather than character streams.
        """
        x = t.Rule("foo", t.List(
                t.Exactly("x")))
        g = t.Grammar("TestGrammar", True, [x])
        self.assert_("\n        tree = True\n" in writePython(g, ""))


    def test_rule(self):
        """
        Test generation of entire rules.
        """

        x = t.Rule("foo", t.Exactly("x"))
        self.assertEqual(writePython(x, ""),
                         dd("""
                            def rule_foo(self):
                                _locals = {'self': self}
                                self.locals['foo'] = _locals
                                _G_exactly_1, lastError = self.exactly('x')
                                self.considerError(lastError, 'foo')
                                return (_G_exactly_1, self.currentError)
                            """))


    def test_grammar(self):
        """
        Test generation of an entire grammar.
        """
        r1 = t.Rule("foo", t.Exactly("x"))
        r2 = t.Rule("baz", t.Exactly("y"))
        x = t.Grammar("BuilderTest", False, [r1, r2])
        self.assertEqual(
            writePython(x, ""),
            dd("""
               def createParserClass(GrammarBase, ruleGlobals):
                   if ruleGlobals is None:
                       ruleGlobals = {}
                   class BuilderTest(GrammarBase):
                       def rule_foo(self):
                           _locals = {'self': self}
                           self.locals['foo'] = _locals
                           _G_exactly_1, lastError = self.exactly('x')
                           self.considerError(lastError, 'foo')
                           return (_G_exactly_1, self.currentError)


                       def rule_baz(self):
                           _locals = {'self': self}
                           self.locals['baz'] = _locals
                           _G_exactly_2, lastError = self.exactly('y')
                           self.considerError(lastError, 'baz')
                           return (_G_exactly_2, self.currentError)


                   if BuilderTest.globals is not None:
                       BuilderTest.globals = BuilderTest.globals.copy()
                       BuilderTest.globals.update(ruleGlobals)
                   else:
                       BuilderTest.globals = ruleGlobals
                   return BuilderTest
                            """))
