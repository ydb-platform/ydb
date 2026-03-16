#from __future__ import unicode_literals

import operator
from textwrap import dedent

import pytest

from ometa.grammar import OMeta, TermOMeta, TreeTransformerGrammar
from ometa.compat import OMeta1
from ometa.runtime import (ParseError, OMetaBase, OMetaGrammarBase, EOFError,
                           expected, TreeTransformerBase)
from ometa.interp import GrammarInterpreter, TrampolinedGrammarInterpreter
from terml.parser import parseTerm as term
from ometa.test.helpers import TestCase

try:
    basestring
except NameError:
    basestring = str


class HandyWrapper(object):
    """
    Convenient grammar wrapper for parsing strings.
    """
    def __init__(self, klass):
        """
        @param klass: The grammar class to be wrapped.
        """
        self.klass = klass


    def __getattr__(self, name):
        """
        Return a function that will instantiate a grammar and invoke the named
        rule.
        @param: Rule name.
        """
        def doIt(s):
            """
            @param s: The string to be parsed by the wrapped grammar.
            """
            obj = self.klass(s)
            ret, err = obj.apply(name)
            try:
                extra, _ = obj.input.head()
            except EOFError:
                try:
                    return ''.join(ret)
                except TypeError:
                    return ret
            else:
                raise err
        return doIt



class OMeta1TestCase(TestCase):
    """
    Tests of OMeta grammar compilation, with v1 syntax.
    """

    classTested = OMeta1

    def compile(self, grammar):
        """
        Produce an object capable of parsing via this grammar.

        @param grammar: A string containing an OMeta grammar.
        """
        m = self.classTested.makeGrammar(dedent(grammar), 'TestGrammar')
        g = m.createParserClass(OMetaBase, globals())
        return HandyWrapper(g)



    def test_literals(self):
        """
        Input matches can be made on literal characters.
        """
        g = self.compile("digit ::= '1'")
        self.assertEqual(g.digit("1"), "1")
        self.assertRaises(ParseError, g.digit, "4")


    def test_multipleRules(self):
        """
        Grammars with more than one rule work properly.
        """
        g = self.compile("""
                          digit ::= '1'
                          aLetter ::= 'a'
                          """)
        self.assertEqual(g.digit("1"), "1")
        self.assertRaises(ParseError, g.digit, "4")


    def test_escapedLiterals(self):
        """
        Input matches can be made on escaped literal characters.
        """
        g = self.compile(r"newline ::= '\n'")
        self.assertEqual(g.newline("\n"), "\n")


    def test_integers(self):
        """
        Input matches can be made on literal integers.
        """
        g = self.compile("stuff ::= 17 0x1F -2 0177")
        self.assertEqual(g.stuff([17, 0x1f, -2, 0o177]), 0o177)
        self.assertRaises(ParseError, g.stuff, [1, 2, 3])


    def test_star(self):
        """
        Input matches can be made on zero or more repetitions of a pattern.
        """
        g = self.compile("xs ::= 'x'*")
        self.assertEqual(g.xs(""), "")
        self.assertEqual(g.xs("x"), "x")
        self.assertEqual(g.xs("xxxx"), "xxxx")
        self.assertRaises(ParseError, g.xs, "xy")


    def test_plus(self):
        """
        Input matches can be made on one or more repetitions of a pattern.
        """
        g = self.compile("xs ::= 'x'+")
        self.assertEqual(g.xs("x"), "x")
        self.assertEqual(g.xs("xxxx"), "xxxx")
        self.assertRaises(ParseError, g.xs, "xy")
        self.assertRaises(ParseError, g.xs, "")


    def test_sequencing(self):
        """
        Input matches can be made on a sequence of patterns.
        """
        g = self.compile("twelve ::= '1' '2'")
        self.assertEqual(g.twelve("12"), "2");
        self.assertRaises(ParseError, g.twelve, "1")


    def test_alternatives(self):
        """
        Input matches can be made on one of a set of alternatives.
        """
        g = self.compile("digit ::= '0' | '1' | '2'")
        self.assertEqual(g.digit("0"), "0")
        self.assertEqual(g.digit("1"), "1")
        self.assertEqual(g.digit("2"), "2")
        self.assertRaises(ParseError, g.digit, "3")


    def test_optional(self):
        """
        Subpatterns can be made optional.
        """
        g = self.compile("foo ::= 'x' 'y'? 'z'")
        self.assertEqual(g.foo("xyz"), 'z')
        self.assertEqual(g.foo("xz"), 'z')


    def test_apply(self):
        """
        Other productions can be invoked from within a production.
        """
        g = self.compile("""
              digit ::= '0' | '1'
              bits ::= <digit>+
            """)
        self.assertEqual(g.bits('0110110'), '0110110')


    def test_negate(self):
        """
        Input can be matched based on its failure to match a pattern.
        """
        g = self.compile("foo ::= ~'0' <anything>")
        self.assertEqual(g.foo("1"), "1")
        self.assertRaises(ParseError, g.foo, "0")


    def test_ruleValue(self):
        """
        Productions can specify a Python expression that provides the result
        of the parse.
        """
        g = self.compile("foo ::= '1' => 7")
        self.assertEqual(g.foo('1'), 7)


    def test_ruleValueEscapeQuotes(self):
        """
        Escaped quotes are handled properly in Python expressions.
        """
        g = self.compile(r"""escapedChar ::= '\'' => '\\\''""")
        self.assertEqual(g.escapedChar("'"), "\\'")


    def test_ruleValueEscapeSlashes(self):
        """
        Escaped slashes are handled properly in Python expressions.
        """
        g = self.compile(r"""escapedChar ::= '\\' => '\\'""")
        self.assertEqual(g.escapedChar("\\"), "\\")


    def test_lookahead(self):
        """
        Doubled negation does lookahead.
        """
        g = self.compile("""
                         foo ::= ~~(:x) <bar x>
                         bar :x ::= :a :b ?(x == a == b) => x
                         """)
        self.assertEqual(g.foo("11"), '1')
        self.assertEqual(g.foo("22"), '2')


    def test_binding(self):
        """
        The result of a parsing expression can be bound to a name.
        """
        g = self.compile("foo ::= '1':x => int(x) * 2")
        self.assertEqual(g.foo("1"), 2)


    def test_bindingAccess(self):
        """
        Bound names in a rule can be accessed on the grammar's "locals" dict.
        """
        G = self.classTested.makeGrammar(
            "stuff ::= '1':a ('2':b | '3':c)", 'TestGrammar').createParserClass(OMetaBase, {})
        g = G("12")
        self.assertEqual(g.apply("stuff")[0], '2')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['b'], '2')
        g = G("13")
        self.assertEqual(g.apply("stuff")[0], '3')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['c'], '3')


    def test_predicate(self):
        """
        Python expressions can be used to determine the success or failure of a
        parse.
        """
        g = self.compile("""
              digit ::= '0' | '1'
              double_bits ::= <digit>:a <digit>:b ?(a == b) => int(b)
           """)
        self.assertEqual(g.double_bits("00"), 0)
        self.assertEqual(g.double_bits("11"), 1)
        self.assertRaises(ParseError, g.double_bits, "10")
        self.assertRaises(ParseError, g.double_bits, "01")


    def test_parens(self):
        """
        Parens can be used to group subpatterns.
        """
        g = self.compile("foo ::= 'a' ('b' | 'c')")
        self.assertEqual(g.foo("ab"), "b")
        self.assertEqual(g.foo("ac"), "c")


    def test_action(self):
        """
        Python expressions can be run as actions with no effect on the result
        of the parse.
        """
        g = self.compile("""foo ::= ('1'*:ones !(False) !(ones.insert(0, '0')) => ''.join(ones))""")
        self.assertEqual(g.foo("111"), "0111")


    def test_bindNameOnly(self):
        """
        A pattern consisting of only a bind name matches a single element and
        binds it to that name.
        """
        g = self.compile("foo ::= '1' :x '2' => x")
        self.assertEqual(g.foo("132"), "3")


    def test_args(self):
        """
        Productions can take arguments.
        """
        g = self.compile("""
              digit ::= ('0' | '1' | '2'):d => int(d)
              foo :x :ignored ::= (?(int(x) > 1) '9' | ?(int(x) <= 1) '8'):d => int(d)
              baz ::= <digit>:a <foo a None>:b => [a, b]
            """)
        self.assertEqual(g.baz("18"), [1, 8])
        self.assertEqual(g.baz("08"), [0, 8])
        self.assertEqual(g.baz("29"), [2, 9])
        self.assertRaises(ParseError, g.baz, "28")


    def test_patternMatch(self):
        """
        Productions can pattern-match on arguments.
        Also, multiple definitions of a rule can be done in sequence.
        """
        g = self.compile("""
              fact 0                       => 1
              fact :n ::= <fact (n - 1)>:m => n * m
           """)
        self.assertEqual(g.fact([3]), 6)


    def test_listpattern(self):
        """
        Brackets can be used to match contents of lists.
        """
        g = self.compile("""
             digit  ::= :x ?(x.isdigit())          => int(x)
             interp ::= [<digit>:x '+' <digit>:y] => x + y
           """)
        self.assertEqual(g.interp([['3', '+', '5']]), 8)

    def test_listpatternresult(self):
        """
        The result of a list pattern is the entire list.
        """
        g = self.compile("""
             digit  ::= :x ?(x.isdigit())          => int(x)
             interp ::= [<digit>:x '+' <digit>:y]:z => (z, x + y)
        """)
        e = ['3', '+', '5']
        self.assertEqual(g.interp([e]), (e, 8))

    def test_recursion(self):
        """
        Rules can call themselves.
        """
        g = self.compile("""
             interp ::= (['+' <interp>:x <interp>:y] => x + y
                       | ['*' <interp>:x <interp>:y] => x * y
                       | :x ?(isinstance(x, str) and x.isdigit()) => int(x))
             """)
        self.assertEqual(g.interp([['+', '3', ['*', '5', '2']]]), 13)


    def test_leftrecursion(self):
         """
         Left-recursion is detected and compiled appropriately.
         """
         g = self.compile("""
               num ::= (<num>:n <digit>:d   => n * 10 + d
                      | <digit>)
               digit ::= :x ?(x.isdigit()) => int(x)
              """)
         self.assertEqual(g.num("3"), 3)
         self.assertEqual(g.num("32767"), 32767)


    def test_characterVsSequence(self):
        """
        Characters (in single-quotes) are not regarded as sequences.
        """
        g = self.compile("""
        interp ::= ([<interp>:x '+' <interp>:y] => x + y
                  | [<interp>:x '*' <interp>:y] => x * y
                  | :x ?(isinstance(x, basestring) and x.isdigit()) => int(x))
        """)
        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)
        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)


    def test_string(self):
        """
        Strings in double quotes match string objects.
        """
        g = self.compile("""
             interp ::= ["Foo" 1 2] => 3
           """)
        self.assertEqual(g.interp([["Foo", 1, 2]]), 3)

    def test_argEscape(self):
        """
        Regression test for bug #239344.
        """
        g = self.compile("""
            memo_arg :arg ::= <anything> ?(False)
            trick ::= <letter> <memo_arg 'c'>
            broken ::= <trick> | <anything>*
        """)
        self.assertEqual(g.broken('ab'), 'ab')


    def test_comments(self):
        """
        Comments in grammars are accepted and ignored.
        """
        g = self.compile("""
        #comment here
        digit ::= ( '0' #second comment
                  | '1') #another one
        #comments after rules are cool too
        bits ::= <digit>+   #last one
        """)
        self.assertEqual(g.bits('0110110'), '0110110')


    def test_accidental_bareword(self):
        """
        Accidental barewords are treated as syntax errors in the grammar.
        """
        self.assertRaises(ParseError,
                          self.compile, """
                          atom ::= ~('|') :a => Regex_Atom(a)
                                   | ' ' atom:a
                          """)




class OMetaTestCase(TestCase):
    """
    Tests of OMeta grammar compilation.
    """

    classTested = OMeta


    def compile(self, grammar, globals=None):
        """
        Produce an object capable of parsing via this grammar.

        @param grammar: A string containing an OMeta grammar.
        """
        g = self.classTested.makeGrammar(grammar, 'TestGrammar').createParserClass(OMetaBase, globals or {})
        return HandyWrapper(g)


    def test_literals(self):
        """
        Input matches can be made on literal characters.
        """
        g = self.compile("digit = '1'")
        self.assertEqual(g.digit("1"), "1")
        self.assertRaises(ParseError, g.digit, "4")


    def test_escaped_char(self):
        """
        Hex escapes are supported in strings in grammars.
        """
        g = self.compile(r"bel = '\x07'")
        self.assertEqual(g.bel("\x07"), "\x07")


    def test_literals_multi(self):
        """
        Input matches can be made on multiple literal characters at
        once.
        """
        g = self.compile("foo = 'foo'")
        self.assertEqual(g.foo("foo"), "foo")
        self.assertRaises(ParseError, g.foo, "for")

    def test_token(self):
        """
        Input matches can be made on tokens, which default to
        consuming leading whitespace.
        """
        g = self.compile('foo = "foo"')
        self.assertEqual(g.foo("    foo"), "foo")
        self.assertRaises(ParseError, g.foo, "fog")


    def test_multipleRules(self):
        """
        Grammars with more than one rule work properly.
        """
        g = self.compile("""
                          digit = '1'
                          aLetter = 'a'
                          """)
        self.assertEqual(g.digit("1"), "1")
        self.assertRaises(ParseError, g.digit, "4")


    def test_escapedLiterals(self):
        """
        Input matches can be made on escaped literal characters.
        """
        g = self.compile(r"newline = '\n'")
        self.assertEqual(g.newline("\n"), "\n")


    def test_integers(self):
        """
        Input matches can be made on literal integers.
        """
        g = self.compile("stuff = 17 0x1F -2 0177")
        self.assertEqual(g.stuff([17, 0x1f, -2, 0o177]), 0o177)
        self.assertRaises(ParseError, g.stuff, [1, 2, 3])


    def test_star(self):
        """
        Input matches can be made on zero or more repetitions of a pattern.
        """
        g = self.compile("xs = 'x'*")
        self.assertEqual(g.xs(""), "")
        self.assertEqual(g.xs("x"), "x")
        self.assertEqual(g.xs("xxxx"), "xxxx")
        self.assertRaises(ParseError, g.xs, "xy")


    def test_plus(self):
        """
        Input matches can be made on one or more repetitions of a pattern.
        """
        g = self.compile("xs = 'x'+")
        self.assertEqual(g.xs("x"), "x")
        self.assertEqual(g.xs("xxxx"), "xxxx")
        self.assertRaises(ParseError, g.xs, "xy")
        self.assertRaises(ParseError, g.xs, "")


    def test_repeat(self):
        """
        Match repetitions can be specifically numbered.
        """
        g = self.compile("xs = 'x'{2, 4}:n 'x'* -> n")
        self.assertEqual(g.xs("xx"), "xx")
        self.assertEqual(g.xs("xxxx"), "xxxx")
        self.assertEqual(g.xs("xxxxxx"), "xxxx")
        self.assertRaises(ParseError, g.xs, "x")
        self.assertRaises(ParseError, g.xs, "")


    def test_repeat_single(self):
        """
        Match repetitions can be specifically numbered.
        """
        g = self.compile("xs = 'x'{3}:n 'x'* -> n")
        self.assertEqual(g.xs("xxx"), "xxx")
        self.assertEqual(g.xs("xxxxxx"), "xxx")
        self.assertRaises(ParseError, g.xs, "xx")

    def test_repeat_zero(self):
        """
        Match repetitions can be specifically numbered.
        """
        g = self.compile("xs = 'x'{0}:n 'y' -> n")
        self.assertEqual(g.xs("y"), "")
        self.assertRaises(ParseError, g.xs, "xy")

    def test_repeat_zero_n(self):
        """
        Match repetitions can be specifically numbered.
        """
        g = self.compile("""
                         xs :n = 'x'{n}:a 'y' -> a
                         start = xs(0)
                         """)
        self.assertEqual(g.start("y"), "")
        self.assertRaises(ParseError, g.start, "xy")


    def test_repeat_var(self):
        """
        Match repetitions can be variables.
        """
        g = self.compile("xs = (:v -> int(v)):n 'x'{n}:xs 'x'* -> xs")
        self.assertEqual(g.xs("2xx"), "xx")
        self.assertEqual(g.xs("4xxxx"), "xxxx")
        self.assertEqual(g.xs("3xxxxxx"), "xxx")
        self.assertRaises(ParseError, g.xs, "2x")
        self.assertRaises(ParseError, g.xs, "1")


    def test_sequencing(self):
        """
        Input matches can be made on a sequence of patterns.
        """
        g = self.compile("twelve = '1' '2'")
        self.assertEqual(g.twelve("12"), "2");
        self.assertRaises(ParseError, g.twelve, "1")


    def test_alternatives(self):
        """
        Input matches can be made on one of a set of alternatives.
        """
        g = self.compile("digit = '0' | '1' | '2'")
        self.assertEqual(g.digit("0"), "0")
        self.assertEqual(g.digit("1"), "1")
        self.assertEqual(g.digit("2"), "2")
        self.assertRaises(ParseError, g.digit, "3")


    def test_optional(self):
        """
        Subpatterns can be made optional.
        """
        g = self.compile("foo = 'x' 'y'? 'z'")
        self.assertEqual(g.foo("xyz"), 'z')
        self.assertEqual(g.foo("xz"), 'z')


    def test_apply(self):
        """
        Other productions can be invoked from within a production.
        """
        g = self.compile("""
              digit = '0' | '1'
              bits = digit+
            """)
        self.assertEqual(g.bits('0110110'), '0110110')


    def test_negate(self):
        """
        Input can be matched based on its failure to match a pattern.
        """
        g = self.compile("foo = ~'0' anything")
        self.assertEqual(g.foo("1"), "1")
        self.assertRaises(ParseError, g.foo, "0")


    def test_ruleValue(self):
        """
        Productions can specify a Python expression that provides the result
        of the parse.
        """
        g = self.compile("foo = '1' -> 7")
        self.assertEqual(g.foo('1'), 7)


    def test_lookahead(self):
        """
        Doubled negation does lookahead.
        """
        g = self.compile("""
                         foo = ~~(:x) bar(x)
                         bar :x = :a :b ?(x == a == b) -> x
                         """)
        self.assertEqual(g.foo("11"), '1')
        self.assertEqual(g.foo("22"), '2')


    def test_binding(self):
        """
        The result of a parsing expression can be bound to a single name
        or names surrounded by parentheses.
        """
        g = self.compile("foo = '1':x -> int(x) * 2")
        self.assertEqual(g.foo("1"), 2)
        g = self.compile("foo = '1':(x) -> int(x) * 2")
        self.assertEqual(g.foo("1"), 2)
        g = self.compile("foo = (-> 3, 4):(x, y) -> x")
        self.assertEqual(g.foo(""), 3)
        g = self.compile("foo = (-> 1, 2):(x, y) -> int(x) * 2 + int(y)")
        self.assertEqual(g.foo(""), 4)


    def test_bindingAccess(self):
        """
        Bound names in a rule can be accessed on the grammar's "locals" dict.
        """
        G = self.classTested.makeGrammar(
            "stuff = '1':a ('2':b | '3':c)", 'TestGrammar').createParserClass(OMetaBase, {})
        g = G("12")
        self.assertEqual(g.apply("stuff")[0], '2')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['b'], '2')
        g = G("13")
        self.assertEqual(g.apply("stuff")[0], '3')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['c'], '3')
        G = self.classTested.makeGrammar(
            "stuff = '1':a ('2':(b) | '345':(c, d, e))", 'TestGrammar').createParserClass(OMetaBase, {})
        g = G("12")
        self.assertEqual(g.apply("stuff")[0], '2')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['b'], '2')
        g = G("1345")
        self.assertEqual(g.apply("stuff")[0], '345')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['c'], '3')
        self.assertEqual(g.locals['stuff']['d'], '4')
        self.assertEqual(g.locals['stuff']['e'], '5')


    def test_predicate(self):
        """
        Python expressions can be used to determine the success or
        failure of a parse.
        """
        g = self.compile("""
              digit = '0' | '1'
              double_bits = digit:a digit:b ?(a == b) -> int(b)
           """)
        self.assertEqual(g.double_bits("00"), 0)
        self.assertEqual(g.double_bits("11"), 1)
        self.assertRaises(ParseError, g.double_bits, "10")
        self.assertRaises(ParseError, g.double_bits, "01")


    def test_parens(self):
        """
        Parens can be used to group subpatterns.
        """
        g = self.compile("foo = 'a' ('b' | 'c')")
        self.assertEqual(g.foo("ab"), "b")
        self.assertEqual(g.foo("ac"), "c")


    def test_action(self):
        """
        Python expressions can be run as actions with no effect on the
        result of the parse.
        """
        g = self.compile("""foo = ('1'*:ones !(False) !(ones.insert(0, '0')) -> ''.join(ones))""")
        self.assertEqual(g.foo("111"), "0111")


    def test_bindNameOnly(self):
        """
        A pattern consisting of only a bind name matches a single element and
        binds it to that name.
        """
        g = self.compile("foo = '1' :x '2' -> x")
        self.assertEqual(g.foo("132"), "3")


    def test_args(self):
        """
        Productions can take arguments.
        """
        g = self.compile("""
              digit = ('0' | '1' | '2'):d -> int(d)
              foo :x = (?(int(x) > 1) '9' | ?(int(x) <= 1) '8'):d -> int(d)
              baz = digit:a foo(a):b -> [a, b]
            """)
        self.assertEqual(g.baz("18"), [1, 8])
        self.assertEqual(g.baz("08"), [0, 8])
        self.assertEqual(g.baz("29"), [2, 9])
        self.assertRaises(ParseError, g.baz, "28")


    def test_patternMatch(self):
        """
        Productions can pattern-match on arguments.
        Also, multiple definitions of a rule can be done in sequence.
        """
        g = self.compile("""
              fact 0                    -> 1
              fact :n = fact((n - 1)):m -> n * m
           """)
        self.assertEqual(g.fact([3]), 6)


    def test_listpattern(self):
        """
        Brackets can be used to match contents of lists.
        """
        g = self.compile("""
             digit  = :x ?(x.isdigit())     -> int(x)
             interp = [digit:x '+' digit:y] -> x + y
           """)
        self.assertEqual(g.interp([['3', '+', '5']]), 8)

    def test_listpatternresult(self):
        """
        The result of a list pattern is the entire list.
        """
        g = self.compile("""
             digit  = :x ?(x.isdigit())       -> int(x)
             interp = [digit:x '+' digit:y]:z -> (z, x + y)
        """)
        e = ['3', '+', '5']
        self.assertEqual(g.interp([e]), (e, 8))

    def test_recursion(self):
        """
        Rules can call themselves.
        """
        g = self.compile("""
             interp = (['+' interp:x interp:y] -> x + y
                       | ['*' interp:x interp:y] -> x * y
                       | :x ?(isinstance(x, str) and x.isdigit()) -> int(x))
             """)
        self.assertEqual(g.interp([['+', '3', ['*', '5', '2']]]), 13)


    def test_leftrecursion(self):
         """
         Left-recursion is detected and compiled appropriately.
         """
         g = self.compile("""
               num = (num:n digit:d   -> n * 10 + d
                      | digit)
               digit = :x ?(x.isdigit()) -> int(x)
              """)
         self.assertEqual(g.num("3"), 3)
         self.assertEqual(g.num("32767"), 32767)


    def test_characterVsSequence(self):
        """
        Characters (in single-quotes) are not regarded as sequences.
        """
        g = self.compile("""
        interp = ([interp:x '+' interp:y] -> x + y
                  | [interp:x '*' interp:y] -> x * y
                  | :x ?(isinstance(x, basestring) and x.isdigit()) -> int(x))
        """)
        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)
        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)


    def test_stringConsumedBy(self):
        """
        OMeta2's "consumed-by" operator works on strings.
        """
        g = self.compile("""
        ident = <letter (letter | digit)*>
        """)
        self.assertEqual(g.ident("a"), "a")
        self.assertEqual(g.ident("abc"), "abc")
        self.assertEqual(g.ident("a1z"), "a1z")
        self.assertRaises(ParseError, g.ident, "1a")


    def test_label(self):
        """
        Custom labels change the 'expected' in the raised exceptions.
        """
        label = 'Letter not starting with digit'
        g = self.compile("ident = (<letter (letter | digit)*>) ^ (" + label + ")")
        self.assertEqual(g.ident("a"), "a")
        self.assertEqual(g.ident("abc"), "abc")
        self.assertEqual(g.ident("a1z"), "a1z")

        e = self.assertRaises(ParseError, g.ident, "1a")
        self.assertEqual(e, ParseError(0, 0, expected(label)).withMessage([("Custom Exception:", label, None)]))

    def test_label2(self):
        """
        Custom labels change the 'expected' in the raised exceptions.
        """
        label = 'lots of xs'
        g = self.compile("xs = ('x'*) ^ (" + label + ")")
        self.assertEqual(g.xs(""), "")
        self.assertEqual(g.xs("x"), "x")
        self.assertEqual(g.xs("xxx"), "xxx")
        e = self.assertRaises(ParseError, g.xs, "xy")
        self.assertEqual(e, ParseError(0, 1, expected(label)).withMessage([("Custom Exception:", label, None)]))


    def test_listConsumedBy(self):
        """
        OMeta2's "consumed-by" operator works on lists.
        """
        g = self.compile("""
        ands = [<"And" (ors | vals)*>:x] -> x
        ors = [<"Or" vals*:x>] -> x
        vals = 1 | 0
        """)
        self.assertEqual(g.ands([["And", ["Or", 1, 0], 1]]),
                         ["And", ["Or", 1, 0], 1])


    def test_string(self):
        """
        Strings in double quotes match string objects.
        """
        g = self.compile("""
             interp = ["Foo" 1 2] -> 3
           """)
        self.assertEqual(g.interp([["Foo", 1, 2]]), 3)

    def test_argEscape(self):
        """
        Regression test for bug #239344.
        """
        g = self.compile("""
            memo_arg :arg = anything ?(False)
            trick = letter memo_arg('c')
            broken = trick | anything*
        """)
        self.assertEqual(g.broken('ab'), 'ab')


class TermActionGrammarTests(OMetaTestCase):

    classTested = TermOMeta

    def test_binding(self):
        """
        The result of a parsing expression can be bound to a name.
        """
        g = self.compile("foo = '1':x -> mul(int(x), 2)",
                         {"mul": operator.mul})
        self.assertEqual(g.foo("1"), 2)


    def test_bindingAccess(self):
        """
        Bound names in a rule can be accessed on the grammar's "locals" dict.
        """
        G = self.classTested.makeGrammar(
            "stuff = '1':a ('2':b | '3':c)", 'TestGrammar').createParserClass(OMetaBase, {})
        g = G("12")
        self.assertEqual(g.apply("stuff")[0], '2')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['b'], '2')
        g = G("13")
        self.assertEqual(g.apply("stuff")[0], '3')
        self.assertEqual(g.locals['stuff']['a'], '1')
        self.assertEqual(g.locals['stuff']['c'], '3')


    def test_predicate(self):
        """
        Term actions can be used to determine the success or
        failure of a parse.
        """
        g = self.compile("""
              digit = '0' | '1'
              double_bits = digit:a digit:b ?(equal(a, b)) -> int(b)
           """, {"equal": operator.eq})
        self.assertEqual(g.double_bits("00"), 0)
        self.assertEqual(g.double_bits("11"), 1)
        self.assertRaises(ParseError, g.double_bits, "10")
        self.assertRaises(ParseError, g.double_bits, "01")


    def test_action(self):
        """
        Term actions can be run as actions with no effect on the
        result of the parse.
        """
        g = self.compile(
            """foo = ('1'*:ones !(False)
                     !(nconc(ones, '0')) -> join(ones))""",
            {"nconc": lambda lst, val: lst.insert(0, val),
             "join": ''.join})
        self.assertEqual(g.foo("111"), "0111")


    def test_patternMatch(self):
        """
        Productions can pattern-match on arguments.
        Also, multiple definitions of a rule can be done in sequence.
        """
        g = self.compile("""
              fact 0                    -> 1
              fact :n = fact(decr(n)):m -> mul(n, m)
           """, {"mul": operator.mul, "decr": lambda x: x -1})
        self.assertEqual(g.fact([3]), 6)


    def test_listpattern(self):
        """
        Brackets can be used to match contents of lists.
        """
        g = self.compile("""
             digit  = :x ?(x.isdigit())     -> int(x)
             interp = [digit:x '+' digit:y] -> add(x, y)
           """, {"add": operator.add})
        self.assertEqual(g.interp([['3', '+', '5']]), 8)


    def test_listpatternresult(self):
        """
        The result of a list pattern is the entire list.
        """
        g = self.compile("""
             digit  = :x ?(x.isdigit())       -> int(x)
             interp = [digit:x '+' digit:y]:z -> [z, plus(x, y)]
        """, {"plus": operator.add})
        e = ['3', '+', '5']
        self.assertEqual(g.interp([e]), [e, 8])


    def test_recursion(self):
        """
        Rules can call themselves.
        """
        g = self.compile("""
             interp = (['+' interp:x interp:y]   -> add(x, y)
                       | ['*' interp:x interp:y] -> mul(x, y)
                       | :x ?(isdigit(x)) -> int(x))
             """, {"mul": operator.mul,
                   "add": operator.add,
                   "isdigit": lambda x: str(x).isdigit()})
        self.assertEqual(g.interp([['+', '3', ['*', '5', '2']]]), 13)


    def test_leftrecursion(self):
         """
         Left-recursion is detected and compiled appropriately.
         """
         g = self.compile("""
               num = (num:n digit:d   -> makeInt(n, d)
                      | digit)
               digit = :x ?(isdigit(x)) -> int(x)
              """, {"makeInt": lambda x, y: x * 10 + y,
                    "isdigit": lambda x: x.isdigit()})
         self.assertEqual(g.num("3"), 3)
         self.assertEqual(g.num("32767"), 32767)

    def test_characterVsSequence(self):
        """
        Characters (in single-quotes) are not regarded as sequences.
        """
        g = self.compile(
            """
        interp = ([interp:x '+' interp:y] -> add(x, y)
                  | [interp:x '*' interp:y] -> mul(x, y)
                  | :x ?(isdigit(x)) -> int(x))
        """,
            {"add": operator.add, "mul": operator.mul,
             "isdigit": lambda x: isinstance(x, basestring) and x.isdigit()})

        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)
        self.assertEqual(g.interp([['3', '+', ['5', '*', '2']]]), 13)


    def test_string(self):
        """
        Strings in double quotes match string objects.
        """
        g = self.compile("""
             interp = ["Foo" 1 2] -> 3
           """)
        self.assertEqual(g.interp([["Foo", 1, 2]]), 3)

    def test_argEscape(self):
        """
        Regression test for bug #239344.
        """
        g = self.compile("""
            memo_arg :arg = anything ?(False)
            trick = letter memo_arg('c')
            broken = trick | anything*
        """)
        self.assertEqual(g.broken('ab'), 'ab')


    def test_lookahead(self):
        """
        Doubled negation does lookahead.
        """
        g = self.compile("""
                         foo = ~~(:x) bar(x)
                         bar :x = :a :b ?(equal(x, a, b)) -> x
                         """,
                         {"equal": lambda i, j, k: i == j == k})
        self.assertEqual(g.foo("11"), '1')
        self.assertEqual(g.foo("22"), '2')


    def test_args(self):
        """
        Productions can take arguments.
        """
        g = self.compile("""
              digit = ('0' | '1' | '2'):d -> int(d)
              foo :x = (?(gt(int(x), 1)) '9' | ?(lte(int(x), 1)) '8'):d -> int(d)
              baz = digit:a foo(a):b -> [a, b]
            """, {"lte": operator.le, "gt": operator.gt})
        self.assertEqual(g.baz("18"), [1, 8])
        self.assertEqual(g.baz("08"), [0, 8])
        self.assertEqual(g.baz("29"), [2, 9])
        self.assertRaises(ParseError, g.foo, "28")



class PyExtractorTest(TestCase):
    """
    Tests for finding Python expressions in OMeta grammars.
    """

    def findInGrammar(self, expr):
        """
        L{OMeta.pythonExpr()} can extract a single Python expression from a
        string, ignoring the text following it.
        """
        o = OMetaGrammarBase(expr + "\nbaz = ...\n")
        self.assertEqual(o.pythonExpr()[0][0], expr)


    def test_expressions(self):
        """
        L{OMeta.pythonExpr()} can recognize various paired delimiters properly
        and include newlines in expressions where appropriate.
        """
        self.findInGrammar("x")
        self.findInGrammar("(x + 1)")
        self.findInGrammar("{x: (y)}")
        self.findInGrammar("x, '('")
        self.findInGrammar('x, "("')
        self.findInGrammar('x, """("""')
        self.findInGrammar('(x +\n 1)')
        self.findInGrammar('[x, "]",\n 1]')
        self.findInGrammar('{x: "]",\ny: "["}')

        o = OMetaGrammarBase("foo(x[1]])\nbaz = ...\n")
        self.assertRaises(ParseError, o.pythonExpr)
        o = OMetaGrammarBase("foo(x[1]\nbaz = ...\n")
        self.assertRaises(ParseError, o.pythonExpr)


class MakeGrammarTest(TestCase):
    """
    Test the definition of grammars via the 'makeGrammar' method.
    """


    def test_makeGrammar(self):
        results = []
        grammar = """
        digit = :x ?('0' <= x <= '9') -> int(x)
        num = (num:n digit:d !(results.append(True)) -> n * 10 + d
               | digit)
        """
        TestGrammar = OMeta.makeGrammar(grammar, "G").createParserClass(OMetaBase, {'results':results})
        g = TestGrammar("314159")
        self.assertEqual(g.apply("num")[0], 314159)
        self.assertNotEqual(len(results), 0)


    def test_brokenGrammar(self):
        grammar = """
        andHandler = handler:h1 'and handler:h2 -> And(h1, h2)
        """
        e = self.assertRaises(ParseError, OMeta.makeGrammar, grammar,
                              "Foo")
        self.assertEquals(e.position, 57)
        self.assertEquals(e.error, [("message", "end of input")])


    def test_subclassing(self):
        """
        A subclass of an OMeta subclass should be able to call rules on its
        parent, and access variables in its scope.
        """
        grammar1 = """
        dig = :x ?(a <= x <= b) -> int(x)
        """
        TestGrammar1 = OMeta.makeGrammar(grammar1, "G").createParserClass(OMetaBase, {'a':'0', 'b':'9'})

        grammar2 = """
        num = (num:n dig:d -> n * base + d
                | dig)
        """
        TestGrammar2 = OMeta.makeGrammar(grammar2, "G2").createParserClass(TestGrammar1, {'base':10})
        g = TestGrammar2("314159")
        self.assertEqual(g.apply("num")[0], 314159)

        grammar3 = """
        dig = :x ?(a <= x <= b or c <= x <= d) -> int(x, base)
        """
        TestGrammar3 = OMeta.makeGrammar(grammar3, "G3").createParserClass(
            TestGrammar2, {'c':'a', 'd':'f', 'base':16})
        g = TestGrammar3("abc123")
        self.assertEqual(g.apply("num")[0], 11256099)


    def test_super(self):
        """
        Rules can call the implementation in a superclass.
        """
        grammar1 = "expr = letter"
        TestGrammar1 = OMeta.makeGrammar(grammar1, "G").createParserClass(OMetaBase, {})
        grammar2 = "expr = super | digit"
        TestGrammar2 = OMeta.makeGrammar(grammar2, "G2").createParserClass(TestGrammar1, {})
        self.assertEqual(TestGrammar2("x").apply("expr")[0], "x")
        self.assertEqual(TestGrammar2("3").apply("expr")[0], "3")

    def test_foreign(self):
        """
        Rules can call the implementation in a superclass.
        """
        grammar_letter = "expr = letter"
        GrammarLetter = OMeta.makeGrammar(grammar_letter, "G").createParserClass(OMetaBase, {})

        grammar_digit = "expr '5' = digit"
        GrammarDigit = OMeta.makeGrammar(grammar_digit, "H").createParserClass(OMetaBase, {})

        grammar = ("expr = !(grammar_digit_global):grammar_digit "
                        "grammar_letter.expr | grammar_digit.expr('5')")
        TestGrammar = OMeta.makeGrammar(grammar, "I").createParserClass(
            OMetaBase,
            {"grammar_letter": GrammarLetter,
             "grammar_digit_global": GrammarDigit
         })

        self.assertEqual(TestGrammar("x").apply("expr")[0], "x")
        self.assertEqual(TestGrammar("3").apply("expr")[0], "3")


class HandyInterpWrapper(object):
    """
    Convenient grammar wrapper for parsing strings.
    """
    def __init__(self, interp):
        self._interp = interp


    def __getattr__(self, name):
        """
        Return a function that will instantiate a grammar and invoke the named
        rule.
        @param: Rule name.
        """
        def doIt(s):
            """
            @param s: The string to be parsed by the wrapped grammar.
            """
            # totally cheating
            tree = not isinstance(s, basestring)

            input, ret, err = self._interp.apply(s, name, tree)
            try:
                extra, _ = input.head()
            except EOFError:
                try:
                    return ''.join(ret)
                except TypeError:
                    return ret
            else:
                raise err
        return doIt


class InterpTestCase(OMetaTestCase):

    def compile(self, grammar, globals=None):
        """
        Produce an object capable of parsing via this grammar.

        @param grammar: A string containing an OMeta grammar.
        """
        g = OMeta(grammar)
        tree = g.parseGrammar('TestGrammar')
        g = GrammarInterpreter(tree, OMetaBase, globals)
        return HandyInterpWrapper(g)


class TrampolinedInterpWrapper(object):
    """
    Convenient grammar wrapper for parsing strings.
    """
    def __init__(self, tree, globals):
        self._tree = tree
        self._globals = globals


    def __getattr__(self, name):
        """
        Return a function that will instantiate a grammar and invoke the named
        rule.
        @param: Rule name.
        """
        def doIt(s):
            """
            @param s: The string to be parsed by the wrapped grammar.
            """
            tree = not isinstance(s, basestring)
            if tree:
                pytest.skip("Not applicable for push parsing")
            results = []
            def whenDone(val, err):
                results.append(val)
            parser = TrampolinedGrammarInterpreter(self._tree, name, whenDone,
                                                   self._globals)
            for i, c in enumerate(s):
                assert len(results) == 0
                parser.receive(c)
            parser.end()
            if results and parser.input.position == len(parser.input.data):
                try:
                    return ''.join(results[0])
                except TypeError:
                    return results[0]
            else:
                raise parser.currentError
        return doIt



class TrampolinedInterpreterTestCase(OMetaTestCase):

    def compile(self, grammar, globals=None):
        g = OMeta(grammar)
        tree = g.parseGrammar('TestGrammar')
        return TrampolinedInterpWrapper(tree, globals)


    def test_failure(self):
        g = OMeta("""
           foo = 'a':one baz:two 'd'+ 'e' -> (one, two)
           baz = 'b' | 'c'
           """, {})
        tree = g.parseGrammar('TestGrammar')
        i = TrampolinedGrammarInterpreter(
            tree, 'foo', callback=lambda x: setattr(self, 'result', x))
        e = self.assertRaises(ParseError, i.receive, 'foobar')
        self.assertEqual(str(e),
        "\nfoobar\n^\nParse error at line 1, column 0:"
        " expected the character 'a'. trail: []\n")


    def test_stringConsumedBy(self):
        called = []
        grammarSource = "rule = <'x'+>:y -> y"
        grammar = OMeta(grammarSource).parseGrammar("Parser")
        def interp(result, error):
            called.append(result)
        trampoline = TrampolinedGrammarInterpreter(grammar, "rule", interp)
        trampoline.receive("xxxxx")
        trampoline.end()
        self.assertEqual(called, ["xxxxx"])



class TreeTransformerTestCase(TestCase):

    def compile(self, grammar, namespace=None):
        """
        Produce an object capable of parsing via this grammar.

        @param grammar: A string containing an OMeta grammar.
        """
        if namespace is None:
            namespace = globals()
        g = TreeTransformerGrammar.makeGrammar(
            dedent(grammar), 'TestGrammar').createParserClass(
                TreeTransformerBase, namespace)
        return g

    def test_termForm(self):
        g = self.compile("Foo(:left :right) -> left.data + right.data")
        self.assertEqual(g.transform(term("Foo(1, 2)"))[0], 3)

    def test_termFormNest(self):
        g = self.compile("Foo(:left Baz(:right)) -> left.data + right.data")
        self.assertEqual(g.transform(term("Foo(1, Baz(2))"))[0], 3)

    def test_listForm(self):
        g = self.compile("Foo(:left [:first :second]) -> left.data + first.data + second.data")
        self.assertEqual(g.transform(term("Foo(1, [2, 3])"))[0], 6)

    def test_emptyList(self):
        g = self.compile("Foo([]) -> 6")
        self.assertEqual(g.transform(term("Foo([])"))[0], 6)

    def test_emptyArgs(self):
        g = self.compile("Foo() -> 6")
        self.assertEqual(g.transform(term("Foo()"))[0], 6)

    def test_emptyArgsMeansEmpty(self):
        g = self.compile("""
                         Foo() -> 6
                         Foo(:x) -> x
                         """)
        self.assertEqual(g.transform(term("Foo(3)"))[0].data, 3)

    def test_subTransform(self):
        g = self.compile("""
                         Foo(:left @right) -> left.data + right
                         Baz(:front :back) -> front.data * back.data
                         """)
        self.assertEqual(g.transform(term("Foo(1, Baz(2, 3))"))[0], 7)

    def test_defaultExpand(self):
        g = self.compile("""
                         Foo(:left @right) -> left.data + right
                         Baz(:front :back) -> front.data * back.data
                         """)
        self.assertEqual(g.transform(term("Blee(Foo(1, 2), Baz(2, 3))"))[0],
                         term("Blee(3, 6)"))

    def test_wide_template(self):
        g = self.compile(
            """
            Pair(@left @right) --> $left, $right
            Name(@n) = ?(n == "a") --> foo
                     |             --> baz
            """)
        self.assertEqual(g.transform(term('Pair(Name("a"), Name("b"))'))[0],
                         "foo, baz")

    def test_tall_template(self):
        g = self.compile(
            """
            Name(@n) = ?(n == "a") --> foo
                     |             --> baz
            Pair(@left @right) {{{
            $left
            also, $right
            }}}
            """)
        self.assertEqual(g.transform(term('Pair(Name("a"), Name("b"))'))[0],
                         "foo\nalso, baz")

    def test_tall_template_suite(self):
        g = self.compile(
            """
            Name(@n) -> n
            If(@test @suite) {{{
            if $test:
              $suite
            }}}
            """)
        self.assertEqual(g.transform(term('If(Name("a"), [Name("foo"), Name("baz")])'))[0],
                         "if a:\n  foo\n  baz")

    def test_foreign(self):
        """
        Rules can call the implementation in a superclass.
        """
        grammar_letter = "expr = letter"
        GrammarLetter = self.compile(grammar_letter, {})

        grammar_digit = "expr '5' = digit"
        GrammarDigit = self.compile(grammar_digit, {})

        grammar = ("expr = !(grammar_digit_global):grammar_digit "
                   "GrammarLetter.expr | grammar_digit.expr('5')")
        TestGrammar = self.compile(grammar, {
            "GrammarLetter": GrammarLetter,
            "grammar_digit_global": GrammarDigit
        })

        self.assertEqual(TestGrammar("x").apply("expr")[0], "x")
        self.assertEqual(TestGrammar("3").apply("expr")[0], "3")



class ErrorReportingTests(TestCase):


    def compile(self, grammar):
        """
        Produce an object capable of parsing via this grammar.

        @param grammar: A string containing an OMeta grammar.
        """
        g = OMeta.makeGrammar(grammar, 'TestGrammar').createParserClass(OMetaBase, {})
        return HandyWrapper(g)


    def test_rawReporting(self):
        """
        Errors from parsing contain enough info to figure out what was
        expected and where.
        """
        g = self.compile("""

        start = ( (person feeling target)
                  | (adjective animal feeling token("some") target))
        adjective = token("crazy") | token("clever") | token("awesome")
        feeling = token("likes") | token("loves") | token("hates")
        animal = token("monkey") | token("horse") | token("unicorn")
        person = token("crazy horse") | token("hacker")
        target = (token("bananas") | token("robots") | token("americans")
                   | token("bacon"))
        """)

        #some warmup
        g.start("clever monkey hates some robots")
        g.start("awesome unicorn loves some bacon")
        g.start("crazy horse hates americans")
        g.start("hacker likes robots")

        e = self.assertRaises(ParseError, g.start,
                              "clever hacker likes bacon")
        self.assertEqual(e.position, 8)
        self.assertEqual(e.error, [('expected', "token", "horse")])

        e = self.assertRaises(ParseError, g.start,
                              "crazy horse likes some grass")

        #matching "some" means second branch of 'start' is taken
        self.assertEqual(e.position, 23)
        self.assertEqual(set(e.error),
                set([('expected', "token", "bananas"),
                     ('expected', 'token', "bacon"),
                     ('expected', "token", "robots"),
                     ('expected', "token", "americans")]))

        e = self.assertRaises(ParseError, g.start,
                              "crazy horse likes mountains")

        #no "some" means first branch of 'start' is taken...
        #but second is also viable
        self.assertEqual(e.position, 18)
        self.assertEqual(set(e.error),
                set([('expected', "token", "some"),
                     ('expected', "token", "bananas"),
                     ('expected', 'token', "bacon"),
                     ('expected', "token", "robots"),
                     ('expected', "token", "americans")]))


    def test_formattedReporting(self):
        """
        Parse errors can be formatted into a nice human-readable view
        containing the erroneous input and possible fixes.
        """
        g = self.compile("""
        dig = '1' | '2' | '3'
        bits = <dig>+
        """)

        input = "123x321"
        e = self.assertRaises(ParseError, g.bits, input)
        self.assertEqual(e.formatError(),
                         dedent("""
                         123x321
                            ^
                         Parse error at line 1, column 3: expected one of '1', '2', or '3'. trail: [dig]
                         """))
        input = "foo\nbaz\nboz\ncharlie\nbuz"
        e = ParseError(input, 12, expected('token', 'foo') + expected(None, 'b'))
        self.assertEqual(e.formatError(),
                         dedent("""
                         charlie
                         ^
                         Parse error at line 4, column 0: expected one of 'b', or token 'foo'. trail: []
                         """))

        input = '123x321'
        e = ParseError(input, 3, expected('digit'))
        self.assertEqual(e.formatError(),
                         dedent("""
                         123x321
                            ^
                         Parse error at line 1, column 3: expected a digit. trail: []
                         """))

    def test_customLabels(self):
        """
        Custom labels replace the 'expected' part of the exception.
        """
        g = self.compile("""
        dig = ('1' | '2' | '3') ^ (1 2 or 3)
        bits = <dig>+
        """)

        input = "123x321"
        e = self.assertRaises(ParseError, g.bits, input)
        self.assertEqual(e.formatError(),
                         dedent("""
                         123x321
                            ^
                         Parse error at line 1, column 3: expected a 1 2 or 3. trail: [dig]
                         """))

    def test_customLabelsFormatting(self):
        """
        Custom labels replace the 'expected' part of the exception.
        """

        input = "foo\nbaz\nboz\ncharlie\nbuz"
        label = 'Fizz Buzz'
        e = ParseError(input, 12, None).withMessage([("Custom Exception:", label, None)])
        self.assertEqual(e.formatError(),
                         dedent("""
                         charlie
                         ^
                         Parse error at line 4, column 0: expected a Fizz Buzz. trail: []
                         """))

    def test_eof(self):
        """
        EOF should raise a nice error.
        """
        import parsley
        g = parsley.makeGrammar("""
        dig = '1' | '2' | '3'
        bits = <dig>+
        """, {})

        input = '123x321'
        e = self.assertRaises(ParseError, g(input).dig)
        self.assertEqual(e.formatError(),
                         dedent("""
                         123x321
                          ^
                         Parse error at line 1, column 1: expected EOF. trail: []
                         """))
