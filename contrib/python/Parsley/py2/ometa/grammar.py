# -*- test-case-name: ometa.test.test_pymeta -*-
"""
Public interface to OMeta, as well as the grammars used to compile grammar
definitions.
"""
import os.path
import string
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from terml.nodes import termMaker as t
import ometa
from ometa._generated.parsley import createParserClass as makeBootGrammar
from ometa.builder import TermActionPythonWriter, moduleFromGrammar, TextWriter
from ometa.runtime import OMetaBase, OMetaGrammarBase

OMeta = makeBootGrammar(OMetaGrammarBase, globals())


def loadGrammar(pkg, name, globals, superclass=OMetaBase):
    try:
        m = __import__('.'.join([pkg.__name__, '_generated', name]),
                       fromlist=[name], level=0)
    except ImportError:
        base = os.path.dirname(os.path.abspath(pkg.__file__))
        src = open(os.path.join(base, name + ".parsley")).read()
        m = OMeta.makeGrammar(src, name)

    return m.createParserClass(superclass, globals)

class TermOMeta(loadGrammar(
        ometa, "parsley_termactions",
        globals(), superclass=OMeta)):

    _writer = TermActionPythonWriter

    @classmethod
    def makeGrammar(cls, grammar, name):
        """
        Define a new parser class with the rules in the given grammar.

        @param grammar: A string containing a PyMeta grammar.
        @param globals: A dict of names that should be accessible by this
        grammar.
        @param name: The name of the class to be generated.
        @param superclass: The class the generated class is a child of.
        """
        g = cls(grammar)
        tree = g.parseGrammar(name)
        modname = "pymeta_grammar__" + name
        filename = "/pymeta_generated_code/" + modname + ".py"
        source = g.writeTerm(tree, grammar)
        return moduleFromGrammar(source, name, modname, filename)

    def writeTerm(self, term, grammar):
        f = StringIO()
        pw = self._writer(term, grammar)
        out = TextWriter(f)
        pw.output(out)
        return f.getvalue().strip()

    def rule_term(self):
        from terml.parser import TermLParser
        tp = TermLParser('')
        tp.input = self.input
        self.input.setMemo('term', None)
        val, err = tp.apply('term')
        self.input = tp.input
        return val, err

    def rule_term_arglist(self):
        from terml.parser import TermLParser
        tp = TermLParser('')
        tp.input = self.input
        val, err = tp.apply('argList')
        self.input = tp.input
        return val, err

TreeTransformerGrammar = loadGrammar(
    ometa, "parsley_tree_transformer",
    globals(), superclass=OMeta)
