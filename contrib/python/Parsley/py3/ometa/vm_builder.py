import os
import importlib.resources
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from terml.nodes import Term, coerceToTerm, termMaker as t

HERE = os.path.dirname(__file__)

def writeBytecode(expr):
    print("Gonna compile %s" % (expr,))
    from ometa.grammar import TreeTransformerGrammar
    from ometa.runtime import TreeTransformerBase
    path = os.path.join(HERE, 'vm.parsley')
    Compiler = TreeTransformerGrammar.makeGrammar(importlib.resources.read_text(__package__, 'vm.parsley'),
            "Compiler").createParserClass(TreeTransformerBase, {"t": t})
    return Compiler.transform(expr)[0]


def bytecodeToPython(expr):
    print("Gonna emit %s" % (expr,))
    from ometa.grammar import TreeTransformerGrammar
    from ometa.runtime import TreeTransformerBase
    Emitter = TreeTransformerGrammar.makeGrammar(importlib.resources.read_text(__package__,'vm_emit.parsley'),
            "Emitter").createParserClass(TreeTransformerBase, {"t": t})
    return Emitter.transform(expr)[0]


def writeBytecodeRule(expr):
    e = GrammarEmitter()
    p = PythonWriter(expr)
    p.output(e)
    return list(e.rules.items())[0]


def writeBytecodeGrammar(expr):
    e = GrammarEmitter()
    p = PythonWriter(expr)
    p.output(e)
    return e.rules


class GrammarEmitter(object):
    def __init__(self):
        self.rules = {}
        self.tree = False

    def emitterForRule(self, name):
        e = Emitter()
        self.rules[name] = e.instrs
        return e


class Emitter(object):

    def __init__(self):
        self.instrs = []

    def emit(self, i, label=None):
        self.instrs.append(i)
        if label is not None:
            self.backpatch(label, len(self.instrs) - 1)
        return len(self.instrs) - 1

    def backpatch(self, fromIdx, toIdx):
        old = self.instrs[fromIdx]
        self.instrs[fromIdx] = Term(old.tag, None, [coerceToTerm(toIdx)])

    def patchNext(self, target):
        self.backpatch(target, len(self.instrs))


class PythonWriter(object):
    """
    Converts an OMeta syntax tree into Python source.
    """
    def __init__(self, tree):
        self.tree = tree

    def output(self, out):
        self._generateNode(out, self.tree)

    def _generateNode(self, out, node, debugname=None):
        name = node.tag.name
        args = node.args
        if node.data is not None:
            out.emit(t.Literal(node.data))
            return
        if name == 'null':
            out.emit(t.Python("None"))
        return getattr(self, "generate_"+name)(out, *args, debugname=debugname)

    def generate_Rule(self, out, name, expr, debugname=None):
        e = out.emitterForRule(name.data)
        self._generateNode(e, expr, name.data)

    def generate_Grammar(self, out, name, takesTreeInput, rules,
                         debugname=None):
        for rule in rules.args:
            self._generateNode(out, rule, debugname)
        out.tree = takesTreeInput

    def generate_Apply(self, out, ruleName, codeName, rawArgs, debugname=None):
        for arg in rawArgs.args:
            self._generateNode(out, arg, debugname)
            out.emit(t.Push())
        if ruleName.data == "super":
            out.emit(t.SuperCall(codeName))
        else:
            out.emit(t.Call(ruleName))

    def generate_ForeignApply(self, out, grammarName, ruleName, codeName,
                              rawArgs, debugname=None):
        for arg in rawArgs.args:
            self._generateNode(out, arg, debugname)
            out.emit(t.Push())
        out.emit(t.ForeignCall(grammarName, ruleName))

    def generate_Exactly(self, out, literal, debugname=None):
        out.emit(t.Match(literal.data))

    def generate_Token(self, out, literal, debugname=None):
        self.generate_Exactly(out, literal)

    def generate_Many(self, out, expr, debugname=None):
        L = out.emit(t.Choice())
        self._generateNode(out, expr, debugname)
        L2 = out.emit(t.Commit())
        out.patchNext(L)
        out.backpatch(L2, L)

    def generate_Many1(self, out, expr, debugname=None):
        self._generateNode(out, expr, debugname)
        self.generate_Many(out, expr, debugname)

    def generate_Repeat(self, out, min, max, expr, debugname=None):
        out.emit(t.Python(str(min.data)))
        out.emit(t.Push())
        out.emit(t.Python(str(max.data)))
        out.emit(t.Push())
        L = out.emit(t.RepeatChoice())
        self._generateNode(out, expr, debugname)
        L2 = out.emit(t.Commit())
        out.patchNext(L)
        out.backpatch(L2, L)

    def generate_Optional(self, out, expr, debugname=None):
        """
        Try to parse an expr and continue if it fails.
        """
        L = out.emit(t.Choice())
        self._generateNode(out, expr, debugname)
        L2 = out.emit(t.Commit())
        out.emit(t.Python("None"), label=L)
        out.patchNext(L2)

    def generate_Or(self, out, exprs, debugname=None):
        if len(exprs.args) == 1:
            self._generateNode(out, exprs.args[0])
            return
        L = None
        lcs = []
        for ex in exprs.args[:-1]:
            L = out.emit(t.Choice(), label=L)
            self._generateNode(out, ex)
            lcs.append(out.emit(t.Commit()))
        out.patchNext(L)
        self._generateNode(out, exprs.args[-1])
        for LC in lcs:
            out.patchNext(LC)

    def generate_Not(self, out, expr, debugname=None):
        L1 = out.emit(t.Choice())
        self._generateNode(out, expr)
        L2 = out.emit(t.Commit())
        out.emit(t.Fail(), label=L1)
        out.patchNext(L2)

    def generate_Lookahead(self, out, expr, debugname=None):
        L1 = out.emit(t.Choice())
        L2 = out.emit(t.Choice())
        self._generateNode(out, expr)
        L3 = out.emit(t.Commit(), label=L2)
        out.emit(t.Fail(), label=L3)
        out.patchNext(L1)

    def generate_And(self, out, exprs, debugname=None):
        for ex in exprs.args:
            self._generateNode(out, ex)

    def generate_Bind(self, out, name, expr, debugname=None):
        self._generateNode(out, expr)
        out.emit(t.Bind(name))

    def generate_Predicate(self, out, expr, debugname=None):
        self._generateNode(out, expr)
        out.emit(t.Predicate())

    def generate_Action(self, out, expr, debugname=None):
        out.emit(t.Python(expr.data))

    def generate_Python(self, out, expr, debugname=None):
        out.emit(t.Python(expr.data))

    def generate_List(self, out, expr, debugname=None):
        out.emit(t.Descend())
        self._generateNode(out, expr)
        out.emit(t.Ascend())

    def generate_TermPattern(self, out, name, expr, debugname=None):
        raise NotImplementedError()

    def generate_StringTemplate(self, out, template, debugname=None):
        raise NotImplementedError()

    def generate_ConsumedBy(self, out, expr, debugname=None):
        out.emit(t.StartSlice())
        self._generateNode(out, expr, debugname)
        out.emit(t.EndSlice())
