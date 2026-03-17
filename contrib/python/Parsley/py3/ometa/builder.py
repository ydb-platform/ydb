# -*- test-case-name: ometa.test.test_builder -*-
import ast
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from types import ModuleType as module
import linecache, sys
from terml.nodes import Term, Tag, coerceToTerm

class TextWriter(object):

    stepSize = 4

    def __init__(self, f, indentSteps=0):
        self.file = f
        self.indentSteps = indentSteps


    def writeln(self, data):
        if data:
            self.file.write(" " * (self.indentSteps * self.stepSize))
            self.file.write(data)
        self.file.write("\n")

    def indent(self):
        return TextWriter(self.file, self.indentSteps + 1)


class PythonWriter(object):
    """
    Converts an OMeta syntax tree into Python source.
    """
    def __init__(self, tree, grammarText):
        self.tree = tree
        self.grammarText = grammarText
        self.gensymCounter = 0
        self.compiledExprCache = None


    def _generate(self, out, expr, retrn=False, debugname=None):
        result = self._generateNode(out, expr, debugname=debugname)
        if retrn:
            out.writeln("return (%s, self.currentError)" % (result,))
        elif result:
            out.writeln(result)

    def output(self, out):
        self._generate(out, self.tree)


    def _generateNode(self, out, node, debugname=None):
        name = node.tag.name
        args = node.args
        if name == 'null':
            return 'None'
        if node.span:
            out.writeln("self._trace(%r, %r, self.input.position)"
                        % (self.grammarText[slice(*node.span)], node.span))
        return getattr(self, "generate_"+name)(out, *args, debugname=debugname)


    def _gensym(self, name):
        """
        Produce a unique name for a variable in generated code.
        """
        self.gensymCounter += 1
        return "_G_%s_%s" % (name, self.gensymCounter)


    def _newThunkFor(self, out, name, expr):
        """
        Define a new function of no arguments.
        @param name: The name of the rule generating this thunk.
        @param expr: A list of lines of Python code.
        """

        fname = self._gensym(name)
        self._writeFunction(out, fname, (),  expr)
        return fname


    def _expr(self, out, typ, e, debugname=None):
        """
        Generate the code needed to execute the expression, and return the
        variable name bound to its value.
        """
        name = self._gensym(typ)
        out.writeln("%s, lastError = %s" % (name, e))
        out.writeln("self.considerError(lastError, %r)" % (debugname and debugname.data,))
        return name


    def _writeFunction(self, out, fname, arglist, expr):
        """
        Generate a function.
        @param out: the TextWriter used for output.
        @param fname: The name of the function generated.
        @param arglist: A list of parameter names.
        @param expr: The term tree to generate the function body from.
        """

        out.writeln("def %s(%s):" % (fname, ", ".join(arglist)))
        self._generate(out.indent(), expr, retrn=True)
        return fname


    def compilePythonExpr(self, out, expr, debugname=None):
        """
        Generate code for running embedded Python expressions.
        """
        try:
            ast.literal_eval(expr)
            return self._expr(out, 'python', '(' + expr + '), None', debugname)
        except ValueError:
            if self.compiledExprCache is None:
                return self._expr(out, 'python',
                                'eval(%r, self.globals, _locals), None' % (expr,),
                                debugname)
            else:
                if expr in self.compiledExprCache:
                    sym = self.compiledExprCache[expr]
                else:
                    sym = self.compiledExprCache[expr] = self._gensym('expr')

                return self._expr(out, 'python',
                                'eval(self.%s, self.globals, _locals), None' % (sym,),
                                debugname)

    def _convertArgs(self, out, rawArgs, debugname):
        return [self._generateNode(out, x, debugname) for x in rawArgs]


    def generate_Apply(self, out, ruleName, codeName, rawArgs, debugname=None):
        """
        Create a call to self.apply(ruleName, *args).
        """
        ruleName = ruleName.data
        args = self._convertArgs(out, rawArgs.args, debugname)
        if ruleName == 'super':
            return self._expr(out, 'apply', 'self.superApply("%s", %s)'
                              % (codeName.data, ', '.join(args)),
                              debugname)
        return self._expr(out, 'apply', 'self._apply(self.rule_%s, "%s", [%s])'
                          % (ruleName, ruleName, ', '.join(args)),
                          debugname)

    def generate_ForeignApply(self, out, grammarName, ruleName, codeName,
            rawArgs, debugname=None):
        """
        Create a call to self.foreignApply(ruleName, *args)
        """
        grammarName = grammarName.data
        ruleName = ruleName.data
        args = self._convertArgs(out, rawArgs.args, debugname)
        call = ('self.foreignApply("%s", "%s", self.globals, _locals, %s)'
                            % (grammarName, ruleName, ', '.join(args)))
        return self._expr(out, 'apply', call, debugname)

    def generate_Exactly(self, out, literal, debugname=None):
        """
        Create a call to self.exactly(expr).
        """
        return self._expr(out, 'exactly', 'self.exactly(%r)' % (literal.data,), debugname)


    def generate_Token(self, out, literal, debugname=None):
        if self.takesTreeInput:
            return self.generate_Exactly(out, literal, debugname)
        else:
            return self._expr(out, 'apply',
                              'self._apply(self.rule_token, "token", ["%s"])'
                              % (literal.data,),
                              debugname)


    def generate_Many(self, out, expr, debugname=None):
        """
        Create a call to self.many(lambda: expr).
        """
        fname = self._newThunkFor(out, "many", expr)
        return self._expr(out, 'many', 'self.many(%s)' % (fname,), debugname)


    def generate_Many1(self, out, expr, debugname=None):
        """
        Create a call to self.many(lambda: expr).
        """
        fname = self._newThunkFor(out, "many1", expr)
        return self._expr(out, 'many1', 'self.many(%s, %s())' % (fname, fname), debugname)


    def generate_Repeat(self, out, min, max, expr, debugname=None):
        """
        Create a call to self.repeat(min, max, lambda: expr).
        """
        fname = self._newThunkFor(out, "repeat", expr)
        if min.tag.name == '.int.':
            min = min.data
        else:
            min = '_locals["%s"]' % min.data
        if max.tag.name == '.int.':
            max = max.data
        else:
            max = '_locals["%s"]' % max.data
        if min == max == 0:
            return "''"
        return self._expr(out, 'repeat', 'self.repeat(%s, %s, %s)'
                          % (min, max, fname))

    def generate_Optional(self, out, expr, debugname=None):
        """
        Try to parse an expr and continue if it fails.
        """
        realf = self._newThunkFor(out, "optional", expr)
        passf = self._gensym("optional")
        out.writeln("def %s():" % (passf,))
        out.indent().writeln("return (None, self.input.nullError())")
        return self._expr(out, 'or', 'self._or([%s])'
                          % (', '.join([realf, passf])),
                          debugname)


    def generate_Or(self, out, exprs, debugname=None):
        """
        Create a call to
        self._or([lambda: expr1, lambda: expr2, ... , lambda: exprN]).
        """
        if len(exprs.args) > 1:
            fnames = [self._newThunkFor(out, "or", expr) for expr in exprs.args]
            return self._expr(out, 'or', 'self._or([%s])' % (', '.join(fnames)), debugname)
        else:
            return self._generateNode(out, exprs.args[0], debugname)


    def generate_Not(self, out, expr, debugname=None):
        """
        Create a call to self._not(lambda: expr).
        """
        fname = self._newThunkFor(out, "not", expr)
        return self._expr(out, "not", "self._not(%s)" % (fname,), debugname)


    def generate_Lookahead(self, out, expr, debugname=None):
        """
        Create a call to self.lookahead(lambda: expr).
        """
        fname = self._newThunkFor(out, "lookahead", expr)
        return self._expr(out, "lookahead", "self.lookahead(%s)" %(fname,), debugname)


    def generate_And(self, out, exprs, debugname=None):
        """
        Generate code for each statement in order.
        """
        v = None
        for ex in exprs.args:
            v = self._generateNode(out, ex, debugname)
        return v


    def generate_Bind(self, out, name, expr, debugname=None):
        """
        Bind the value of 'expr' to a name in the _locals dict.
        """
        v = self._generateNode(out, expr, debugname)
        if name.data:
            ref = "_locals['%s']" % (name.data,)
            out.writeln("%s = %s" % (ref, v))
        else:
            for i, n in enumerate(name.args):
                ref = "_locals['%s']" % (n.data,)
                out.writeln("%s = %s[%i]" %(ref, v, i))
        return v


    def generate_Predicate(self, out, expr, debugname=None):
        """
        Generate a call to self.pred(lambda: expr).
        """

        fname = self._newThunkFor(out, "pred", expr)
        return self._expr(out, "pred", "self.pred(%s)" %(fname,), debugname)


    def generate_Action(self, out, expr, debugname=None):
        """
        Generate this embedded Python expression on its own line.
        """
        return self.compilePythonExpr(out, expr.data, debugname)


    def generate_Python(self, out, expr, debugname=None):
        """
        Generate this embedded Python expression on its own line.
        """
        return self.compilePythonExpr(out, expr.data, debugname)


    def generate_List(self, out, expr, debugname=None):
        """
        Generate a call to self.listpattern(lambda: expr).
        """
        fname = self._newThunkFor(out, "listpattern", expr)
        return  self._expr(out, "listpattern", "self.listpattern(%s)" %(fname,),
                           debugname)

    def generate_Label(self, out, expr, label, debugname=None):
        """
        Generate code for expr, and create label.
        """
        fname = self._newThunkFor(out, "label", expr)
        return self._expr(out, 'label', 'self.label(%s, "%s")' % (fname, label.data, ), debugname)

    def generate_TermPattern(self, out, name, expr, debugname=None):
        fname = self._newThunkFor(out, "termpattern", expr)
        return self._expr(out, 'termpattern', 'self.termpattern(%r, %s)' % (name.data, fname),
                          debugname)

    def generate_StringTemplate(self, out, template, debugname=None):
        out.writeln("from terml.parser import parseTerm as term")
        return self._expr(out, 'stringtemplate', 'self.stringtemplate(%s, _locals)' % (template,))

    def generate_ConsumedBy(self, out, expr, debugname=None):
        """
        Generate a call to self.consumedBy(lambda: expr).
        """
        fname = self._newThunkFor(out, "consumedby", expr)
        return  self._expr(out, "consumedby", "self.consumedby(%s)" %(fname,),
                           debugname)


    def generate_Rule(self, prevOut, name, expr, debugname=None):
        prevOut.writeln("def rule_%s(self):" % (name.data,))
        out = prevOut.indent()
        out.writeln("_locals = {'self': self}")
        out.writeln("self.locals[%r] = _locals" % (name.data,))
        self._generate(prevOut.indent(), expr, retrn=True, debugname=name)

    def generate_Grammar(self, out, name, takesTreeInput, rules,
                         debugname=None):
        self.compiledExprCache = {}
        self.takesTreeInput = takesTreeInput.tag.name == 'true'
        out.writeln("def createParserClass(GrammarBase, ruleGlobals):")
        funcOut = out.indent()
        funcOut.writeln("if ruleGlobals is None:")
        funcOut.indent().writeln("ruleGlobals = {}")
        funcOut.writeln("class %s(GrammarBase):" % (name.data,))
        out = funcOut.indent()
        for rule in rules.args:
            self._generateNode(out, rule, debugname)
            out.writeln("")
            out.writeln("")
        if self.takesTreeInput:
            out.writeln("tree = %s" % self.takesTreeInput)
        for expr, sym in self.compiledExprCache.items():
            out.writeln("%s = compile(%r, '<string>', 'eval')" % (sym, expr))
        funcOut.writeln(
            "if %s.globals is not None:" % (name.data,))
        out.writeln("%s.globals = %s.globals.copy()" % (name.data,
                                                        name.data))
        out.writeln("%s.globals.update(ruleGlobals)" % (name.data,))
        funcOut.writeln(
            "else:")
        out.writeln("%s.globals = ruleGlobals" % (name.data,))
        funcOut.writeln("return " + name.data)
        self.compiledExprCache = None


class _Term2PythonAction(object):
    def leafData(bldr, data, span):
        return repr(data)

    def leafTag(bldr, tag, span):
        return tag.name

    def term(bldr, tag, args):
        if tag == '.tuple.':
            return "[%s]" % (', '.join(args),)
        elif tag == '.attr.':
            return "(%s)" % (', '.join(args),)
        elif tag == '.bag.':
            return "dict(%s)" % (', '.join(args),)
        if not args:
            return tag
        return "%s(%s)" % (tag, ', '.join(args))


class TermActionPythonWriter(PythonWriter):
    builder = _Term2PythonAction

    def _convertArgs(self, out, termArgs, debugname):
        return [self._termAsPython(out, a, debugname) for a in termArgs]


    def generate_Predicate(self, out, term, debugname=None):
        """
        Generate a call to self.pred(lambda: expr).
        """

        fname = self._newThunkFor(out, "pred", Term(Tag("Action"), None,
                                                    [term], None))
        return self._expr(out, "pred", "self.pred(%s)" %(fname,), debugname)

    def generate_Action(self, out, term, debugname=None):
        return self._termAsPython(out, term, debugname)

    generate_Python = generate_Action

    def _termAsPython(self, out, term, debugname):
        if not term.args:
            if term.data is None:
                return self.compilePythonExpr(out, term.tag.name, debugname)
            else:
                name = self._gensym("literal")
                out.writeln("%s = %r" % (name, term.data))
                return name
        else:
            return self.compilePythonExpr(out, term.build(self.builder()), debugname)


def writePython(tree, txt):
    f = StringIO()
    out = TextWriter(f)
    pw = PythonWriter(tree, txt)
    pw.output(out)
    return f.getvalue().strip()


class GeneratedCodeLoader(object):
    """
    Object for use as a module's __loader__, to display generated
    source.
    """
    def __init__(self, source):
        self.source = source
    def get_source(self, name):
        return self.source



def moduleFromGrammar(source, className, modname, filename):
    mod = module(str(modname))
    mod.__name__ = modname
    mod.__loader__ = GeneratedCodeLoader(source)
    code = compile(source, filename, "exec")
    eval(code, mod.__dict__)
    sys.modules[modname] = mod
    linecache.getlines(filename, mod.__dict__)
    return mod

