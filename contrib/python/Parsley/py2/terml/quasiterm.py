from ometa.grammar import loadGrammar
from ometa.runtime import EOFError
import terml
from terml.parser import TermLParser
from terml.qnodes import ValueHole, PatternHole, QTerm, QSome, QFunctor


def interleave(l, *r):
    if r:
        raise NotImplementedError()
    return l

def _or(l, *r):
    if r:
        raise NotImplementedError()
    return l

def some(value, quant):
    if quant:
        return QSome(value, quant)
    else:
        return value

def dollarHole(i):
    return ValueHole(None, i, False)

def patternHole(i):
    return PatternHole(None, i, False)

def taggedHole(t, h):
    return h.__class__(t, h.name, h.isFunctorHole)

def leafInternal(tag, data, span=None):
    return QFunctor(tag, data, span)


def makeTerm(t, args=None, span=None):
    if args is None:
        return t
    else:
        if isinstance(t, QTerm):
            if t.data:
                if not args:
                    return t
                else:
                    raise ValueError("Literal terms can't have arguments")
    return QTerm(t.asFunctor(), None, args and tuple(args), span)


QTermParser = loadGrammar(terml, "quasiterm", TermLParser.globals, TermLParser)
QTermParser.globals.update(globals())


def quasiterm(termString):
    """
    Build a quasiterm from a string.
    """
    p = QTermParser(termString)
    result, error = p.apply("term")
    try:
        p.input.head()
    except EOFError:
        pass
    else:
        raise error
    return result
