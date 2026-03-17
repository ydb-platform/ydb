import string
from ometa.grammar import loadGrammar
from ometa.runtime import character, EOFError
import terml
from terml.nodes import Tag, Term, termMaker

try:
    integer_types = (int, long)
except NameError:
    integer_types = (int,)


## Functions called from grammar actions

def concat(*bits):
    return ''.join(map(str, bits))

Character = termMaker.Character

def makeFloat(sign, ds, tail):
        return float((sign or '') + ds + tail)

def signedInt(sign, x, base=10):
    return int(str((sign or '')+x), base)

def join(x):
    return ''.join(x)

def makeHex(sign, hs):
    return int((sign or '') + ''.join(hs), 16)

def makeOctal(sign, ds):
    return int((sign or '') + '0'+''.join(ds), 8)

def isDigit(x):
    return x in string.digits

def isOctDigit(x):
    return x in string.octdigits

def isHexDigit(x):
    return x in string.hexdigits

def contains(container, value):
    return value in container

def cons(first, rest):
    return [first] + rest

def Character(char):
    return character(char)

def makeTag(nameSegs):
    return Tag('::'.join(nameSegs))

def prefixedTag(tagnameSegs):
    return makeTag([''] + tagnameSegs)

def tagString(string):
    return '"' + string + '"'

def numberType(n):
    if isinstance(n, float):
        return ".float64."
    elif isinstance(n, integer_types):
        return ".int."
    raise ValueError("wtf")

def leafInternal(tag, data, span=None):
    return Term(tag, data, None, None)

def makeTerm(t, args=None, span=None):
    if isinstance(t, Term):
        if t.data is not None:
            if not args:
                return t
            else:
                raise ValueError("Literal terms can't have arguments")
    return Term(t.asFunctor(), None, args and tuple(args), span)


def Tuple(args, span=None):
    return Term(Tag(".tuple."), None, tuple(args), span)

def Bag(args, span=None):
    return Term(Tag(".bag."), None, tuple(args), span)

def LabelledBag(f, arg, span=None):
    return Term(f.asFunctor(), None, (arg,), span)

def Attr(k, v, span=None):
    return Term(Tag(".attr."), None, (k, v), span)

TermLParser = loadGrammar(terml, "terml", globals())


def parseTerm(termString):
    """
    Build a TermL term tree from a string.
    """
    p = TermLParser(termString)
    result, error = p.apply("term")
    try:
        p.input.head()
    except EOFError:
        pass
    else:
        raise error
    return result
