"""Snuggs are s-expressions for Numpy."""

# This file is a modified version of snuggs 1.4.7. The numpy
# requirement has been removed and support for keyword arguments in
# expressions has been added.
#
# The original license follows.
#
# Copyright (c) 2014 Mapbox
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from collections import OrderedDict
import functools
import operator
import re
from typing import Mapping

from pyparsing import (  # type: ignore
    Keyword,
    oneOf,
    Literal,
    QuotedString,
    ParseException,
    Forward,
    Group,
    OneOrMore,
    ParseResults,
    Regex,
    ZeroOrMore,
    alphanums,
    pyparsing_common,
    replace_with,
)

__all__ = ["eval"]
__version__ = "1.4.7"


class Context(object):
    def __init__(self):
        self._data = OrderedDict()

    def add(self, name, val):
        self._data[name] = val

    def get(self, name):
        return self._data[name]

    def lookup(self, index, subindex=None):
        s = list(self._data.values())[int(index) - 1]
        if subindex:
            return s[int(subindex) - 1]
        else:
            return s

    def clear(self):
        self._data = OrderedDict()


_ctx = Context()


class ctx(object):
    def __init__(self, kwd_dict=None, **kwds):
        self.kwds = kwd_dict or kwds

    def __enter__(self):
        _ctx.clear()
        for k, v in self.kwds.items():
            _ctx.add(k, v)
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.kwds = None
        _ctx.clear()


class ExpressionError(SyntaxError):
    """A Snuggs-specific syntax error."""

    filename = "<string>"
    lineno = 1


op_map = {
    "*": lambda *args: functools.reduce(lambda x, y: operator.mul(x, y), args),
    "+": lambda *args: functools.reduce(lambda x, y: operator.add(x, y), args),
    "/": lambda *args: functools.reduce(lambda x, y: operator.truediv(x, y), args),
    "-": lambda *args: functools.reduce(lambda x, y: operator.sub(x, y), args),
    "&": lambda *args: functools.reduce(lambda x, y: operator.and_(x, y), args),
    "|": lambda *args: functools.reduce(lambda x, y: operator.or_(x, y), args),
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
    "truth": operator.truth,
    "is": operator.is_,
    "not": operator.not_,
}


def compose(f, g):
    """Compose two functions.

    compose(f, g)(x) = f(g(x)).

    """
    return lambda x, *args, **kwds: f(g(x))


func_map: Mapping = {}

higher_func_map: Mapping = {
    "compose": compose,
    "map": map,
    "partial": functools.partial,
    "reduce": functools.reduce,
    "attrgetter": operator.attrgetter,
    "methodcaller": operator.methodcaller,
    "itemgetter": operator.itemgetter,
}

nil = Keyword("null").set_parse_action(replace_with(None))
true = Keyword("true").set_parse_action(replace_with(True))
false = Keyword("false").set_parse_action(replace_with(False))


def resolve_var(source, loc, toks):
    try:
        return _ctx.get(toks[0])
    except KeyError:
        err = ExpressionError("name '{}' is not defined".format(toks[0]))
        err.text = source
        err.offset = loc + 1
        raise err


var = pyparsing_common.identifier.set_parse_action(resolve_var)
string = QuotedString("'") | QuotedString('"')
lparen = Literal("(").suppress()
rparen = Literal(")").suppress()
op = oneOf(" ".join(op_map.keys())).set_parse_action(
    lambda source, loc, toks: op_map[toks[0]]
)


def resolve_func(source, loc, toks):
    try:
        return func_map[toks[0]]
    except (AttributeError, KeyError):
        err = ExpressionError("'{}' is not a function or operator".format(toks[0]))
        err.text = source
        err.offset = loc + 1
        raise err


# The look behind assertion is to disambiguate between functions and
# variables.
func = Regex(r"(?<=\()[{}]+".format(alphanums + "_")).set_parse_action(resolve_func)

higher_func = oneOf(" ".join(higher_func_map.keys())).set_parse_action(
    lambda source, loc, toks: higher_func_map[toks[0]]
)

func_expr = Forward()
higher_func_expr = Forward()
expr = higher_func_expr | func_expr


class KeywordArg:
    def __init__(self, name):
        self.name = name


kwarg = Regex(r":[{}]+".format(alphanums + "_")).set_parse_action(
    lambda source, loc, toks: KeywordArg(toks[0][1:])
)

operand = (
    higher_func_expr
    | func_expr
    | true
    | false
    | nil
    | var
    | kwarg
    | pyparsing_common.sci_real
    | pyparsing_common.real
    | pyparsing_common.signed_integer
    | string
)

func_expr << Group(
    lparen + (higher_func_expr | op | func) + OneOrMore(operand) + rparen
)

higher_func_expr << Group(
    lparen
    + higher_func
    + (nil | higher_func_expr | op | func | OneOrMore(operand))
    + ZeroOrMore(operand)
    + rparen
)


def processArg(arg):
    if isinstance(arg, ParseResults):
        return processList(arg)
    else:
        return arg


def processList(lst):
    items = [processArg(x) for x in lst[1:]]
    args = []
    kwds = {}

    # An iterator is used instead of implicit iteration to allow
    # skipping ahead in the keyword argument case.
    itemitr = iter(items)

    for item in itemitr:
        if isinstance(item, KeywordArg):
            # The next item after the keyword arg marker is its value.
            # This advances the iterator in a way that is compatible
            # with the for loop.
            val = next(itemitr)
            key = item.name
            kwds[key] = val
        else:
            args.append(item)

    func = processArg(lst[0])

    # list and tuple are two builtins that take a single argument,
    # whereas args is a list. On a KeyError, the call is retried
    # without arg unpacking.
    try:
        return func(*args, **kwds)
    except TypeError:
        return func(args, **kwds)


def handleLine(line):
    try:
        result = expr.parseString(line)
        return processList(result[0])
    except ParseException as exc:
        text = str(exc)
        m = re.search(r"(Expected .+) \(at char (\d+)\), \(line:(\d+)", text)
        msg = m.group(1)
        if "map|partial" in msg:
            msg = "expected a function or operator"
        err = ExpressionError(msg)
        err.text = line
        err.offset = int(m.group(2)) + 1
        raise err


def eval(source, kwd_dict=None, **kwds):
    """Evaluate a snuggs expression.

    Parameters
    ----------
    source : str
        Expression source.
    kwd_dict : dict
        A dict of items that form the evaluation context. Deprecated.
    kwds : dict
        A dict of items that form the valuation context.

    Returns
    -------
    object

    """
    kwd_dict = kwd_dict or kwds
    with ctx(kwd_dict):
        return handleLine(source)
