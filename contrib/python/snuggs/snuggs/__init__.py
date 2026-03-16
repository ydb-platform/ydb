"""Snuggs are s-expressions for Numpy."""

from collections import OrderedDict
import functools
import itertools
import operator
import re
import sys

from pyparsing import (
    alphanums, ZeroOrMore, nums, oneOf, Word, Literal, Combine, QuotedString,
    ParseException, Forward, Group, CaselessLiteral, Optional, alphas,
    OneOrMore, ParseResults, pyparsing_common)

import numpy


__all__ = ['eval']
__version__ = "1.4.7"

# Python 2-3 compatibility
string_types = (str,) if sys.version_info[0] >= 3 else (basestring,)  # flake8: noqa


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
    '*': lambda *args: functools.reduce(lambda x, y: operator.mul(x, y), args),
    '+': lambda *args: functools.reduce(lambda x, y: operator.add(x, y), args),
    '/': lambda *args: functools.reduce(lambda x, y: operator.truediv(x, y), args),
    '-': lambda *args: functools.reduce(lambda x, y: operator.sub(x, y), args),
    '&': lambda *args: functools.reduce(lambda x, y: operator.and_(x, y), args),
    '|': lambda *args: functools.reduce(lambda x, y: operator.or_(x, y), args),
    '<': operator.lt,
    '<=': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt}

def asarray(*args):
    if len(args) == 1 and hasattr(args[0], '__iter__'):
        return numpy.asanyarray(list(args[0]))
    else:
        return numpy.asanyarray(list(args))


func_map = {
    'asarray': asarray,
    'read': _ctx.lookup,
    'take': lambda a, idx: numpy.take(a, idx - 1, axis=0)}

higher_func_map = {
    'map': map if sys.version_info[0] >= 3 else itertools.imap,
    'partial': functools.partial}

# Definition of the grammar.
decimal = Literal('.')
e = CaselessLiteral('E')
sign = Literal('+') | Literal('-')
number = Word(nums)
name = pyparsing_common.identifier
nil = Literal('nil').setParseAction(lambda s, l, t: [None])

def resolve_var(s, l, t):
    try:
        return _ctx.get(t[0])
    except KeyError:
        err = ExpressionError(
            "name '%s' is not defined" % t[0])
        err.text = s
        err.offset = l + 1
        raise err


var = name.setParseAction(resolve_var)

string = QuotedString("'") | QuotedString('"')

lparen = Literal('(').suppress()
rparen = Literal(')').suppress()

op = oneOf(' '.join(op_map.keys())).setParseAction(
    lambda s, l, t: op_map[t[0]])


def resolve_func(s, l, t):
    try:
        return func_map[t[0]] if t[0] in func_map else getattr(numpy, t[0])
    except AttributeError:
        err = ExpressionError(
            "'%s' is not a function or operator" % t[0])
        err.text = s
        err.offset = l + 1
        raise err


func = Word(alphanums + '_').setParseAction(resolve_func)

higher_func = oneOf('map partial').setParseAction(
    lambda s, l, t: higher_func_map[t[0]])

func_expr = Forward()
higher_func_expr = Forward()
expr = higher_func_expr | func_expr

operand = higher_func_expr | func_expr | nil | var | pyparsing_common.number | string

func_expr << Group(
    lparen +
    (higher_func_expr | op | func) +
    operand +
    ZeroOrMore(operand) +
    rparen)

higher_func_expr << Group(
    lparen +
    higher_func +
    (nil | higher_func_expr | op | func) +
    ZeroOrMore(operand) +
    rparen)


def processArg(arg):
    if not isinstance(arg, ParseResults):
        return arg
    else:
        return processList(arg)


def processList(lst):
    args = [processArg(x) for x in lst[1:]]
    func = processArg(lst[0])
    return func(*args)


def handleLine(line):
    try:
        result = expr.parseString(line)
        return processList(result[0])
    except ParseException as exc:
        text = str(exc)
        m = re.search(r'(Expected .+) \(at char (\d+)\), \(line:(\d+)', text)
        msg = m.group(1)
        if 'map|partial' in msg:
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
