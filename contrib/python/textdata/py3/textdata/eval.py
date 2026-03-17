"""
Support module for evaluating strings, often into
natural Python values.
"""

from ast import literal_eval as ast_literal_eval
from .util import noquotes


def literal_eval(s):
    """
    Wrapper around ``ast.literal_eval`` that returns its return value,
    if possible, but returns the original string in cases where
    ``ast.literal_eval`` raises an exception.
    """
    try:
        return ast_literal_eval(s)
    except (ValueError, SyntaxError):
        return s


# evaluation functions
identity = lambda s: s
minimal  = lambda s: s.strip()
natural  = lambda s: literal_eval(s.strip())
full     = lambda s: literal_eval(noquotes(s.strip()))


# mapping of evaluate parameter to evaluation functions
EVALUATE = {
    'none':    identity,
    None:      identity,

    'minimal': minimal,
    False:     minimal,

    'natural': natural,
    True:      natural,

    'full':    full,
}


def evaluation(value, how='natural'):
    """
    Standard value evaluator. Defaults to the "natural"
    Python literal encoding.
    """
    if hasattr(how, '__call__'):
        evaluator = how
    else:
        try:
            evaluator = EVALUATE[how]
        except KeyError:
            raise ValueError('{!r} not a known evaluation mode'.format(how))
    try:
        return evaluator(value)
    except Exception as e:
        print(e)
        return minimal(value)
