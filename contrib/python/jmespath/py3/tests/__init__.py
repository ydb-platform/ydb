import sys
from jmespath import ast

import unittest
import json
from collections import OrderedDict


# Helper method used to create an s-expression
# of the AST to make unit test assertions easier.
# You get a nice string diff on assert failures.
def as_s_expression(node):
    parts = []
    _as_s_expression(node, parts)
    return ''.join(parts)


def _as_s_expression(node, parts):
    parts.append("(%s" % (node.__class__.__name__.lower()))
    if isinstance(node, ast.Field):
        parts.append(" %s" % node.name)
    elif isinstance(node, ast.FunctionExpression):
        parts.append(" %s" % node.name)
    elif isinstance(node, ast.KeyValPair):
        parts.append(" %s" % node.key_name)
    for child in node.children:
        parts.append(" ")
        _as_s_expression(child, parts)
    parts.append(")")


