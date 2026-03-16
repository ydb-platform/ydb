"""
The is a backwards compatible shim for the ast module.

This is the best way to make the ast module work the same in both python 2 and 3.
This is essentially what the ast module was doing until 3.12, when it started throwing
deprecation warnings.
"""

from ast import *


# Ideally we don't import anything else

if 'TypeAlias' in globals():

    # Add n and s properties to Constant so it can stand in for Num, Str and Bytes
    Constant.n = property(lambda self: self.value, lambda self, value: setattr(self, 'value', value))  # type: ignore[assignment]
    Constant.s = property(lambda self: self.value, lambda self, value: setattr(self, 'value', value))  # type: ignore[assignment]


    # These classes are redefined from the ones in ast that complain about deprecation
    # They will continue to work once they are removed from ast

    class Str(Constant):  # type: ignore[no-redef]
        def __new__(cls, s, *args, **kwargs):
            return Constant(value=s, *args, **kwargs)


    class Bytes(Constant):  # type: ignore[no-redef]
        def __new__(cls, s, *args, **kwargs):
            return Constant(value=s, *args, **kwargs)


    class Num(Constant):  # type: ignore[no-redef]
        def __new__(cls, n, *args, **kwargs):
            return Constant(value=n, *args, **kwargs)


    class NameConstant(Constant):  # type: ignore[no-redef]
        def __new__(cls, *args, **kwargs):
            return Constant(*args, **kwargs)


    class Ellipsis(Constant):  # type: ignore[no-redef]
        def __new__(cls, *args, **kwargs):
            return Constant(value=literal_eval('...'), *args, **kwargs)


# Create a dummy class for missing AST nodes
for _node_type in [
    'AnnAssign',
    'AsyncFor',
    'AsyncFunctionDef',
    'AsyncFunctionDef',
    'AsyncWith',
    'Bytes',
    'Constant',
    'DictComp',
    'Exec',
    'ListComp',
    'MatchAs',
    'MatchMapping',
    'MatchStar',
    'NameConstant',
    'NamedExpr',
    'Nonlocal',
    'ParamSpec',
    'SetComp',
    'Starred',
    'TryStar',
    'TypeVar',
    'TypeVarTuple',
    'TemplateStr',
    'Interpolation',
    'YieldFrom',
    'arg',
    'withitem',
]:
    if _node_type not in globals():
        globals()[_node_type] = type(_node_type, (AST,), {})
