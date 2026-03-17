##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Restricted Python Expressions."""

import ast

from .compile import compile_restricted_eval


nltosp = str.maketrans('\r\n', '  ')

# No restrictions.
default_guarded_getattr = getattr


def default_guarded_getitem(ob, index):
    # No restrictions.
    return ob[index]


def default_guarded_getiter(ob):
    # No restrictions.
    return ob


class RestrictionCapableEval:
    """A base class for restricted code."""

    globals = {'__builtins__': None}
    # restricted
    rcode = None

    # unrestricted
    ucode = None

    # Names used by the expression
    used = None

    def __init__(self, expr):
        """Create a restricted expression

        where:

          expr -- a string containing the expression to be evaluated.
        """
        expr = expr.strip()
        self.__name__ = expr
        expr = expr.translate(nltosp)
        self.expr = expr
        # Catch syntax errors.
        self.prepUnrestrictedCode()

    def prepRestrictedCode(self):
        if self.rcode is None:
            result = compile_restricted_eval(self.expr, '<string>')
            if result.errors:
                raise SyntaxError(result.errors[0])
            self.used = tuple(result.used_names)
            self.rcode = result.code

    def prepUnrestrictedCode(self):
        if self.ucode is None:
            exp_node = compile(
                self.expr,
                '<string>',
                'eval',
                ast.PyCF_ONLY_AST)

            co = compile(exp_node, '<string>', 'eval')

            # Examine the ast to discover which names the expression needs.
            if self.used is None:
                used = set()
                for node in ast.walk(exp_node):
                    if isinstance(node, ast.Name):
                        if isinstance(node.ctx, ast.Load):
                            used.add(node.id)

                self.used = tuple(used)

            self.ucode = co

    def eval(self, mapping):
        # This default implementation is probably not very useful. :-(
        # This is meant to be overridden.
        self.prepRestrictedCode()

        global_scope = {
            '_getattr_': default_guarded_getattr,
            '_getitem_': default_guarded_getitem,
            '_getiter_': default_guarded_getiter,
        }

        global_scope.update(self.globals)

        for name in self.used:
            if (name not in global_scope) and (name in mapping):
                global_scope[name] = mapping[name]

        return eval(self.rcode, global_scope)

    def __call__(self, **kw):
        return self.eval(kw)
