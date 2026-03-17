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
"""
transformer module:

uses Python standard library ast module and its containing classes to transform
the parsed python code to create a modified AST for a byte code generation.
"""


import ast
import contextlib
import textwrap


# For AugAssign the operator must be converted to a string.
IOPERATOR_TO_STR = {
    ast.Add: '+=',
    ast.Sub: '-=',
    ast.Mult: '*=',
    ast.Div: '/=',
    ast.Mod: '%=',
    ast.Pow: '**=',
    ast.LShift: '<<=',
    ast.RShift: '>>=',
    ast.BitOr: '|=',
    ast.BitXor: '^=',
    ast.BitAnd: '&=',
    ast.FloorDiv: '//=',
    ast.MatMult: '@=',
}

# For creation allowed magic method names. See also
# https://docs.python.org/3/reference/datamodel.html#special-method-names
ALLOWED_FUNC_NAMES = frozenset([
    '__init__',
    '__contains__',
    '__lt__',
    '__le__',
    '__eq__',
    '__ne__',
    '__gt__',
    '__ge__',
])


FORBIDDEN_FUNC_NAMES = frozenset([
    'print',
    'printed',
    'builtins',
    'breakpoint',
])

# Attributes documented in the `inspect` module, but defined on the listed
# objects. See also https://docs.python.org/3/library/inspect.html
INSPECT_ATTRIBUTES = frozenset([
    # on traceback objects:
    "tb_frame",
    # "tb_lasti",  # int
    # "tb_lineno",  # int
    "tb_next",
    # on frame objects:
    "f_back",
    "f_builtins",
    "f_code",
    "f_generator",
    "f_globals",
    # "f_lasti",  # int
    # "f_lineno",  # int
    "f_locals",
    "f_trace",
    # on code objects:
    # "co_argcount",  # int
    "co_code",
    # "co_cellvars",  # tuple of str
    # "co_consts",   # tuple of str
    # "co_filename",  # str
    # "co_firstlineno",  # int
    # "co_flags",  # int
    # "co_lnotab",  # mapping between ints and indices
    # "co_freevars",  # tuple of strings
    # "co_posonlyargcount",  # int
    # "co_kwonlyargcount",  # int
    # "co_name",  # str
    # "co_qualname",  # str
    # "co_names",  # str
    # "co_nlocals",  # int
    # "co_stacksize",  # int
    # "co_varnames",  # tuple of str
    # on generator objects:
    "gi_frame",
    # "gi_running",  # bool
    # "gi_suspended",  # bool
    "gi_code",
    "gi_yieldfrom",
    # on coroutine objects:
    "cr_await",
    "cr_frame",
    # "cr_running",  # bool
    "cr_code",
    "cr_origin",
])


# When new ast nodes are generated they have no 'lineno', 'end_lineno',
# 'col_offset' and 'end_col_offset'. This function copies these fields from the
# incoming node:
def copy_locations(new_node, old_node):
    assert 'lineno' in new_node._attributes
    new_node.lineno = old_node.lineno

    assert 'end_lineno' in new_node._attributes
    new_node.end_lineno = old_node.end_lineno

    assert 'col_offset' in new_node._attributes
    new_node.col_offset = old_node.col_offset

    assert 'end_col_offset' in new_node._attributes
    new_node.end_col_offset = old_node.end_col_offset

    ast.fix_missing_locations(new_node)


class PrintInfo:
    def __init__(self):
        self.print_used = False
        self.printed_used = False

    @contextlib.contextmanager
    def new_print_scope(self):
        old_print_used = self.print_used
        old_printed_used = self.printed_used

        self.print_used = False
        self.printed_used = False

        try:
            yield
        finally:
            self.print_used = old_print_used
            self.printed_used = old_printed_used


class RestrictingNodeTransformer(ast.NodeTransformer):

    def __init__(self, errors=None, warnings=None, used_names=None):
        super().__init__()
        self.errors = [] if errors is None else errors
        self.warnings = [] if warnings is None else warnings

        # All the variables used by the incoming source.
        # Internal names/variables, like the ones from 'gen_tmp_name', don't
        # have to be added.
        # 'used_names' is for example needed by 'RestrictionCapableEval' to
        # know wich names it has to supply when calling the final code.
        self.used_names = {} if used_names is None else used_names

        # Global counter to construct temporary variable names.
        self._tmp_idx = 0

        self.print_info = PrintInfo()

    def gen_tmp_name(self):
        # 'check_name' ensures that no variable is prefixed with '_'.
        # => Its safe to use '_tmp..' as a temporary variable.
        name = '_tmp%i' % self._tmp_idx
        self._tmp_idx += 1
        return name

    def error(self, node, info):
        """Record a security error discovered during transformation."""
        lineno = getattr(node, 'lineno', None)
        self.errors.append(
            f'Line {lineno}: {info}')

    def warn(self, node, info):
        """Record a security error discovered during transformation."""
        lineno = getattr(node, 'lineno', None)
        self.warnings.append(
            f'Line {lineno}: {info}')

    def guard_iter(self, node):
        """
        Converts:
            for x in expr
        to
            for x in _getiter_(expr)

        Also used for
        * list comprehensions
        * dict comprehensions
        * set comprehensions
        * generator expresions
        """
        node = self.node_contents_visit(node)

        if isinstance(node.target, ast.Tuple):
            spec = self.gen_unpack_spec(node.target)
            new_iter = ast.Call(
                func=ast.Name('_iter_unpack_sequence_', ast.Load()),
                args=[node.iter, spec, ast.Name('_getiter_', ast.Load())],
                keywords=[])
        else:
            new_iter = ast.Call(
                func=ast.Name("_getiter_", ast.Load()),
                args=[node.iter],
                keywords=[])

        copy_locations(new_iter, node.iter)
        node.iter = new_iter
        return node

    def is_starred(self, ob):
        return isinstance(ob, ast.Starred)

    def gen_unpack_spec(self, tpl):
        """Generate a specification for 'guarded_unpack_sequence'.

        This spec is used to protect sequence unpacking.
        The primary goal of this spec is to tell which elements in a sequence
        are sequences again. These 'child' sequences have to be protected
        again.

        For example there is a sequence like this:
            (a, (b, c), (d, (e, f))) = g

        On a higher level the spec says:
            - There is a sequence of len 3
            - The element at index 1 is a sequence again with len 2
            - The element at index 2 is a sequence again with len 2
              - The element at index 1 in this subsequence is a sequence again
                with len 2

        With this spec 'guarded_unpack_sequence' does something like this for
        protection (len checks are omitted):

            t = list(_getiter_(g))
            t[1] = list(_getiter_(t[1]))
            t[2] = list(_getiter_(t[2]))
            t[2][1] = list(_getiter_(t[2][1]))
            return t

        The 'real' spec for the case above is then:
            spec = {
                'min_len': 3,
                'childs': (
                    (1, {'min_len': 2, 'childs': ()}),
                    (2, {
                            'min_len': 2,
                            'childs': (
                                (1, {'min_len': 2, 'childs': ()})
                            )
                        }
                    )
                )
            }

        So finally the assignment above is converted into:
            (a, (b, c), (d, (e, f))) = guarded_unpack_sequence(g, spec)
        """
        spec = ast.Dict(keys=[], values=[])

        spec.keys.append(ast.Constant('childs'))
        spec.values.append(ast.Tuple([], ast.Load()))

        # starred elements in a sequence do not contribute into the min_len.
        # For example a, b, *c = g
        # g must have at least 2 elements, not 3. 'c' is empyt if g has only 2.
        min_len = len([ob for ob in tpl.elts if not self.is_starred(ob)])
        offset = 0

        for idx, val in enumerate(tpl.elts):
            # After a starred element specify the child index from the back.
            # Since it is unknown how many elements from the sequence are
            # consumed by the starred element.
            # For example a, *b, (c, d) = g
            # Then (c, d) has the index '-1'
            if self.is_starred(val):
                offset = min_len + 1

            elif isinstance(val, ast.Tuple):
                el = ast.Tuple([], ast.Load())
                el.elts.append(ast.Constant(idx - offset))
                el.elts.append(self.gen_unpack_spec(val))
                spec.values[0].elts.append(el)

        spec.keys.append(ast.Constant('min_len'))
        spec.values.append(ast.Constant(min_len))

        return spec

    def protect_unpack_sequence(self, target, value):
        spec = self.gen_unpack_spec(target)
        return ast.Call(
            func=ast.Name('_unpack_sequence_', ast.Load()),
            args=[value, spec, ast.Name('_getiter_', ast.Load())],
            keywords=[])

    def gen_unpack_wrapper(self, node, target):
        """Helper function to protect tuple unpacks.

        node: used to copy the locations for the new nodes.
        target: is the tuple which must be protected.

        It returns a tuple with two element.

        Element 1: Is a temporary name node which must be used to
                   replace the target.
                   The context (store, param) is defined
                   by the 'ctx' parameter..

        Element 2: Is a try .. finally where the body performs the
                   protected tuple unpack of the temporary variable
                   into the original target.
        """

        # Generate a tmp name to replace the tuple with.
        tmp_name = self.gen_tmp_name()

        # Generates an expressions which protects the unpack.
        # converter looks like 'wrapper(tmp_name)'.
        # 'wrapper' takes care to protect sequence unpacking with _getiter_.
        converter = self.protect_unpack_sequence(
            target,
            ast.Name(tmp_name, ast.Load()))

        # Assign the expression to the original names.
        # Cleanup the temporary variable.
        # Generates:
        # try:
        #     # converter is 'wrapper(tmp_name)'
        #     arg = converter
        # finally:
        #     del tmp_arg
        try_body = [ast.Assign(targets=[target], value=converter)]
        finalbody = [self.gen_del_stmt(tmp_name)]
        cleanup = ast.Try(
            body=try_body, finalbody=finalbody, handlers=[], orelse=[])

        # This node is used to catch the tuple in a tmp variable.
        tmp_target = ast.Name(tmp_name, ast.Store())

        copy_locations(tmp_target, node)
        copy_locations(cleanup, node)

        return (tmp_target, cleanup)

    def gen_none_node(self):
        return ast.NameConstant(value=None)

    def gen_del_stmt(self, name_to_del):
        return ast.Delete(targets=[ast.Name(name_to_del, ast.Del())])

    def transform_slice(self, slice_):
        """Transform slices into function parameters.

        ast.Slice nodes are only allowed within a ast.Subscript node.
        To use a slice as an argument of ast.Call it has to be converted.
        Conversion is done by calling the 'slice' function from builtins
        """

        if isinstance(slice_, ast.expr):
            # Python 3.9+
            return slice_

        elif isinstance(slice_, ast.Index):
            return slice_.value

        elif isinstance(slice_, ast.Slice):
            # Create a python slice object.
            args = []

            if slice_.lower:
                args.append(slice_.lower)
            else:
                args.append(self.gen_none_node())

            if slice_.upper:
                args.append(slice_.upper)
            else:
                args.append(self.gen_none_node())

            if slice_.step:
                args.append(slice_.step)
            else:
                args.append(self.gen_none_node())

            return ast.Call(
                func=ast.Name('slice', ast.Load()),
                args=args,
                keywords=[])

        elif isinstance(slice_, ast.ExtSlice):
            dims = ast.Tuple([], ast.Load())
            for item in slice_.dims:
                dims.elts.append(self.transform_slice(item))
            return dims

        else:  # pragma: no cover
            # Index, Slice and ExtSlice are only defined Slice types.
            raise NotImplementedError(f"Unknown slice type: {slice_}")

    def check_name(self, node, name, allow_magic_methods=False):
        """Check names if they are allowed.

        If ``allow_magic_methods is True`` names in `ALLOWED_FUNC_NAMES`
        are additionally allowed although their names start with `_`.

        """
        if name is None:
            return

        if (name.startswith('_')
                and name != '_'
                and not (allow_magic_methods
                         and name in ALLOWED_FUNC_NAMES
                         and node.col_offset != 0)):
            self.error(
                node,
                '"{name}" is an invalid variable name because it '
                'starts with "_"'.format(name=name))
        elif name.endswith('__roles__'):
            self.error(node, '"%s" is an invalid variable name because '
                       'it ends with "__roles__".' % name)
        elif name in FORBIDDEN_FUNC_NAMES:
            self.error(node, f'"{name}" is a reserved name.')

    def check_function_argument_names(self, node):
        for arg in node.args.args:
            self.check_name(node, arg.arg)

        if node.args.vararg:
            self.check_name(node, node.args.vararg.arg)

        if node.args.kwarg:
            self.check_name(node, node.args.kwarg.arg)

        for arg in node.args.kwonlyargs:
            self.check_name(node, arg.arg)

    def check_import_names(self, node):
        """Check the names being imported.

        This is a protection against rebinding dunder names like
        _getitem_, _write_ via imports.

        => 'from _a import x' is ok, because '_a' is not added to the scope.
        """
        for name in node.names:
            if '*' in name.name:
                self.error(node, '"*" imports are not allowed.')
            self.check_name(node, name.name)
            if name.asname:
                self.check_name(node, name.asname)

        return self.node_contents_visit(node)

    def inject_print_collector(self, node, position=0):
        print_used = self.print_info.print_used
        printed_used = self.print_info.printed_used

        if print_used or printed_used:
            # Add '_print = _print_(_getattr_)' add the top of a
            # function/module.
            _print = ast.Assign(
                targets=[ast.Name('_print', ast.Store())],
                value=ast.Call(
                    func=ast.Name("_print_", ast.Load()),
                    args=[ast.Name("_getattr_", ast.Load())],
                    keywords=[]))

            if isinstance(node, ast.Module):
                _print.lineno = position
                _print.col_offset = position
                _print.end_lineno = position
                _print.end_col_offset = position
                ast.fix_missing_locations(_print)
            else:
                copy_locations(_print, node)

            node.body.insert(position, _print)

            if not printed_used:
                self.warn(node, "Prints, but never reads 'printed' variable.")

            elif not print_used:
                self.warn(node, "Doesn't print, but reads 'printed' variable.")

    # Special Functions for an ast.NodeTransformer

    def generic_visit(self, node):
        """Reject ast nodes which do not have a corresponding `visit_` method.

        This is needed to prevent new ast nodes from new Python versions to be
        trusted before any security review.

        To access `generic_visit` on the super class use `node_contents_visit`.
        """
        self.warn(
            node,
            '{0.__class__.__name__}'
            ' statement is not known to RestrictedPython'.format(node)
        )
        self.not_allowed(node)

    def not_allowed(self, node):
        self.error(
            node,
            f'{node.__class__.__name__} statements are not allowed.')

    def node_contents_visit(self, node):
        """Visit the contents of a node."""
        return super().generic_visit(node)

    # ast for Literals

    def visit_Constant(self, node):
        """Allow constant literals with restriction for Ellipsis.

        Constant replaces Num, Str, Bytes, NameConstant and Ellipsis in
        Python 3.8+.
        :see: https://docs.python.org/dev/whatsnew/3.8.html#deprecated
        """
        if node.value is Ellipsis:
            # Deny using `...`.
            # Special handling necessary as ``self.not_allowed(node)``
            # would return the Error Message:
            # 'Constant statements are not allowed.'
            # which is only partial true.
            self.error(node, 'Ellipsis statements are not allowed.')
            return
        return self.node_contents_visit(node)

    def visit_Interactive(self, node):
        """Allow single mode without restrictions."""
        return self.node_contents_visit(node)

    def visit_List(self, node):
        """Allow list literals without restrictions."""
        return self.node_contents_visit(node)

    def visit_Tuple(self, node):
        """Allow tuple literals without restrictions."""
        return self.node_contents_visit(node)

    def visit_Set(self, node):
        """Allow set literals without restrictions."""
        return self.node_contents_visit(node)

    def visit_Dict(self, node):
        """Allow dict literals without restrictions."""
        return self.node_contents_visit(node)

    def visit_FormattedValue(self, node):
        """Allow f-strings without restrictions."""
        return self.node_contents_visit(node)

    def visit_TemplateStr(self, node):
        """Template strings are allowed by default.

        As Template strings are a very basic template mechanism, that needs
        additional rendering logic to be useful, they are not blocked by
        default.
        Those rendering logic would be affected by RestrictedPython as well.
        """
        return self.node_contents_visit(node)

    def visit_Interpolation(self, node):
        """Interpolations are allowed by default.

        As Interpolations are part of Template Strings, they are needed
        to be reached in the context of RestrictedPython as Template Strings
        are allowed. As a user has to provide additional rendering logic
        to make use of Template Strings, the security implications of
        Interpolations are limited in the context of RestrictedPython.
        """
        return self.node_contents_visit(node)

    def visit_JoinedStr(self, node):
        """Allow joined string without restrictions."""
        return self.node_contents_visit(node)

    # ast for Variables

    def visit_Name(self, node):
        """Prevents access to protected names.

        Converts use of the name 'printed' to this expression: '_print()'
        """

        node = self.node_contents_visit(node)

        if isinstance(node.ctx, ast.Load):
            if node.id == 'printed':
                self.print_info.printed_used = True
                new_node = ast.Call(
                    func=ast.Name("_print", ast.Load()),
                    args=[],
                    keywords=[])

                copy_locations(new_node, node)
                return new_node

            elif node.id == 'print':
                self.print_info.print_used = True
                new_node = ast.Attribute(
                    value=ast.Name('_print', ast.Load()),
                    attr="_call_print",
                    ctx=ast.Load())

                copy_locations(new_node, node)
                return new_node

            self.used_names[node.id] = True

        self.check_name(node, node.id)
        return node

    def visit_Load(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_Store(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_Del(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_Starred(self, node):
        """

        """
        return self.node_contents_visit(node)

    # Expressions

    def visit_Expression(self, node):
        """Allow Expression statements without restrictions.

        They are in the AST when using the `eval` compile mode.
        """
        return self.node_contents_visit(node)

    def visit_Expr(self, node):
        """Allow Expr statements (any expression) without restrictions."""
        return self.node_contents_visit(node)

    def visit_UnaryOp(self, node):
        """
        UnaryOp (Unary Operations) is the overall element for:
        * Not --> which should be allowed
        * UAdd --> Positive notation of variables (e.g. +var)
        * USub --> Negative notation of variables (e.g. -var)
        """
        return self.node_contents_visit(node)

    def visit_UAdd(self, node):
        """Allow positive notation of variables. (e.g. +var)"""
        return self.node_contents_visit(node)

    def visit_USub(self, node):
        """Allow negative notation of variables. (e.g. -var)"""
        return self.node_contents_visit(node)

    def visit_Not(self, node):
        """Allow the `not` operator."""
        return self.node_contents_visit(node)

    def visit_Invert(self, node):
        """Allow `~` expressions."""
        return self.node_contents_visit(node)

    def visit_BinOp(self, node):
        """Allow binary operations."""
        return self.node_contents_visit(node)

    def visit_Add(self, node):
        """Allow `+` expressions."""
        return self.node_contents_visit(node)

    def visit_Sub(self, node):
        """Allow `-` expressions."""
        return self.node_contents_visit(node)

    def visit_Mult(self, node):
        """Allow `*` expressions."""
        return self.node_contents_visit(node)

    def visit_Div(self, node):
        """Allow `/` expressions."""
        return self.node_contents_visit(node)

    def visit_FloorDiv(self, node):
        """Allow `//` expressions."""
        return self.node_contents_visit(node)

    def visit_Mod(self, node):
        """Allow `%` expressions."""
        return self.node_contents_visit(node)

    def visit_Pow(self, node):
        """Allow `**` expressions."""
        return self.node_contents_visit(node)

    def visit_LShift(self, node):
        """Allow `<<` expressions."""
        return self.node_contents_visit(node)

    def visit_RShift(self, node):
        """Allow `>>` expressions."""
        return self.node_contents_visit(node)

    def visit_BitOr(self, node):
        """Allow `|` expressions."""
        return self.node_contents_visit(node)

    def visit_BitXor(self, node):
        """Allow `^` expressions."""
        return self.node_contents_visit(node)

    def visit_BitAnd(self, node):
        """Allow `&` expressions."""
        return self.node_contents_visit(node)

    def visit_MatMult(self, node):
        """Allow multiplication (`@`)."""
        return self.node_contents_visit(node)

    def visit_BoolOp(self, node):
        """Allow bool operator without restrictions."""
        return self.node_contents_visit(node)

    def visit_And(self, node):
        """Allow bool operator `and` without restrictions."""
        return self.node_contents_visit(node)

    def visit_Or(self, node):
        """Allow bool operator `or` without restrictions."""
        return self.node_contents_visit(node)

    def visit_Compare(self, node):
        """Allow comparison expressions without restrictions."""
        return self.node_contents_visit(node)

    def visit_Eq(self, node):
        """Allow == expressions."""
        return self.node_contents_visit(node)

    def visit_NotEq(self, node):
        """Allow != expressions."""
        return self.node_contents_visit(node)

    def visit_Lt(self, node):
        """Allow < expressions."""
        return self.node_contents_visit(node)

    def visit_LtE(self, node):
        """Allow <= expressions."""
        return self.node_contents_visit(node)

    def visit_Gt(self, node):
        """Allow > expressions."""
        return self.node_contents_visit(node)

    def visit_GtE(self, node):
        """Allow >= expressions."""
        return self.node_contents_visit(node)

    def visit_Is(self, node):
        """Allow `is` expressions."""
        return self.node_contents_visit(node)

    def visit_IsNot(self, node):
        """Allow `is not` expressions."""
        return self.node_contents_visit(node)

    def visit_In(self, node):
        """Allow `in` expressions."""
        return self.node_contents_visit(node)

    def visit_NotIn(self, node):
        """Allow `not in` expressions."""
        return self.node_contents_visit(node)

    def visit_Call(self, node):
        """Checks calls with '*args' and '**kwargs'.

        Note: The following happens only if '*args' or '**kwargs' is used.

        Transfroms 'foo(<all the possible ways of args>)' into
        _apply_(foo, <all the possible ways for args>)

        The thing is that '_apply_' has only '*args', '**kwargs', so it gets
        Python to collapse all the myriad ways to call functions
        into one manageable from.

        From there, '_apply_()' wraps args and kws in guarded accessors,
        then calls the function, returning the value.
        """

        if isinstance(node.func, ast.Name):
            if node.func.id == 'exec':
                self.error(node, 'Exec calls are not allowed.')
            elif node.func.id == 'eval':
                self.error(node, 'Eval calls are not allowed.')

        needs_wrap = False

        for pos_arg in node.args:
            if isinstance(pos_arg, ast.Starred):
                needs_wrap = True

        for keyword_arg in node.keywords:
            if keyword_arg.arg is None:
                needs_wrap = True

        node = self.node_contents_visit(node)

        if not needs_wrap:
            return node

        node.args.insert(0, node.func)
        node.func = ast.Name('_apply_', ast.Load())
        copy_locations(node.func, node.args[0])
        return node

    def visit_keyword(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_IfExp(self, node):
        """Allow `if` expressions without restrictions."""
        return self.node_contents_visit(node)

    def visit_Attribute(self, node):
        """Checks and mutates attribute access/assignment.

        'a.b' becomes '_getattr_(a, "b")'
        'a.b = c' becomes '_write_(a).b = c'
        'del a.b' becomes 'del _write_(a).b'

        The _write_ function should return a security proxy.
        """
        if node.attr.startswith('_') and node.attr != '_':
            self.error(
                node,
                '"{name}" is an invalid attribute name because it starts '
                'with "_".'.format(name=node.attr))

        if node.attr.endswith('__roles__'):
            self.error(
                node,
                '"{name}" is an invalid attribute name because it ends '
                'with "__roles__".'.format(name=node.attr))

        if node.attr in INSPECT_ATTRIBUTES:
            self.error(
                node,
                f'"{node.attr}" is a restricted name,'
                ' that is forbidden to access in RestrictedPython.',
            )

        if isinstance(node.ctx, ast.Load):
            node = self.node_contents_visit(node)
            new_node = ast.Call(
                func=ast.Name('_getattr_', ast.Load()),
                args=[node.value, ast.Constant(node.attr)],
                keywords=[])

            copy_locations(new_node, node)
            return new_node

        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            node = self.node_contents_visit(node)
            new_value = ast.Call(
                func=ast.Name('_write_', ast.Load()),
                args=[node.value],
                keywords=[])

            copy_locations(new_value, node.value)
            node.value = new_value
            return node

        else:  # pragma: no cover
            # Impossible Case only ctx Load, Store and Del are defined in ast.
            raise NotImplementedError(
                f"Unknown ctx type: {type(node.ctx)}")

    # Subscripting

    def visit_Subscript(self, node):
        """Transforms all kinds of subscripts.

        'foo[bar]' becomes '_getitem_(foo, bar)'
        'foo[:ab]' becomes '_getitem_(foo, slice(None, ab, None))'
        'foo[ab:]' becomes '_getitem_(foo, slice(ab, None, None))'
        'foo[a:b]' becomes '_getitem_(foo, slice(a, b, None))'
        'foo[a:b:c]' becomes '_getitem_(foo, slice(a, b, c))'
        'foo[a, b:c] becomes '_getitem_(foo, (a, slice(b, c, None)))'
        'foo[a] = c' becomes '_write_(foo)[a] = c'
        'del foo[a]' becomes 'del _write_(foo)[a]'

        The _write_ function should return a security proxy.
        """
        node = self.node_contents_visit(node)

        # 'AugStore' and 'AugLoad' are defined in 'Python.asdl' as possible
        # 'expr_context'. However, according to Python/ast.c
        # they are NOT used by the implementation => No need to worry here.
        # Instead ast.c creates 'AugAssign' nodes, which can be visited.

        if isinstance(node.ctx, ast.Load):
            new_node = ast.Call(
                func=ast.Name('_getitem_', ast.Load()),
                args=[node.value, self.transform_slice(node.slice)],
                keywords=[])

            copy_locations(new_node, node)
            return new_node

        elif isinstance(node.ctx, (ast.Del, ast.Store)):
            new_value = ast.Call(
                func=ast.Name('_write_', ast.Load()),
                args=[node.value],
                keywords=[])

            copy_locations(new_value, node)
            node.value = new_value
            return node

        else:  # pragma: no cover
            # Impossible Case only ctx Load, Store and Del are defined in ast.
            raise NotImplementedError(
                f"Unknown ctx type: {type(node.ctx)}")

    def visit_Index(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_Slice(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_ExtSlice(self, node):
        """

        """
        return self.node_contents_visit(node)

    # Comprehensions

    def visit_ListComp(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_SetComp(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_GeneratorExp(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_DictComp(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_comprehension(self, node):
        """

        """
        return self.guard_iter(node)

    # Statements

    def visit_Assign(self, node):
        """

        """

        node = self.node_contents_visit(node)

        if not any(isinstance(t, ast.Tuple) for t in node.targets):
            return node

        # Handle sequence unpacking.
        # For briefness this example omits cleanup of the temporary variables.
        # Check 'transform_tuple_assign' how its done.
        #
        # - Single target (with nested support)
        # (a, (b, (c, d))) = <exp>
        # is converted to
        # (a, t1) = _getiter_(<exp>)
        # (b, t2) = _getiter_(t1)
        # (c, d) = _getiter_(t2)
        #
        # - Multi targets
        # (a, b) = (c, d) = <exp>
        # is converted to
        # (c, d) = _getiter_(<exp>)
        # (a, b) = _getiter_(<exp>)
        # Why is this valid ? The original bytecode for this multi targets
        # behaves the same way.

        # ast.NodeTransformer works with list results.
        # He injects it at the right place of the node's parent statements.
        new_nodes = []

        # python fills the right most target first.
        for target in reversed(node.targets):
            if isinstance(target, ast.Tuple):
                wrapper = ast.Assign(
                    targets=[target],
                    value=self.protect_unpack_sequence(target, node.value))
                new_nodes.append(wrapper)
            else:
                new_node = ast.Assign(targets=[target], value=node.value)
                new_nodes.append(new_node)

        for new_node in new_nodes:
            copy_locations(new_node, node)

        return new_nodes

    def visit_AugAssign(self, node):
        """Forbid certain kinds of AugAssign

        According to the language reference (and ast.c) the following nodes
        are are possible:
        Name, Attribute, Subscript

        Note that although augmented assignment of attributes and
        subscripts is disallowed, augmented assignment of names (such
        as 'n += 1') is allowed.
        'n += 1' becomes 'n = _inplacevar_("+=", n, 1)'
        """

        node = self.node_contents_visit(node)

        if isinstance(node.target, ast.Attribute):
            self.error(
                node,
                "Augmented assignment of attributes is not allowed.")
            return node

        elif isinstance(node.target, ast.Subscript):
            self.error(
                node,
                "Augmented assignment of object items "
                "and slices is not allowed.")
            return node

        elif isinstance(node.target, ast.Name):
            new_node = ast.Assign(
                targets=[node.target],
                value=ast.Call(
                    func=ast.Name('_inplacevar_', ast.Load()),
                    args=[
                        ast.Constant(IOPERATOR_TO_STR[type(node.op)]),
                        ast.Name(node.target.id, ast.Load()),
                        node.value
                    ],
                    keywords=[]))

            copy_locations(new_node, node)
            return new_node
        else:  # pragma: no cover
            # Impossible Case - Only Node Types:
            # * Name
            # * Attribute
            # * Subscript
            # defined, those are checked before.
            raise NotImplementedError(
                f"Unknown target type: {type(node.target)}")

    def visit_Raise(self, node):
        """Allow `raise` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Assert(self, node):
        """Allow assert statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Delete(self, node):
        """Allow `del` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Pass(self, node):
        """Allow `pass` statements without restrictions."""
        return self.node_contents_visit(node)

    # Imports

    def visit_Import(self, node):
        """Allow `import` statements with restrictions.
        See check_import_names."""
        return self.check_import_names(node)

    def visit_ImportFrom(self, node):
        """Allow `import from` statements with restrictions.
        See check_import_names."""
        return self.check_import_names(node)

    def visit_alias(self, node):
        """Allow `as` statements in import and import from statements."""
        return self.node_contents_visit(node)

    # Control flow

    def visit_If(self, node):
        """Allow `if` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_For(self, node):
        """Allow `for` statements with some restrictions."""
        return self.guard_iter(node)

    def visit_While(self, node):
        """Allow `while` statements."""
        return self.node_contents_visit(node)

    def visit_Break(self, node):
        """Allow `break` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Continue(self, node):
        """Allow `continue` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Try(self, node):
        """Allow `try` without restrictions."""
        return self.node_contents_visit(node)

    def visit_TryStar(self, node):
        """Disallow `ExceptionGroup` due to a potential sandbox escape."""
        self.not_allowed(node)

    def visit_ExceptHandler(self, node):
        """Protect exception handlers."""
        node = self.node_contents_visit(node)
        self.check_name(node, node.name)
        return node

    def visit_With(self, node):
        """Protect tuple unpacking on with statements."""
        node = self.node_contents_visit(node)

        for item in reversed(node.items):
            if isinstance(item.optional_vars, ast.Tuple):
                tmp_target, unpack = self.gen_unpack_wrapper(
                    node,
                    item.optional_vars)

                item.optional_vars = tmp_target
                node.body.insert(0, unpack)

        return node

    def visit_withitem(self, node):
        """Allow `with` statements (context managers) without restrictions."""
        return self.node_contents_visit(node)

    # Function and class definitions

    def visit_FunctionDef(self, node):
        """Allow function definitions (`def`) with some restrictions."""
        self.check_name(node, node.name, allow_magic_methods=True)
        self.check_function_argument_names(node)

        with self.print_info.new_print_scope():
            node = self.node_contents_visit(node)
            self.inject_print_collector(node)
        return node

    def visit_Lambda(self, node):
        """Allow lambda with some restrictions."""
        self.check_function_argument_names(node)
        return self.node_contents_visit(node)

    def visit_arguments(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_arg(self, node):
        """

        """
        return self.node_contents_visit(node)

    def visit_Return(self, node):
        """Allow `return` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Yield(self, node):
        """Allow `yield`statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_YieldFrom(self, node):
        """Allow `yield`statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Global(self, node):
        """Allow `global` statements without restrictions."""
        return self.node_contents_visit(node)

    def visit_Nonlocal(self, node):
        """Deny `nonlocal` statements."""
        self.not_allowed(node)

    def visit_ClassDef(self, node):
        """Check the name of a class definition."""
        self.check_name(node, node.name)
        node = self.node_contents_visit(node)
        if any(keyword.arg == 'metaclass' for keyword in node.keywords):
            self.error(
                node, 'The keyword argument "metaclass" is not allowed.')
        CLASS_DEF = textwrap.dedent('''\
            class {0.name}(metaclass=__metaclass__):
                pass
        '''.format(node))
        new_class_node = ast.parse(CLASS_DEF).body[0]
        new_class_node.body = node.body
        new_class_node.bases = node.bases
        new_class_node.decorator_list = node.decorator_list
        return new_class_node

    def visit_Module(self, node):
        """Add the print_collector (only if print is used) at the top."""
        node = self.node_contents_visit(node)

        # Inject the print collector after 'from __future__ import ....'
        position = 0
        for position, child in enumerate(node.body):  # pragma: no branch
            if not isinstance(child, ast.ImportFrom):
                break

            if not child.module == '__future__':
                break

        self.inject_print_collector(node, position)
        return node

    # Async und await

    def visit_AsyncFunctionDef(self, node):
        """Deny async functions."""
        self.not_allowed(node)

    def visit_Await(self, node):
        """Deny async functionality."""
        self.not_allowed(node)

    def visit_AsyncFor(self, node):
        """Deny async functionality."""
        self.not_allowed(node)

    def visit_AsyncWith(self, node):
        """Deny async functionality."""
        self.not_allowed(node)

    # Assignment expressions (walrus operator ``:=``)
    # New in 3.8
    def visit_NamedExpr(self, node):
        """Allow assignment expressions under some circumstances."""
        # while the grammar requires ``node.target`` to be a ``Name``
        # the abstract syntax is more permissive and allows an ``expr``.
        # We support only a ``Name``.
        # This is safe as the expression can only add/modify local
        # variables. While this may hide global variables, an
        # (implicitly performed) name check guarantees (as usual)
        # that no essential global variable is hidden.
        node = self.node_contents_visit(node)  # this checks ``node.target``
        target = node.target
        if not isinstance(target, ast.Name):
            self.error(
                node,
                "Assignment expressions are only allowed for simple targets")
        return node
