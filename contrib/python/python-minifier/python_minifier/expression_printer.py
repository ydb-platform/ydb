import sys

import python_minifier.ast_compat as ast

from python_minifier.token_printer import Delimiter, TokenPrinter
from python_minifier.util import is_constant_node


class ExpressionPrinter(object):
    """
    Builds the smallest possible exact representation of an ast
    """

    def __init__(self, prefer_single_line=False):

        self.precedences = {
            'Lambda': 2,  # Lambda
            'IfExp': 3,  # IfExp
            'comprehension': 3.5,
            'Or': 4,  # BoolOp
            'And': 5,
            'Not': 6,
            'In': 7, 'NotIn': 7, 'Is': 7, 'IsNot': 7, 'Lt': 7, 'LtE': 7, 'Gt': 7, 'GtE': 7, 'NotEq': 7, 'Eq': 7,  # Compare
            'BitOr': 8,
            'BitXor': 9,
            'BitAnd': 10,
            'LShift': 11, 'RShift': 11,
            'Add': 12, 'Sub': 12,
            'Mult': 13, 'Div': 13, 'FloorDiv': 13, 'Mod': 13, 'MatMult': 13,
            'UAdd': 14, 'USub': 14, 'Invert': 14,
            'Pow': 15,
            'Await': 16,
            'Subscript': 17, 'Call': 17, 'Attribute': 17,
            'Tuple': 18, 'Set': 18, 'List': 18, 'Dict': 18, 'ListComp': 18, 'SetComp': 18, 'DictComp': 18, 'GeneratorExp': 18,  # Container
        }

        self.printer = TokenPrinter(prefer_single_line=prefer_single_line)

    def __call__(self, module):
        """
        Generate the source code for an AST

        :param module: The Module to generate code for
        :type module: ast.Module
        :rtype: str

        """

        self.visit(module)
        return str(self.printer)

    def precedence(self, node):
        """
        The precedence of an expression

        Node will usually be an operator or literal.
        Nodes with no precedence value return 0.

        :param node: The AST node to decide precedence for
        :type node: ast.Node
        :rtype: int

        """

        if isinstance(node, (ast.BinOp, ast.UnaryOp, ast.BoolOp)):
            return self.precedences[node.op.__class__.__name__]
        elif isinstance(node, ast.Compare):
            return min(self.precedences[n.__class__.__name__] for n in node.ops)

        # Python2 parses negative ints as an ast.Num with a negative value.
        # Make sure the Num get the precedence of the USub operator in this case.
        if sys.version_info < (3, 0) and is_constant_node(node, ast.Num):
            if str(node.n)[0] == '-':
                return self.precedences['USub']

        return self.precedences.get(node.__class__.__name__, 0)

    def visit(self, node):
        """
        Visit a node

        Call the correct visit_ method based on the node type.
        Prefer to call the correct method directly if you already know
        the node type.

        :param node: The node to visit
        :type node: ast.Node

        """

        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.visit_Unknown)
        return visitor(node)

    def visit_Unknown(self, node):
        raise RuntimeError('Unknown node %r' % node)

    # region Literals

    def visit_Constant(self, node):
        if node.value is None or node.value is True or node.value is False:
            return self.visit_NameConstant(node)
        elif isinstance(node.value, (int, float, complex)):
            return self.visit_Num(node)
        elif isinstance(node.value, str):
            return self.visit_Str(node)
        elif isinstance(node.value, bytes):
            return self.visit_Bytes(node)
        elif node.value == Ellipsis:
            return self.visit_Ellipsis(node)

        raise RuntimeError('Unknown Constant value %r' % type(node.value))

    def visit_Num(self, node):
        if isinstance(node.n, float):
            self.printer.floatnumber(node.n)
        elif isinstance(node.n, complex):
            self.printer.imagnumber(node.n)
        else:
            # int or possibly long in python2
            self.printer.integer(node.n)

    def visit_Str(self, node):
        self.printer.stringliteral(node.s)

    def visit_Bytes(self, node):
        self.printer.bytesliteral(node.s)

    def visit_List(self, node):
        self.printer.delimiter('[')
        self._exprlist(node.elts)
        self.printer.delimiter(']')

    def visit_Tuple(self, node):

        if len(node.elts) == 0:
            self.printer.delimiter('(')
            self.printer.delimiter(')')
            return

        self._exprlist(node.elts)

        if len(node.elts) == 1:
            self.printer.delimiter(',')

    def visit_Set(self, node):
        self.printer.delimiter('{')
        self._exprlist(node.elts)
        self.printer.delimiter('}')

    def visit_Dict(self, node):
        assert isinstance(node, ast.Dict)

        def key_datum(key, datum):

            if key is None:
                self.printer.operator('**')

                if 0 < self.precedence(datum) <= 7:
                    self.printer.delimiter('(')
                    self._expression(datum)
                    self.printer.delimiter(')')
                else:
                    self._expression(datum)
            else:
                self._expression(key)
                self.printer.delimiter(':')

                self._expression(datum)

        self.printer.delimiter('{')

        delimiter = Delimiter(self.printer)
        for key, datum in zip(node.keys, node.values):
            delimiter.new_item()
            key_datum(key, datum)

        self.printer.delimiter('}')

    def visit_Ellipsis(self, node):
        self.printer.delimiter('.')
        self.printer.delimiter('.')
        self.printer.delimiter('.')

    def visit_NameConstant(self, node):
        self.printer.keyword(repr(node.value))

    # endregion

    # region Variables

    def visit_Name(self, node):
        self.printer.identifier(node.id)

    def visit_Starred(self, node):
        self.printer.operator('*')
        if 0 < self.precedence(node.value) <= 7:
            self.printer.delimiter('(')
            self._expression(node.value)
            self.printer.delimiter(')')
        else:
            self._expression(node.value)

    # endregion

    # region Expressions

    def visit_UnaryOp(self, node):
        self.visit(node.op)

        if sys.version_info < (3, 0) and isinstance(node.op, ast.USub) and is_constant_node(node.operand, ast.Num):
            # For: -(1), which is parsed as a UnaryOp(USub, Num(1)).
            # Without this special case it would be printed as -1
            # This is fine, but python 2 will then parse it at Num(-1) so the AST wouldn't round-trip.

            self.printer.delimiter('(')
            self.visit_Num(node.operand)
            self.printer.delimiter(')')
            return

        right_precedence = self.precedence(node.operand)
        op_precedence = self.precedence(node)

        if right_precedence != 0 and (
            (op_precedence > right_precedence)
        ):
            self.printer.delimiter('(')
            self._expression(node.operand)
            self.printer.delimiter(')')
        else:
            self._expression(node.operand)

    def visit_UAdd(self, node):
        self.printer.operator('+')

    def visit_USub(self, node):
        self.printer.operator('-')

    def visit_Not(self, node):
        self.printer.keyword('not')

    def visit_Invert(self, node):
        self.printer.operator('~')

    def visit_BinOp(self, node):
        self._lhs(node.left, node.op)
        self.visit(node.op)
        self._rhs(node.right, node.op)

    def visit_Add(self, node):
        self.printer.operator('+')

    def visit_Sub(self, node):
        self.printer.operator('-')

    def visit_Mult(self, node):
        self.printer.operator('*')

    def visit_Div(self, node):
        self.printer.operator('/')

    def visit_FloorDiv(self, node):
        self.printer.operator('//')

    def visit_Mod(self, node):
        self.printer.operator('%')

    def visit_Pow(self, node):
        self.printer.operator('**')

    def visit_LShift(self, node):
        self.printer.operator('<<')

    def visit_RShift(self, node):
        self.printer.operator('>>')

    def visit_BitOr(self, node):
        self.printer.operator('|')

    def visit_BitXor(self, node):
        self.printer.operator('^')

    def visit_BitAnd(self, node):
        self.printer.operator('&')

    def visit_MatMult(self, node):
        self.printer.operator('@')

    def visit_BoolOp(self, node):
        first = True

        op_precedence = self.precedence(node.op)

        for v in node.values:
            if first:
                first = False
            else:
                self._expression(node.op)

            value_precedence = self.precedence(v)

            if value_precedence != 0 and (
                (op_precedence > value_precedence)
                or (op_precedence == value_precedence
                    and self._is_left_associative(node.op))
            ):
                self.printer.delimiter('(')
                self._expression(v)
                self.printer.delimiter(')')
            else:
                self._expression(v)

    def visit_And(self, node):
        self.printer.keyword('and')

    def visit_Or(self, node):
        self.printer.keyword('or')

    def visit_Compare(self, node):

        left_precedence = self.precedence(node.left)
        op_precedence = self.precedence(node.ops[0])

        if left_precedence != 0 and ((op_precedence > left_precedence) or (op_precedence == left_precedence)):
            self.printer.delimiter('(')
            self._expression(node.left)
            self.printer.delimiter(')')
        else:
            self._expression(node.left)

        for op, comparator in zip(node.ops, node.comparators):
            self._expression(op)
            self._rhs(comparator, op)

    def visit_Eq(self, node):
        self.printer.operator('==')

    def visit_NotEq(self, node):
        self.printer.operator('!=')

    def visit_Lt(self, node):
        self.printer.operator('<')

    def visit_LtE(self, node):
        self.printer.operator('<=')

    def visit_Gt(self, node):
        self.printer.operator('>')

    def visit_GtE(self, node):
        self.printer.operator('>=')

    def visit_Is(self, node):
        self.printer.keyword('is')

    def visit_IsNot(self, node):
        self.printer.keyword('is')
        self.printer.keyword('not')

    def visit_In(self, node):
        self.printer.keyword('in')

    def visit_NotIn(self, node):
        self.printer.keyword('not')
        self.printer.keyword('in')

    def visit_Call(self, node):

        self._lhs(node.func, node)

        self.printer.delimiter('(')

        single_call = len(node.args) == 1 and not node.keywords and not hasattr(node, 'starargs') and not hasattr(node, 'kwargs')

        delimiter = Delimiter(self.printer)

        for arg in node.args:
            delimiter.new_item()

            if single_call and isinstance(arg, ast.GeneratorExp):
                self.visit_GeneratorExp(arg, omit_parens=True)
            else:
                self._expression(arg)

        if node.keywords:
            for kwarg in node.keywords:
                delimiter.new_item()

                assert isinstance(kwarg, ast.keyword)
                self.visit_keyword(kwarg)

        if hasattr(node, 'starargs') and node.starargs is not None:
            delimiter.new_item()

            self.printer.operator('*')
            self._expression(node.starargs)

        if hasattr(node, 'kwargs') and node.kwargs is not None:
            delimiter.new_item()

            self.printer.operator('**')
            self.visit(node.kwargs)

        self.printer.delimiter(')')

    def visit_keyword(self, node):
        if node.arg is None:
            self.printer.operator('**')
            self._expression(node.value)
        else:
            self.printer.identifier(node.arg)
            self.printer.delimiter('=')
            self._expression(node.value)

    def visit_IfExp(self, node):

        self._rhs(node.body, node)

        self.printer.keyword('if')

        self._rhs(node.test, node)

        self.printer.keyword('else')

        self._expression(node.orelse)

    def visit_Attribute(self, node):
        value_precedence = self.precedence(node.value)
        attr_precedence = self.precedence(node)

        if (value_precedence != 0 and (attr_precedence > value_precedence)) or is_constant_node(node.value, ast.Num):
            self.printer.delimiter('(')
            self._expression(node.value)
            self.printer.delimiter(')')
        else:
            self._expression(node.value)

        self.printer.delimiter('.')
        self.printer.identifier(node.attr)

    # endregion

    # region Subscripting

    def visit_Subscript(self, node):

        value_precedence = self.precedence(node.value)
        slice_precedence = 17  # self.precedence(node)

        if value_precedence != 0 and (slice_precedence > value_precedence):
            self.printer.delimiter('(')
            self._expression(node.value)
            self.printer.delimiter(')')
        else:
            self._expression(node.value)

        self.printer.delimiter('[')

        if isinstance(node.slice, ast.Index):
            self.visit_Index(node.slice)
        elif isinstance(node.slice, ast.Slice):
            self.visit_Slice(node.slice)
        elif isinstance(node.slice, ast.ExtSlice):
            self.visit_ExtSlice(node.slice)
        elif is_constant_node(node.slice, ast.Ellipsis):
            self.visit_Ellipsis(node)
        elif sys.version_info >= (3, 9) and isinstance(node.slice, ast.Tuple):
            self.visit_Tuple(node.slice)
        elif sys.version_info >= (3, 9):
            self._expression(node.slice)
        else:
            raise AssertionError('Unknown slice type %r' % node.slice)

        self.printer.delimiter(']')

    def visit_Index(self, node):
        self._expression(node.value)

    def visit_Slice(self, node):
        if node.lower:
            self._expression(node.lower)
        self.printer.delimiter(':')

        if node.upper:
            self._expression(node.upper)
        if node.step:
            self.printer.delimiter(':')
            self._expression(node.step)

    def visit_ExtSlice(self, node):

        delimiter = Delimiter(self.printer)
        for s in node.dims:
            delimiter.new_item()
            self._expression(s)

        if len(node.dims) == 1:
            self.printer.delimiter(',')

    # endregion

    # region Comprehensions

    def visit_ListComp(self, node):
        self.printer.delimiter('[')
        self._expression(node.elt)
        [self.visit_comprehension(x) for x in node.generators]
        self.printer.delimiter(']')

    def visit_SetComp(self, node):
        self.printer.delimiter('{')
        self._expression(node.elt)
        [self.visit_comprehension(x) for x in node.generators]
        self.printer.delimiter('}')

    def visit_GeneratorExp(self, node, omit_parens=False):

        if not omit_parens:
            self.printer.delimiter('(')

        self._expression(node.elt)
        [self.visit_comprehension(x) for x in node.generators]

        if not omit_parens:
            self.printer.delimiter(')')

    def visit_DictComp(self, node):
        self.printer.delimiter('{')
        self._expression(node.key)
        self.printer.delimiter(':')
        self._expression(node.value)
        [self.visit_comprehension(x) for x in node.generators]
        self.printer.delimiter('}')

    def visit_comprehension(self, node):
        assert isinstance(node, ast.comprehension)

        if hasattr(node, 'is_async') and node.is_async:
            self.printer.keyword('async')

        self.printer.keyword('for')
        self._exprlist([node.target])
        self.printer.keyword('in')

        self._rhs(node.iter, node)

        if node.ifs:
            for i in node.ifs:
                self.printer.keyword('if')
                self._rhs(i, node)

    # endregion

    # region Function and Class definitions

    def visit_Lambda(self, node):

        self.printer.keyword('lambda')

        self.visit_arguments(node.args)

        self.printer.delimiter(':')

        self._expression(node.body)

    def visit_arguments(self, node):
        args = getattr(node, 'posonlyargs', []) + node.args

        delimiter = Delimiter(self.printer)

        count_no_defaults = len(args) - len(node.defaults)
        for i, arg in enumerate(args):
            delimiter.new_item()

            self._expression(arg)

            if i >= count_no_defaults:
                self.printer.delimiter('=')
                self._expression(node.defaults[i - count_no_defaults])

            if hasattr(node, 'posonlyargs') and node.posonlyargs and i + 1 == len(node.posonlyargs):
                self.printer.delimiter(',')
                self.printer.operator('/')

        if node.vararg:
            delimiter.new_item()

            self.printer.operator('*')

            if hasattr(node, 'varargannotation'):
                self.printer.identifier(node.vararg)
                if node.varargannotation is not None:
                    self.printer.delimiter(':')
                    self._expression(node.varargannotation)
            elif isinstance(node.vararg, str):
                self.printer.identifier(node.vararg)
            else:
                self.visit(node.vararg)

        if hasattr(node, 'kwonlyargs') and node.kwonlyargs:

            if not node.vararg:
                delimiter.new_item()
                self.printer.operator('*')

            for i, arg in enumerate(node.kwonlyargs):
                self.printer.delimiter(',')
                self.visit_arg(arg)

                if node.kw_defaults[i] is not None:
                    self.printer.delimiter('=')
                    self._expression(node.kw_defaults[i])

        if node.kwarg:
            delimiter.new_item()

            self.printer.operator('**')

            if hasattr(node, 'kwargannotation'):
                self.printer.identifier(node.kwarg)
                if node.kwargannotation is not None:
                    self.printer.delimiter(':')
                    self._expression(node.kwargannotation)
            elif isinstance(node.kwarg, str):
                self.printer.identifier(node.kwarg)
            else:
                self.visit(node.kwarg)

    def visit_arg(self, node):
        if isinstance(node, ast.Name):
            # Python 2 uses Name nodes
            self.visit_Name(node)
            return

        self.printer.identifier(node.arg)

        if node.annotation:
            self.printer.delimiter(':')
            self._expression(node.annotation)

    def visit_Repr(self, node):
        self.printer.delimiter('`')
        self._expression(node.value)
        self.printer.delimiter('`')

    # endregion

    def visit_Expression(self, node):
        self._expression(node.body)

    def _expression(self, expression):
        if isinstance(expression, (ast.Yield, ast.YieldFrom)):
            self.printer.delimiter('(')
            self._yield_expr(expression)
            self.printer.delimiter(')')
        elif isinstance(expression, ast.Tuple) and len(expression.elts) > 0:
            self.printer.delimiter('(')
            self.visit_Tuple(expression)
            self.printer.delimiter(')')
        elif isinstance(expression, ast.NamedExpr):
            self.printer.delimiter('(')
            self.visit_NamedExpr(expression)
            self.printer.delimiter(')')
        else:
            self.visit(expression)

    def _testlist(self, test):
        if isinstance(test, (ast.Yield, ast.YieldFrom)):
            self.printer.delimiter('(')
            self._yield_expr(test)
            self.printer.delimiter(')')
        elif isinstance(test, ast.NamedExpr):
            self.printer.delimiter('(')
            self.visit_NamedExpr(test)
            self.printer.delimiter(')')
        else:
            self.visit(test)

    def _exprlist(self, exprlist):
        delimiter = Delimiter(self.printer)
        for expr in exprlist:
            delimiter.new_item()
            self._expression(expr)

    def _yield_expr(self, yield_node):
        if isinstance(yield_node, ast.Yield):
            self.printer.keyword('yield')
        elif isinstance(yield_node, ast.YieldFrom):
            self.printer.keyword('yield')
            self.printer.keyword('from')

        if yield_node.value is not None:
            self._expression(yield_node.value)

    @staticmethod
    def _is_right_associative(operator):
        return isinstance(operator, ast.Pow)

    @staticmethod
    def _is_left_associative(operator):
        return not isinstance(operator, ast.Pow)

    def _lhs(self, left_node, op_node):
        left_precedence = self.precedence(left_node)
        op_precedence = self.precedence(op_node)

        if left_precedence != 0 and (
            (op_precedence > left_precedence)
            or (op_precedence == left_precedence and self._is_right_associative(op_node))
        ):
            self.printer.delimiter('(')
            self._expression(left_node)
            self.printer.delimiter(')')
        else:
            self._expression(left_node)

    def _rhs(self, right_node, op_node):
        right_precedence = self.precedence(right_node)
        op_precedence = self.precedence(op_node)

        if isinstance(op_node, ast.Pow) and right_precedence == 14:
            op_precedence = right_precedence

        if right_precedence != 0 and (
            (op_precedence > right_precedence)
            or (op_precedence == right_precedence and self._is_left_associative(op_node))
        ):
            self.printer.delimiter('(')
            self._expression(right_node)
            self.printer.delimiter(')')
        else:
            self._expression(right_node)

    def visit_JoinedStr(self, node):
        assert isinstance(node, ast.JoinedStr)

        import python_minifier.f_string

        if sys.version_info < (3, 12):
            pep701 = False
        else:
            pep701 = True

        self.printer.fstring(str(python_minifier.f_string.OuterFString(node, pep701=pep701)))

    def visit_TemplateStr(self, node):
        assert isinstance(node, ast.TemplateStr)

        import python_minifier.t_string

        self.printer.tstring(str(python_minifier.t_string.TString(node)))

    def visit_NamedExpr(self, node):
        self._expression(node.target)
        self.printer.operator(':=')
        self._expression(node.value)

    def visit_Await(self, node):
        assert isinstance(node, ast.Await)
        self.printer.keyword('await')
        self._rhs(node.value, node)
