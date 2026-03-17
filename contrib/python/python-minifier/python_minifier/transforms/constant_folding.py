import math
import sys

import python_minifier.ast_compat as ast
from python_minifier.ast_annotation import get_parent

from python_minifier.ast_compare import compare_ast
from python_minifier.expression_printer import ExpressionPrinter
from python_minifier.transforms.suite_transformer import SuiteTransformer
from python_minifier.util import is_constant_node


def is_foldable_constant(node):
    """
    Check if a node is a constant expression that can participate in folding.

    We can asume that children have already been folded, so foldable constants are either:
    - Simple literals (Num, NameConstant)
    - UnaryOp(USub/Invert) on a Num - these don't fold to shorter forms,
      so they remain after child visiting. UAdd and Not would have been
      folded away since they always produce shorter results.
    """
    if is_constant_node(node, (ast.Num, ast.NameConstant)):
        return True

    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, (ast.USub, ast.Invert)):
            return is_constant_node(node.operand, ast.Num)

    return False


class FoldConstants(SuiteTransformer):
    """
    Fold Constants if it would reduce the size of the source
    """

    def __init__(self):
        super(FoldConstants, self).__init__()

    def fold(self, node):
        # Evaluate the expression
        try:
            original_expression = unparse_expression(node)
            original_value = safe_eval(original_expression)
        except Exception:
            return node

        # Choose the best representation of the value
        if isinstance(original_value, float) and math.isnan(original_value):
            # There is no nan literal.
            # we could use float('nan'), but that complicates folding as it's not a Constant
            return node
        elif isinstance(original_value, bool):
            new_node = ast.NameConstant(value=original_value)
        elif isinstance(original_value, (int, float, complex)):
            try:
                if repr(original_value).startswith('-') and not sys.version_info < (3, 0):
                    # Represent negative numbers as a USub UnaryOp, so that the ast roundtrip is correct
                    new_node = ast.UnaryOp(op=ast.USub(), operand=ast.Num(n=-original_value))
                else:
                    new_node = ast.Num(n=original_value)
            except Exception:
                # repr(value) failed, most likely due to some limit
                return node
        else:
            return node

        # Evaluate the new value representation
        try:
            folded_expression = unparse_expression(new_node)
            folded_value = safe_eval(folded_expression)
        except Exception:
            # This can happen if the value is too large to be represented as a literal
            # or if the value is unparsed as nan, inf or -inf - which are not valid python literals
            return node

        if len(folded_expression) >= len(original_expression):
            # Result is not shorter than original expression
            return node

        # Check the folded expression parses back to the same AST
        try:
            folded_ast = ast.parse(folded_expression, 'folded expression', mode='eval')
            compare_ast(new_node, folded_ast.body)
        except Exception:
            # This can happen if the printed value doesn't parse back to the same AST
            # e.g. complex numbers can be parsed as BinOp
            return node

        # Check the folded value is the same as the original value
        if not equal_value_and_type(folded_value, original_value):
            return node

        # New representation is shorter and has the same value, so use it
        return self.add_child(new_node, get_parent(node), node.namespace)

    def visit_BinOp(self, node):

        node.left = self.visit(node.left)
        node.right = self.visit(node.right)

        # Check this is a constant expression that could be folded
        # We don't try to fold strings or bytes, since they have probably been arranged this way to make the source shorter and we are unlikely to beat that
        if not is_foldable_constant(node.left):
            return node
        if not is_foldable_constant(node.right):
            return node

        if isinstance(node.op, ast.Div):
            # Folding div is subtle, since it can have different results in Python 2 and Python 3
            # Do this once target version options have been implemented
            return node

        if isinstance(node.op, ast.Pow):
            # This can be folded, but it is unlikely to reduce the size of the source
            # It can also be slow to evaluate
            return node

        return self.fold(node)

    def visit_UnaryOp(self, node):

        node.operand = self.visit(node.operand)

        # Only fold if the operand is a foldable constant
        if not is_foldable_constant(node.operand):
            return node

        # Only fold these unary operators
        if not isinstance(node.op, (ast.USub, ast.UAdd, ast.Invert, ast.Not)):
            return node

        return self.fold(node)


def equal_value_and_type(a, b):
    if type(a) != type(b):
        return False

    if isinstance(a, float) and math.isnan(a) and not math.isnan(b):
        return False

    return a == b


def safe_eval(expression):
    empty_globals = {}
    empty_locals = {}

    # This will return the value, or could raise an exception
    return eval(expression, empty_globals, empty_locals)


def unparse_expression(node):
    expression_printer = ExpressionPrinter()
    return expression_printer(node)
