import sys

import python_minifier.ast_compat as ast

from python_minifier.transforms.suite_transformer import SuiteTransformer
from python_minifier.util import is_constant_node


class RemoveDebug(SuiteTransformer):
    """
    Remove if statements where the condition tests __debug__ is True

    If a statement is syntactically necessary, use an empty expression instead
    """

    def __call__(self, node):
        return self.visit(node)

    def constant_value(self, node):
        if sys.version_info < (3, 4):
            return node.id == 'True'
        elif is_constant_node(node, ast.NameConstant):
            return node.value
        return None

    def can_remove(self, node):
        if not isinstance(node, ast.If):
            return False

        def is_simple_debug_check():
            # Simple case: if __debug__:
            if isinstance(node.test, ast.Name) and node.test.id == '__debug__':
                return True
            return False

        def is_truthy_debug_comparison():
            # Comparison case: if __debug__ is True / False / etc.
            if not isinstance(node.test, ast.Compare):
                return False

            if not isinstance(node.test.left, ast.Name):
                return False

            if node.test.left.id != '__debug__':
                return False

            if len(node.test.ops) == 1:
                op = node.test.ops[0]
                comparator_value = self.constant_value(node.test.comparators[0])

                if isinstance(op, ast.Is) and comparator_value is True:
                    return True
                if isinstance(op, ast.IsNot) and comparator_value is False:
                    return True
                if isinstance(op, ast.Eq) and comparator_value is True:
                    return True

            return False

        if is_simple_debug_check() or is_truthy_debug_comparison():
            return True
        return False

    def suite(self, node_list, parent):

        without_debug = [self.visit(a) for a in filter(lambda n: not self.can_remove(n), node_list)]

        if len(without_debug) == 0:
            if isinstance(parent, ast.Module):
                return []
            else:
                return [self.add_child(ast.Expr(value=ast.Num(0)), parent=parent)]

        return without_debug
