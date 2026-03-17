import python_minifier.ast_compat as ast

from python_minifier.transforms.suite_transformer import SuiteTransformer
from python_minifier.util import is_constant_node


def find_doc(node):

    if isinstance(node, ast.Attribute) and node.attr == '__doc__':
        raise ValueError('__doc__ found!')

    for child in ast.iter_child_nodes(node):
        find_doc(child)


def _doc_in_module(module):
    try:
        find_doc(module)
        return False
    except Exception:
        return True


class RemoveLiteralStatements(SuiteTransformer):
    """
    Remove literal expressions from the code

    This includes docstrings
    """

    def __call__(self, node):
        if _doc_in_module(node):
            return node
        return self.visit(node)

    def visit_Module(self, node):
        for binding in node.bindings:
            if binding.name == '__doc__':
                node.body = [self.visit(a) for a in node.body]
                return node

        node.body = self.suite(node.body, parent=node)
        return node

    def is_literal_statement(self, node):
        if not isinstance(node, ast.Expr):
            return False

        return is_constant_node(node.value, (ast.Num, ast.Str, ast.NameConstant, ast.Bytes))

    def suite(self, node_list, parent):
        without_literals = [self.visit(n) for n in node_list if not self.is_literal_statement(n)]

        if len(without_literals) == 0:
            if isinstance(parent, ast.Module):
                return []
            else:
                return [self.add_child(ast.Expr(value=ast.Num(0)), parent=parent)]

        return without_literals
