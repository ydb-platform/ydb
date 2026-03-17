import python_minifier.ast_compat as ast

from python_minifier.transforms.suite_transformer import SuiteTransformer


class RemovePass(SuiteTransformer):
    """
    Remove Pass keywords from source

    If a statement is syntactically necessary, use an empty expression instead
    """

    def __call__(self, node):
        return self.visit(node)

    def suite(self, node_list, parent):
        without_pass = [self.visit(a) for a in filter(lambda n: not isinstance(n, ast.Pass), node_list)]

        if len(without_pass) == 0:
            if isinstance(parent, ast.Module):
                return []
            else:
                return [self.add_child(ast.Expr(value=ast.Num(0)), parent=parent)]

        return without_pass
