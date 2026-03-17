import python_minifier.ast_compat as ast

from python_minifier.transforms.suite_transformer import SuiteTransformer


class CombineImports(SuiteTransformer):
    """
    Combine multiple import statements where possible

    This doesn't change the order of imports

    """

    def _combine_import(self, node_list, parent):

        alias = []
        namespace = None

        for statement in node_list:
            namespace = statement.namespace
            if isinstance(statement, ast.Import):
                alias += statement.names
            else:
                if alias:
                    yield self.add_child(ast.Import(names=alias), parent=parent, namespace=namespace)
                    alias = []

                yield statement

        if alias:
            yield self.add_child(ast.Import(names=alias), parent=parent, namespace=namespace)

    def _combine_import_from(self, node_list, parent):

        prev_import = None
        alias = []

        def combine(statement):
            if not isinstance(statement, ast.ImportFrom):
                return False

            if len(statement.names) == 1 and statement.names[0].name == '*':
                return False

            if prev_import is None:
                return True

            if statement.module == prev_import.module and statement.level == prev_import.level:
                return True

            return False

        for statement in node_list:
            if combine(statement):
                prev_import = statement
                alias += statement.names
            else:
                if alias:
                    yield self.add_child(
                        ast.ImportFrom(module=prev_import.module, names=alias, level=prev_import.level), parent=parent, namespace=prev_import.namespace
                    )
                    alias = []

                yield statement

        if alias:
            yield self.add_child(
                ast.ImportFrom(module=prev_import.module, names=alias, level=prev_import.level), parent=parent, namespace=prev_import.namespace
            )

    def suite(self, node_list, parent):
        a = list(self._combine_import(node_list, parent))
        b = list(self._combine_import_from(a, parent))

        return [self.visit(n) for n in b]
