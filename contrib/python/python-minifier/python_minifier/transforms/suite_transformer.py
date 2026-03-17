import python_minifier.ast_compat as ast
from python_minifier.ast_annotation import get_parent, add_parent as add_node_parent

from python_minifier.rename.mapper import add_parent


class NodeVisitor(object):
    def visit(self, node):
        """Visit a node."""
        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node):
        """Called if no explicit visitor function exists for a node."""
        for _field, value in ast.iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        self.visit(item)
            elif isinstance(value, ast.AST):
                self.visit(value)

    def visit_Constant(self, node):
        if node.value in [None, True, False]:
            method = 'visit_NameConstant'
        elif isinstance(node.value, (int, float, complex)):
            method = 'visit_Num'
        elif isinstance(node.value, str):
            method = 'visit_Str'
        elif isinstance(node.value, bytes):
            method = 'visit_Bytes'
        elif node.value == Ellipsis:
            method = 'visit_Ellipsis'
        else:
            raise RuntimeError('Unknown Constant value %r' % type(node.value))

        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)


class SuiteTransformer(NodeVisitor):
    """
    Transform suites of instructions
    """

    def __call__(self, node):
        return self.visit(node)

    def visit_ClassDef(self, node):
        node.bases = [self.visit(b) for b in node.bases]

        if hasattr(node, 'type_params') and node.type_params is not None:
            node.type_params = [self.visit(t) for t in node.type_params]

        node.body = self.suite(node.body, parent=node)
        node.decorator_list = [self.visit(d) for d in node.decorator_list]

        if hasattr(node, 'starargs') and node.starargs is not None:
            node.starargs = self.visit(node.starargs)

        if hasattr(node, 'kwargs') and node.kwargs is not None:
            node.kwargs = self.visit(node.kwargs)

        if hasattr(node, 'keywords'):
            node.keywords = [self.visit(kw) for kw in node.keywords]

        return node

    def visit_FunctionDef(self, node):
        node.args = self.visit(node.args)
        node.body = self.suite(node.body, parent=node)
        node.decorator_list = [self.visit(d) for d in node.decorator_list]

        if hasattr(node, 'returns') and node.returns is not None:
            node.returns = self.visit(node.returns)

        return node

    def visit_AsyncFunctionDef(self, node):
        return self.visit_FunctionDef(node)

    def visit_For(self, node):
        node.target = self.visit(node.target)
        node.iter = self.visit(node.iter)

        node.body = self.suite(node.body, parent=node)

        if node.orelse:
            node.orelse = self.suite(node.orelse, parent=node)

        return node

    def visit_AsyncFor(self, node):
        return self.visit_For(node)

    def visit_If(self, node):
        node.test = self.visit(node.test)

        node.body = self.suite(node.body, parent=node)

        if node.orelse:
            node.orelse = self.suite(node.orelse, parent=node)

        return node

    def visit_Try(self, node):
        node.body = self.suite(node.body, parent=node)

        node.handlers = [self.visit(h) for h in node.handlers]

        if node.orelse:
            node.orelse = self.suite(node.orelse, parent=node)

        if node.finalbody:
            node.finalbody = self.suite(node.finalbody, parent=node)

        return node

    def visit_While(self, node):
        node.test = self.visit(node.test)

        node.body = self.suite(node.body, parent=node)

        if node.orelse:
            node.orelse = self.suite(node.orelse, parent=node)

        return node

    def visit_With(self, node):

        if hasattr(node, 'items'):
            node.items = [self.visit(i) for i in node.items]
        else:
            if node.context_expr:
                node.context_expr = self.visit(node.context_expr)
            if node.optional_vars:
                node.optional_vars = self.visit(node.optional_vars)

        node.body = self.suite(node.body, parent=node)
        return node

    def visit_AsyncWith(self, node):
        return self.visit_With(node)

    def visit_Module(self, node):
        node.body = self.suite(node.body, parent=node)
        return node

    def suite(self, node_list, parent):
        return [self.visit(node) for node in node_list]

    def generic_visit(self, node):
        for field, old_value in ast.iter_fields(node):
            if isinstance(old_value, list):
                new_values = []
                for value in old_value:
                    if isinstance(value, ast.AST):
                        value = self.visit(value)
                        if value is None:
                            continue
                        elif not isinstance(value, ast.AST):
                            new_values.extend(value)
                            continue
                    new_values.append(value)
                old_value[:] = new_values
            elif isinstance(old_value, ast.AST):
                new_node = self.visit(old_value)
                if new_node is None:
                    delattr(node, field)
                else:
                    setattr(node, field, new_node)
        return node

    def add_child(self, child, parent, namespace=None):
        def nearest_function_namespace(node):
            """
            Return the namespace node for the nearest function scope.

            This could be itself.

            :param node: The node to get the function namespace of
            :type node: ast.Node
            :rtype: ast.Node

            """

            if isinstance(node, (ast.FunctionDef, ast.Module, ast.AsyncFunctionDef)):
                return node
            return nearest_function_namespace(get_parent(node))

        if namespace is None:
            namespace = nearest_function_namespace(parent)

        add_node_parent(child, parent=parent)
        add_parent(child, namespace=namespace)
        return child
