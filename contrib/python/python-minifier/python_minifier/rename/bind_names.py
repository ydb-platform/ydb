import python_minifier.ast_compat as ast

from python_minifier.rename.binding import NameBinding
from python_minifier.rename.util import arg_rename_in_place, builtins, get_global_namespace
from python_minifier.transforms.suite_transformer import NodeVisitor


class NameBinder(NodeVisitor):
    """
    Create a NameBinding for each name that is bound

    The NameBinding is added to the bindings dictionary in the namespace node the name is local to.
    """

    def __call__(self, module):
        assert isinstance(module, ast.Module)
        module.tainted = False
        module.preserved = set()
        return self.visit(module)

    def get_binding(self, name, namespace):
        if name in namespace.global_names and not isinstance(namespace, ast.Module):
            return self.get_binding(name, get_global_namespace(namespace))

        # nonlocal names should not create a binding in any context
        assert name not in namespace.nonlocal_names

        for binding in namespace.bindings:
            if binding.name == name:
                break
        else:  # weeee!
            binding = NameBinding(name)
            namespace.bindings.append(binding)

            if name in dir(builtins):
                binding.disallow_rename()

        if name in namespace.nonlocal_names and isinstance(namespace, ast.Module):
            # This is actually a syntax error - but we want the same syntax error after minifying!
            binding.disallow_rename()

        if isinstance(namespace, ast.ClassDef):
            # This name will become an attribute of the class, so it can't be renamed
            binding.disallow_rename()

        return binding

    def visit_Name(self, node):
        if node.id in node.namespace.nonlocal_names:
            # A nonlocal name does not create a binding.
            # We will resolve the binding later
            return

        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.get_binding(node.id, node.namespace).add_reference(node)

        if isinstance(node.ctx, ast.Param):
            binding = self.get_binding(node.id, node.namespace)

            if arg_rename_in_place(node):
                binding.add_reference(node)
            else:
                binding.add_reference(node, reserved=node.id)

                if isinstance(node.namespace, ast.Lambda):
                    # Lambda function arguments can't be renamed without breaking keyword arguments
                    binding.disallow_rename()

    def visit_ClassDef(self, node):
        if node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        if node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.visit_FunctionDef(node)

    def visit_alias(self, node):
        if node.name == '*':
            get_global_namespace(node).tainted = True

        root_module = node.name.split('.')[0]

        if root_module == 'timeit':
            get_global_namespace(node).tainted = True

        if node.asname is not None:
            if node.asname not in node.namespace.nonlocal_names:
                self.get_binding(node.asname, node.namespace).add_reference(node)
        else:
            # This binds the root module only for a dotted import

            if root_module not in node.namespace.nonlocal_names:
                binding = self.get_binding(root_module, node.namespace)
                binding.add_reference(node)

                if '.' in node.name:
                    binding.disallow_rename()

    def visit_arguments(self, node):
        # varargs, kwarg can't be nonlocal
        if isinstance(node.vararg, str):
            binding = self.get_binding(node.vararg, node.namespace)
            binding.add_reference(node)

        if isinstance(node.kwarg, str):
            binding = self.get_binding(node.kwarg, node.namespace)
            binding.add_reference(node)

        self.generic_visit(node)

    def visit_arg(self, node):
        # Args can't be nonlocal
        binding = self.get_binding(node.arg, node.namespace)

        if arg_rename_in_place(node):
            binding.add_reference(node)
        else:
            binding.add_reference(node, reserved=node.arg)

            if isinstance(node.namespace, ast.Lambda):
                # Lambda function arguments can't be renamed without breaking keyword arguments
                binding.disallow_rename()

        self.generic_visit(node)

    def visit_ExceptHandler(self, node):
        if node.name is not None:
            if isinstance(node.name, str) and node.name not in node.namespace.nonlocal_names:
                # python 3
                self.get_binding(node.name, node.namespace).add_reference(node)
            else:
                # In python 2 the name is a Name node,
                # which will be visited by generic_visit
                pass

        self.generic_visit(node)

    def visit_Global(self, node):
        for name in node.names:
            self.get_binding(name, node.namespace).add_reference(node)

    def visit_MatchAs(self, node):
        if node.name is not None and node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)

        self.generic_visit(node)

    def visit_MatchStar(self, node):
        if node.name is not None and node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)

        self.generic_visit(node)

    def visit_MatchMapping(self, node):
        if node.rest is not None and node.rest not in node.namespace.nonlocal_names:
            self.get_binding(node.rest, node.namespace).add_reference(node)

        self.generic_visit(node)

    def visit_TypeVar(self, node):
        if node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)

        get_global_namespace(node.namespace).preserved.add(node.name)

    def visit_TypeVarTuple(self, node):
        if node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)

        get_global_namespace(node.namespace).preserved.add(node.name)

    def visit_ParamSpec(self, node):
        if node.name not in node.namespace.nonlocal_names:
            self.get_binding(node.name, node.namespace).add_reference(node)

        get_global_namespace(node.namespace).preserved.add(node.name)


def bind_names(module):
    """
    Bind names to their local namespace

    :param module: The module to bind names in
    :type: :class:`ast.Module`

    """

    NameBinder()(module)
