import sys

import python_minifier.ast_compat as ast

from python_minifier.util import is_constant_node


def create_is_namespace():

    namespace_nodes = (ast.FunctionDef, ast.Lambda, ast.ClassDef, ast.Module, ast.GeneratorExp)

    if sys.version_info >= (2, 7):
        namespace_nodes += (ast.SetComp, ast.DictComp)

    if sys.version_info >= (3, 0):
        namespace_nodes += (ast.ListComp,)

    if sys.version_info >= (3, 5):
        namespace_nodes += (ast.AsyncFunctionDef,)

    return lambda node: isinstance(node, namespace_nodes)


is_namespace = create_is_namespace()


def iter_child_namespaces(node):

    for child in ast.iter_child_nodes(node):
        if is_namespace(child):
            yield child
        else:
            for c in iter_child_namespaces(child):
                yield c


def get_global_namespace(node):
    """
    Return the global namespace for a node

    :rtype: :class:`ast.Module`

    """

    if node.namespace is node:
        return node

    return get_global_namespace(node.namespace)


def get_nonlocal_namespace(node):
    """
    Return the nonlocal namespace for a node

    The nonlocal namespace is the closest parent function scope's namespace.
    """

    if isinstance(node.namespace, ast.ClassDef):
        return get_nonlocal_namespace(node.namespace)

    return node.namespace


def arg_rename_in_place(node):
    """
    Can this argument node by safely renamed

    'self', 'cls', 'args', and 'kwargs' are not commonly referenced by the caller, so
    can be safely renamed. Comprehension arguments are not accessible from outside, so
    can be renamed.

    If the argument is positional-only, it can be safely renamed

    Other arguments may be referenced by the caller as keyword arguments, so should not be
    renamed in place. The name assigner may still decide to bind the argument to a new name
    inside the function namespace.

    :param node: The argument node
    :rtype node: :class:`ast.arg`
    :rtype: bool

    """

    func = node.namespace

    if isinstance(func, ast.comprehension):
        return True

    if isinstance(func.namespace, ast.ClassDef) and not isinstance(func, ast.Lambda):
        all_args = (func.args.posonlyargs if hasattr(func.args, 'posonlyargs') else []) + func.args.args
        if len(all_args) > 0 and node is all_args[0]:
            if len(func.decorator_list) == 0:
                # rename 'self'
                return True
            elif (
                len(func.decorator_list) == 1
                and isinstance(func.decorator_list[0], ast.Name)
                and func.decorator_list[0].id == 'classmethod'
            ):
                # rename 'cls'
                return True

    if func.args.vararg is node or func.args.kwarg is node:
        # starargs
        return True

    if hasattr(func.args, 'posonlyargs') and node in func.args.posonlyargs:
        return True

    return False


def insert(suite, new_node):
    """
    Insert a node into a suite

    Inserts new_node as early as possible in the suite, but after docstrings and `import __future__` statements.

    :param suite: The existing suite to insert the node into
    :param new_node: The node to insert
    :return: :class:`collections.Iterable[Node]`

    """

    inserted = False
    for node in suite:

        if not inserted:
            if (isinstance(node, ast.ImportFrom) and node.module == '__future__') or (
                isinstance(node, ast.Expr) and is_constant_node(node.value, ast.Str)
            ):
                pass
            else:
                yield new_node
                inserted = True

        yield node

    if not inserted:
        yield new_node


def allow_rename_locals(node, rename_locals, preserve_locals=None):

    if preserve_locals is None:
        preserve_locals = []

    if not isinstance(node, ast.Module) and is_namespace(node):
        for binding in node.bindings:
            if rename_locals is False:
                binding.disallow_rename()
            elif binding.name in preserve_locals:
                binding.disallow_rename()

    for child in ast.iter_child_nodes(node):
        allow_rename_locals(child, rename_locals, preserve_locals)


def find__all__(module):

    names = []

    def is_assign_all_node(node):
        if isinstance(node, ast.Assign):
            for name in node.targets:
                if isinstance(name, ast.Name) and name.id == '__all__':
                    return True

        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            if isinstance(node.target, ast.Name) and node.target.id == '__all__':
                return True

        return False

    for node in ast.iter_child_nodes(module):
        if not is_assign_all_node(node):
            continue

        if not isinstance(node.value, ast.List):
            continue

        for el in node.value.elts:
            if is_constant_node(el, ast.Str):
                names.append(el.s)

    return names


def allow_rename_globals(module, rename_globals=False, preserve_globals=None):

    if preserve_globals is None:
        preserve_globals = []

    preserve_globals.extend(find__all__(module))

    for binding in module.bindings:
        if rename_globals is False or binding.name in preserve_globals:
            binding.disallow_rename()


try:
    import builtins
except ImportError:
    # noinspection PyCompatibility
    import __builtin__ as builtins  # type: ignore
