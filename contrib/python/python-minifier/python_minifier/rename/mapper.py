"""
For each node in an AST set the namespace to use for name binding and resolution
"""

import python_minifier.ast_compat as ast
from python_minifier.ast_annotation import get_parent

from python_minifier.rename.util import is_namespace


def add_parent_to_arguments(arguments, func):
    arguments.namespace = func

    for arg in getattr(arguments, 'posonlyargs', []) + arguments.args:
        add_parent(arg, func)
        if hasattr(arg, 'annotation') and arg.annotation is not None:
            add_parent(arg.annotation, func.namespace)

    if hasattr(arguments, 'kwonlyargs'):
        for arg in arguments.kwonlyargs:
            add_parent(arg, func)
            if arg.annotation is not None:
                add_parent(arg.annotation, func.namespace)

        for node in arguments.kw_defaults:
            if node is not None:
                add_parent(node, func.namespace)

    for node in arguments.defaults:
        add_parent(node, func.namespace)

    if arguments.vararg:
        if hasattr(arguments, 'varargannotation') and arguments.varargannotation is not None:
            add_parent(arguments.varargannotation, func.namespace)
        elif isinstance(arguments.vararg, str):
            pass
        else:
            add_parent(arguments.vararg, func)

    if arguments.kwarg:
        if hasattr(arguments, 'kwargannotation') and arguments.kwargannotation is not None:
            add_parent(arguments.kwargannotation, func.namespace)
        elif isinstance(arguments.kwarg, str):
            pass
        else:
            add_parent(arguments.kwarg, func)


def add_parent_to_functiondef(functiondef):
    """
    Add correct parent and namespace attributes to functiondef nodes
    """

    if functiondef.args is not None:
        add_parent_to_arguments(functiondef.args, func=functiondef)

    for node in functiondef.body:
        add_parent(node, namespace=functiondef)

    for node in functiondef.decorator_list:
        add_parent(node, namespace=functiondef.namespace)

    if hasattr(functiondef, 'type_params') and functiondef.type_params is not None:
        for node in functiondef.type_params:
            add_parent(node, namespace=functiondef.namespace)

    if hasattr(functiondef, 'returns') and functiondef.returns is not None:
        add_parent(functiondef.returns, namespace=functiondef.namespace)


def add_parent_to_classdef(classdef):
    """
    Add correct parent and namespace attributes to classdef nodes
    """

    for node in classdef.bases:
        add_parent(node, namespace=classdef.namespace)

    if hasattr(classdef, 'keywords'):
        for node in classdef.keywords:
            add_parent(node, namespace=classdef.namespace)

    if hasattr(classdef, 'starargs') and classdef.starargs is not None:
        add_parent(classdef.starargs, namespace=classdef.namespace)

    if hasattr(classdef, 'kwargs') and classdef.kwargs is not None:
        add_parent(classdef.kwargs, namespace=classdef.namespace)

    for node in classdef.body:
        add_parent(node, namespace=classdef)

    for node in classdef.decorator_list:
        add_parent(node, namespace=classdef.namespace)

    if hasattr(classdef, 'type_params') and classdef.type_params is not None:
        for node in classdef.type_params:
            add_parent(node, namespace=classdef.namespace)


def add_parent_to_comprehension(node, namespace):
    assert isinstance(node, (ast.GeneratorExp, ast.SetComp, ast.DictComp, ast.ListComp))

    if hasattr(node, 'elt'):
        add_parent(node.elt, namespace=node)
    elif hasattr(node, 'key'):
        add_parent(node.key, namespace=node)
        add_parent(node.value, namespace=node)

    iter_namespace = namespace
    for generator in node.generators:
        generator.namespace = node

        add_parent(generator.target, namespace=node)
        add_parent(generator.iter, namespace=iter_namespace)

        for if_ in generator.ifs:
            add_parent(if_, namespace=node)

        iter_namespace = node

def namedexpr_namespace(node):
    """
    Get the namespace for a NamedExpr target
    """

    if not isinstance(node, (ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)):
        return node

    return namedexpr_namespace(node.namespace)

def add_parent_to_namedexpr(node):
    assert isinstance(node, ast.NamedExpr)

    add_parent(node.target, namespace=namedexpr_namespace(node.namespace))
    add_parent(node.value, namespace=node.namespace)

def add_parent(node, namespace=None):
    """
    Add a namespace attribute to child nodes

    :param node: The tree to add namespace properties to
    :type node: :class:`ast.AST`
    :param namespace: The namespace Node that this node is in
    :type namespace: ast.Lambda or ast.Module or ast.FunctionDef or ast.AsyncFunctionDef or ast.ClassDef or ast.DictComp or ast.SetComp or ast.ListComp or ast.Generator

    """

    node.namespace = namespace if namespace is not None else node

    if is_namespace(node):
        node.bindings = []
        node.global_names = set()
        node.nonlocal_names = set()

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            add_parent_to_functiondef(node)
        elif isinstance(node, (ast.GeneratorExp, ast.SetComp, ast.DictComp, ast.ListComp)):
            add_parent_to_comprehension(node, namespace=namespace)
        elif isinstance(node, ast.Lambda):
            add_parent_to_arguments(node.args, func=node)
            add_parent(node.body, namespace=node)
        elif isinstance(node, ast.ClassDef):
            add_parent_to_classdef(node)
        else:
            for child in ast.iter_child_nodes(node):
                add_parent(child, namespace=node)

        return

    if isinstance(node, ast.Global):
        namespace.global_names.update(node.names)
    if isinstance(node, ast.Nonlocal):
        namespace.nonlocal_names.update(node.names)

    if isinstance(node, ast.Name) and isinstance(namespace, ast.ClassDef):
        if isinstance(node.ctx, ast.Load):
            namespace.nonlocal_names.add(node.id)
        elif isinstance(node.ctx, ast.Store) and isinstance(get_parent(node), ast.AugAssign):
            namespace.nonlocal_names.add(node.id)

    if isinstance(node, ast.NamedExpr):
        # NamedExpr is 'special'
        add_parent_to_namedexpr(node)
        return

    for child in ast.iter_child_nodes(node):
        add_parent(child, namespace=namespace)


def add_namespace(module):
    add_parent(module)
