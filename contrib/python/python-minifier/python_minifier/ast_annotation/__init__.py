"""
This module provides utilities for annotating Abstract Syntax Tree (AST) nodes with parent references.
"""

import ast

class _NoParent(ast.AST):
    """A placeholder class used to indicate that a node has no parent."""

    def __repr__(self):
        # type: () -> str
        return 'NoParent()'

def add_parent(node, parent=_NoParent()):
    # type: (ast.AST, ast.AST) -> None
    """
    Recursively adds a parent reference to each node in the AST.

    >>> tree = ast.parse('a = 1')
    >>> add_parent(tree)
    >>> get_parent(tree.body[0]) == tree
    True

    :param node: The current AST node.
    :param parent: The parent :class:`ast.AST` node.
    """

    node._parent = parent  # type: ignore[attr-defined]
    for child in ast.iter_child_nodes(node):
        add_parent(child, node)

def get_parent(node):
    # type: (ast.AST) -> ast.AST
    """
    Retrieves the parent of the given AST node.

    >>> tree = ast.parse('a = 1')
    >>> add_parent(tree)
    >>> get_parent(tree.body[0]) == tree
    True

    :param node: The AST node whose parent is to be retrieved.
    :return: The parent AST node.
    :raises ValueError: If the node has no parent.
    """

    if not hasattr(node, '_parent') or isinstance(node._parent, _NoParent):  # type: ignore[attr-defined]
        raise ValueError('Node has no parent')

    return node._parent  # type: ignore[attr-defined]

def set_parent(node, parent):
    # type: (ast.AST, ast.AST) -> None
    """
    Replace the parent of the given AST node.

    Create a simple AST:
    >>> tree = ast.parse('a = func()')
    >>> add_parent(tree)
    >>> isinstance(tree.body[0], ast.Assign) and isinstance(tree.body[0].value, ast.Call)
    True
    >>> assign = tree.body[0]
    >>> call = tree.body[0].value
    >>> get_parent(call) == assign
    True

    Replace the parent of the call node:
    >>> tree.body[0] = call
    >>> set_parent(call, tree)
    >>> get_parent(call) == tree
    True
    >>> from python_minifier.ast_printer import print_ast
    >>> print(print_ast(tree))
    Module(body=[
        Call(Name('func'))
    ])

    :param node: The AST node whose parent is to be set.
    :param parent: The parent AST node.
    """

    node._parent = parent  # type: ignore[attr-defined]
