import python_minifier.ast_compat as ast


def is_constant_node(node, types):
    """
    Is a node one of the specified node types

    A node type may be an actual ast class or a tuple of many.

    If types includes a specific Constant type (Str, Bytes, Num etc),
    returns true for Constant nodes of the correct type.

    :type node: ast.AST
    :param types:
    :rtype: bool

    """

    if not isinstance(types, tuple):
        types = (types,)

    for node_type in types:
        assert not isinstance(node_type, str)

    if isinstance(node, types):
        return True

    if isinstance(node, ast.Constant):
        if type(node.value) in [type(None), type(True), type(False)]:
            return ast.NameConstant in types
        elif isinstance(node.value, (int, float, complex)):
            return ast.Num in types
        elif isinstance(node.value, str):
            return ast.Str in types
        elif isinstance(node.value, bytes):
            return ast.Bytes in types
        elif node.value == Ellipsis:
            return ast.Ellipsis in types
        else:
            raise RuntimeError('Unknown Constant value %r' % type(node.value))

    return False
