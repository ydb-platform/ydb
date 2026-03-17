import python_minifier.ast_compat as ast


class CompareError(RuntimeError):
    """
    Raised when an AST compares unequal.
    """

    def __init__(self, lnode, rnode, msg=None):
        self.lnode = lnode
        self.rnode = rnode
        self.msg = msg

    def __repr__(self):
        return 'NodeError(%r, %r)' % (self.lnode, self.rnode)

    def namespace(self, node):
        if hasattr(node, 'namespace'):
            if isinstance(node.namespace, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                return self.namespace(node.namespace) + '.' + node.namespace.name
            elif isinstance(node.namespace, ast.Module):
                return ''
            else:
                return repr(node.namespace.__class__)

        return None

    def __str__(self):
        error = ''

        if self.msg:
            error += self.msg

        if self.namespace(self.lnode):
            error += ' in namespace ' + self.namespace(self.lnode)

        if self.lnode and hasattr(self.lnode, 'lineno'):
            error += ' at source %i:%i' % (self.lnode.lineno, self.lnode.col_offset)

        return error


def compare_ast(l_ast, r_ast):
    """
    Compare Python Abstract Syntax Trees

    >>> compare_ast(l_ast, r_ast)

    If the AST's are not identical, an exception will be raised.

    """

    def counter():
        i = 0
        while True:
            yield i
            i += 1

    if type(l_ast) != type(r_ast):
        raise CompareError(l_ast, r_ast, msg='Nodes do not match! %r != %r' % (l_ast, r_ast))

    for field in sorted(set(l_ast._fields + r_ast._fields)):

        if field == 'kind' and isinstance(l_ast, ast.Constant):
            continue

        if field == 'str' and hasattr(ast, 'Interpolation') and isinstance(l_ast, ast.Interpolation):
            continue

        if isinstance(getattr(l_ast, field, None), list):

            l_list = getattr(l_ast, field, None)
            r_list = getattr(r_ast, field, None)

            if len(l_list) != len(r_list):
                raise CompareError(
                    l_list,
                    r_list,
                    'List does not have the same number of elements! len(%s.%s)=%r, len(%s.%s)=%r'
                    % (type(l_ast), field, len(l_list), type(r_ast), field, len(r_list)),
                )

            for i, left, right in zip(counter(), l_list, r_list):
                if isinstance(left, ast.AST) or isinstance(right, ast.AST):
                    compare_ast(left, right)
                elif left != right:
                    raise CompareError(
                        l_ast,
                        r_ast,
                        'Fields do not match! %s.%s[%i]=%r, %s.%s[%i]=%r'
                        % (type(l_ast), field, i, left, type(r_ast), field, i, right),
                    )

        else:
            left_field = getattr(l_ast, field, None)
            right_field = getattr(r_ast, field, None)

            if isinstance(left_field, ast.AST) or isinstance(right_field, ast.AST):
                compare_ast(left_field, right_field)
            elif left_field != right_field:
                raise CompareError(
                    l_ast,
                    r_ast,
                    'Fields do not match! %s.%s=%r, %s.%s=%r' % (type(l_ast), field, left_field, type(r_ast), field, right_field),
                )
