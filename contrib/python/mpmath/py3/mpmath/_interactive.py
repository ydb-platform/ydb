import ast
import io
import re
import tokenize


class IntegerDivisionWrapper(ast.NodeTransformer):
    """Wrap all int divisions in a call to :class:`~fractions.Fraction`."""

    def visit_BinOp(self, node):
        def is_integer(x):
            if isinstance(x, ast.Constant) and isinstance(x.value, int):
                return True
            if isinstance(x, ast.UnaryOp) and isinstance(x.op, (ast.USub,
                                                                ast.UAdd)):
                return is_integer(x.operand)
            if isinstance(x, ast.BinOp) and isinstance(x.op, (ast.Add,
                                                              ast.Sub,
                                                              ast.Mult,
                                                              ast.Pow)):
                return is_integer(x.left) and is_integer(x.right)
            return False

        if isinstance(node.op, ast.Div) and all(map(is_integer,
                                                    [node.left, node.right])):
            return ast.Call(ast.Name('Fraction', ast.Load()),
                            [node.left, node.right], [])
        return self.generic_visit(node)


class _WrapFloats(ast.NodeTransformer):
    """Wrap float literals by calls to specified type."""
    def __init__(self, lines, type):
        super().__init__()
        self.lines = lines
        self.type = type

    def visit_Constant(self, node):
        if isinstance(node.value, (float, complex)):
            line = self.lines[node.lineno - 1]
            value = line[node.col_offset:node.end_col_offset]
            is_complex = value.endswith(('j', 'J'))
            if is_complex:
                value = value[:-1]
            value = ast.Constant(value)
            value = ast.Call(ast.Name(self.type, ast.Load()), [value], [])
            if is_complex:
                value = ast.BinOp(left=value, op=ast.Mult(),
                                  right=ast.Constant(1j))
            return value
        return node


def wrap_float_literals(lines):
    """Wraps all float/complex literals with mpmath classes."""
    source = ''.join(lines)
    tree = ast.parse(source)
    tree = _WrapFloats(lines, 'mpf').visit(tree)
    ast.fix_missing_locations(tree)
    source = ast.unparse(tree)
    return source.splitlines(keepends=True)


_HEXFLT_MATCHER = re.compile(r"""
    (?: [^"' ]|^)[ ]*(?P<hexflt>
         0x
         [0-9a-z]+
         (?: \.[0-9a-z]*)?
         p(?:[+-])?[0-9]+
     )
""", re.VERBOSE | re.IGNORECASE)
_BINFLT_MATCHER = re.compile(r"""
    (?: [^"' ]|^)[ ]*(?P<binflt>
         0b
         [01]+
         (?: \.[01]*)?
         p(?:[+-])?[0-9]+
     )
""", re.VERBOSE | re.IGNORECASE)


def wrap_hexbinfloats(lines):
    new_lines = []
    for line in lines:
        for r in _HEXFLT_MATCHER.findall(line):
            line = line.replace(r, 'mpf("' + r + '", base=16)')
        for r in _BINFLT_MATCHER.findall(line):
            line = line.replace(r, 'mpf("' + r + '", base=2)')
        new_lines.append(line)
    return new_lines
