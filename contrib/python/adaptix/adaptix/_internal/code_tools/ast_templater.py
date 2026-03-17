import ast
from ast import AST, NodeTransformer
from collections.abc import Mapping


class Substitutor(NodeTransformer):
    __slots__ = ("substitution", )

    def __init__(self, substitution: Mapping[str, AST]):
        self._substitution = substitution

    def visit_Name(self, node: ast.Name):  # noqa: N802
        if node.id in self._substitution:
            return self._substitution[node.id]
        return node


def ast_substitute(template: str, **kwargs: AST) -> AST:
    substitution = {f"__{key}__": value for key, value in kwargs.items()}
    return Substitutor(substitution).generic_visit(ast.parse(template))
