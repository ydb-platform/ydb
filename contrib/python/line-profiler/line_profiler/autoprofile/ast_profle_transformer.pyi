from typing import List
import _ast
import ast
from typing import Union


def ast_create_profile_node(modname,
                            profiler_name: str = ...,
                            attr: str = ...) -> (_ast.Expr):
    ...


class AstProfileTransformer(ast.NodeTransformer):

    def __init__(self,
                 profile_imports: bool = False,
                 profiled_imports: List[str] | None = None,
                 profiler_name: str = 'profile') -> None:
        ...

    def visit_FunctionDef(self, node) -> (_ast.FunctionDef):
        ...

    def visit_Import(
        self, node: _ast.Import
    ) -> (Union[_ast.Import, List[Union[_ast.Import, _ast.Expr]]]):
        ...

    def visit_ImportFrom(
        self, node: _ast.ImportFrom
    ) -> (Union[_ast.ImportFrom, List[Union[_ast.ImportFrom, _ast.Expr]]]):
        ...
