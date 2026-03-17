import ast
import inspect
from collections.abc import Callable, Sequence
from types import ModuleType


def make_fragments_collector(*, typing_modules: Sequence[str]) -> Callable[[ast.Module], list[ast.stmt]]:
    def check_condition(expr: ast.expr) -> bool:
        # searches for `TYPE_CHECKING`
        if (
            isinstance(expr, ast.Name)
            and isinstance(expr.ctx, ast.Load)
            and expr.id == "TYPE_CHECKING"
        ):
            return True

        # searches for `typing.TYPE_CHECKING`
        if (  # noqa: SIM103
            isinstance(expr, ast.Attribute)
            and expr.attr == "TYPE_CHECKING"
            and isinstance(expr.ctx, ast.Load)
            and isinstance(expr.value, ast.Name)
            and expr.value.id in typing_modules
            and isinstance(expr.value.ctx, ast.Load)
        ):
            return True
        return False

    def collect_type_checking_only_fragments(module: ast.Module) -> list[ast.stmt]:
        fragments = []
        for stmt in module.body:
            if isinstance(stmt, ast.If) and not stmt.orelse and check_condition(stmt.test):
                fragments.extend(stmt.body)

        return fragments

    return collect_type_checking_only_fragments


default_collector = make_fragments_collector(typing_modules=["typing"])


def exec_type_checking(
    module: ModuleType,
    *,
    collector: Callable[[ast.Module], list[ast.stmt]] = default_collector,
) -> None:
    """This function scans module source code,
    collects fragments under ``if TYPE_CHECKING`` and ``if typing.TYPE_CHECKING``
    and executes them in the context of module.
    After these, all imports and type definitions became available at runtime for analysis.

    By default, it ignores ``if`` with ``else`` branch.

    :param module: A module for processing
    :param collector: A function collecting code fragments to execute
    """
    source = inspect.getsource(module)
    fragments = collector(ast.parse(source))
    code = compile(ast.Module(fragments, type_ignores=[]), f"<exec_type_checking of {module}>", "exec")
    namespace = module.__dict__.copy()
    exec(code, namespace)  # noqa: S102
    for k, v in namespace.items():
        if not hasattr(module, k):
            setattr(module, k, v)
