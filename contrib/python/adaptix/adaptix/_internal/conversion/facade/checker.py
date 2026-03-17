import ast
import inspect
from textwrap import dedent


def ensure_function_is_stub(func):
    try:
        raw_source = inspect.getsource(func)
    except OSError:
        return

    source = dedent(raw_source)
    try:
        ast_module = ast.parse(source)
    except SyntaxError:
        return
    func_body = ast_module.body[0].body
    if len(func_body) == 1:
        body_element = func_body[0]
        if isinstance(body_element, ast.Pass):
            return
        if isinstance(body_element, ast.Expr) and isinstance(body_element.value, ast.Constant):
            value = body_element.value.value
            if value == Ellipsis or isinstance(value, str):
                return
    raise ValueError("Body of function must be empty")
