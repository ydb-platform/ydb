import builtins
import linecache
import re
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from dishka.text_rendering import get_name

NOT_ALLOWED_SYMBOLS = re.compile(r"\W", flags=re.ASCII)
MAX_NAME_LENGTH = 30
MAX_ITEMS_PER_LINE = 5


class CodeBuilder:
    def __init__(self, *, is_async: bool) -> None:
        self.globals: dict[str, Any] = {}
        self.locals: set[str] = set()
        self.reverse_globals: dict[Any, str] = {}
        self.indent_level = 0
        self.indent_str = ""
        self.code = ""
        if is_async:
            self.await_str = "await "
            self.async_str = "async "
        else:
            self.await_str = ""
            self.async_str = ""

    def _make_global_name(self, obj: Any, name: str | None = None) -> str:
        if name is None:
            name = get_name(obj, include_module=False)
            if len(name) > MAX_NAME_LENGTH:
                name = "val_" + get_name(type(obj), include_module=False)
        if not name.isidentifier():
            name = "_" + NOT_ALLOWED_SYMBOLS.sub("_", name)
        if (
            name not in self.locals and
            (name not in self.globals or self.globals[name] == obj)
        ):
            return name

        i = 0
        while True:
            new_name = f"{name}_{i}"
            if (
                new_name not in self.locals and
                (new_name not in self.globals or self.globals[new_name] == obj)
            ):
                return new_name
            i += 1

    def assign_expr(self, target: str, value: str) -> None:
        if target.isidentifier():
            raise ValueError(  # noqa: TRY003
                f"Name {target} is a valid identifier, use `assign_local`",
            )
        self.statement(f"{target} = {value}")

    def assign_local(self, name: str, value: str) -> None:
        if not name.isidentifier():
            raise ValueError(  # noqa: TRY003
                f"Name {name} is not a valid identifier",
            )
        if name in self.globals:
            raise ValueError(  # noqa: TRY003
                f"Name {name} is already defined as global",
            )
        self.locals.add(name)
        self.statement(f"{name} = {value}")

    def global_(self, obj: Any, preferred_name: str | None = None) -> str:
        if type(obj) in (str, int, bool):
            return repr(obj)
        if obj in (None, ()):
            return repr(obj)
        if obj is ...:
            return "..."

        try:
            if obj in self.reverse_globals:
                return self.reverse_globals[obj]
        except TypeError:  # unhashable object
            name = self._make_global_name(obj, preferred_name)
            self.globals[name] = obj
            return name

        name = self._make_global_name(obj, preferred_name)
        self.globals[name] = obj
        self.reverse_globals[obj] = name
        return name

    @contextmanager
    def block(self) -> Iterator[None]:
        self.indent_level += 1
        self.indent_str = "    " * self.indent_level
        yield
        self.indent_level -= 1
        self.indent_str = "    " * self.indent_level

    def statement(self, expr: str) -> None:
        self.code += self.indent_str + expr + "\n"

    @contextmanager
    def if_(self, expr: str) -> Iterator[None]:
        self.statement("if " + expr + ":")
        with self.block():
            yield None

    @contextmanager
    def elif_(self, expr: str) -> Iterator[None]:
        self.statement("elif " + expr + ":")
        with self.block():
            yield None

    @contextmanager
    def else_(self) -> Iterator[None]:
        self.statement("else:")
        with self.block():
            yield None

    def call(self, func: str, *args: str, **kwargs: str) -> str:
        if (
            func.isidentifier() and
            func not in self.globals and
            func not in self.locals and
            (func.startswith("_") or not hasattr(builtins, func))
        ):
            raise ValueError(f"Function {func} is not defined")  # noqa: TRY003
        args_list = [*args]
        args_list.extend(f"{name}={value}" for name, value in kwargs.items())

        if len(args_list) > MAX_ITEMS_PER_LINE:
            args_str = ",\n".join(args_list)
        else:
            args_str = ", ".join(args_list)
        return f"{func}({args_str})"

    def await_(self, expr: str) -> str:
        return f"{self.await_str}{expr}"

    def return_(self, expr: str) -> None:
        self.statement(f"return {expr}")

    @contextmanager
    def def_(self, name: str, args: list[str]) -> Iterator[None]:
        if name in self.globals:
            raise ValueError(  # noqa: TRY003
                f"Name {name} already defined as {self.globals[name]}",
            )
        self.globals[name] = object()  # stub to detect conflicts
        args_str = ", ".join(args)
        self.statement(f"{self.async_str}def {name}({args_str}):")
        old_locals = self.locals
        self.locals = set()
        self.locals.update(args)
        with self.block():
            yield None
        self.locals = old_locals

    def try_(self) -> AbstractContextManager[None]:
        self.statement("try:")
        return self.block()

    def except_(
        self,
        exception: type[Exception],
    ) -> AbstractContextManager[None]:
        name = self.global_(exception)
        self.statement(f"except {name}:")
        return self.block()

    def raise_(self, expr: str) -> None:
        self.statement(f"raise {expr}")

    def or_(self, left: str, right: str) -> str:
        return f"({left} or {right})"

    def and_(self, left: str, right: str) -> str:
        return f"({left} and {right})"

    def not_(self, expr: str) -> str:
        return f"not ({expr})"

    def list_literal(self, *items: str) -> str:
        if len(items) > MAX_ITEMS_PER_LINE:
            items_str = "\n, ".join(items)
        else:
            items_str = ", ".join(items)
        return f"[{items_str}]"

    def compile(self, source_file_name: str) -> dict[str, Any]:
        lines = self.code.splitlines(keepends=True)
        linecache.cache[source_file_name] = (
            len(self.code), None, lines, source_file_name,
        )
        compiled = compile(self.code, source_file_name, "exec")
        exec(compiled, self.globals)  # noqa: S102
        return self.globals
