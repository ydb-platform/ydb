import linecache
from abc import ABC, abstractmethod
from collections import defaultdict
from threading import Lock
from typing import Any, Callable

from .code_builder import CodeBuilder


class ClosureCompiler(ABC):
    """Abstract class compiling closures"""

    @abstractmethod
    def compile(
        self,
        base_id: str,
        filename_maker: Callable[[str], str],
        builder: CodeBuilder,
        namespace: dict[str, Any],
    ) -> Callable:
        """Execute content of builder and return value that body returned (it is must be a closure).
        :param base_id: string that used to generate unique id
        :param filename_maker: function taking unique id and returning full filename
        :param builder: Builder containing the body of function that creates closure
        :param namespace: Global variables
        :return: closure object
        """


class ConcurrentCounter:
    __slots__ = ("_lock", "_name_to_idx")

    def __init__(self) -> None:
        self._lock = Lock()
        self._name_to_idx: dict[str, int] = defaultdict(lambda: 0)

    def generate_idx(self, name: str) -> int:
        with self._lock:
            idx = self._name_to_idx[name]
            self._name_to_idx[name] += 1
            return idx


_counter = ConcurrentCounter()


class BasicClosureCompiler(ClosureCompiler):
    def _make_source_builder(self, builder: CodeBuilder) -> CodeBuilder:
        main_builder = CodeBuilder()

        main_builder += "def _closure_maker():"
        with main_builder:
            main_builder.extend(builder)

        return main_builder

    def _compile(self, source: str, unique_filename: str, namespace: dict[str, Any]):
        code_obj = compile(source, unique_filename, "exec")

        local_namespace: dict[str, Any] = {}
        exec(code_obj, namespace, local_namespace)  # noqa: S102
        linecache.cache[unique_filename] = (
            len(source),
            None,
            source.splitlines(keepends=True),
            unique_filename,
        )
        return local_namespace["_closure_maker"]()

    def _get_unique_id(self, base_id: str) -> str:
        idx = _counter.generate_idx(base_id)
        if idx == 0:
            return base_id
        return f"{base_id} {idx}"

    def compile(
        self,
        base_id: str,
        filename_maker: Callable[[str], str],
        builder: CodeBuilder,
        namespace: dict[str, Any],
    ) -> Callable:
        source = self._make_source_builder(builder).string()
        unique_id = self._get_unique_id(base_id)
        return self._compile(source, filename_maker(unique_id), namespace)
