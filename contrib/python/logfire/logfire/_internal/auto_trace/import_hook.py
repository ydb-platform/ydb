from __future__ import annotations

import ast
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import spec_from_loader
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, cast

from ..utils import log_internal_error
from .rewrite_ast import compile_source
from .types import AutoTraceModule

if TYPE_CHECKING:
    from ..main import Logfire


@dataclass
class LogfireFinder(MetaPathFinder):
    """The import hook entry point, inserted into `sys.meta_path` to apply AST rewriting to matching modules."""

    logfire: Logfire
    modules_filter: Callable[[AutoTraceModule], bool]
    min_duration: int

    def find_spec(
        self, fullname: str, path: Sequence[str] | None, target: ModuleType | None = None
    ) -> ModuleSpec | None:
        """This is the method that is called by the import system.

        It uses the other existing meta path finders to do most of the standard work,
        particularly finding the module's source code and filename.
        If it finds a module spec that matches the filter, it returns a new spec that uses the LogfireLoader.
        """
        for plain_spec in self._find_plain_specs(fullname, path, target):
            # Not all loaders have get_source, but it's an abstract method of the standard ABC InspectLoader.
            # In particular it's implemented by `importlib.machinery.SourceFileLoader`
            # which is provided by default.
            get_source = getattr(plain_spec.loader, 'get_source', None)
            if not callable(get_source):  # pragma: no cover
                continue

            try:
                source = cast(str, get_source(fullname))
            except Exception:  # pragma: no cover
                continue

            if not source:
                continue

            # We fully expect plain_spec.origin and self.get_filename(...)
            # to be the same thing (a valid filename), but they're optional.
            filename = plain_spec.origin
            if not filename:  # pragma: no cover
                try:
                    filename = cast('str | None', plain_spec.loader.get_filename(fullname))  # type: ignore
                except Exception:
                    pass

            if not self.modules_filter(AutoTraceModule(fullname, filename)):
                return None  # tell the import system to try the next meta path finder

            try:
                tree = ast.parse(source)
            except Exception:  # pragma: no cover
                # The plain finder gave us invalid source code. Try another one.
                # A very likely case is that the source code really is invalid,
                # in which case we'll eventually return None and the normal system will raise the error,
                # giving the user a normal traceback instead of a confusing and ugly one mentioning logfire.
                continue

            filename = filename or f'<{fullname}>'

            try:
                execute = compile_source(tree, filename, fullname, self.logfire, self.min_duration)
            except Exception:  # pragma: no cover
                # Auto-tracing failed with an unexpected error. Ensure that this doesn't crash the whole application.
                # This error handling is why we compile in the finder. Once we return a loader, we've committed to it.
                log_internal_error()
                return None  # tell the import system to try the next meta path finder

            loader = LogfireLoader(plain_spec, execute)
            return spec_from_loader(fullname, loader)

    def _find_plain_specs(
        self, fullname: str, path: Sequence[str] | None, target: ModuleType | None
    ) -> Iterator[ModuleSpec]:
        """Yield module specs returned by other finders on `sys.meta_path`."""
        for finder in sys.meta_path:
            # Skip this finder or any like it to avoid infinite recursion.
            if isinstance(finder, LogfireFinder):
                continue

            try:
                plain_spec = finder.find_spec(fullname, path, target)
            except Exception:  # pragma: no cover
                continue

            if plain_spec:
                yield plain_spec


@dataclass
class LogfireLoader(Loader):
    """An import loader produced by LogfireFinder which executes a modified AST of the module's source code."""

    plain_spec: ModuleSpec
    """A spec for the module that was returned by another meta path finder (see `LogfireFinder._find_plain_specs`)."""

    execute: Callable[[dict[str, Any]], None]
    """A function which accepts module globals and executes the compiled code."""

    def exec_module(self, module: ModuleType):
        """Execute a modified AST of the module's source code in the module's namespace.

        This is called by the import system.
        """
        self.execute(module.__dict__)

    # This is required when `exec_module` is defined.
    # It returns None to indicate that the usual module creation process should be used.
    def create_module(self, spec: ModuleSpec):
        return None

    def get_code(self, _name: str):
        # `python -m` uses the `runpy` module which calls this method instead of going through the normal protocol.
        # So return some code which can be executed with the module namespace.
        # Here `__loader__` will be this object, i.e. `self`.
        source = '__loader__.execute(globals())'
        return compile(source, '<string>', 'exec', dont_inherit=True)

    def __getattr__(self, item: str):
        """Forward to the plain spec's loader (likely a `SourceFileLoader`)."""
        return getattr(self.plain_spec.loader, item)
