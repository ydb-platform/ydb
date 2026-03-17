"""Restrictive importer for workflow sandbox.

.. warning::
    This API for this module is considered unstable and may change in future.
"""

from __future__ import annotations

import builtins
import functools
import importlib
import importlib.util
import logging
import sys
import threading
import types
import warnings
from contextlib import ExitStack, contextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    no_type_check,
)

from typing_extensions import ParamSpec

import temporalio.workflow

from ._restrictions import (
    RestrictedModule,
    RestrictedWorkflowAccessError,
    RestrictionContext,
    SandboxRestrictions,
)

logger = logging.getLogger(__name__)

# Set to true to log lots of sandbox details
LOG_TRACE = False
_trace_depth = 0


def _trace(message: object, *args: object) -> None:
    if LOG_TRACE:
        global _trace_depth
        logger.debug(("  " * _trace_depth) + str(message), *args)


class Importer:
    """Importer that restricts modules."""

    def __init__(
        self, restrictions: SandboxRestrictions, restriction_context: RestrictionContext
    ) -> None:
        """Create importer."""
        self.restrictions = restrictions
        self.restriction_context = restriction_context
        self.new_modules: Dict[str, types.ModuleType] = {
            "sys": sys,
            "builtins": builtins,
            # Even though we don't want to, we have to have __main__ because
            # stdlib packages like inspect and others expect it to be present
            "__main__": types.ModuleType("__main__"),
        }
        self.modules_checked_for_restrictions: Set[str] = set()
        self.import_func = self._import if not LOG_TRACE else self._traced_import
        # Pre-collect restricted builtins
        self.restricted_builtins: List[Tuple[str, _ThreadLocalCallable, Callable]] = []
        builtin_matcher = restrictions.invalid_module_members.child_matcher(
            "__builtins__"
        )
        if builtin_matcher:

            def restrict_built_in(name: str, orig: Any, *args, **kwargs):
                # Check if restricted against matcher
                if (
                    builtin_matcher
                    and builtin_matcher.match_access(
                        restriction_context, name, include_use=True
                    )
                    and not temporalio.workflow.unsafe.is_sandbox_unrestricted()
                ):
                    raise RestrictedWorkflowAccessError(f"__builtins__.{name}")
                return orig(*args, **kwargs)

            for k in dir(builtins):
                if not k.startswith("_") and (
                    k in builtin_matcher.access
                    or k in builtin_matcher.use
                    or k in builtin_matcher.children
                ):
                    thread_local = _get_thread_local_builtin(k)
                    self.restricted_builtins.append(
                        (
                            k,
                            thread_local,
                            functools.partial(restrict_built_in, k, thread_local.orig),
                        )
                    )

            # Need to unwrap params for isinstance and issubclass. We have
            # chosen to do it this way instead of customize __instancecheck__
            # and __subclasscheck__ because we may have proxied the second
            # parameter which does not have a way to override. It is unfortunate
            # we have to change these globals for everybody.
            def unwrap_second_param(orig: Any, a: Any, b: Any) -> Any:
                a = RestrictionContext.unwrap_if_proxied(a)
                b = RestrictionContext.unwrap_if_proxied(b)
                return orig(a, b)

            thread_local_is_inst = _get_thread_local_builtin("isinstance")
            self.restricted_builtins.append(
                (
                    "isinstance",
                    thread_local_is_inst,
                    functools.partial(unwrap_second_param, thread_local_is_inst.orig),
                )
            )
            thread_local_is_sub = _get_thread_local_builtin("issubclass")
            self.restricted_builtins.append(
                (
                    "issubclass",
                    thread_local_is_sub,
                    functools.partial(unwrap_second_param, thread_local_is_sub.orig),
                )
            )

    @contextmanager
    def applied(self) -> Iterator[None]:
        """Context manager to apply this restrictive import.

        .. warning::
            This currently alters global sys.modules and builtins.__import__
            while it is running and therefore should be locked against other
            code running at the same time.
        """
        orig_importer = Importer.current_importer()
        Importer._thread_local_current.importer = self
        try:
            with _thread_local_sys_modules.applied(sys, "modules", self.new_modules):
                with _thread_local_import.applied(
                    builtins, "__import__", self.import_func
                ):
                    with self._builtins_restricted():
                        yield None
        finally:
            Importer._thread_local_current.importer = orig_importer

    @contextmanager
    def _unapplied(self) -> Iterator[None]:
        orig_importer = Importer.current_importer()
        Importer._thread_local_current.importer = None
        # Set orig modules, then unset on complete
        try:
            with _thread_local_sys_modules.unapplied():
                with _thread_local_import.unapplied():
                    with self._builtins_unrestricted():
                        yield None
        finally:
            Importer._thread_local_current.importer = orig_importer

    def _traced_import(
        self,
        name: str,
        globals: Optional[Mapping[str, object]] = None,
        locals: Optional[Mapping[str, object]] = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> types.ModuleType:
        _trace("Importing %s (fromlist: %s, level: %s)", name, fromlist, level)
        global _trace_depth
        _trace_depth += 1
        try:
            return self._import(name, globals, locals, fromlist, level)
        finally:
            _trace_depth -= 1

    def _import(
        self,
        name: str,
        globals: Optional[Mapping[str, object]] = None,
        locals: Optional[Mapping[str, object]] = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> types.ModuleType:
        # We have to resolve the full name, it can be relative at different
        # levels
        full_name = _resolve_module_name(name, globals, level)

        # Check module restrictions and passthrough modules
        if full_name not in sys.modules:
            # Make sure not an entirely invalid module
            self._assert_valid_module(full_name)

            # Check if passthrough
            passthrough_mod = self._maybe_passthrough_module(full_name)
            if passthrough_mod:
                # Load all parents. Usually Python does this for us, but not on
                # passthrough.
                parent, _, child = full_name.rpartition(".")
                if parent and parent not in sys.modules:
                    _trace(
                        "Importing parent module %s before passing through %s",
                        parent,
                        name,
                    )
                    self.import_func(parent, globals, locals)
                    # Set the passthrough on the parent
                    setattr(sys.modules[parent], child, passthrough_mod)
                # Set the passthrough on sys.modules and on the parent
                sys.modules[full_name] = passthrough_mod
                # Put it on the parent
                if parent:
                    setattr(sys.modules[parent], child, sys.modules[full_name])
                # All children of this module that are on the original sys
                # modules but not here and are passthrough

            # If the module is __temporal_main__ and not already in sys.modules,
            # we load it from whatever file __main__ was originally in
            if full_name == "__temporal_main__":
                orig_mod = _thread_local_sys_modules.orig["__main__"]
                new_spec = importlib.util.spec_from_file_location(
                    full_name, orig_mod.__file__
                )
                if not new_spec:
                    raise ImportError(
                        f"No spec for __main__ file at {orig_mod.__file__}"
                    )
                elif not new_spec.loader:
                    raise ImportError(
                        f"Spec for __main__ file at {orig_mod.__file__} has no loader"
                    )
                new_mod = importlib.util.module_from_spec(new_spec)
                sys.modules[full_name] = new_mod
                new_spec.loader.exec_module(new_mod)

        mod = importlib.__import__(name, globals, locals, fromlist, level)
        # Check for restrictions if necessary and apply
        if mod.__name__ not in self.modules_checked_for_restrictions:
            self.modules_checked_for_restrictions.add(mod.__name__)
            restricted_mod = self._maybe_restrict_module(mod)
            if restricted_mod:
                sys.modules[mod.__name__] = restricted_mod
                mod = restricted_mod

        return mod

    def _assert_valid_module(self, name: str) -> None:
        if (
            self.restrictions.invalid_modules.match_access(
                self.restriction_context, *name.split(".")
            )
            and not temporalio.workflow.unsafe.is_sandbox_unrestricted()
        ):
            raise RestrictedWorkflowAccessError(name)

    def module_configured_passthrough(self, name: str) -> bool:
        """Whether the given module name is configured as passthrough."""
        if (
            self.restrictions.passthrough_all_modules
            or name in self.restrictions.passthrough_modules
        ):
            return True
        # Iterate backwards looking if configured passthrough
        end_dot = -1
        while True:
            end_dot = name.find(".", end_dot + 1)
            if end_dot == -1:
                return False
            elif name[:end_dot] in self.restrictions.passthrough_modules:
                break
        return True

    def _maybe_passthrough_module(self, name: str) -> Optional[types.ModuleType]:
        # If imports not passed through and all modules are not passed through
        # and name not in passthrough modules, check parents
        if (
            not temporalio.workflow.unsafe.is_imports_passed_through()
            and not self.module_configured_passthrough(name)
        ):
            return None
        # Do the pass through
        with self._unapplied():
            _trace("Passing module %s through from host", name)
            global _trace_depth
            _trace_depth += 1
            # Use our import outside of the sandbox
            try:
                return importlib.import_module(name)
            finally:
                _trace_depth -= 1

    def _maybe_restrict_module(
        self, mod: types.ModuleType
    ) -> Optional[types.ModuleType]:
        """Implements :py:meth:`_Environment.maybe_restrict_module`."""
        matcher = self.restrictions.invalid_module_members.child_matcher(
            *mod.__name__.split(".")
        )
        if not matcher:
            # No restrictions
            return None
        _trace("Restricting module %s during import", mod.__name__)
        return RestrictedModule(mod, self.restriction_context, matcher)

    @contextmanager
    def _builtins_restricted(self) -> Iterator[None]:
        if not self.restricted_builtins:
            yield None
            return
        with ExitStack() as stack:
            for name, thread_local, restrict_fn in self.restricted_builtins:
                _trace("Restricting builtin %s", name)
                stack.enter_context(thread_local.applied(builtins, name, restrict_fn))
            yield None

    @contextmanager
    def _builtins_unrestricted(self) -> Iterator[None]:
        if not self.restricted_builtins:
            yield None
            return
        with ExitStack() as stack:
            for _, thread_local, _ in self.restricted_builtins:
                stack.enter_context(thread_local.unapplied())
            yield None

    _thread_local_current = threading.local()

    @staticmethod
    def current_importer() -> Optional[Importer]:
        """Get the current importer if any."""
        return Importer._thread_local_current.__dict__.get("importer")


_T = TypeVar("_T")


class _ThreadLocalOverride(Generic[_T]):
    def __init__(self, orig: _T) -> None:
        self.orig = orig
        self.thread_local = threading.local()
        self.applied_counter = 0
        self.applied_counter_lock = threading.Lock()

    @property
    def maybe_current(self) -> Optional[_T]:
        return self.thread_local.__dict__.get("data")

    @property
    def current(self) -> _T:
        return self.thread_local.__dict__.get("data", self.orig)

    @current.setter
    def current(self, v: _T) -> None:
        self.thread_local.data = v

    @current.deleter
    def current(self) -> None:
        self.thread_local.__dict__.pop("data", None)

    @contextmanager
    def applied(self, obj: Any, attr: str, current: _T) -> Iterator[None]:
        # Function carefully crafted to support nesting and situations where
        # other threads may have already set this on obj
        orig_current = self.maybe_current

        # Replace the attribute if it is not ourself. We have to do this
        # atomically so we know it is only done once and can increment the
        # counter for undoing it.
        with self.applied_counter_lock:
            self.applied_counter += 1
            if getattr(obj, attr) is not self:
                setattr(obj, attr, self)

        self.current = current
        try:
            yield None
        finally:
            if orig_current is None:
                del self.current
            else:
                self.current = orig_current
            # Set back the original value once once when this counter reaches
            # 0. This ensures that it is only unset when all are done.
            with self.applied_counter_lock:
                self.applied_counter -= 1
                if self.applied_counter == 0:
                    setattr(obj, attr, self.orig)

    @contextmanager
    def unapplied(self) -> Iterator[None]:
        # Function carefully crafted to support nesting
        orig_current = self.maybe_current
        if orig_current is not None:
            del self.current
        try:
            yield None
        finally:
            if orig_current is not None:
                self.current = orig_current


class _ThreadLocalSysModules(
    _ThreadLocalOverride[Dict[str, types.ModuleType]],
    MutableMapping[str, types.ModuleType],
):
    def __contains__(self, key: object) -> bool:
        if key in self.current:
            return True
        return (
            isinstance(key, str)
            and self._lazily_passthrough_if_available(key) is not None
        )

    def __delitem__(self, key: str) -> None:
        del self.current[key]

    def __getitem__(self, key: str) -> types.ModuleType:
        try:
            return self.current[key]
        except KeyError:
            if module := self._lazily_passthrough_if_available(key):
                return module
            raise

    def __len__(self) -> int:
        return len(self.current)

    def __iter__(self) -> Iterator[str]:
        return iter(self.current)

    def __setitem__(self, key: str, value: types.ModuleType) -> None:
        self.current[key] = value

    # Below methods are not in mutable mapping. Python chose not to put
    # everything in MutableMapping they do in dict (see
    # https://bugs.python.org/issue22101). So when someone calls
    # sys.modules.copy() it breaks (which is exactly what the inspect module
    # does sometimes).

    def __or__(
        self, other: Mapping[str, types.ModuleType]
    ) -> Dict[str, types.ModuleType]:
        if sys.version_info < (3, 9):
            raise NotImplementedError
        return self.current.__or__(other)

    def __ior__(
        self, other: Mapping[str, types.ModuleType]
    ) -> Dict[str, types.ModuleType]:
        if sys.version_info < (3, 9):
            raise NotImplementedError
        return self.current.__ior__(other)

    __ror__ = __or__

    def copy(self) -> Dict[str, types.ModuleType]:
        return self.current.copy()

    @classmethod
    def fromkeys(cls, *args, **kwargs) -> Any:
        return dict.fromkeys(*args, **kwargs)

    def _lazily_passthrough_if_available(self, key: str) -> Optional[types.ModuleType]:
        # We only lazily pass through if it's in orig, lazy not disabled, and
        # module configured as pass through
        if (
            key in self.orig
            and (importer := Importer.current_importer())
            and not importer.restrictions.disable_lazy_sys_module_passthrough
            and importer.module_configured_passthrough(key)
        ):
            orig = self.orig[key]
            self.current[key] = orig
            return orig
        return None


_thread_local_sys_modules = _ThreadLocalSysModules(sys.modules)

_P = ParamSpec("_P")


class _ThreadLocalCallable(_ThreadLocalOverride[Callable[_P, _T]]):  # type: ignore
    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T:
        return self.current(*args, **kwargs)


_thread_local_import = _ThreadLocalCallable(builtins.__import__)

_thread_local_builtins: Dict[str, _ThreadLocalCallable] = {}


def _get_thread_local_builtin(name: str) -> _ThreadLocalCallable:
    ret = _thread_local_builtins.get(name)
    if not ret:
        ret = _ThreadLocalCallable(getattr(builtins, name))
        _thread_local_builtins[name] = ret
    return ret


def _resolve_module_name(
    name: str, globals: Optional[Mapping[str, object]], level: int
) -> str:
    if level == 0:
        return name
    # Calc the package from globals
    package = _calc___package__(globals or {})
    # Logic taken from importlib._resolve_name
    bits = package.rsplit(".", level - 1)
    if len(bits) < level:
        raise ImportError("Attempted relative import beyond top-level package")
    base = bits[0]
    return f"{base}.{name}" if name else base


# Copied from importlib._calc__package__
@no_type_check
def _calc___package__(globals: Mapping[str, object]) -> str:
    """Calculate what __package__ should be.
    __package__ is not guaranteed to be defined or could be set to None
    to represent that its proper value is unknown.
    """
    package = globals.get("__package__")
    spec = globals.get("__spec__")
    if package is not None:
        if spec is not None and package != spec.parent:
            warnings.warn(
                "__package__ != __spec__.parent " f"({package!r} != {spec.parent!r})",
                DeprecationWarning,
                stacklevel=3,
            )
        return package
    elif spec is not None:
        return spec.parent
    else:
        warnings.warn(
            "can't resolve package from __spec__ or __package__, "
            "falling back on __name__ and __path__",
            ImportWarning,
            stacklevel=3,
        )
        package = globals["__name__"]
        if "__path__" not in globals:
            package = package.rpartition(".")[0]
    return package
