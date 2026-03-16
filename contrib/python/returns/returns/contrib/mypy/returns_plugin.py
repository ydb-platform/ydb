"""
Custom mypy plugin to solve the temporary problem with python typing.

Important: we don't do anything ugly here.
We only solve problems of the current typing implementation.

``mypy`` API docs are here:
https://mypy.readthedocs.io/en/latest/extending_mypy.html

We use ``pytest-mypy-plugins`` to test that it works correctly, see:
https://github.com/mkurnikov/pytest-mypy-plugins
"""

from collections.abc import Callable, Mapping
from typing import ClassVar, TypeAlias, final

from mypy.plugin import (
    AttributeContext,
    FunctionContext,
    MethodContext,
    MethodSigContext,
    Plugin,
)
from mypy.types import CallableType
from mypy.types import Type as MypyType

from returns.contrib.mypy import _consts
from returns.contrib.mypy._features import (
    curry,
    do_notation,
    flow,
    kind,
    partial,
    pipe,
)

# Type aliases
# ============

#: Type for a function hook.
_FunctionCallback: TypeAlias = Callable[[FunctionContext], MypyType]

#: Type for attribute hook.
_AttributeCallback: TypeAlias = Callable[[AttributeContext], MypyType]

#: Type for a method hook.
_MethodCallback: TypeAlias = Callable[[MethodContext], MypyType]

#: Type for a method signature hook.
_MethodSigCallback: TypeAlias = Callable[[MethodSigContext], CallableType]


# Interface
# =========


@final
class _ReturnsPlugin(Plugin):
    """Our main plugin to dispatch different callbacks to specific features."""

    _function_hook_plugins: ClassVar[Mapping[str, _FunctionCallback]] = {
        _consts.TYPED_PARTIAL_FUNCTION: partial.analyze,
        _consts.TYPED_CURRY_FUNCTION: curry.analyze,
        _consts.TYPED_FLOW_FUNCTION: flow.analyze,
        _consts.TYPED_PIPE_FUNCTION: pipe.analyze,
        _consts.TYPED_KIND_DEKIND: kind.dekind,
    }

    _method_sig_hook_plugins: ClassVar[Mapping[str, _MethodSigCallback]] = {
        _consts.TYPED_PIPE_METHOD: pipe.signature,
        _consts.TYPED_KIND_KINDED_CALL: kind.kinded_signature,
    }

    _method_hook_plugins: ClassVar[Mapping[str, _MethodCallback]] = {
        _consts.TYPED_PIPE_METHOD: pipe.infer,
        _consts.TYPED_KIND_KINDED_CALL: kind.kinded_call,
        _consts.TYPED_KIND_KINDED_GET: kind.kinded_get_descriptor,
        **dict.fromkeys(_consts.DO_NOTATION_METHODS, do_notation.analyze),
    }

    def get_function_hook(
        self,
        fullname: str,
    ) -> _FunctionCallback | None:
        """
        Called for function return types from ``mypy``.

        Runs on each function call in the source code.
        We are only interested in a particular subset of all functions.
        So, we return a function handler for them.

        Otherwise, we return ``None``.
        """
        return self._function_hook_plugins.get(fullname)

    def get_attribute_hook(
        self,
        fullname: str,
    ) -> _AttributeCallback | None:
        """Called for any exiting or ``__getattr__`` attribute access."""
        if fullname.startswith(_consts.TYPED_KINDN_ACCESS):
            name_parts = fullname.split('.')
            attribute_name = name_parts[-1]
            if attribute_name.startswith('__') and attribute_name.endswith(
                '__'
            ):
                return None
            return kind.attribute_access
        return None

    def get_method_signature_hook(
        self,
        fullname: str,
    ) -> _MethodSigCallback | None:
        """Called for method signature from ``mypy``."""
        return self._method_sig_hook_plugins.get(fullname)

    def get_method_hook(
        self,
        fullname: str,
    ) -> _MethodCallback | None:
        """Called for method return types from ``mypy``."""
        return self._method_hook_plugins.get(fullname)


def plugin(version: str) -> type[Plugin]:
    """Plugin's public API and entrypoint."""
    return _ReturnsPlugin
