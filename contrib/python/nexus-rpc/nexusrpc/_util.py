from __future__ import annotations

import functools
import inspect
import typing
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Type

from typing_extensions import TypeGuard

import nexusrpc

if TYPE_CHECKING:
    import nexusrpc
    from nexusrpc import InputT, OutputT
    from nexusrpc._common import ServiceT
    from nexusrpc.handler._operation_handler import OperationHandler


def get_service_definition(
    obj: Any,
) -> Optional[nexusrpc.ServiceDefinition]:
    """Return the :py:class:`nexusrpc.ServiceDefinition` for the object, or None"""
    # getattr would allow a non-decorated class to act as a service
    # definition if it inherits from a decorated class.
    if isinstance(obj, type):
        defn = obj.__dict__.get("__nexus_service__")
    else:
        defn = getattr(obj, "__dict__", {}).get("__nexus_service__")
    if defn and not isinstance(defn, nexusrpc.ServiceDefinition):
        raise ValueError(
            f"Service definition {obj.__name__} has a __nexus_service__ attribute that is not a ServiceDefinition."
        )
    return defn


def set_service_definition(
    cls: Type[ServiceT], service_definition: nexusrpc.ServiceDefinition
) -> None:
    """Set the :py:class:`nexusrpc.ServiceDefinition` for this object."""
    if not isinstance(cls, type):
        raise TypeError(f"Expected {cls} to be a class, but is {type(cls)}.")
    setattr(cls, "__nexus_service__", service_definition)


def get_operation_definition(
    obj: Any,
) -> Optional[nexusrpc.Operation]:
    """Return the :py:class:`nexusrpc.Operation` for the object, or None

    ``obj`` should be a decorated operation start method.
    """
    return getattr(obj, "__nexus_operation__", None)


def set_operation_definition(
    obj: Any,
    operation_definition: nexusrpc.Operation,
) -> None:
    """Set the :py:class:`nexusrpc.Operation` for this object.

    ``obj`` should be an operation start method.
    """
    setattr(obj, "__nexus_operation__", operation_definition)


def get_operation_factory(
    obj: Any,
) -> tuple[
    Optional[Callable[[Any], OperationHandler[InputT, OutputT]]],
    Optional[nexusrpc.Operation[InputT, OutputT]],
]:
    """Return the :py:class:`Operation` for the object along with the factory function.

    ``obj`` should be a decorated operation start method.
    """
    op_defn = get_operation_definition(obj)
    if op_defn:
        factory = obj
    else:
        if factory := getattr(obj, "__nexus_operation_factory__", None):
            op_defn = get_operation_definition(factory)
    if not isinstance(op_defn, nexusrpc.Operation):
        return None, None
    return factory, op_defn


def set_operation_factory(
    obj: Any,
    operation_factory: Callable[[Any], OperationHandler[InputT, OutputT]],
) -> None:
    """Set the :py:class:`OperationHandler` factory for this object.

    ``obj`` should be an operation start method.
    """
    setattr(obj, "__nexus_operation_factory__", operation_factory)


# Copied from https://github.com/modelcontextprotocol/python-sdk
#
# Copyright (c) 2024 Anthropic, PBC.
#
# Modified to use TypeGuard.
#
# This file is licensed under the MIT License.
def is_async_callable(obj: Any) -> TypeGuard[Callable[..., Awaitable[Any]]]:
    """
    Return True if `obj` is an async callable.

    Supports partials of async callable class instances.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(getattr(obj, "__call__", None))
    )


def is_callable(obj: Any) -> TypeGuard[Callable[..., Any]]:
    """
    Return True if `obj` is a callable.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func
    return inspect.isfunction(obj) or (callable(obj) and hasattr(obj, "__call__"))


def get_callable_name(fn: Callable[..., Any]) -> str:
    method_name = getattr(fn, "__name__", None)
    if not method_name and callable(fn) and hasattr(fn, "__call__"):
        method_name = fn.__class__.__name__
    if not method_name:
        raise TypeError(
            f"Could not determine callable name: "
            f"expected {fn} to be a function or callable instance."
        )
    return method_name


def is_subtype(type1: Type[Any], type2: Type[Any]) -> bool:
    # Note that issubclass() argument 2 cannot be a parameterized generic
    # TODO(nexus-preview): review desired type compatibility logic
    if type1 == type2:
        return True
    return issubclass(type1, typing.get_origin(type2) or type2)


# See
# https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older

try:
    from inspect import get_annotations  # type: ignore
except ImportError:
    import functools
    import sys
    import types

    # This is inspect.get_annotations from Python 3.13.5
    def get_annotations(obj, *, globals=None, locals=None, eval_str=False):  # type: ignore[misc]
        """Compute the annotations dict for an object.

        obj may be a callable, class, or module.
        Passing in an object of any other type raises TypeError.

        Returns a dict.  get_annotations() returns a new dict every time
        it's called; calling it twice on the same object will return two
        different but equivalent dicts.

        This function handles several details for you:

        * If eval_str is true, values of type str will
            be un-stringized using eval().  This is intended
            for use with stringized annotations
            ("from __future__ import annotations").
        * If obj doesn't have an annotations dict, returns an
            empty dict.  (Functions and methods always have an
            annotations dict; classes, modules, and other types of
            callables may not.)
        * Ignores inherited annotations on classes.  If a class
            doesn't have its own annotations dict, returns an empty dict.
        * All accesses to object members and dict values are done
            using getattr() and dict.get() for safety.
        * Always, always, always returns a freshly-created dict.

        eval_str controls whether or not values of type str are replaced
        with the result of calling eval() on those values:

        * If eval_str is true, eval() is called on values of type str.
        * If eval_str is false (the default), values of type str are unchanged.

        globals and locals are passed in to eval(); see the documentation
        for eval() for more information.  If either globals or locals is
        None, this function may replace that value with a context-specific
        default, contingent on type(obj):

        * If obj is a module, globals defaults to obj.__dict__.
        * If obj is a class, globals defaults to
            sys.modules[obj.__module__].__dict__ and locals
            defaults to the obj class namespace.
        * If obj is a callable, globals defaults to obj.__globals__,
            although if obj is a wrapped function (using
            functools.update_wrapper()) it is first unwrapped.
        """
        if isinstance(obj, type):
            # class
            obj_dict = getattr(obj, "__dict__", None)
            if obj_dict and hasattr(obj_dict, "get"):
                ann = obj_dict.get("__annotations__", None)
                if isinstance(ann, types.GetSetDescriptorType):
                    ann = None
            else:
                ann = None

            obj_globals = None
            module_name = getattr(obj, "__module__", None)
            if module_name:
                module = sys.modules.get(module_name, None)
                if module:
                    obj_globals = getattr(module, "__dict__", None)
            obj_locals = dict(vars(obj))
            unwrap = obj
        elif isinstance(obj, types.ModuleType):
            # module
            ann = getattr(obj, "__annotations__", None)
            obj_globals = getattr(obj, "__dict__")
            obj_locals = None
            unwrap = None
        elif callable(obj):
            # this includes types.Function, types.BuiltinFunctionType,
            # types.BuiltinMethodType, functools.partial, functools.singledispatch,
            # "class funclike" from Lib/test/test_inspect... on and on it goes.
            ann = getattr(obj, "__annotations__", None)
            obj_globals = getattr(obj, "__globals__", None)
            obj_locals = None
            unwrap = obj
        else:
            raise TypeError(f"{obj!r} is not a module, class, or callable.")

        if ann is None:
            return {}

        if not isinstance(ann, dict):
            raise ValueError(f"{obj!r}.__annotations__ is neither a dict nor None")

        if not ann:
            return {}

        if not eval_str:
            return dict(ann)

        if unwrap is not None:
            while True:
                if hasattr(unwrap, "__wrapped__"):
                    unwrap = unwrap.__wrapped__  # type: ignore
                    continue
                if isinstance(unwrap, functools.partial):
                    unwrap = unwrap.func  # type: ignore
                    continue
                break
            if hasattr(unwrap, "__globals__"):
                obj_globals = unwrap.__globals__  # type: ignore

        if globals is None:
            globals = obj_globals
        if locals is None:
            locals = obj_locals or {}

        # "Inject" type parameters into the local namespace
        # (unless they are shadowed by assignments *in* the local namespace),
        # as a way of emulating annotation scopes when calling `eval()`
        if type_params := getattr(obj, "__type_params__", ()):
            locals = {param.__name__: param for param in type_params} | locals

        return_value = {
            key: value if not isinstance(value, str) else eval(value, globals, locals)
            for key, value in ann.items()
        }
        return return_value


get_annotations = get_annotations
