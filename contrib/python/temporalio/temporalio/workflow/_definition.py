from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast, overload

import temporalio.common

from ..types import (
    CallableAsyncType,
    CallableType,
    ClassType,
    MethodSyncNoParam,
    SelfType,
)
from ._handlers import (
    UpdateMethodMultiParam,
    _QueryDefinition,
    _SignalDefinition,
    _UpdateDefinition,
)

__all__ = [
    "DynamicWorkflowConfig",
    "defn",
    "dynamic_config",
    "init",
    "run",
]


@overload
def defn(cls: ClassType) -> ClassType: ...


@overload
def defn(
    *,
    name: str | None = None,
    sandboxed: bool = True,
    failure_exception_types: Sequence[type[BaseException]] = [],
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Callable[[ClassType], ClassType]: ...


@overload
def defn(
    *,
    sandboxed: bool = True,
    dynamic: bool = False,
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Callable[[ClassType], ClassType]: ...


def defn(
    cls: ClassType | None = None,
    *,
    name: str | None = None,
    sandboxed: bool = True,
    dynamic: bool = False,
    failure_exception_types: Sequence[type[BaseException]] = [],
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Callable[[ClassType], ClassType]:
    """Decorator for workflow classes.

    This must be set on any registered workflow class (it is ignored if on a
    base class).

    Args:
        cls: The class to decorate.
        name: Name to use for the workflow. Defaults to class ``__name__``. This
            cannot be set if dynamic is set.
        sandboxed: Whether the workflow should run in a sandbox. Default is
            true.
        dynamic: If true, this workflow will be dynamic. Dynamic workflows have
            to accept a single 'Sequence[RawValue]' parameter. This cannot be
            set to true if name is present.
        failure_exception_types: The types of exceptions that, if a
            workflow-thrown exception extends, will cause the workflow/update to
            fail instead of suspending the workflow via task failure. These are
            applied in addition to ones set on the worker constructor. If
            ``Exception`` is set, it effectively will fail a workflow/update in
            all user exception cases. WARNING: This setting is experimental.
        versioning_behavior: Specifies the versioning behavior to use for this workflow.
    """

    def decorator(cls: ClassType) -> ClassType:
        # This performs validation
        _Definition._apply_to_class(
            cls,
            workflow_name=name or cls.__name__ if not dynamic else None,
            sandboxed=sandboxed,
            failure_exception_types=failure_exception_types,
            versioning_behavior=versioning_behavior,
        )
        return cls

    if cls is not None:
        return decorator(cls)
    return decorator


def init(
    init_fn: CallableType,
) -> CallableType:
    """Decorator for the workflow init method.

    This may be used on the __init__ method of the workflow class to specify
    that it accepts the same workflow input arguments as the ``@workflow.run``
    method. If used, the parameters of your  __init__ and ``@workflow.run``
    methods must be identical.

    Args:
        init_fn: The __init__ method to decorate.
    """
    if init_fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on the __init__ method")

    setattr(init_fn, "__temporal_workflow_init", True)
    return init_fn


def run(fn: CallableAsyncType) -> CallableAsyncType:
    """Decorator for the workflow run method.

    This must be used on one and only one async method defined on the same class
    as ``@workflow.defn``. This can be defined on a base class method but must
    then be explicitly overridden and defined on the workflow class.

    Run methods can only have positional parameters. Best practice is to only
    take a single object/dataclass argument that can accept more fields later if
    needed.

    Args:
        fn: The function to decorate.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be an async function")
    # Disallow local classes because we need to have the class globally
    # referenceable by name
    if "<locals>" in fn.__qualname__:
        raise ValueError(
            "Local classes unsupported, @workflow.run cannot be on a local class"
        )
    setattr(fn, "__temporal_workflow_run", True)
    # TODO(cretz): Why is MyPy unhappy with this return?
    return fn  # type: ignore[return-value]


@dataclass(frozen=True)
class DynamicWorkflowConfig:
    """Returned by functions using the :py:func:`dynamic_config` decorator, see it for more."""

    failure_exception_types: Sequence[type[BaseException]] | None = None
    """The types of exceptions that, if a workflow-thrown exception extends, will cause the
    workflow/update to fail instead of suspending the workflow via task failure. These are applied
    in addition to ones set on the worker constructor. If ``Exception`` is set, it effectively will
    fail a workflow/update in all user exception cases.

    Always overrides the equivalent parameter on :py:func:`defn` if set not-None.

        WARNING: This setting is experimental.
    """
    versioning_behavior: temporalio.common.VersioningBehavior = (
        temporalio.common.VersioningBehavior.UNSPECIFIED
    )
    """Specifies the versioning behavior to use for this workflow.

    Always overrides the equivalent parameter on :py:func:`defn`.
    """


def dynamic_config(
    fn: MethodSyncNoParam[SelfType, DynamicWorkflowConfig],
) -> MethodSyncNoParam[SelfType, DynamicWorkflowConfig]:
    """Decorator to allow configuring a dynamic workflow's behavior.

    Because dynamic workflows may conceptually represent more than one workflow type, it may be
    desirable to have different settings for fields that would normally be passed to
    :py:func:`defn`, but vary based on the workflow type name or other information available in
    the workflow's context. This function will be called after the workflow's :py:func:`init`,
    if it has one, but before the workflow's :py:func:`run` method.

    The method must only take self as a parameter, and any values set in the class it returns will
    override those provided to :py:func:`defn`.

    Cannot be specified on non-dynamic workflows.

    Args:
        fn: The function to decorate.
    """
    if inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow dynamic_config method must be synchronous")
    params = list(inspect.signature(fn).parameters.values())
    if len(params) != 1:
        raise ValueError("Workflow dynamic_config method must only take self parameter")

    # Add marker attribute
    setattr(fn, "__temporal_workflow_dynamic_config", True)
    return fn


@dataclass(frozen=True)
class _Definition:
    name: str | None
    cls: type
    run_fn: Callable[..., Awaitable]
    signals: Mapping[str | None, _SignalDefinition]
    queries: Mapping[str | None, _QueryDefinition]
    updates: Mapping[str | None, _UpdateDefinition]
    sandboxed: bool
    failure_exception_types: Sequence[type[BaseException]]
    # Types loaded on post init if both are None
    arg_types: list[type] | None = None
    ret_type: type | None = None
    versioning_behavior: temporalio.common.VersioningBehavior | None = None
    dynamic_config_fn: Callable[..., DynamicWorkflowConfig] | None = None

    @staticmethod
    def from_class(cls: type) -> _Definition | None:  # type: ignore[reportSelfClsParameterName]
        # We make sure to only return it if it's on _this_ class
        defn = getattr(cls, "__temporal_workflow_definition", None)
        if defn and defn.cls == cls:
            return defn
        return None

    @staticmethod
    def must_from_class(cls: type) -> _Definition:  # type: ignore[reportSelfClsParameterName]
        ret = _Definition.from_class(cls)
        if ret:
            return ret
        cls_name = getattr(cls, "__name__", "<unknown>")
        raise ValueError(
            f"Workflow {cls_name} missing attributes, was it decorated with @workflow.defn?"
        )

    @staticmethod
    def from_run_fn(fn: Callable[..., Awaitable[Any]]) -> _Definition | None:
        return getattr(fn, "__temporal_workflow_definition", None)

    @staticmethod
    def must_from_run_fn(fn: Callable[..., Awaitable[Any]]) -> _Definition:
        ret = _Definition.from_run_fn(fn)
        if ret:
            return ret
        fn_name = getattr(fn, "__qualname__", "<unknown>")
        raise ValueError(
            f"Function {fn_name} missing attributes, was it decorated with @workflow.run and was its class decorated with @workflow.defn?"
        )

    @classmethod
    def get_name_and_result_type(
        cls, name_or_run_fn: str | Callable[..., Awaitable[Any]]
    ) -> tuple[str, type | None]:
        if isinstance(name_or_run_fn, str):
            return name_or_run_fn, None
        elif callable(name_or_run_fn):
            defn = cls.must_from_run_fn(name_or_run_fn)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic workflow explicitly")
            return defn.name, defn.ret_type
        else:
            raise TypeError("Workflow must be a string or callable")  # type: ignore[reportUnreachable]

    @staticmethod
    def _apply_to_class(
        cls: type,  # type: ignore[reportSelfClsParameterName]
        *,
        workflow_name: str | None,
        sandboxed: bool,
        failure_exception_types: Sequence[type[BaseException]],
        versioning_behavior: temporalio.common.VersioningBehavior,
    ) -> None:
        # Check it's not being doubly applied
        if _Definition.from_class(cls):
            raise ValueError("Class already contains workflow definition")
        issues: list[str] = []

        # Collect run fn and all signal/query/update fns
        init_fn: Callable[..., None] | None = None
        run_fn: Callable[..., Awaitable[Any]] | None = None
        dynamic_config_fn: Callable[..., DynamicWorkflowConfig] | None = None
        seen_run_attr = False
        signals: dict[str | None, _SignalDefinition] = {}
        queries: dict[str | None, _QueryDefinition] = {}
        updates: dict[str | None, _UpdateDefinition] = {}
        for name, member in inspect.getmembers(cls):
            if hasattr(member, "__temporal_workflow_run"):
                seen_run_attr = True
                if not _is_unbound_method_on_cls(member, cls):
                    issues.append(
                        f"@workflow.run method {name} must be defined on {cls.__qualname__}"
                    )
                elif run_fn is not None:
                    issues.append(
                        f"Multiple @workflow.run methods found (at least on {name} and {run_fn.__name__})"
                    )
                else:
                    # We can guarantee the @workflow.run decorator did
                    # validation of the function itself
                    run_fn = member
            elif hasattr(member, "__temporal_signal_definition"):
                signal_defn = cast(
                    _SignalDefinition, getattr(member, "__temporal_signal_definition")
                )
                if signal_defn.name in signals:
                    defn_name = signal_defn.name or "<dynamic>"
                    # TODO(cretz): Remove cast when https://github.com/python/mypy/issues/5485 fixed
                    other_fn = cast(Callable, signals[signal_defn.name].fn)
                    issues.append(
                        f"Multiple signal methods found for {defn_name} "
                        f"(at least on {name} and {other_fn.__name__})"
                    )
                else:
                    signals[signal_defn.name] = signal_defn
            elif hasattr(member, "__temporal_query_definition"):
                query_defn = cast(
                    _QueryDefinition, getattr(member, "__temporal_query_definition")
                )
                if query_defn.name in queries:
                    defn_name = query_defn.name or "<dynamic>"
                    issues.append(
                        f"Multiple query methods found for {defn_name} "
                        f"(at least on {name} and {queries[query_defn.name].fn.__name__})"
                    )
                else:
                    queries[query_defn.name] = query_defn
            elif name == "__init__" and hasattr(member, "__temporal_workflow_init"):
                init_fn = member
            elif hasattr(member, "__temporal_workflow_dynamic_config"):
                if workflow_name:
                    issues.append(
                        "@workflow.dynamic_config can only be used in dynamic workflows, but "
                        f"workflow class {workflow_name} ({cls.__name__}) is not dynamic"
                    )
                if dynamic_config_fn:
                    issues.append(
                        "@workflow.dynamic_config can only be defined once per workflow"
                    )
                dynamic_config_fn = member
            elif isinstance(member, UpdateMethodMultiParam):
                update_defn = member._defn
                if update_defn.name in updates:
                    defn_name = update_defn.name or "<dynamic>"
                    issues.append(
                        f"Multiple update methods found for {defn_name} "
                        f"(at least on {name} and {updates[update_defn.name].fn.__name__})"
                    )
                elif update_defn.validator and not _parameters_identical_up_to_naming(
                    update_defn.fn, update_defn.validator
                ):
                    issues.append(
                        f"Update validator method {update_defn.validator.__name__} parameters "
                        f"do not match update method {update_defn.fn.__name__} parameters"
                    )
                else:
                    updates[update_defn.name] = update_defn

        # Check base classes haven't defined things with different decorators
        for base_cls in inspect.getmro(cls)[1:]:
            for _, base_member in inspect.getmembers(base_cls):
                # We only care about methods defined on this class
                if not inspect.isfunction(base_member) or not _is_unbound_method_on_cls(
                    base_member, base_cls
                ):
                    continue
                if hasattr(base_member, "__temporal_workflow_run"):
                    seen_run_attr = True
                    if not run_fn or base_member.__name__ != run_fn.__name__:
                        issues.append(
                            f"@workflow.run defined on {base_member.__qualname__} but not on the override"
                        )
                elif hasattr(base_member, "__temporal_signal_definition"):
                    signal_defn = cast(
                        _SignalDefinition,
                        getattr(base_member, "__temporal_signal_definition"),
                    )
                    if signal_defn.name not in signals:
                        issues.append(
                            f"@workflow.signal defined on {base_member.__qualname__} but not on the override"
                        )
                elif hasattr(base_member, "__temporal_query_definition"):
                    query_defn = cast(
                        _QueryDefinition,
                        getattr(base_member, "__temporal_query_definition"),
                    )
                    if query_defn.name not in queries:
                        issues.append(
                            f"@workflow.query defined on {base_member.__qualname__} but not on the override"
                        )
                elif isinstance(base_member, UpdateMethodMultiParam):
                    update_defn = base_member._defn
                    if update_defn.name not in updates:
                        issues.append(
                            f"@workflow.update defined on {base_member.__qualname__} but not on the override"
                        )

        if not seen_run_attr:
            issues.append("Missing @workflow.run method")
        if init_fn and run_fn:
            if not _parameters_identical_up_to_naming(init_fn, run_fn):
                issues.append(
                    "@workflow.init and @workflow.run method parameters do not match"
                )
        if issues:
            if len(issues) == 1:
                raise ValueError(f"Invalid workflow class: {issues[0]}")
            raise ValueError(
                f"Invalid workflow class for {len(issues)} reasons: {', '.join(issues)}"
            )

        assert run_fn
        assert seen_run_attr
        defn = _Definition(
            name=workflow_name,
            cls=cls,
            run_fn=run_fn,
            signals=signals,
            queries=queries,
            updates=updates,
            sandboxed=sandboxed,
            failure_exception_types=failure_exception_types,
            versioning_behavior=versioning_behavior,
            dynamic_config_fn=dynamic_config_fn,
        )
        setattr(cls, "__temporal_workflow_definition", defn)
        setattr(run_fn, "__temporal_workflow_definition", defn)

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            dynamic = self.name is None
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.run_fn)
            # If dynamic, must be a sequence of raw values
            if dynamic and (
                not arg_types
                or len(arg_types) != 1
                or arg_types[0] != Sequence[temporalio.common.RawValue]
            ):
                raise TypeError(
                    "Dynamic workflow must accept a single Sequence[temporalio.common.RawValue]"
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)


def _parameters_identical_up_to_naming(fn1: Callable, fn2: Callable) -> bool:
    """Return True if the functions have identical parameter lists, ignoring parameter names."""

    def params(fn: Callable) -> list[inspect.Parameter]:
        # Ignore name when comparing parameters (remaining fields are kind,
        # default, and annotation).
        return [p.replace(name="x") for p in inspect.signature(fn).parameters.values()]

    # We require that any type annotations present match exactly; i.e. we do
    # not support any notion of subtype compatibility.
    return params(fn1) == params(fn2)


def _is_unbound_method_on_cls(fn: Callable[..., Any], cls: type) -> bool:
    # Python 3 does not make this easy, ref https://stackoverflow.com/questions/3589311
    return (
        inspect.isfunction(fn)
        and inspect.getmodule(fn) is inspect.getmodule(cls)
        and fn.__qualname__.rsplit(".", 1)[0] == cls.__name__
    )
