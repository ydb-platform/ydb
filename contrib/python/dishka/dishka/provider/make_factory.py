import warnings
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from inspect import (
    Parameter,
    isasyncgenfunction,
    isbuiltin,
    isclass,
    iscoroutinefunction,
    isfunction,
    isgeneratorfunction,
    ismethod,
    signature,
    unwrap,
)
from types import UnionType
from typing import (
    Annotated,
    Any,
    Protocol,
    TypeAlias,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from dishka._adaptix.type_tools.basic_utils import (  # type: ignore[attr-defined]
    get_type_vars,
    is_bare_generic,
    is_protocol,
    strip_alias,
)
from dishka._adaptix.type_tools.fundamentals import (
    get_all_type_hints,
)
from dishka._adaptix.type_tools.generic_resolver import (
    GenericResolver,
    MembersStorage,
)
from dishka.dependency_source import (
    CompositeDependencySource,
    DependencySource,
    Factory,
    ensure_composite,
)
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import (
    dependency_key_to_hint,
    hint_to_dependency_key,
)
from dishka.entities.marker import BaseMarker, BoolMarker
from dishka.entities.provides_marker import AnyOf, ProvideMultiple
from dishka.entities.scope import BaseScope
from dishka.entities.type_alias_type import (
    is_type_alias_type,
    unwrap_type_alias,
)
from dishka.exceptions import WhenOverrideConflictError
from dishka.text_rendering import get_name
from .exceptions import (
    CannotUseProtocolError,
    MissingHintsError,
    MissingReturnHintError,
    NotAFactoryError,
    UndefinedTypeAnalysisError,
    UnsupportedGeneratorReturnTypeError,
)
from .unpack_provides import unpack_factory

_empty = signature(lambda a: 0).parameters["a"].annotation
_protocol_init = type("_stub_proto", (Protocol,), {}).__init__  # type: ignore[misc, arg-type]
ProvideSource: TypeAlias = (
    Callable[..., Any]
    | classmethod  # type: ignore[type-arg]
    | staticmethod  # type: ignore[type-arg]
    | type
)


def _is_bound_method(obj: Any) -> bool:
    return ismethod(obj) and bool(obj.__self__)


def _resolve_init(tp: type) -> Any:
    init = tp.__init__  # type: ignore[misc]
    if init is not _protocol_init:
        return init
    for cls in tp.__mro__:
        if cls is object:
            continue
        init_candidate = cls.__dict__.get("__init__")
        if init_candidate is not None and init_candidate is not _protocol_init:
            return init_candidate
    return init


def _get_init_members(tp: type) -> MembersStorage[str, None]:
    real_init = _resolve_init(tp)
    type_hints = get_all_type_hints(real_init)  # type: ignore[no-untyped-call]
    if "__init__" in tp.__dict__:
        overridden = frozenset(type_hints)
    else:
        overridden = frozenset()

    return MembersStorage(
        meta=None,
        members=type_hints,
        overridden=overridden,
    )


def _get_kw_dependencies(
    hints: Mapping[str, Any], params: Mapping[str, Parameter],
) -> dict[str, Any]:
    return {
        name: hint_to_dependency_key(unwrap_type_alias(hints.get(name)))
        for name, param in params.items()
        if param.kind is Parameter.KEYWORD_ONLY
    }


def _get_dependencies(
    hints: Mapping[str, Any], params: Mapping[str, Parameter],
) -> list[Any]:
    return [
        hint_to_dependency_key(unwrap_type_alias(hints.get(name)))
        for name, param in params.items()
        if param.kind in (
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]


def _guess_factory_type(source: Any) -> FactoryType:
    if isasyncgenfunction(source):
        return FactoryType.ASYNC_GENERATOR
    elif isgeneratorfunction(source):
        return FactoryType.GENERATOR
    elif iscoroutinefunction(source):
        return FactoryType.ASYNC_FACTORY
    else:
        return FactoryType.FACTORY


def _type_repr(hint: Any) -> str:
    if hint is type(None):
        return "None"
    return get_name(hint, include_module=True)


def _async_generator_result(hint: Any) -> Any:
    hint = unwrap_type_alias(hint)
    if get_origin(hint) is ProvideMultiple:
        return ProvideMultiple[tuple(  # type: ignore[misc]
            _async_generator_result(x) for x in get_args(hint)
        )]
    origin = get_origin(hint)
    if origin is AsyncIterable:
        return get_args(hint)[0]
    elif origin is AsyncIterator:
        return get_args(hint)[0]
    elif origin is AsyncGenerator:
        return get_args(hint)[0]
    # errors
    name = _type_repr(hint)
    if origin is Iterable:
        args = ", ".join(_type_repr(a) for a in get_args(hint))
        guess = "AsyncIterable"
    elif origin is Iterator:
        args = ", ".join(_type_repr(a) for a in get_args(hint))
        guess = "AsyncIterator"
    elif origin is Generator:
        args = ", ".join(_type_repr(a) for a in get_args(hint)[:2])
        guess = "AsyncGenerator"
    else:
        args = name
        guess = "AsyncIterable"

    raise UnsupportedGeneratorReturnTypeError(name, guess, args, is_async=True)


def _generator_result(hint: Any) -> Any:
    hint = unwrap_type_alias(hint)
    if get_origin(hint) is ProvideMultiple:
        return ProvideMultiple[tuple(  # type: ignore[misc]
            _generator_result(x) for x in get_args(hint)
        )]
    origin = get_origin(hint)
    if origin is Iterable:
        return get_args(hint)[0]
    elif origin is Iterator:
        return get_args(hint)[0]
    elif origin is Generator:
        return get_args(hint)[0]
    # errors
    name = _type_repr(hint)
    if origin is AsyncIterable:
        args = ", ".join(_type_repr(a) for a in get_args(hint))
        guess = "Iterable"
    elif origin is AsyncIterator:
        args = ", ".join(_type_repr(a) for a in get_args(hint))
        guess = "Iterator"
    elif origin is AsyncGenerator:
        args = ", ".join(_type_repr(a) for a in get_args(hint)) + ", None"
        guess = "Generator"
    else:
        args = name
        guess = "Iterable"

    raise UnsupportedGeneratorReturnTypeError(name, guess, args)


def _alias_to_anyof(possible_dependency: Any) -> Any:
    if not is_type_alias_type(possible_dependency):
        return possible_dependency
    options: tuple[Any, ...] = (possible_dependency, )
    while is_type_alias_type(possible_dependency):
        possible_dependency = possible_dependency.__value__
        options = (possible_dependency, *options)
    return AnyOf[options]


def _clean_result_hint(
    factory_type: FactoryType,
    possible_dependency: Any,
) -> Any:
    if factory_type == FactoryType.ASYNC_GENERATOR:
        possible_dependency = _async_generator_result(possible_dependency)
    elif factory_type == FactoryType.GENERATOR:
        possible_dependency = _generator_result(possible_dependency)

    return _alias_to_anyof(possible_dependency)


def _params_without_hints(func: Any, *, skip_self: bool) -> Sequence[str]:
    if func is object.__init__:
        return []
    if func is _protocol_init:
        return []
    params = signature(func).parameters
    return [
        p.name
        for i, p in enumerate(params.values())
        if p.annotation is _empty
        if i > 0 or not skip_self
    ]


def _make_factory_by_class(
        *,
        provides: Any,
        scope: BaseScope | None,
        source: type,
        cache: bool,
        override: bool,
        when: BaseMarker | None,
) -> Factory:
    if not provides:
        provides = source

    init = _resolve_init(strip_alias(source))
    if missing_hints := _params_without_hints(init, skip_self=True):
        raise MissingHintsError(source, missing_hints, append_init=True)
    # we need to fix concrete generics and normal classes as well
    # as classes can be children of concrete generics
    res = GenericResolver(_get_init_members)
    try:
        hints = dict(res.get_resolved_members(source).members)
    except NameError as e:
        raise UndefinedTypeAnalysisError(source, str(e.name)) from e

    hints.pop("return", _empty)
    params = signature(init).parameters

    return Factory(
        dependencies=_get_dependencies(hints, params)[1:],
        kw_dependencies=_get_kw_dependencies(hints, params),
        type_=FactoryType.FACTORY,
        source=source,
        scope=scope,
        provides=hint_to_dependency_key(provides),
        is_to_bind=False,
        cache=cache,
        when_override=calc_override(when=when, override=override),
        when_active=when,
        when_component=None,
        when_dependencies=[],
    )


def _check_self_name(
        source: Callable[..., Any] | classmethod,  # type: ignore[type-arg]
        self: Parameter | None,
) -> None:
    if isinstance(source, classmethod):
        return
    if self and self.name == "self":
        return
    warnings.warn(
        f"You are trying to use function `{source.__name__}` "
        "without `self` argument "
        "inside Provider class, it would be treated as method. "
        "Consider wrapping it with `staticmethod`, "
        "registering on a provider instance or adding `self`.",
        stacklevel=6,
    )


def _make_factory_by_function(
        *,
        provides: Any,
        scope: BaseScope | None,
        source: Callable[..., Any] | classmethod,  # type: ignore[type-arg]
        cache: bool,
        is_in_class: bool,
        override: bool,
        check_self_name: bool,
        when: BaseMarker | None,
) -> Factory:
    # typing.cast is applied as unwrap takes a Callable object
    raw_source = unwrap(cast(Callable[..., Any], source))
    missing_hints = _params_without_hints(raw_source, skip_self=is_in_class)
    if missing_hints:
        raise MissingHintsError(source, missing_hints)

    params = signature(raw_source).parameters
    factory_type = _guess_factory_type(raw_source)

    try:
        hints = get_type_hints(source, include_extras=True)
    except NameError as e:
        raise UndefinedTypeAnalysisError(source, str(e.name)) from e
    if is_in_class:
        self = next(iter(params.values()), None)
        if self and self.name not in hints:
            # add self to dependencies, so it can be easily removed
            # if we will bind factory to provider instance
            hints = {self.name: Any, **hints}
        if check_self_name:
            _check_self_name(source, self)
    possible_dependency = hints.pop("return", _empty)

    if not provides:
        if possible_dependency is _empty:
            raise MissingReturnHintError(source)
        try:
            provides = _clean_result_hint(factory_type, possible_dependency)
        except TypeError as e:
            name = get_name(source, include_module=True)
            raise TypeError(f"Failed to analyze `{name}`. \n" + str(e)) from e
    return Factory(
        dependencies=_get_dependencies(hints, params),
        kw_dependencies=_get_kw_dependencies(hints, params),
        type_=factory_type,
        source=source,
        scope=scope,
        provides=hint_to_dependency_key(provides),
        is_to_bind=is_in_class,
        cache=cache,
        when_override=calc_override(when=when, override=override),
        when_active=when,
        when_component=None,
        when_dependencies=[],
    )


def _make_factory_by_static_method(
        *,
        provides: Any,
        scope: BaseScope | None,
        source: staticmethod,  # type: ignore[type-arg]
        cache: bool,
        override: bool,
        when: BaseMarker | None,
) -> Factory:
    if missing_hints := _params_without_hints(source, skip_self=False):
        raise MissingHintsError(source, missing_hints)
    factory_type = _guess_factory_type(source.__wrapped__)
    try:
        hints = get_type_hints(source, include_extras=True)
    except NameError as e:
        raise UndefinedTypeAnalysisError(source, str(e.name)) from e

    params = signature(source).parameters
    possible_dependency = hints.pop("return", _empty)

    if not provides:
        if possible_dependency is _empty:
            raise MissingReturnHintError(source)
        try:
            provides = _clean_result_hint(factory_type, possible_dependency)
        except TypeError as e:
            name = get_name(source, include_module=True)
            raise TypeError(f"Failed to analyze `{name}`. \n" + str(e)) from e
    return Factory(
        dependencies=_get_dependencies(hints, params),
        kw_dependencies=_get_kw_dependencies(hints, params),
        type_=factory_type,
        source=source,
        scope=scope,
        provides=hint_to_dependency_key(provides),
        is_to_bind=False,
        cache=cache,
        when_override=calc_override(when=when, override=override),
        when_active=when,
        when_component=None,
        when_dependencies=[],
    )


def calc_override(
    *,
    when: BaseMarker | None,
    override: bool,
) -> BaseMarker | None:
    if when is not None:
        return when
    if override:
        return BoolMarker(True)
    return None


def _make_factory_by_other_callable(
        *,
        provides: Any,
        scope: BaseScope | None,
        source: Callable[..., Any],
        cache: bool,
        override: bool,
        when: BaseMarker | None,
) -> Factory:
    if _is_bound_method(source):
        to_check = source.__func__  # type: ignore[attr-defined]
        is_in_class = True
    else:
        call_method = source.__call__   # type: ignore[operator]
        if _is_bound_method(call_method):
            to_check = call_method.__func__
            is_in_class = True
        else:
            to_check = call_method
            is_in_class = False
    factory = _make_factory_by_function(
        provides=provides,
        source=to_check,
        cache=cache,
        scope=scope,
        is_in_class=is_in_class,
        override=override,
        check_self_name=False,
        when=when,
    )
    if factory.is_to_bind:
        dependencies = factory.dependencies[1:]  # remove `self`
    else:
        dependencies = factory.dependencies

    return Factory(
        dependencies=dependencies,
        kw_dependencies=factory.kw_dependencies,
        type_=factory.type,
        source=source,
        scope=scope,
        provides=factory.provides,
        is_to_bind=False,
        cache=cache,
        when_override=calc_override(when=when, override=override),
        when_active=when,
        when_component=None,
        when_dependencies=[],
    )


def _extract_source(
    provides: Any,
    source: ProvideSource,
) -> tuple[Any, ProvideSource]:
    if get_origin(source) is Annotated:
        source = get_args(source)[0]

    if get_origin(source) is ProvideMultiple:
        if provides is None:
            provides = source
        source = get_args(source)[0]

    if is_bare_generic(source):
        source = source[get_type_vars(source)]  # type: ignore[index]
    return provides, source


def make_factory(
        *,
        provides: Any,
        scope: BaseScope | None,
        source: ProvideSource,
        cache: bool,
        is_in_class: bool,
        override: bool,
        when: BaseMarker | None = None,
) -> Factory:
    provides, source = _extract_source(provides, source)

    if source and is_protocol(source):
        raise CannotUseProtocolError(source)

    source_origin = get_origin(source)

    if source_origin is UnionType:
        raise NotAFactoryError(source)

    if isclass(source) or isclass(source_origin):
        return _make_factory_by_class(
            provides=provides,
            scope=scope,
            source=cast(type, source),
            cache=cache,
            override=override,
            when=when,
        )
    elif isfunction(source) or isinstance(source, classmethod):
        return _make_factory_by_function(
            provides=provides,
            scope=scope,
            source=source,
            cache=cache,
            is_in_class=is_in_class,
            override=override,
            check_self_name=True,
            when=when,
        )
    elif isbuiltin(source):
        return _make_factory_by_function(
            provides=provides,
            scope=scope,
            source=source,
            cache=cache,
            is_in_class=False,
            override=override,
            check_self_name=False,
            when=when,
        )
    elif isinstance(source, staticmethod):
        return _make_factory_by_static_method(
            provides=provides,
            scope=scope,
            source=source,
            cache=cache,
            override=override,
            when=when,
        )
    elif callable(source) and not source_origin:
        return _make_factory_by_other_callable(
            provides=provides,
            scope=scope,
            source=source,
            cache=cache,
            override=override,
            when=when,
        )
    else:
        raise NotAFactoryError(source)


def _provide(
        *,
        source: ProvideSource | None = None,
        scope: BaseScope | None = None,
        provides: Any = None,
        cache: bool = True,
        is_in_class: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    if when and override:
        raise WhenOverrideConflictError
    composite = ensure_composite(source)
    factory = make_factory(
        provides=provides, scope=scope,
        source=composite.origin, cache=cache,
        is_in_class=is_in_class,
        override=override,
        when=when,
    )
    composite.dependency_sources.extend(unpack_factory(factory))
    if not recursive:
        return composite

    additional_sources: list[DependencySource] = []
    for src in composite.dependency_sources:
        if not isinstance(src, Factory):
            # we expect Factory and Alias here
            continue
        for dependency in src.dependencies:
            additional = _provide(
                provides=dependency_key_to_hint(dependency),
                scope=scope,
                source=dependency.type_hint,
                cache=cache,
                is_in_class=is_in_class,
                override=override,
                when=when,
            )
            additional_sources.extend(additional.dependency_sources)
    composite.dependency_sources.extend(additional_sources)
    return composite


def provide_on_instance(
        *,
        source: ProvideSource | None = None,
        scope: BaseScope | None = None,
        provides: Any = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    return _provide(
        provides=provides, scope=scope, source=source, cache=cache,
        is_in_class=False,
        recursive=recursive, override=override,
        when=when,
    )


@overload
def provide(
        *,
        scope: BaseScope | None = None,
        provides: Any = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> Callable[[Callable[..., Any]], CompositeDependencySource]:
    ...


@overload
def provide(
        source: ProvideSource | None,
        *,
        scope: BaseScope | None = None,
        provides: Any = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    ...


def provide(
        source: ProvideSource | None = None,
        *,
        scope: BaseScope | None = None,
        provides: Any = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource | Callable[
    [Callable[..., Any]], CompositeDependencySource,
]:
    """
    Mark a method or class as providing some dependency.

    If used as a method decorator then return annotation is used
    to determine what is provided. User `provides` to override that.
    Method parameters are analyzed and passed automatically.

    If used with a class a first parameter than `__init__` method parameters
    are passed automatically. If no provides is passed then it is
    supposed that class itself is a provided dependency.

    Return value must be saved as a `Provider` class attribute and
    not intended for direct usage

    :param source: Method to decorate or class.
    :param scope: Scope of the dependency to limit its lifetime
    :param provides: Dependency type which is provided by this factory
    :param cache: save created object to scope cache or not
    :param recursive: register dependencies as factories as well
    :param override: dependency override
    :return: instance of Factory or a decorator returning it
    """
    if source is not None:
        return _provide(
            provides=provides, scope=scope, source=source, cache=cache,
            is_in_class=True, recursive=recursive, override=override,
            when=when,
        )

    def scoped(func: Callable[..., Any]) -> CompositeDependencySource:
        return _provide(
            provides=provides, scope=scope, source=func, cache=cache,
            is_in_class=True, recursive=recursive, override=override,
            when=when,
        )

    return scoped


def _provide_all(
        *,
        provides: Sequence[Any],
        scope: BaseScope | None,
        cache: bool,
        is_in_class: bool,
        recursive: bool,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    composite = CompositeDependencySource(None)
    for single_provides in provides:
        source = _provide(
            source=single_provides,
            provides=None,
            scope=scope,
            cache=cache,
            is_in_class=is_in_class,
            recursive=recursive,
            override=override,
            when=when,
        )
        composite.dependency_sources.extend(source.dependency_sources)
    return composite


def provide_all(
        *provides: Any,
        scope: BaseScope | None = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    return _provide_all(
        provides=provides, scope=scope,
        cache=cache, is_in_class=True,
        recursive=recursive, override=override,
        when=when,
    )


def provide_all_on_instance(
        *provides: Any,
        scope: BaseScope | None = None,
        cache: bool = True,
        recursive: bool = False,
        override: bool = False,
        when: BaseMarker | None = None,
) -> CompositeDependencySource:
    return _provide_all(
        provides=provides, scope=scope,
        cache=cache, is_in_class=False,
        recursive=recursive, override=override,
        when=when,
    )
