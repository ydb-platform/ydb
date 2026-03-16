from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from os import PathLike
from pathlib import Path
from typing import Any, ForwardRef, Literal, Optional, TypeVar

from ..common import Dumper, Loader, TypeHint
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import LocStack
from ..provider.located_request import for_predicate
from ..provider.location import TypeHintLoc
from ..special_cases_optimization import as_is_stub
from ..type_tools import NormTypeAlias, is_new_type, strip_tags
from ..type_tools.basic_utils import eval_forward_ref
from ..utils import MappingHashWrapper
from .load_error import BadVariantLoadError, LoadError
from .provider_template import DelegatingProvider, DumperProvider, LoaderProvider
from .request_cls import DumperRequest, LoaderRequest, StrictCoercionRequest
from .utils import try_normalize_type

ResponseT = TypeVar("ResponseT")


class NewTypeUnwrappingProvider(DelegatingProvider):
    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        if not is_new_type(tp):
            raise CannotProvide

        return tp.__supertype__

    def _get_error_text(self) -> str:
        return "Try to unwrap NewType"


class TypeHintTagsUnwrappingProvider(DelegatingProvider):
    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        norm = try_normalize_type(tp)
        unwrapped = strip_tags(norm)
        if unwrapped.source == tp:  # type has not changed, continue search
            raise CannotProvide

        return unwrapped.source

    def _get_error_text(self) -> str:
        return "Try to unwrap type hints tag"


class TypeAliasUnwrappingProvider(DelegatingProvider):
    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        norm = try_normalize_type(tp)
        if not isinstance(norm, NormTypeAlias):
            raise CannotProvide

        return norm.value[tuple(arg.source for arg in norm.args)] if norm.args else norm.value

    def _get_error_text(self) -> str:
        return "Try to unwrap TypeAlias"


class ForwardRefEvaluatingProvider(DelegatingProvider):
    def _get_proxy_target(self, tp: TypeHint) -> TypeHint:
        if not isinstance(tp, ForwardRef):
            raise CannotProvide

        if tp.__forward_module__ is None:
            raise CannotProvide("ForwardRef cannot be evaluated", is_terminal=True, is_demonstrative=True)

        return eval_forward_ref(tp.__forward_module__.__dict__, tp)

    def _get_error_text(self) -> str:
        return "Try to evaluate ForwardRef"


def _is_exact_zero_or_one(arg):
    return type(arg) is int and arg in (0, 1)


@dataclass
@for_predicate(Literal)
class LiteralProvider(LoaderProvider, DumperProvider):
    tuple_size_limit: int = 4

    def _get_allowed_values_collection(self, args: Collection) -> Collection:
        if len(args) > self.tuple_size_limit:
            return set(args)
        return tuple(args)

    def _get_allowed_values_repr(self, args: Collection, mediator: Mediator, loc_stack: LocStack) -> Collection:
        enum_cases = [arg for arg in args if isinstance(arg, Enum)]
        if not enum_cases:
            return frozenset(args)

        literal_dumper = self.provide_dumper(mediator, DumperRequest(loc_stack))
        return frozenset(literal_dumper(arg) if isinstance(arg, Enum) else arg for arg in args)

    def _get_enum_types(self, cases: Collection) -> Collection:
        seen: set[type[Enum]] = set()
        enum_types = []
        for case in cases:
            case_type = type(case)
            if case_type not in seen:
                enum_types.append(case_type)
                seen.add(case_type)
        return enum_types

    def _fetch_enum_loaders(
        self,
        mediator: Mediator,
        request: LoaderRequest,
        enum_classes: Iterable[type[Enum]],
    ) -> Iterable[Loader[Enum]]:
        requests = [request.append_loc(TypeHintLoc(type=enum_cls)) for enum_cls in enum_classes]
        return mediator.mandatory_provide_by_iterable(
            requests,
            lambda: "Cannot create loader for literal. Loaders for enums cannot be created",
        )

    def _fetch_bytes_loader(
        self,
        mediator: Mediator,
        request: LoaderRequest,
    ) -> Loader[bytes]:
        request = request.append_loc(TypeHintLoc(type=bytes))
        return mediator.mandatory_provide(
            request,
            lambda _: "Cannot create loader for literal. Loader for bytes cannot be created",
        )

    def _fetch_enum_dumpers(
        self,
        mediator: Mediator,
        request: DumperRequest,
        enum_classes: Iterable[type[Enum]],
    ) -> Mapping[type[Enum], Dumper[Enum]]:
        requests = [request.append_loc(TypeHintLoc(type=enum_cls)) for enum_cls in enum_classes]
        dumpers = mediator.mandatory_provide_by_iterable(
            requests,
            lambda: "Cannot create dumper for literal. Dumpers for enums cannot be created",
        )
        return dict(zip(enum_classes, dumpers))

    def _fetch_bytes_dumper(
        self,
        mediator: Mediator,
        request: DumperRequest,
    ) -> Dumper[bytes]:
        request = request.append_loc(TypeHintLoc(type=bytes))
        return mediator.mandatory_provide(
            request,
            lambda _: "Cannot create dumper for literal. Dumper for bytes cannot be created",
        )

    def _get_literal_loader_with_enum(  # noqa: C901
        self,
        basic_loader: Loader,
        enum_loaders: Sequence[Loader[Enum]],
        allowed_values: Collection,
    ) -> Loader:
        if not enum_loaders:
            return basic_loader

        if len(enum_loaders) == 1:
            enum_loader = enum_loaders[0]

            def wrapped_loader_with_single_enum(data):
                try:
                    enum_value = enum_loader(data)
                except LoadError:
                    pass
                else:
                    if enum_value in allowed_values:
                        return enum_value
                return basic_loader(data)

            return wrapped_loader_with_single_enum

        def wrapped_loader_with_enums(data):
            for loader in enum_loaders:
                try:
                    enum_value = loader(data)
                except LoadError:
                    pass
                else:
                    if enum_value in allowed_values:
                        return enum_value
            return basic_loader(data)

        return wrapped_loader_with_enums

    def _get_literal_loader_with_bytes(
        self,
        basic_loader: Loader,
        allowed_values: Collection,
        bytes_loader: Loader,
    ) -> Loader:
        def wrapped_loader_with_bytes(data):
            try:
                bytes_value = bytes_loader(data)
            except LoadError:
                pass
            else:
                if bytes_value in allowed_values:
                    return bytes_value
            return basic_loader(data)

        return wrapped_loader_with_bytes

    def _get_literal_loader_many(self, *loaders: Loader, basic_loader: Loader) -> Loader:
        if len(loaders) == 1:
            return loaders[0]

        def wrapped_loader_many(data):
            for c, loader in enumerate(loaders):
                try:
                    return loader(data)
                except LoadError:
                    last_iteration = len(loaders) - 1
                    if c != last_iteration:
                        continue
            return basic_loader(data)

        return wrapped_loader_many

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        norm = try_normalize_type(request.last_loc.type)
        strict_coercion = mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack))
        enum_cases = tuple(arg for arg in norm.args if isinstance(arg, Enum))
        bytes_cases = tuple(arg for arg in norm.args if isinstance(arg, bytes))
        enum_loaders = tuple(self._fetch_enum_loaders(mediator, request, self._get_enum_types(enum_cases)))
        bytes_loader = self._fetch_bytes_loader(mediator, request)
        allowed_values_repr = self._get_allowed_values_repr(norm.args, mediator, request.loc_stack)
        return mediator.cached_call(
            self._make_loader,
            cases=norm.args,
            bytes_cases=bytes_cases,
            strict_coercion=strict_coercion,
            enum_loaders=enum_loaders,
            bytes_loader=bytes_loader,
            allowed_values_repr=allowed_values_repr,
        )

    def _make_loader(
        self,
        *,
        cases: Sequence[Any],
        strict_coercion: bool,
        enum_loaders: Sequence[Loader],
        allowed_values_repr: Collection[str],
        bytes_cases: Sequence[bytes],
        bytes_loader: Loader[bytes],
    ) -> Loader:
        if strict_coercion and any(isinstance(arg, bool) or _is_exact_zero_or_one(arg) for arg in cases):
            allowed_values_with_types = self._get_allowed_values_collection(
                [(type(el), el) for el in cases],
            )

            # since True == 1 and False == 0
            def literal_loader_sc(data):
                if (type(data), data) in allowed_values_with_types:
                    return data
                raise BadVariantLoadError(allowed_values_repr, data)

            return self._get_literal_loader_with_enum(
                literal_loader_sc,
                enum_loaders,
                allowed_values_with_types,
            )

        allowed_values = self._get_allowed_values_collection(cases)

        def literal_loader(data):
            if data in allowed_values:
                return data
            raise BadVariantLoadError(allowed_values_repr, data)

        if bytes_cases and not enum_loaders:
            return self._get_literal_loader_with_bytes(literal_loader, allowed_values, bytes_loader)

        if not bytes_cases:
            return self._get_literal_loader_with_enum(literal_loader, enum_loaders, allowed_values)

        return self._get_literal_loader_many(
            self._get_literal_loader_with_bytes(literal_loader, allowed_values, bytes_loader),
            self._get_literal_loader_with_enum(literal_loader, enum_loaders, allowed_values),
            basic_loader=literal_loader,
        )

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        norm = try_normalize_type(request.last_loc.type)
        enum_cases = [arg for arg in norm.args if isinstance(arg, Enum)]
        bytes_cases = tuple(arg for arg in norm.args if isinstance(arg, bytes))

        if not enum_cases and not bytes_cases:
            return as_is_stub

        enum_dumpers = self._fetch_enum_dumpers(mediator, request, self._get_enum_types(enum_cases))
        bytes_dumper = self._fetch_bytes_dumper(mediator, request)

        return mediator.cached_call(
            self._make_dumper,
            enum_dumpers_wrapper=MappingHashWrapper(enum_dumpers),
            bytes_dumper=bytes_dumper,
        )

    def _get_enum_dumper(self, enum_dumpers: Mapping[type[Enum], Dumper[Enum]]) -> Dumper:
        if len(enum_dumpers) == 1:
            enum_dumper = next(iter(enum_dumpers.values()))

            def literal_dumper_with_single_enum(data):
                if isinstance(data, Enum):
                    return enum_dumper(data)
                return data

            return literal_dumper_with_single_enum

        def literal_dumper_with_enums(data):
            if isinstance(data, Enum):
                return enum_dumpers[type(data)](data)
            return data

        return literal_dumper_with_enums

    def _get_bytes_literal_dumper(self, bytes_dumper: Dumper[bytes]) -> Dumper:
        def literal_dumper_with_bytes(data):
            if isinstance(data, bytes):
                return bytes_dumper(data)
            return data

        return literal_dumper_with_bytes

    def _make_dumper(
        self,
        enum_dumpers_wrapper: MappingHashWrapper[Mapping[type[Enum], Dumper[Enum]]],
        bytes_dumper: Optional[Dumper[bytes]],
    ):
        enum_dumpers = enum_dumpers_wrapper.mapping

        if not bytes_dumper:
            return self._get_enum_dumper(enum_dumpers)

        if not enum_dumpers:
            return self._get_bytes_literal_dumper(bytes_dumper)

        bytes_literal_dumper = self._get_bytes_literal_dumper(bytes_dumper)
        enum_literal_dumper = self._get_enum_dumper(enum_dumpers)

        def literal_dumper_many(data):
            if isinstance(data, bytes):
                return bytes_literal_dumper(data)
            if isinstance(data, Enum):
                return enum_literal_dumper(data)
            return data

        return literal_dumper_many


def path_like_dumper(data):
    return data.__fspath__()


@for_predicate(PathLike[str])
class PathLikeProvider(LoaderProvider, DumperProvider):
    _impl = Path

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.mandatory_provide(
            LoaderRequest(
                loc_stack=request.loc_stack.replace_last_type(self._impl),
            ),
            lambda x: f"Cannot create loader for {PathLike}. Loader for {Path} cannot be created",
        )

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return path_like_dumper
