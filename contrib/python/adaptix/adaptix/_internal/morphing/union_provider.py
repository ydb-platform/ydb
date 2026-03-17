import collections.abc
from collections.abc import Iterable, Sequence
from typing import Any, Literal, Optional, Union

from ..common import Dumper, Loader, TypeHint
from ..compat import CompatExceptionGroup
from ..datastructures import ClassDispatcher
from ..definitions import DebugTrail
from ..provider.essential import AggregateCannotProvide, CannotProvide, Mediator
from ..provider.loc_stack_filtering import LocStack
from ..provider.loc_stack_tools import format_type
from ..provider.located_request import LocatedRequest, for_predicate
from ..provider.location import GenericParamLoc
from ..special_cases_optimization import as_is_stub
from ..type_tools import BaseNormType, is_subclass_soft, strip_tags
from ..type_tools.normalize_type import NoneType
from .concrete_provider import none_loader
from .load_error import LoadError, TypeLoadError, UnionLoadError
from .provider_template import DumperProvider, LoaderProvider
from .request_cls import DebugTrailRequest, DumperRequest, LoaderRequest
from .sentinel_provider import check_is_sentinel
from .utils import try_normalize_type


@for_predicate(Union)
class UnionProvider(LoaderProvider, DumperProvider):
    def _get_loc_stacks_to_request(
        self,
        mediator: Mediator,
        request: LocatedRequest,
        norm: BaseNormType,
        target: str,
    ) -> Sequence[LocStack]:
        loc_stacks = [
            request.loc_stack.append_with(
                GenericParamLoc(
                    type=norm_tp.source,
                    generic_pos=i,
                ),
            )
            for i, norm_tp in enumerate(norm.args)
        ]
        filtered_loc_stacks = [
            loc_stack for loc_stack in loc_stacks
            if not check_is_sentinel(mediator, loc_stack)
        ]
        if not filtered_loc_stacks:
            raise CannotProvide(
                f"Cannot create {target} for union consisting only of sentinel",
                is_terminal=True,
                is_demonstrative=True,
            )
        return tuple(filtered_loc_stacks)

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        norm = try_normalize_type(request.last_loc.type)
        debug_trail = mediator.mandatory_provide(DebugTrailRequest(loc_stack=request.loc_stack))
        loc_stacks = self._get_loc_stacks_to_request(
            mediator=mediator,
            request=request,
            norm=norm,
            target="loader",
        )
        loaders = mediator.mandatory_provide_by_iterable(
            [
                request.with_loc_stack(loc_stack) for loc_stack in loc_stacks
            ],
            lambda: "Cannot create loader for union. Loaders for some union cases cannot be created",
        )
        if len(loaders) == 1:
            return loaders[0]

        if (not_none_loader := self._parse_single_optional_loader(loaders)) is not None:
            if debug_trail in (DebugTrail.ALL, DebugTrail.FIRST):
                return mediator.cached_call(self._produce_single_optional_dt_loader, norm.source, not_none_loader)
            if debug_trail == DebugTrail.DISABLE:
                return mediator.cached_call(self._produce_single_optional_dt_disable_loader, not_none_loader)
            raise ValueError

        if debug_trail == DebugTrail.DISABLE:
            return mediator.cached_call(self._produce_loader_dt_disable, tuple(loaders))
        if debug_trail == DebugTrail.FIRST:
            return mediator.cached_call(self._produce_loader_dt_first, norm.source, tuple(loaders))
        if debug_trail == DebugTrail.ALL:
            return mediator.cached_call(self._produce_loader_dt_all, norm.source, tuple(loaders))
        raise ValueError

    def _parse_single_optional_loader(self, loaders: Sequence[Loader]) -> Optional[Loader]:
        try:
            [first, second] = loaders
        except ValueError:
            return None
        if first != none_loader and second == none_loader:
            return first
        if first == none_loader and second != none_loader:
            return second
        return None

    def _produce_single_optional_dt_disable_loader(self, loader: Loader) -> Loader:
        def optional_dt_disable_loader(data):
            if data is None:
                return None
            return loader(data)

        return optional_dt_disable_loader

    def _produce_single_optional_dt_loader(self, tp, loader: Loader) -> Loader:
        def optional_dt_loader(data):
            if data is None:
                return None
            try:
                return loader(data)
            except LoadError as e:
                raise UnionLoadError(f"while loading {tp}", [TypeLoadError(None, data), e])

        return optional_dt_loader

    def _produce_loader_dt_disable(self, loader_iter: Iterable[Loader]) -> Loader:
        def union_loader(data):
            for loader in loader_iter:
                try:
                    return loader(data)
                except LoadError:
                    pass
            raise LoadError

        return union_loader

    def _produce_loader_dt_first(self, tp, loader_iter: Iterable[Loader]) -> Loader:
        def union_loader_dt_first(data):
            errors = []
            for loader in loader_iter:
                try:
                    return loader(data)
                except LoadError as e:
                    errors.append(e)

            raise UnionLoadError(f"while loading {tp}", errors)

        return union_loader_dt_first

    def _produce_loader_dt_all(self, tp, loader_iter: Iterable[Loader]) -> Loader:
        def union_loader_dt_all(data):
            errors = []
            has_unexpected_error = False
            for loader in loader_iter:
                try:
                    result = loader(data)
                except LoadError as e:
                    errors.append(e)
                except Exception as e:
                    errors.append(e)
                    has_unexpected_error = True
                else:
                    if not has_unexpected_error:
                        return result

            if has_unexpected_error:
                raise CompatExceptionGroup(f"while loading {tp}", errors)
            raise UnionLoadError(f"while loading {tp}", errors)

        return union_loader_dt_all

    def _is_allowed_origin(self, origin) -> bool:
        return (
            origin is None
            or origin is Literal
            or (isinstance(origin, type) and not is_subclass_soft(origin, collections.abc.Callable))
        )

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        norm = try_normalize_type(request.last_loc.type)
        loc_stacks = self._get_loc_stacks_to_request(
            mediator=mediator,
            request=request,
            norm=norm,
            target="dumper",
        )
        dumpers = mediator.mandatory_provide_by_iterable(
            [
                request.with_loc_stack(loc_stack) for loc_stack in loc_stacks
            ],
            lambda: "Cannot create dumper for union. Dumpers for some union cases cannot be created",
        )
        if len(dumpers) == 1:
            return dumpers[0]
        if all(dumper == as_is_stub for dumper in dumpers):
            return as_is_stub

        if (not_none_dumper := self._parse_single_optional_dumper(norm, dumpers)) is not None:
            return self._produce_single_optional_dumper(not_none_dumper)

        types_with_forbidden_origins = [
            case.source for case in norm.args
            if not self._is_allowed_origin(case.origin)
        ]
        if types_with_forbidden_origins:
            raise AggregateCannotProvide(
                "All cases of union must be class or Literal",
                [
                    CannotProvide(
                        f"Found {format_type(tp)}",
                        is_demonstrative=True,
                    )
                    for tp in types_with_forbidden_origins
                ],
                is_terminal=True,
                is_demonstrative=True,
            )
        return mediator.cached_call(
            self._make_dumper,
            tuple(loc_stack.last.type for loc_stack in loc_stacks),
            tuple(dumpers),
        )

    def _make_dumper(self, args: Iterable[TypeHint], dumpers: Iterable[Dumper]) -> Dumper:
        non_literals, literal = self._extract_literal(args, dumpers)
        dumper_class_dispatcher = ClassDispatcher[Any, Dumper](
            {
                self._get_class_for_dumping(case): dumper
                for case, dumper in non_literals
            },
        )
        if literal is not None:
            return self._produce_dumper_with_literal(dumper_class_dispatcher, literal[0].args, literal[1])

        return self._produce_dumper(dumper_class_dispatcher)

    def _get_class_for_dumping(self, norm: BaseNormType) -> type:
        if norm.origin is None:
            return NoneType
        return strip_tags(norm).origin

    def _parse_single_optional_dumper(self, norm: BaseNormType, dumpers: Iterable[Dumper]) -> Optional[Dumper]:
        try:
            [(first_norm, first_dumper), (second_norm, second_dumper)] = zip(norm.args, dumpers)
        except ValueError:
            return None
        if first_norm.origin is None and first_dumper == as_is_stub:
            return second_dumper
        if second_norm.origin is None and second_dumper == as_is_stub:
            return first_dumper
        return None

    def _extract_literal(self, args: Iterable[TypeHint], dumpers: Iterable[Dumper]):
        non_literals: list[tuple[BaseNormType, Dumper]] = []
        literal: Optional[tuple[BaseNormType, Dumper]] = None
        for arg, dumper in zip(args, dumpers):
            norm = try_normalize_type(arg)
            if norm.origin == Literal:
                literal = (norm, dumper)
            else:
                non_literals.append((norm, dumper))

        return non_literals, literal

    def _produce_single_optional_dumper(self, dumper: Dumper) -> Dumper:
        def optional_dumper(data):
            if data is None:
                return None
            return dumper(data)

        return optional_dumper

    def _produce_dumper_with_literal(
        self,
        dumper_class_dispatcher: ClassDispatcher[Any, Dumper],
        literal_cases: Sequence[Any],
        literal_dumper: Dumper,
    ) -> Dumper:
        def union_dumper_with_literal(data):
            if data in literal_cases:
                return literal_dumper(data)
            return dumper_class_dispatcher.dispatch(type(data))(data)

        return union_dumper_with_literal

    def _produce_dumper(self, dumper_class_dispatcher: ClassDispatcher[Any, Dumper]) -> Dumper:
        def union_dumper(data):
            return dumper_class_dispatcher.dispatch(type(data))(data)

        return union_dumper
