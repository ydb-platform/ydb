import collections.abc
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import replace
from typing import Any, Callable, Union, final

from ..common import Coercer, OneArgCoercer, TypeHint
from ..morphing.utils import try_normalize_type
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import LocStackChecker
from ..provider.location import GenericParamLoc
from ..special_cases_optimization import as_is_stub, as_is_stub_with_ctx
from ..type_tools import BaseNormType, is_generic, is_parametrized, is_subclass_soft, normalize_type, strip_tags
from .provider_template import CoercerProvider
from .request_cls import CoercerRequest


class NormTypeCoercerProvider(CoercerProvider, ABC):
    @final
    def _provide_coercer(self, mediator: Mediator, request: CoercerRequest) -> Coercer:
        norm_src = try_normalize_type(request.src.last.type)
        norm_dst = try_normalize_type(request.dst.last.type)
        return self._provide_coercer_norm_types(mediator, request, norm_src, norm_dst)

    @abstractmethod
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        ...


class SameTypeCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        if norm_src == norm_dst:
            return as_is_stub_with_ctx
        raise CannotProvide


class DstAnyCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        if norm_dst.origin == Any:
            return as_is_stub_with_ctx
        raise CannotProvide


class SubclassCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        if (
            is_generic(norm_src.source)
            or is_parametrized(norm_src.source)
            or is_generic(norm_dst.source)
            or is_parametrized(norm_dst.source)
        ):
            raise CannotProvide
        if is_subclass_soft(norm_src.origin, norm_dst.origin):
            return as_is_stub_with_ctx
        raise CannotProvide


class MatchingCoercerProvider(CoercerProvider):
    def __init__(self, src_lsc: LocStackChecker, dst_lsc: LocStackChecker, coercer: OneArgCoercer):
        self._src_lsc = src_lsc
        self._dst_lsc = dst_lsc
        self._one_arg_coercer = coercer

    def _provide_coercer(self, mediator: Mediator, request: CoercerRequest) -> Coercer:
        if (
            self._src_lsc.check_loc_stack(mediator, request.src)
            and self._dst_lsc.check_loc_stack(mediator, request.dst)
        ):
            if self._one_arg_coercer == as_is_stub:
                return as_is_stub_with_ctx
            one_arg_coercer = self._one_arg_coercer
            return lambda x, ctx: one_arg_coercer(x)
        raise CannotProvide


class UnionSubcaseCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        if norm_dst.origin != Union:
            raise CannotProvide

        if norm_src.origin == Union:
            src_args_set = set(map(strip_tags, norm_src.args))
            dst_args_set = set(map(strip_tags, norm_dst.args))
            if src_args_set.issubset(dst_args_set):
                return as_is_stub_with_ctx
        elif norm_src.origin in [strip_tags(arg).origin for arg in norm_dst.args]:
            return as_is_stub_with_ctx
        raise CannotProvide


class OptionalCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        if not (self._is_optional(norm_dst) and self._is_optional(norm_src)):
            raise CannotProvide

        not_none_src = self._get_not_none(norm_src)
        not_none_dst = self._get_not_none(norm_dst)
        not_none_coercer = mediator.mandatory_provide(
            request.append_loc(
                src_loc=GenericParamLoc(type=not_none_src.source, generic_pos=0),
                dst_loc=GenericParamLoc(type=not_none_dst.source, generic_pos=0),
            ),
            lambda x: "Cannot create coercer for optionals. Coercer for wrapped value cannot be created",
        )
        if not_none_coercer == as_is_stub_with_ctx:
            return as_is_stub_with_ctx

        def optional_coercer(data, ctx):
            if data is None:
                return None
            return not_none_coercer(data, ctx)

        return optional_coercer

    def _is_optional(self, norm: BaseNormType) -> bool:
        return norm.origin == Union and None in [case.origin for case in norm.args]

    def _get_not_none(self, norm: BaseNormType) -> BaseNormType:
        return next(case for case in norm.args if case.origin is not None)


class TypeHintTagsUnwrappingProvider(CoercerProvider):
    def _unwrap_type(self, tp: TypeHint) -> TypeHint:
        try:
            norm = normalize_type(tp)
        except ValueError:
            return tp
        return strip_tags(norm).source

    def _provide_coercer(self, mediator: Mediator, request: CoercerRequest) -> Coercer:
        src_tp = request.src.last.type
        dst_tp = request.dst.last.type
        unwrapped_src_tp = self._unwrap_type(src_tp)
        unwrapped_dst_tp = self._unwrap_type(dst_tp)
        if unwrapped_src_tp == src_tp and unwrapped_dst_tp == dst_tp:
            raise CannotProvide
        return mediator.delegating_provide(
            replace(
                request,
                src=request.src.replace_last_type(unwrapped_src_tp),
                dst=request.dst.replace_last_type(unwrapped_dst_tp),
            ),
        )


class IterableCoercerProvider(NormTypeCoercerProvider):
    CONCRETE_ORIGINS = {set, list, tuple, deque}
    ABC_TO_IMPL = {
        collections.abc.Iterable: tuple,
        collections.abc.Reversible: tuple,
        collections.abc.Collection: tuple,
        collections.abc.Sequence: tuple,
        collections.abc.MutableSequence: list,
        # exclude ByteString, because it does not process as Iterable
        collections.abc.Set: frozenset,
        collections.abc.MutableSet: set,
    }

    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        src_arg_tp = self._parse_source(norm_src)
        dst_factory, dst_arg_tp = self._parse_destination(norm_dst)
        element_coercer = mediator.mandatory_provide(
            request.append_loc(
                src_loc=GenericParamLoc(type=src_arg_tp, generic_pos=0),
                dst_loc=GenericParamLoc(type=dst_arg_tp, generic_pos=0),
            ),
            lambda x: "Cannot create coercer for iterables. Coercer for element cannot be created",
        )

        def iterable_coercer(data, ctx):
            return dst_factory(element_coercer(element, ctx) for element in data)

        return iterable_coercer

    def _parse_source(self, norm: BaseNormType) -> TypeHint:
        if norm.origin is tuple and norm.args[-1] != Ellipsis:
            raise CannotProvide("Constant-length tuple is not supported yet", is_demonstrative=True)
        if norm.origin in self.CONCRETE_ORIGINS or norm.origin in self.ABC_TO_IMPL:
            return norm.args[0].source
        raise CannotProvide

    def _parse_destination(self, norm: BaseNormType) -> tuple[Callable, TypeHint]:
        if norm.origin is tuple and norm.args[-1] != Ellipsis:
            raise CannotProvide("Constant-length tuple is not supported yet", is_demonstrative=True)
        if norm.origin in self.CONCRETE_ORIGINS:
            return norm.origin, norm.args[0].source
        if norm.origin in self.ABC_TO_IMPL:
            return self.ABC_TO_IMPL[norm.origin], norm.args[0].source
        raise CannotProvide


class DictCoercerProvider(NormTypeCoercerProvider):
    def _provide_coercer_norm_types(
        self,
        mediator: Mediator,
        request: CoercerRequest,
        norm_src: BaseNormType,
        norm_dst: BaseNormType,
    ) -> Coercer:
        src_key_tp, src_value_tp = self._parse_source(norm_src)
        dst_key_tp, dst_value_tp = self._parse_destination(norm_dst)
        key_coercer = mediator.mandatory_provide(
            request.append_loc(
                src_loc=GenericParamLoc(type=src_key_tp, generic_pos=0),
                dst_loc=GenericParamLoc(type=dst_key_tp, generic_pos=0),
            ),
            lambda x: "Cannot create coercer for dicts. Coercer for key cannot be created",
        )
        value_coercer = mediator.mandatory_provide(
            request.append_loc(
                src_loc=GenericParamLoc(type=src_value_tp, generic_pos=1),
                dst_loc=GenericParamLoc(type=dst_value_tp, generic_pos=1),
            ),
            lambda x: "Cannot create coercer for dicts. Coercer for value cannot be created",
        )

        def dict_coercer(data, ctx):
            return {key_coercer(key, ctx): value_coercer(value, ctx) for key, value in data.items()}

        return dict_coercer

    def _parse_source(self, norm: BaseNormType) -> tuple[TypeHint, TypeHint]:
        if norm.origin in (dict, collections.abc.Mapping, collections.abc.MutableMapping):
            return norm.args[0].source, norm.args[1].source
        raise CannotProvide

    def _parse_destination(self, norm: BaseNormType) -> tuple[TypeHint, TypeHint]:
        if norm.origin in (dict, collections.abc.Mapping, collections.abc.MutableMapping):
            return norm.args[0].source, norm.args[1].source
        raise CannotProvide
