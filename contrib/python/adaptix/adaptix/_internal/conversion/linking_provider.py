import itertools
from collections.abc import Iterable, Mapping
from typing import Callable, Optional, TypeVar, Union

from ..common import Coercer, OneArgCoercer, VarTuple
from ..model_tools.definitions import DefaultFactory, DefaultValue, InputField, InputShape, Param, ParamKind
from ..model_tools.introspection.callable import get_callable_shape
from ..provider.essential import CannotProvide, Mediator, mandatory_apply_by_iterable
from ..provider.loc_stack_filtering import LocStackChecker
from ..provider.loc_stack_tools import format_loc_stack, get_callable_name
from ..provider.location import FieldLoc
from ..utils import add_note
from .provider_template import LinkingProvider
from .request_cls import (
    ConstantLinking,
    FieldLinking,
    FunctionLinking,
    LinkingRequest,
    LinkingResult,
    LinkingSource,
    ModelLinking,
)


class DefaultLinkingProvider(LinkingProvider):
    def _provide_linking(self, mediator: Mediator, request: LinkingRequest) -> LinkingResult:
        target_field_id = request.destination.last.cast(FieldLoc).field_id

        for source in self._iterate_sources(request):
            if source.last.cast(FieldLoc).field_id == target_field_id:
                return LinkingResult(
                    linking=FieldLinking(source=source, coercer=None),
                )
        raise CannotProvide

    def _iterate_sources(self, request: LinkingRequest) -> Iterable[LinkingSource]:
        if len(request.destination) == 2:  # noqa: PLR2004
            yield from reversed(request.context.loc_stacks)
        yield from request.sources


class MatchingLinkingProvider(LinkingProvider):
    def __init__(self, src_lsc: LocStackChecker, dst_lsc: LocStackChecker, coercer: Optional[OneArgCoercer]):
        self._src_lsc = src_lsc
        self._dst_lsc = dst_lsc
        self._one_arg_coercer = coercer

    def _get_coercer(self) -> Optional[Coercer]:
        one_arg_coercer = self._one_arg_coercer
        if one_arg_coercer is None:
            return None
        return lambda x, ctx: one_arg_coercer(x)

    def _provide_linking(self, mediator: Mediator, request: LinkingRequest) -> LinkingResult:
        if not self._dst_lsc.check_loc_stack(mediator, request.destination):
            raise CannotProvide

        for source in itertools.chain(request.sources, reversed(request.context.loc_stacks)):
            if self._src_lsc.check_loc_stack(mediator, source):
                return LinkingResult(linking=FieldLinking(source=source, coercer=self._get_coercer()))
        raise CannotProvide


class ConstantLinkingProvider(LinkingProvider):
    def __init__(self, dst_lsc: LocStackChecker, default: Union[DefaultValue, DefaultFactory]):
        self._dst_lsc = dst_lsc
        self._default = default

    def _provide_linking(self, mediator: Mediator, request: LinkingRequest) -> LinkingResult:
        if self._dst_lsc.check_loc_stack(mediator, request.destination):
            return LinkingResult(linking=ConstantLinking(self._default))
        raise CannotProvide


T = TypeVar("T")


class FunctionLinkingProvider(LinkingProvider):
    def __init__(self, func: Callable, dst_lsc: LocStackChecker):
        self._func = func
        self._dst_lsc = dst_lsc
        self._input_shape = self._get_input_shape()

    def _get_input_shape(self) -> InputShape:
        return get_callable_shape(self._func).input

    def _get_linking(
        self,
        mediator: Mediator,
        request: LinkingRequest,
        name_to_field_source: Mapping[str, LinkingSource],
        name_to_context_source: Mapping[str, LinkingSource],
        input_field: InputField,
        param: Param,
        idx: int,
    ) -> LinkingResult:
        if param.kind == ParamKind.KW_ONLY:
            try:
                source = name_to_field_source[param.name]
            except KeyError:
                raise CannotProvide(
                    f"Cannot match function parameter ‹{input_field.id}› with any model field",
                    is_terminal=True,
                    is_demonstrative=True,
                )
        else:
            if idx == 0:
                return LinkingResult(linking=ModelLinking())

            try:
                source = name_to_context_source[param.name]
            except KeyError:
                raise CannotProvide(
                    f"Cannot match function parameter ‹{input_field.id}› with any converter parameter",
                    is_terminal=True,
                    is_demonstrative=True,
                )

        return LinkingResult(linking=FieldLinking(source=source, coercer=None))

    def _create_param_specs(
        self,
        mediator: Mediator,
        request: LinkingRequest,
        error_describer: Callable[[], str],
    ) -> VarTuple[FunctionLinking.ParamSpec]:
        name_to_field_source = {
            source.last.cast_or_raise(FieldLoc, CannotProvide).field_id: source
            for source in request.sources
        }
        name_to_context_source = {
            loc_stack.last.field_id: loc_stack
            for loc_stack in request.context.loc_stacks
        }

        param_specs = mandatory_apply_by_iterable(
            lambda field, param, idx: FunctionLinking.ParamSpec(
                field=field,
                linking=self._get_linking(
                    mediator=mediator,
                    request=request,
                    name_to_field_source=name_to_field_source,
                    name_to_context_source=name_to_context_source,
                    input_field=field,
                    param=param,
                    idx=idx,
                ),
                param_kind=param.kind,
            ),
            zip(self._input_shape.fields, self._input_shape.params, itertools.count()),
            error_describer,
        )
        return tuple(param_specs)

    def _provide_linking(self, mediator: Mediator, request: LinkingRequest) -> LinkingResult:
        if not self._dst_lsc.check_loc_stack(mediator, request.destination):
            raise CannotProvide

        try:
            param_specs = self._create_param_specs(
                mediator,
                request,
                lambda: (
                    f"Cannot create linking for function ‹{get_callable_name(self._func)}›."
                    f" Linkings for some parameters are not found"
                ),
            )
        except CannotProvide as e:
            if len(request.sources) > 0:
                src_desc = format_loc_stack(request.sources[0].reversed_slice(1))
                dst_desc = format_loc_stack(request.destination)
                add_note(e, f"Linking: {src_desc} ──▷ {dst_desc}")
            raise

        return LinkingResult(
            linking=FunctionLinking(func=self._func, param_specs=param_specs),
        )
