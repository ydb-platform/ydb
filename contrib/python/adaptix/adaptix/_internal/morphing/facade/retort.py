import collections.abc
from abc import ABC
from collections.abc import ByteString, Iterable, Mapping, MutableMapping  # noqa: PYI057
from datetime import date, datetime, time
from ipaddress import IPv4Address, IPv4Interface, IPv4Network, IPv6Address, IPv6Interface, IPv6Network
from itertools import chain
from pathlib import Path, PosixPath, PurePath, PurePosixPath, PureWindowsPath, WindowsPath
from typing import Any, Optional, TypeVar, overload
from uuid import UUID

from ...common import Dumper, Loader, TypeHint, VarTuple
from ...definitions import DebugTrail
from ...provider.essential import Provider, Request
from ...provider.loc_stack_filtering import LocStack, P, VarTupleLSC
from ...provider.location import TypeHintLoc
from ...provider.shape_provider import BUILTIN_SHAPE_PROVIDER
from ...provider.value_provider import ValueProvider
from ...retort.error_renderer import ErrorRenderer
from ...retort.operating_retort import OperatingRetort
from ...retort.searching_retort import default_error_renderer
from ...struct_trail import render_trail_as_note
from ...type_tools.basic_utils import is_generic_class
from ...utils import Omittable, Omitted
from ..concrete_provider import (
    BOOL_PROVIDER,
    COMPLEX_PROVIDER,
    DECIMAL_PROVIDER,
    FLOAT_PROVIDER,
    FRACTION_PROVIDER,
    INT_PROVIDER,
    STR_PROVIDER,
    ZONE_INFO_PROVIDER,
    BytearrayBase64Provider,
    BytesBase64Provider,
    BytesIOBase64Provider,
    IOBytesBase64Provider,
    IsoFormatProvider,
    LiteralStringProvider,
    NoneProvider,
    RegexPatternProvider,
    SecondsTimedeltaProvider,
    SelfTypeProvider,
)
from ..constant_length_tuple_provider import ConstantLengthTupleProvider
from ..dict_provider import DefaultDictProvider, DictProvider
from ..generic_provider import (
    ForwardRefEvaluatingProvider,
    LiteralProvider,
    NewTypeUnwrappingProvider,
    PathLikeProvider,
    TypeAliasUnwrappingProvider,
    TypeHintTagsUnwrappingProvider,
)
from ..iterable_provider import IterableProvider
from ..json_schema.definitions import JSONSchema, ResolvedJSONSchema
from ..json_schema.providers import InlineJSONSchemaProvider, JSONSchemaRefProvider
from ..json_schema.request_cls import JSONSchemaContext, JSONSchemaRequest
from ..model.crown_definitions import ExtraSkip
from ..model.dumper_provider import ModelDumperProvider
from ..model.loader_provider import ModelLoaderProvider
from ..model.request_filtering import AnyModelLSC
from ..name_layout.component import BuiltinExtraMoveAndPoliciesMaker, BuiltinSievesMaker, BuiltinStructureMaker
from ..name_layout.name_mapping import SkipPrivateFieldsNameMappingProvider
from ..name_layout.provider import BuiltinNameLayoutProvider
from ..provider_template import ABCProxy, ToVarTupleProxy
from ..request_cls import DebugTrailRequest, DumperRequest, LoaderRequest, StrictCoercionRequest
from ..union_provider import UnionProvider
from .provider import (
    as_is_dumper,
    as_is_loader,
    as_sentinel,
    bound,
    dumper,
    enum_by_exact_value,
    flag_by_exact_value,
    loader,
    name_mapping,
)


class FilledRetort(OperatingRetort, ABC):
    """A retort contains builtin providers"""

    recipe = [
        NoneProvider(),

        as_is_loader(Any),
        as_is_dumper(Any),
        as_is_loader(object),
        as_is_dumper(object),
        as_sentinel(Omitted),

        IsoFormatProvider(datetime),
        IsoFormatProvider(date),
        IsoFormatProvider(time),
        SecondsTimedeltaProvider(),

        flag_by_exact_value(),
        enum_by_exact_value(),  # it has higher priority than scalar types for Enum with mixins

        INT_PROVIDER,
        FLOAT_PROVIDER,
        STR_PROVIDER,
        BOOL_PROVIDER,
        DECIMAL_PROVIDER,
        FRACTION_PROVIDER,
        COMPLEX_PROVIDER,
        ZONE_INFO_PROVIDER,

        BytesBase64Provider(),
        BytesIOBase64Provider(),
        IOBytesBase64Provider(),
        BytearrayBase64Provider(),

        *chain.from_iterable(
            (
                loader(tp, tp),
                dumper(tp, tp.__str__),  # type: ignore[arg-type]
            )
            for tp in [
                UUID,
                IPv4Address, IPv6Address,
                IPv4Network, IPv6Network,
                IPv4Interface, IPv6Interface,
            ]
        ),
        *chain.from_iterable(
            (
                loader(tp, tp),
                dumper(tp, tp.__fspath__),  # type: ignore[attr-defined]
            )
            for tp in [
                PurePath, Path,
                PurePosixPath, PosixPath,
                PureWindowsPath, WindowsPath,
            ]
        ),
        PathLikeProvider(),

        bound(list, IterableProvider(dump_as=list)),
        bound(VarTupleLSC(), IterableProvider(dump_as=tuple)),
        bound(set, IterableProvider(dump_as=list, json_schema_unique_items=True)),
        bound(frozenset, IterableProvider(dump_as=tuple, json_schema_unique_items=True)),
        bound(collections.deque, IterableProvider(dump_as=list)),
        ConstantLengthTupleProvider(),

        LiteralProvider(),
        UnionProvider(),
        DictProvider(),
        DefaultDictProvider(),
        RegexPatternProvider(),
        SelfTypeProvider(),
        LiteralStringProvider(),

        ABCProxy(Mapping, dict),
        ABCProxy(MutableMapping, dict),
        ABCProxy(ByteString, bytes),

        ToVarTupleProxy(collections.abc.Iterable),
        ToVarTupleProxy(collections.abc.Reversible),
        ToVarTupleProxy(collections.abc.Collection),
        ToVarTupleProxy(collections.abc.Sequence),
        ABCProxy(collections.abc.MutableSequence, list),
        ABCProxy(collections.abc.Set, frozenset),
        ABCProxy(collections.abc.MutableSet, set),

        name_mapping(
            JSONSchema,
            omit_default=True,
        ),
        name_mapping(
            ResolvedJSONSchema,
            omit_default=True,
        ),
        name_mapping(
            chain=None,
            skip=(),
            only=P.ANY,
            map=[
                SkipPrivateFieldsNameMappingProvider(),
            ],
            trim_trailing_underscore=True,
            name_style=None,
            as_list=False,
            omit_default=False,
            extra_in=ExtraSkip(),
            extra_out=ExtraSkip(),
        ),
        BuiltinNameLayoutProvider(
            structure_maker=BuiltinStructureMaker(),
            sieves_maker=BuiltinSievesMaker(),
            extra_move_maker=BuiltinExtraMoveAndPoliciesMaker(),
            extra_policies_maker=BuiltinExtraMoveAndPoliciesMaker(),
        ),
        ModelLoaderProvider(),
        ModelDumperProvider(),

        bound(AnyModelLSC(), InlineJSONSchemaProvider(inline=False)),
        InlineJSONSchemaProvider(inline=True),
        JSONSchemaRefProvider(),

        BUILTIN_SHAPE_PROVIDER,

        NewTypeUnwrappingProvider(),
        TypeHintTagsUnwrappingProvider(),
        TypeAliasUnwrappingProvider(),
        ForwardRefEvaluatingProvider(),
    ]


T = TypeVar("T")
RequestT = TypeVar("RequestT", bound=Request)
AR = TypeVar("AR", bound="AdornedRetort")


class AdornedRetort(OperatingRetort):
    """A retort implementing high-level user interface"""

    def __init__(
        self,
        *,
        recipe: Iterable[Provider] = (),
        strict_coercion: bool = True,
        debug_trail: DebugTrail = DebugTrail.ALL,
        error_renderer: Optional[ErrorRenderer] = default_error_renderer,
    ):
        self._strict_coercion = strict_coercion
        self._debug_trail = debug_trail
        super().__init__(recipe=recipe, error_renderer=error_renderer)

    def _calculate_derived(self):
        super()._calculate_derived()
        self._loader_cache = {}
        self._dumper_cache = {}

    def replace(
        self: AR,
        *,
        strict_coercion: Omittable[bool] = Omitted(),
        debug_trail: Omittable[DebugTrail] = Omitted(),
        error_renderer: Omittable[Optional[ErrorRenderer]] = Omitted(),
    ) -> AR:
        with self._clone() as clone:
            if not isinstance(strict_coercion, Omitted):
                clone._strict_coercion = strict_coercion
            if not isinstance(debug_trail, Omitted):
                clone._debug_trail = debug_trail
            if not isinstance(error_renderer, Omitted):
                clone._error_renderer = error_renderer
        return clone

    def extend(self: AR, *, recipe: Iterable[Provider]) -> AR:
        with self._clone() as clone:
            clone._instance_recipe = (
                tuple(recipe) + clone._instance_recipe
            )

        return clone

    def _get_recipe_tail(self) -> VarTuple[Provider]:
        return (
            ValueProvider(StrictCoercionRequest, self._strict_coercion),
            ValueProvider(DebugTrailRequest, self._debug_trail),
        )

    def get_loader(self, tp: type[T]) -> Loader[T]:
        try:
            return self._loader_cache[tp]
        except KeyError:
            pass
        loader_ = self._make_loader(tp)
        self._loader_cache[tp] = loader_
        return loader_

    def _make_loader(self, tp: type[T]) -> Loader[T]:
        loader_ = self._facade_provide(
            LoaderRequest(loc_stack=LocStack(TypeHintLoc(type=tp))),
            error_message=f"Cannot produce loader for type {tp!r}",
        )
        if self._debug_trail == DebugTrail.FIRST:
            def trail_rendering_wrapper(data):
                try:
                    return loader_(data)
                except Exception as e:
                    render_trail_as_note(e)
                    raise

            return trail_rendering_wrapper

        return loader_

    def get_dumper(self, tp: type[T]) -> Dumper[T]:
        try:
            return self._dumper_cache[tp]
        except KeyError:
            pass
        dumper_ = self._make_dumper(tp)
        self._dumper_cache[tp] = dumper_
        return dumper_

    def _make_dumper(self, tp: type[T]) -> Dumper[T]:
        dumper_ = self._facade_provide(
            DumperRequest(loc_stack=LocStack(TypeHintLoc(type=tp))),
            error_message=f"Cannot produce dumper for type {tp!r}",
        )
        if self._debug_trail == DebugTrail.FIRST:
            def trail_rendering_wrapper(data):
                try:
                    return dumper_(data)
                except Exception as e:
                    render_trail_as_note(e)
                    raise

            return trail_rendering_wrapper

        return dumper_

    @overload
    def load(self, data: Any, tp: type[T], /) -> T:
        ...

    @overload
    def load(self, data: Any, tp: TypeHint, /) -> Any:
        ...

    def load(self, data: Any, tp: TypeHint, /):
        return self.get_loader(tp)(data)

    @overload
    def dump(self, data: T, tp: type[T], /) -> Any:
        ...

    @overload
    def dump(self, data: Any, tp: Optional[TypeHint] = None, /) -> Any:
        ...

    def dump(self, data: Any, tp: Optional[TypeHint] = None, /) -> Any:
        if tp is None:
            tp = type(data)
            if is_generic_class(tp):
                raise ValueError(
                    f"Cannot infer the actual type of generic class instance ({tp!r}),"
                    " you have to explicitly pass the type of object",
                )
        return self.get_dumper(tp)(data)

    def make_json_schema(self, tp: TypeHint, ctx: JSONSchemaContext) -> JSONSchema:
        return self._facade_provide(
            JSONSchemaRequest(loc_stack=LocStack(TypeHintLoc(type=tp)), ctx=ctx),
            error_message=f"Cannot produce JSONSchema for type {tp!r}",
        )


class Retort(FilledRetort, AdornedRetort):
    pass
