from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import timezone
from enum import Enum, EnumMeta
from types import MappingProxyType
from typing import Any, Callable, Optional, TypeVar, Union

from ...common import Catchable, Dumper, Loader, TypeHint, VarTuple
from ...model_tools.definitions import Default, DescriptorAccessor, NoDefault, OutputField
from ...model_tools.introspection.callable import get_callable_shape
from ...name_style import NameStyle
from ...provider.essential import Provider
from ...provider.facade.provider import bound, bound_by_any
from ...provider.loc_stack_filtering import (
    AnyLocStackChecker,
    LocStackChecker,
    LocStackPattern,
    OrLocStackChecker,
    P,
    Pred,
    create_loc_stack_checker,
)
from ...provider.overlay_schema import OverlayProvider
from ...provider.provider_wrapper import Chain, ChainingProvider
from ...provider.shape_provider import PropertyExtender
from ...provider.value_provider import ValueProvider
from ...special_cases_optimization import as_is_stub
from ...utils import Omittable, Omitted
from ..concrete_provider import DatetimeFormatProvider, DateTimestampProvider, DatetimeTimestampProvider
from ..dict_provider import DefaultDictProvider
from ..enum_provider import (
    ByNameEnumMappingGenerator,
    EnumExactValueProvider,
    EnumNameProvider,
    EnumValueProvider,
    FlagByExactValueProvider,
    FlagByListProvider,
)
from ..load_error import LoadError, ValidationLoadError
from ..model.loader_provider import InlinedShapeModelLoaderProvider
from ..name_layout.base import ExtraIn, ExtraOut
from ..name_layout.component import ExtraMoveAndPoliciesOverlay, SievesOverlay, StructureOverlay
from ..name_layout.name_mapping import (
    ConstNameMappingProvider,
    DictNameMappingProvider,
    FuncNameMappingProvider,
    NameMap,
)
from ..request_cls import DumperRequest, LoaderRequest
from ..sentinel_provider import SentinelProvider

T = TypeVar("T")


def make_chain(chain: Optional[Chain], provider: Provider) -> Provider:
    if chain is None:
        return provider

    return ChainingProvider(chain, provider)


def loader(pred: Pred, func: Loader, chain: Optional[Chain] = None) -> Provider:
    """Basic provider to define custom loader.

    :param pred: Predicate specifying where loader should be used. See :ref:`predicate-system` for details.
    :param func: Function that acts as loader.
        It must take one positional argument of raw data and return the processed value.
    :param chain: Controls how the function will interact with the previous loader.

        When ``None`` is passed, the specified function will fully replace the previous loader.

        If a parameter is ``Chain.FIRST``,
        the specified function will take raw data and its result will be passed to previous loader.

        If the parameter is ``Chain.LAST``, the specified function gets result of the previous loader.

    :return: Desired provider
    """
    return bound(
        pred,
        make_chain(
            chain,
            ValueProvider(LoaderRequest, func),
        ),
    )


def dumper(pred: Pred, func: Dumper, chain: Optional[Chain] = None) -> Provider:
    """Basic provider to define custom dumper.

    :param pred: Predicate specifying where dumper should be used. See :ref:`predicate-system` for details.
    :param func: Function that acts as dumper.
        It must take one positional argument of raw data and return the processed value.
    :param chain: Controls how the function will interact with the previous dumper.

        When ``None`` is passed, the specified function will fully replace the previous dumper.

        If a parameter is ``Chain.FIRST``,
        the specified function will take raw data and its result will be passed to previous dumper.

        If the parameter is ``Chain.LAST``, the specified function gets result of the previous dumper.

    :return: Desired provider
    """
    return bound(
        pred,
        make_chain(
            chain,
            ValueProvider(DumperRequest, func),
        ),
    )


def as_is_loader(pred: Pred) -> Provider:
    """Provider that creates loader which does nothing with input data.

    :param pred: Predicate specifying where loader should be used. See :ref:`predicate-system` for details.
    :return: Desired provider
    """
    return loader(pred, as_is_stub)


def as_is_dumper(pred: Pred) -> Provider:
    """Provider that creates dumper which does nothing with input data.

    :param pred: Predicate specifying where dumper should be used. See :ref:`predicate-system` for details.
    :return: Desired provider
    """
    return dumper(pred, as_is_stub)


def constructor(pred: Pred, func: Callable) -> Provider:
    input_shape = get_callable_shape(func).input
    return bound(
        pred,
        InlinedShapeModelLoaderProvider(shape=input_shape),
    )


def _name_mapping_convert_map(name_map: Omittable[NameMap]) -> VarTuple[Provider]:
    if isinstance(name_map, Omitted):
        return ()
    if isinstance(name_map, Mapping):
        return (
            DictNameMappingProvider(name_map),
        )
    result: list[Provider] = []
    for element in name_map:
        if isinstance(element, Provider):
            result.append(element)
        elif isinstance(element, Mapping):
            result.append(
                DictNameMappingProvider(element),
            )
        else:
            pred, value = element
            result.append(
                bound(pred, FuncNameMappingProvider(value))
                if callable(value) else
                bound(pred, ConstNameMappingProvider(value)),
            )
    return tuple(result)


def _name_mapping_convert_preds(value: Omittable[Union[Iterable[Pred], Pred]]) -> Omittable[LocStackChecker]:
    if isinstance(value, Omitted):
        return value
    if isinstance(value, Iterable) and not isinstance(value, str):
        return OrLocStackChecker([create_loc_stack_checker(el) for el in value])
    return create_loc_stack_checker(value)


def _name_mapping_convert_omit_default(
    value: Omittable[Union[Iterable[Pred], Pred, bool]],
) -> Omittable[LocStackChecker]:
    if isinstance(value, bool):
        return AnyLocStackChecker() if value else ~AnyLocStackChecker()
    return _name_mapping_convert_preds(value)


def _name_mapping_extra(value: Union[str, Iterable[str], T]) -> Union[str, Iterable[str], T]:
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return tuple(value)
    return value


def name_mapping(
    pred: Omittable[Pred] = Omitted(),
    *,
    # filtering which fields are presented
    skip: Omittable[Union[Iterable[Pred], Pred]] = Omitted(),
    only: Omittable[Union[Iterable[Pred], Pred]] = Omitted(),
    # mutating names of presented fields
    map: Omittable[NameMap] = Omitted(),  # noqa: A002
    as_list: Omittable[bool] = Omitted(),
    trim_trailing_underscore: Omittable[bool] = Omitted(),
    name_style: Omittable[Optional[NameStyle]] = Omitted(),
    # filtering of dumped data
    omit_default: Omittable[Union[Iterable[Pred], Pred, bool]] = Omitted(),
    # policy for data that does not map to fields
    extra_in: Omittable[ExtraIn] = Omitted(),
    extra_out: Omittable[ExtraOut] = Omitted(),
    # chaining with next matching provider
    chain: Optional[Chain] = Chain.FIRST,
) -> Provider:
    """A name mapping decides which fields will be presented
    to the outside world and how they will look.

    The mapping process consists of two stages:
    1. Determining which fields are presented
    2. Mutating names of presented fields

    `skip` parameter has higher priority than `only`.

    Mutating parameters works in that way:
    Mapper tries to use the value from the map.
    If the field is not presented in the map,
    trim trailing underscore and convert name style.

    The field must follow snake_case to could be converted.

    :param only:
    :param pred:
    :param skip:
    :param map:
    :param as_list:
    :param trim_trailing_underscore:
    :param name_style:
    :param omit_default:
    :param extra_in:
    :param extra_out:
    :param chain:
    """
    return bound(
        pred,
        OverlayProvider(
            overlays=[
                StructureOverlay(
                    skip=_name_mapping_convert_preds(skip),
                    only=_name_mapping_convert_preds(only),
                    map=_name_mapping_convert_map(map),
                    trim_trailing_underscore=trim_trailing_underscore,
                    name_style=name_style,
                    as_list=as_list,
                ),
                SievesOverlay(
                    omit_default=_name_mapping_convert_omit_default(omit_default),
                ),
                ExtraMoveAndPoliciesOverlay(
                    extra_in=_name_mapping_extra(extra_in),
                    extra_out=_name_mapping_extra(extra_out),
                ),
            ],
            chain=chain,
        ),
    )


NameOrProp = Union[str, property]


def with_property(
    pred: Pred,
    prop: NameOrProp,
    tp: Omittable[TypeHint] = Omitted(),
    /, *,
    default: Default = NoDefault(),
    access_error: Optional[Catchable] = None,
    metadata: Mapping[Any, Any] = MappingProxyType({}),
) -> Provider:
    """Provider registering property for a model for dumping.
    This property will be treated as a usual field, so it can be mapped or skipped.

    :param pred: Predicate specifying class to add property. See :ref:`predicate-system` for details.
    :param prop: A property to register. It can be a string representing attribute name or property object.
        The property object will be converted to attribute name via ``prop.fget.__name__``
    :param tp: Output type of property. By default, this type is inferred from a property object of concrete class.
        If getter function has no specified return type, it will be supposed as ``Any``.
    :param default: A default value for this field.
    :param access_error: Omit field when such error is raised. This will mark the field as optional.
    :param metadata: Additional metadata of the field.
    """
    attr_name = _ensure_attr_name(prop)

    field = OutputField(
        id=attr_name,
        type=tp,
        accessor=DescriptorAccessor(attr_name, access_error),
        default=default,
        metadata=metadata,
        original=None,
    )

    return bound(
        pred,
        PropertyExtender(
            output_fields=[field],
            infer_types_for=[field.id] if tp == Omitted() else [],
        ),
    )


def _ensure_attr_name(prop: NameOrProp) -> str:
    if isinstance(prop, str):
        return prop

    fget = prop.fget
    if fget is None:
        raise ValueError(f"Property {prop} has no fget")

    return fget.__name__


EnumPred = Union[TypeHint, str, EnumMeta, LocStackPattern]


def enum_by_name(
    *preds: EnumPred,
    name_style: Optional[NameStyle] = None,
    map: Optional[Mapping[Union[str, Enum], str]] = None,  # noqa: A002
) -> Provider:
    """Provider that represents enum members to the outside world by their name.

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Enums.
        See :ref:`predicate-system` for details.
    :param name_style: Name style for representing members to the outside world.
        If it is set, the provider will automatically convert the names of enum members to the specified convention.
    :param map: Mapping for representing members to the outside world.
        If it is set, the provider will use it to rename members individually;
        its keys can either be member names as strings or member instances.
    :return: Desired provider
    """
    return bound_by_any(
        preds,
        EnumNameProvider(
            ByNameEnumMappingGenerator(name_style=name_style, map=map),
        ),
    )


def enum_by_exact_value(*preds: EnumPred) -> Provider:
    """Provider that represents enum members to the outside world by their value without any processing.

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Enums.
        See :ref:`predicate-system` for details.
    :return: Desired provider
    """
    return bound_by_any(preds, EnumExactValueProvider())


def enum_by_value(first_pred: EnumPred, /, *preds: EnumPred, tp: TypeHint) -> Provider:
    """Provider that represents enum members to the outside world by their value by loader and dumper of specified type.
    The loader will call the loader of the :paramref:`tp` and pass it to the enum constructor.
    The dumper will get value from eum member and pass it to the dumper of the :paramref:`tp`.

    :param first_pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    :param preds: Additional predicates. The provider will be applied if any predicates meet the conditions.
    :param tp: Type of enum members.
        This type must cover all enum members for the correct operation of loader and dumper
    :return: Desired provider
    """
    return bound_by_any([first_pred, *preds], EnumValueProvider(tp))


def flag_by_exact_value(*preds: EnumPred) -> Provider:
    """Provider that represents flag members to the outside world by their value without any processing.
    It does not support flags with skipped bits and negative values (it is recommended to use ``enum.auto()``
    to define flag values instead of manually specifying them).

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Flags.
        See :ref:`predicate-system` for details.
    :return: Desired provider
    """
    return bound_by_any(preds, FlagByExactValueProvider())


def flag_by_member_names(
    *preds: EnumPred,
    allow_single_value: bool = False,
    allow_duplicates: bool = True,
    allow_compound: bool = True,
    name_style: Optional[NameStyle] = None,
    map: Optional[Mapping[Union[str, Enum], str]] = None,  # noqa: A002
) -> Provider:
    """Provider that represents flag members to the outside world by list of their names.

    Loader takes a flag members name list and returns united flag member
    (given members combined by operator ``|``, namely `bitwise or`).

    Dumper takes a flag member and returns a list of names of flag members, included in the given flag member.

    :param preds: Predicates specifying where the provider should be used.
        The provider will be applied if any predicates meet the conditions,
        if no predicates are passed, the provider will be used for all Flags.
        See :ref:`predicate-system` for details.
    :param allow_single_value: Allows calling the loader with a single value.
        If this is allowed, singlular values are treated as one element list.
    :param allow_duplicates: Allows calling the loader with a list containing non-unique elements.
        Unless this is allowed, loader will raise :exc:`.DuplicatedValuesLoadError` in that case.
    :param allow_compound: Allows the loader to accept names of compound members
        (e.g. ``WHITE = RED | GREEN | BLUE``) and the dumper to return names of compound members.
        If this is allowed, dumper will use compound members names to serialize value.
    :param name_style: Name style for representing members to the outside world.
        If it is set, the provider will automatically convert the names of all flag members to the specified convention.
    :param map: Mapping for representing members to the outside world.
        If it is set, the provider will use it to rename members individually;
        its keys can either be member names as strings or member instances.
    :return: Desired provider
    """
    return bound_by_any(
        preds,
        FlagByListProvider(
            ByNameEnumMappingGenerator(name_style=name_style, map=map),
            allow_single_value=allow_single_value,
            allow_duplicates=allow_duplicates,
            allow_compound=allow_compound,
        ),
    )


def validator(
    pred: Pred,
    func: Callable[[Any], bool],
    error: Union[str, Callable[[Any], LoadError], None] = None,
    chain: Chain = Chain.LAST,
) -> Provider:
    exception_factory = (
        (lambda x: ValidationLoadError(error, x))
        if error is None or isinstance(error, str) else
        error
    )

    def validating_loader(data):
        if func(data):
            return data
        raise exception_factory(data)

    return loader(pred, validating_loader, chain)


def default_dict(pred: Pred, default_factory: Callable) -> Provider:
    """Provider for ``defaultdict`` class

    :param pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    :param default_factory: default_factory parameter of the ``defaultdict`` instance to be created by the loader
    """
    return bound(pred, DefaultDictProvider(default_factory))


def datetime_by_timestamp(pred: Pred = P.ANY, *, tz: Optional[timezone] = timezone.utc) -> Provider:
    """Provider that can load/dump datetime object from/to UNIX timestamp.

    :param pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    :param tz: tz parameter which will be passed to the datetime.fromtimestamp method.
    """

    return bound(pred, DatetimeTimestampProvider(tz))


def datetime_by_format(pred: Pred = P.ANY, *, fmt: str) -> Provider:
    """Provider that can load/dump datetime object from/to format string e.g "%d/%m/%y %H:%M"

    :param pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    :param fmt: format parameter which will be passed to datetime.strptime method.
    """
    return bound(pred, DatetimeFormatProvider(fmt))


def date_by_timestamp(pred: Pred = P.ANY) -> Provider:
    """Provider that can load date object from UNIX timestamp.
    Note that date objects can`t be dumped to the UNIX timestamp

    :param pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    """
    return bound(pred, DateTimestampProvider())


def as_sentinel(pred: Pred) -> Provider:
    """Mark the type as a sentinel.
    Sentinels are not meant to be represented externally.
    They serve to differentiate a missing field from one explicitly set to ``None`` (``null``).

    If a field in the model is a union type that includes a sentinel, it must have a default value set to that sentinel.
    Additionally, the :paramref:`.name_mapping.omit_default` option must be enabled.

    When these conditions are met, if a field is missing during loading,
    the class instance will receive the sentinel value.
    This makes it possible to distinguish a missing field from one explicitly set to ``null``.
    When dumping data, fields with sentinel values will be omitted.

    Sentinel types only can be used within a union.
    They do not have loaders or dumpers, as they are not intended to appear in external data.

    :param pred: Predicate specifying where the provider should be used.
        See :ref:`predicate-system` for details.
    """
    return bound(pred, SentinelProvider())

