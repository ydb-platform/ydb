from ._internal.common import Dumper, Loader, TypeHint
from ._internal.definitions import DebugTrail
from ._internal.morphing.facade.func import dump, load
from ._internal.morphing.facade.provider import (
    as_is_dumper,
    as_is_loader,
    as_sentinel,
    constructor,
    date_by_timestamp,
    datetime_by_format,
    datetime_by_timestamp,
    default_dict,
    dumper,
    enum_by_exact_value,
    enum_by_name,
    enum_by_value,
    flag_by_exact_value,
    flag_by_member_names,
    loader,
    name_mapping,
    validator,
    with_property,
)
from ._internal.morphing.facade.retort import AdornedRetort, FilledRetort, Retort
from ._internal.morphing.model.crown_definitions import (
    ExtraCollect,
    Extractor,
    ExtraForbid,
    ExtraKwargs,
    ExtraSkip,
    Saturator,
)
from ._internal.morphing.name_layout.base import ExtraIn, ExtraOut
from ._internal.name_style import NameStyle
from ._internal.provider.facade.provider import bound
from ._internal.retort.searching_retort import ProviderNotFoundError
from ._internal.utils import Omittable, Omitted, create_deprecated_alias_getter
from .provider import (
    AggregateCannotProvide,
    CannotProvide,
    Chain,
    LocStackPattern,
    Mediator,
    P,
    Provider,
    Request,
    create_loc_stack_checker,
)

__all__ = (
    "AdornedRetort",
    "AggregateCannotProvide",
    "CannotProvide",
    "Chain",
    "DebugTrail",
    "Dumper",
    "ExtraCollect",
    "ExtraForbid",
    "ExtraIn",
    "ExtraKwargs",
    "ExtraOut",
    "ExtraSkip",
    "Extractor",
    "FilledRetort",
    "Loader",
    "LocStackPattern",
    "Mediator",
    "NameStyle",
    "Omittable",
    "Omitted",
    "P",
    "Provider",
    "ProviderNotFoundError",
    "Request",
    "Retort",
    "Saturator",
    "TypeHint",
    "as_is_dumper",
    "as_is_loader",
    "as_sentinel",
    "bound",
    "constructor",
    "create_loc_stack_checker",
    "date_by_timestamp",
    "datetime_by_format",
    "datetime_by_timestamp",
    "default_dict",
    "dump",
    "dumper",
    "enum_by_exact_value",
    "enum_by_name",
    "enum_by_value",
    "flag_by_exact_value",
    "flag_by_member_names",
    "load",
    "loader",
    "name_mapping",
    "provider",
    "retort",
    "validator",
    "with_property",
)

__getattr__ = create_deprecated_alias_getter(
    __name__,
    {
        "NoSuitableProvider": "ProviderNotFoundError",
    },
)
