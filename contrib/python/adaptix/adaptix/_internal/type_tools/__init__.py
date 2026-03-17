from .basic_utils import (
    create_union,
    is_bare_generic,
    is_generic,
    is_generic_class,
    is_named_tuple_class,
    is_new_type,
    is_parametrized,
    is_protocol,
    is_subclass_soft,
    is_typed_dict_class,
    is_user_defined_generic,
)
from .fundamentals import get_all_type_hints, get_generic_args, get_type_vars, is_pydantic_class, strip_alias
from .norm_utils import is_class_var, strip_tags
from .normalize_type import (
    AnyNormTypeVarLike,
    BaseNormType,
    NormParamSpec,
    NormParamSpecMarker,
    NormTV,
    NormTVTuple,
    NormTypeAlias,
    make_norm_type,
    normalize_type,
)
from .type_evaler import exec_type_checking, make_fragments_collector
