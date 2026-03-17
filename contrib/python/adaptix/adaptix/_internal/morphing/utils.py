from ..common import TypeHint
from ..provider.essential import CannotProvide
from ..type_tools import BaseNormType, normalize_type


def try_normalize_type(tp: TypeHint) -> BaseNormType:
    try:
        return normalize_type(tp)
    except ValueError:
        raise CannotProvide(f"{tp} cannot be normalized")
