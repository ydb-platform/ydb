import typing as tp

from ....lib.std import (
    builtin_map,
    itertools_groupby,
)
from ....lib.operators import (
    first_item,
    second_item,
)


# ---

def group_and_collect_values_globally_with_same_computable_key[
    Key: tp.Hashable, Value,
](
    iterable: tp.Iterable[Value],
    *,
    key_fn: tp.Callable[[Value], Key],
) -> tp.Dict[Key, tp.List[Value]]:
    result_mapping = {}
    for key, values in itertools_groupby(iterable, key=key_fn):
        if key not in result_mapping:
            result_mapping[key] = list(values)
        else:
            result_mapping[key].extend(values)
    return result_mapping


# ---

def group_and_collect_values_globally_by_key[
    Key: tp.Hashable, Value,
](
    iterable: tp.Iterable[tp.Tuple[Key, Value]],
) -> tp.Dict[Key, tp.List[Value]]:
    result_mapping = {}
    for key, pairs in itertools_groupby(iterable, key=first_item):
        values = builtin_map(second_item, pairs)
        if key not in result_mapping:
            result_mapping[key] = list(values)
        else:
            result_mapping[key].extend(values)
    return result_mapping
