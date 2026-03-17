from ....lib.std import (
    builtin_map,
    itertools_groupby,
    functools_reduce,
)
from ....lib.undefined import UNDEFINED
from ....lib.operators import (
    first_item,
    second_item,
)


# ---

def group_and_fold_values_globally_with_same_computable_key(iterable, *, key_fn, initial_fn=UNDEFINED, fold_fn):
    groups_it = itertools_groupby(iterable, key=key_fn)

    if UNDEFINED.is_not(initial_fn):
        result_mapping = {}
        for key, values in groups_it:
            state_value = result_mapping.get(key, UNDEFINED)
            if state_value is UNDEFINED:
                state_value = initial_fn()

            result_mapping[key] = (
                functools_reduce(
                    fold_fn,
                    values,
                    state_value,
                )
            )
    else:
        result_mapping = {}
        for key, values in groups_it:
            state_value = result_mapping.get(key, UNDEFINED)
            if UNDEFINED.is_not(state_value):
                result_mapping[key] = (
                    functools_reduce(
                        fold_fn,
                        values,
                        state_value,
                    )
                )
            else:
                result_mapping[key] = functools_reduce(fold_fn, values)


    return result_mapping


def group_and_fold_values_globally_by_key(iterable, *, initial_fn=UNDEFINED, fold_fn):
    groups_it = itertools_groupby(iterable, key=first_item)

    if UNDEFINED.is_not(initial_fn):
        result_mapping = {}
        for key, pairs in groups_it:
            state_value = result_mapping.get(key, UNDEFINED)
            if state_value is UNDEFINED:
                state_value = initial_fn()

            result_mapping[key] = (
                functools_reduce(
                    fold_fn,
                    builtin_map(second_item, pairs),
                    state_value,
                )
            )
    else:
        result_mapping = {}
        for key, pairs in groups_it:
            values = builtin_map(second_item, pairs)
            state_value = result_mapping.get(key, UNDEFINED)
            if UNDEFINED.is_not(state_value):
                result_mapping[key] = (
                    functools_reduce(
                        fold_fn,
                        values,
                        state_value,
                    )
                )
            else:
                result_mapping[key] = functools_reduce(fold_fn, values)


    return result_mapping
