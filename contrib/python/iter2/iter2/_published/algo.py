# flake8: noqa

# 1. non-wrapped algorithms

from .._internal.algorithms.plain.filtering import filter_none_items
from .._internal.algorithms.plain.merging import flatten_iterable


# 2. algorithms as transformers

from .._internal.algorithms.wrapped.as_transformers.no_overloads import (

    # --- enumerating ---

    enumerate_items,
    enumerate_packed_items,

    # --- filtering ---

    filter_items,
    filter_items_by_type_predicate,
    filter_items_by_type,

    # --- folding ---

    count_items,

    collect_first_items,
    collect_last_items,

    deduplicate_same_consecutive_items,

    # --- generating ---

    cycle_through_items,

    # --- groupping ---

    group_consecutive_values_with_same_computable_key,
    group_consecutive_values_by_key,

    group_and_collect_values_globally_with_same_computable_key,
    group_and_collect_values_globally_by_key,

    iterate_in_chunks_of_at_most_size,
    iterate_in_chunks_of_exact_size_and_drop_left,

    iterate_pairwise,
    iterate_with_sliding_window_of_at_most_size,
    iterate_with_sliding_window_of_exact_size_with_padding,

    # --- merging ---

    iterate_alternating_with_item,

    # --- searching ---

    find_first_occurrence,

    # --- slicing ---

    slice_items,
    drop_first_items_and_iterate_rest,
    iterate_through_first_items,
    iterate_with_fixed_step,

    drop_items_while_true_and_iterate_rest,
    iterate_through_items_while_true_and_drop_first_false,

    # --- splitting --

    split_by_condition,

    # --- tapping ---

    map_tap_items,
    map_tap_items_periodically,
    map_tap_packed_items,
    map_tap_packed_items_periodically,
)

from .._internal.algorithms.wrapped.as_transformers.with_overloads import (
    fold_items,
    fold_cumulative,
    unfold_cumulative,

    group_and_fold_values_globally_with_same_computable_key,
    group_and_fold_values_globally_by_key,

    iterate_in_chunks_of_exact_size_with_padding,

    unzip_into_lists,

    find_min_value,
    find_max_value,
)


# 3. Iterator2 builders

from .._internal.algorithms.wrapped.as_iterator2_builders.no_overloads import (
    from_args,
    count_from,

    cycle_through_items,
    yield_item_repeatedly,
    repeatedly_call_forever,
)

from .._internal.algorithms.wrapped.as_iterator2_builders.with_overloads import (
    cartesian_product,

    iterate_sequentially_through,

    zip_shortest,
    zip_longest,
    zip_same_size,

    iterate_alternating,
)
