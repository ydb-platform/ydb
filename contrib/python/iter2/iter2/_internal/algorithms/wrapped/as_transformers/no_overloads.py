from ....algorithms.plain import (
    enumerating,
    filtering,
    folding,
    generating,
    groupping,
    merging,
    searching,
    slicing,
    splitting,
    tapping,
)

from ._make_transformer import make_iterable_transformer as TF


# --- enumerating ---

enumerate_items = TF(enumerating.enumerate_items)
enumerate_packed_items = TF(enumerating.enumerate_packed_items)


# --- filtering ---

filter_items = TF(filtering.filter_items)
filter_items_by_type_predicate = TF(filtering.filter_items_by_type_predicate)
filter_items_by_type = TF(filtering.filter_items_by_type)


# --- folding ---

count_items = TF(folding.count_items)

deduplicate_same_consecutive_items = TF(folding.deduplicate_same_consecutive_items)

collect_first_items = TF(folding.collect_first_items)
collect_last_items = TF(folding.collect_last_items)


# --- generating ---

cycle_through_items = TF(generating.cycle_through_items)


# --- groupping ---

group_consecutive_values_with_same_computable_key = TF(groupping.group_consecutive_values_with_same_computable_key)
group_consecutive_values_by_key = TF(groupping.group_consecutive_values_by_key)

group_and_collect_values_globally_with_same_computable_key = TF(groupping.group_and_collect_values_globally_with_same_computable_key)
group_and_collect_values_globally_by_key = TF(groupping.group_and_collect_values_globally_by_key)

iterate_in_chunks_of_at_most_size = TF(groupping.iterate_in_chunks_of_at_most_size)
iterate_in_chunks_of_exact_size_and_drop_left = TF(groupping.iterate_in_chunks_of_exact_size_and_drop_left)

iterate_pairwise = TF(groupping.iterate_pairwise)
iterate_with_sliding_window_of_at_most_size = TF(groupping.iterate_with_sliding_window_of_at_most_size)
iterate_with_sliding_window_of_exact_size_with_padding = TF(groupping.iterate_with_sliding_window_of_exact_size_with_padding)


# --- merging ---

iterate_alternating_with_item = TF(merging.iterate_alternating_with_item)


# --- searching ---

find_first_occurrence = TF(searching.find_first_occurrence)


# --- slicing ---

slice_items = TF(slicing.slice_items)
drop_first_items_and_iterate_rest = TF(slicing.drop_first_items_and_iterate_rest)
iterate_through_first_items = TF(slicing.iterate_through_first_items)
iterate_with_fixed_step = TF(slicing.iterate_with_fixed_step)

drop_items_while_true_and_iterate_rest = TF(slicing.drop_items_while_true_and_iterate_rest)
iterate_through_items_while_true_and_drop_first_false = TF(slicing.iterate_through_items_while_true_and_drop_first_false)


# --- splitting --

split_by_condition = TF(splitting.split_by_condition)


# --- tapping ---

map_tap_items = TF(tapping.map_tap_items)
map_tap_items_periodically = TF(tapping.map_tap_items_periodically)
map_tap_packed_items = TF(tapping.map_tap_packed_items)
map_tap_packed_items_periodically = TF(tapping.map_tap_packed_items_periodically)
