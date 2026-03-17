from ....algorithms.plain import (
    folding,
    groupping,
    searching,
    splitting,
)

from ._make_transformer import make_iterable_transformer as TF


# --- folding ---

fold_items = TF(folding.fold_items)

fold_cumulative = TF(folding.fold_cumulative)
unfold_cumulative = TF(folding.unfold_cumulative)


# --- groupping ---

group_and_fold_values_globally_with_same_computable_key = TF(groupping.group_and_fold_values_globally_with_same_computable_key)
group_and_fold_values_globally_by_key = TF(groupping.group_and_fold_values_globally_by_key)

iterate_in_chunks_of_exact_size_with_padding = TF(groupping.iterate_in_chunks_of_exact_size_with_padding)


# --- searching ---

find_min_value = TF(searching.find_min_value)
find_max_value = TF(searching.find_max_value)


# --- splitting ---

unzip_into_lists = TF(splitting.unzip_into_lists)
