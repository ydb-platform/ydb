# flake8: noqa

from ._groupping_consecutive import (
    group_consecutive_values_with_same_computable_key,
    group_consecutive_values_by_key,
)

from ._groupping_and_collecting_globally import (
    group_and_collect_values_globally_with_same_computable_key,
    group_and_collect_values_globally_by_key,
)

from ._groupping_and_folding_globally import (
    group_and_fold_values_globally_with_same_computable_key,
    group_and_fold_values_globally_by_key,
)

from ._sized_chunks import (
    iterate_in_chunks_of_at_most_size,
    iterate_in_chunks_of_exact_size_with_padding,
    iterate_in_chunks_of_exact_size_and_drop_left,
)

from ._sliding_window import (
    iterate_pairwise,
    iterate_with_sliding_window_of_at_most_size,
    iterate_with_sliding_window_of_exact_size_with_padding,
)
