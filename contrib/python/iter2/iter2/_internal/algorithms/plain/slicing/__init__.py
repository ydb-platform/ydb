# flake8: noqa

from ._fix_size_slicing import (
    slice_items,
    drop_first_items_and_iterate_rest,
    iterate_through_first_items,
    iterate_with_fixed_step,
)

from ._conditional_slicing import (
    drop_items_while_true_and_iterate_rest,
    iterate_through_items_while_true_and_drop_first_false,
)
