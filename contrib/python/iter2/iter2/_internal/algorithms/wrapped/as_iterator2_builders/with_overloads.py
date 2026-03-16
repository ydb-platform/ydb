from ....algorithms.plain import (
    generating,
    merging,
)

from ._make_iterator2_builder import make_iterator2_from_iterator_builder as It2


# ---

cartesian_product = It2(generating.cartesian_product)


# ---

iterate_sequentially_through = It2(merging.iterate_sequentially_through)

zip_shortest = It2(merging.zip_shortest)
zip_longest = It2(merging.zip_longest)
zip_same_size = It2(merging.zip_same_size)

iterate_alternating = It2(merging.iterate_alternating)
