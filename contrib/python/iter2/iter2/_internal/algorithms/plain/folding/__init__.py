# flake8: noqa

from ._fold import fold_items

from ._count_items import count_items


# ---

from ._cumulative_folding import fold_cumulative

from ._cumulative_unfolding import unfold_cumulative

from ._dedupping import deduplicate_same_consecutive_items

from ._consuming import consume_iterator

# ---

from ._collect_first_and_last_items import (
    collect_first_items,
    collect_last_items,
)
