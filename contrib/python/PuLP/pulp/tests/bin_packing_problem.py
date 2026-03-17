from pulp import *
import random
from itertools import product


def _bin_packing_instance(bins, seed=0):
    packed_bins = [[] for _ in range(bins)]
    bin_size = bins * 100
    random.seed(seed)
    for i in range(len(packed_bins)):
        remaining_size = bin_size
        while remaining_size >= 1:
            item = random.randrange(1, remaining_size + 10)
            packed_bins[i].append(item)
            remaining_size -= item
        packed_bins[i][-1] += remaining_size
    all_items_with_bin = [(n, i) for i, l in enumerate(packed_bins) for n in l]

    random.shuffle(all_items_with_bin)
    items, packing = zip(*all_items_with_bin)
    return items, packing, bin_size


def create_bin_packing_problem(bins, seed=0):
    items, packing, bin_size = _bin_packing_instance(bins=bins, seed=seed)

    prob = LpProblem("bin_packing", LpMinimize)

    bin_indices = [i for i in range(len(items))]
    item_indices = [i for i in range(len(items))]

    using_bin = LpVariable.dicts("y", bin_indices, cat=LpBinary)
    items_packed = LpVariable.dicts(
        "x", indices=product(item_indices, bin_indices), cat=LpBinary
    )

    prob += lpSum(using_bin), "objective"

    # pack every item
    for i in item_indices:
        prob += lpSum(items_packed[i, b] for b in bin_indices) == 1, f"pack_item_{i}"

    # no bin overfilled
    for b in bin_indices:
        expr = (
            lpSum([items[i] * items_packed[i, b] for i in item_indices])
            <= bin_size * using_bin[b]
        )
        prob += expr, f"respect_bin_size_{b}"

    return prob
