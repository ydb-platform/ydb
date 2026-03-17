from orderly_set import OrderedSet, StableSet, StableSetEq, OrderlySet, SortedSet

stable_sets = [StableSet]
stableeq_sets = [StableSetEq]
ordered_sets = [OrderedSet, OrderlySet, SortedSet]

# from orderedset import OrderedSet as OrderedSet2
# ordered_sets += [OrderedSet2]

set_and_stable_sets = [set] + stable_sets
stables = stableeq_sets + stable_sets
stableeq_and_orderly_sets = stableeq_sets + ordered_sets
stableeq_and_orderly_sets_except_sorted_set = stableeq_sets + [OrderedSet, OrderlySet]
stable_and_orderly_sets = stable_sets + stableeq_sets + ordered_sets
stable_and_orderly_sets_except_sorted_set = stable_sets + stableeq_sets + [OrderedSet, OrderlySet]
sets_and_stable_sets = [set] + stable_sets + stableeq_sets
all_sets = [set] + stable_sets + stableeq_sets + ordered_sets
all_sets_except_orderly_set = [set] + stable_sets + stableeq_sets + [OrderedSet]
