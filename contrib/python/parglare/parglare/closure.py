from parglare.grammar import EMPTY, NonTerminal

LR_0 = 0
LR_1 = 1


def closure(state, itemset_type, first_sets=None):
    """
    For the given LRState calculates its LR(0)/LR(1) itemset closure.

    Args:
    state(LRState):
    itemset_type(int): LR_0 or LR_1
    first_sets(dict of sets): Used in LR_1 itemsets calculation.
    """
    from parglare.tables import LRItem

    items_to_process = list(state.items)
    while items_to_process:
        item = items_to_process.pop()
        symbol = item.symbol_at_position
        if not isinstance(symbol, NonTerminal):
            continue

        # Calculate follow set that is possible after the
        # non-terminal at the given position of the current
        # item.
        if itemset_type is LR_1:
            follow = _new_item_follow(item, first_sets)
        for prod in [p for p in state.grammar.productions if p.symbol == symbol]:
            new_item = LRItem(prod, 0, set(follow) if itemset_type is LR_1 else None)
            if new_item not in state.items:
                # If the item doesn't exists yet add it and reprocess it.
                state.items.append(new_item)
                items_to_process.append(new_item)
            elif itemset_type is LR_1:
                # If the item already exists, this newly created item might
                # still have a wider follows set. If so, update with the
                # current new item follows set if we are building LR_1 items
                # set.
                existing_item = next(i for i in state.items if i == new_item)
                if not follow.issubset(existing_item.follow):
                    existing_item.follow.update(follow)
                    # If there was an update in the follow set of the existing
                    # item we have to process it again as we have to update
                    # follows of all items that were created from it.
                    items_to_process.append(existing_item)


def _new_item_follow(item, first_sets):
    """
    Returns follow set of possible terminals after the item's current
    non-terminal.

    Args:
    item (LRItem): The source item which is causing the creation of the
        new item.
    first_sets(dict of sets): The dict of set of first items keyed by
        a grammar symbol.
    """

    new_follow = set()
    for s in item.production.rhs[item.position + 1 :]:
        new_follow.update(first_sets[s])
        if EMPTY not in new_follow:
            # If EMPTY can't be derived at current position then we have found
            # the whole follow set.
            break
        else:
            # If the EMPTY is possible at current position in this loop we must
            # continue to include firsts of the next grammar symbol. EMPTY
            # can't be a member of the follow set.
            new_follow.remove(EMPTY)
    else:
        # If the rest of production can be EMPTY we shall inherit all elements
        # of the source item follow set.
        new_follow.update(item.follow)

    return new_follow
