from typing import Set
from typing import Dict
from typing import Mapping
from typing import TypeVar
from typing import Iterable
from typing import Collection
from collections import defaultdict
from heapq import heappop
from heapq import heappush


class CycleError(ValueError):
    """
    Raised when cycles exist in the input graph.

    The second element in the args attribute of instances will contain the
    sequence of nodes in which the cycle lies.
    """


T = TypeVar("T")


def topological_sort(
    items: Iterable[T], dependency_graph: Mapping[T, Collection[T]]
) -> Iterable[T]:
    # Tag each item with its input order
    pqueue = list(enumerate(items))
    ordering = {item: ix for ix, item in pqueue}
    seen_since_last_change = 0
    output: Set[T] = set()

    # Map blockers to the list of items they block
    blocked_on: Dict[T, Set[T]] = defaultdict(set)
    blocked: Set[T] = set()

    while pqueue:
        if seen_since_last_change == len(pqueue) + len(blocked):
            raise_cycle_error(ordering, pqueue, blocked_on)

        _, n = heappop(pqueue)

        blockers = {
            d for d in dependency_graph.get(n, []) if d not in output and d in ordering
        }
        if not blockers:
            seen_since_last_change = 0
            output.add(n)
            if n in blocked:
                blocked.remove(n)
            yield n
            for b in blocked_on.pop(n, []):
                if not any(b in other for other in blocked_on.values()):
                    heappush(pqueue, (ordering[b], b))
        else:
            if n in blocked:
                seen_since_last_change += 1
            else:
                seen_since_last_change = 0
                blocked.add(n)
                for b in blockers:
                    blocked_on[b].add(n)
    if blocked_on:
        raise_cycle_error(ordering, pqueue, blocked_on)


def raise_cycle_error(ordering, pqueue, blocked_on):
    bad = next((item for item in blocked_on if item not in ordering), None)
    if bad:
        raise ValueError(f"Dependency graph contains a non-existent node {bad!r}")
    unresolved = {n for _, n in pqueue}
    unresolved.update(*blocked_on.values())
    if unresolved:
        raise CycleError(
            f"Dependency graph loop detected among {unresolved!r}",
            list(sorted(unresolved, key=ordering.get)),
        )
    raise AssertionError("raise_cycle_error called but no unresovled nodes exist")
