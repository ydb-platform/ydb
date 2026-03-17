from typing import cast, overload, Optional, List, Set, Any, TypeVar
from typing import Generator, Sequence, Iterable

T = TypeVar("T")


def is_sequence(obj: Any) -> bool:
    return isinstance(obj, Sequence) and not isinstance(obj, (str, bytes))


def is_listish(obj: Any) -> bool:
    """Check if something quacks like a list."""
    if isinstance(obj, (list, tuple, set)):
        return True
    return is_sequence(obj)


def unique_list(lst: Sequence[T]) -> List[T]:
    """Make a list unique, retaining order of initial appearance."""
    uniq = []
    for item in lst:
        if item not in uniq:
            uniq.append(item)
    return uniq


@overload
def ensure_list(obj: None) -> List[None]:
    pass


@overload
def ensure_list(obj: Iterable[T]) -> List[T]:
    pass


@overload
def ensure_list(obj: T) -> List[T]:
    pass


def ensure_list(obj: Any) -> List[T]:
    """Make the returned object a list, otherwise wrap as single item."""
    if obj is None:
        return []
    if is_listish(obj):
        return [o for o in cast(Sequence[T], obj)]
    return [cast(T, obj)]


def chunked_iter(
    iterable: Iterable[T], batch_size: int = 500
) -> Generator[List[T], None, None]:
    """Pick `batch_size` items from an iterable and treat them as a batch list."""
    batch = list()
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = list()
    if len(batch) > 0:
        yield batch


def chunked_iter_sets(
    iterable: Iterable[T], batch_size: int = 500
) -> Generator[Set[T], None, None]:
    """Pick `batch_size` items from an iterable and treat them as a batch set."""
    batch = set()
    for item in iterable:
        batch.add(item)
        if len(batch) >= batch_size:
            yield batch
            batch = set()
    if len(batch) > 0:
        yield batch


def first(lst: Sequence[T]) -> Optional[T]:
    """Return the first non-null element in the list, or None."""
    item: T
    for item in ensure_list(lst):
        if item is not None:
            return item
    return None
