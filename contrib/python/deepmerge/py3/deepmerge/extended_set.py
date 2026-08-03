from typing import Sequence, Any, Dict, List


class ExtendedSet(set):
    """
    ExtendedSet is an extension of set, which allows for usage
    of types that are typically not allowed in a set
    (e.g. unhashable).

    The following types that cannot be used in a set are supported:

    - unhashable types, granted they are idempotent.
    """

    def __init__(self, elements: Sequence) -> None:
        # Elements are grouped into buckets keyed by their (possibly lossy)
        # hash. Membership and insertion compare a candidate against the
        # members of the matching bucket using equality, so two distinct
        # elements that merely collide on the hash are not treated as equal.
        self._buckets: Dict[int, List[Any]] = {}
        for element in elements:
            self._insert(element)

    def _insert(self, element: Any) -> None:
        bucket = self._buckets.setdefault(self._hash_element(element), [])
        if not any(element == existing for existing in bucket):
            bucket.append(element)
        return

    def _hash_element(self, element: Any) -> int:
        if getattr(element, "__hash__") is not None:
            return hash(element)
        elif isinstance(element, dict):
            sorted_keys = sorted(element.keys())
            return hash(",".join([f"{key}:{element[key]}" for key in sorted_keys]))
        else:
            return hash(str(element))

    def __contains__(self, obj: Any) -> bool:
        bucket = self._buckets.get(self._hash_element(obj))
        if bucket is None:
            return False
        return any(obj == existing for existing in bucket)
