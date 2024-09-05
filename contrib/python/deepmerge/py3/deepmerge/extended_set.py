class ExtendedSet(set):
    """
    ExtendedSet is an extension of set, which allows for usage
    of types that are typically not allowed in a set
    (e.g. unhashable).

    The following types that cannot be used in a set are supported:

    - unhashable types
    """

    def __init__(self, elements):
        self._values_by_hash = {self._hash(e): e for e in elements}

    def _insert(self, element):
        self._values_by_hash[self._hash(element)] = element

    def _hash(self, element):
        if getattr(element, "__hash__") is not None:
            return hash(element)
        else:
            return hash(str(element))

    def __contains__(self, obj):
        return self._hash(obj) in self._values_by_hash
