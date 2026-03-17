try:
    from ._radix import Radix as _Radix
except Exception as e:
    from .radix import Radix as _Radix

__version__ = '0.10.0'
__all__ = ['Radix']


# This acts as an entrypoint to the underlying object (be it a C
# extension or pure python representation, pickle files will work)
class Radix(object):
    def __init__(self):
        self._radix = _Radix()
        self.add = self._radix.add
        self.delete = self._radix.delete
        self.search_exact = self._radix.search_exact
        self.search_best = self._radix.search_best
        self.search_worst = self._radix.search_worst
        self.search_covered = self._radix.search_covered
        self.search_covering = self._radix.search_covering
        self.nodes = self._radix.nodes
        self.prefixes = self._radix.prefixes

    def __iter__(self):
        for elt in self._radix:
            yield elt

    def __getstate__(self):
        return [(elt.prefix, elt.data) for elt in self]

    def __setstate__(self, state):
        for prefix, data in state:
            node = self._radix.add(prefix)
            for key in data:
                node.data[key] = data[key]

    def __reduce__(self):
        return (Radix, (), self.__getstate__())
