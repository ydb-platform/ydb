class LazyDict(dict):
    """Simple implementation of a lazy dictionary. If a value set is callable
    without parameters, the value will be called the first time is requested
    and stored for future requests.
    E.g:
        dict = LazyDict()
        dict['foo'] = lambda: return 123
        dict['bar'] = dict
        dict['chu'] = dict() # you can also pass non-callable values
        assert dict['foo'] == 123
        assert dict['bar'] == {}
        assert dict['chu'] == {}

    For Internal use only."""

    def values(self):
        return [self.__getitem__(key) for key in self.keys()]

    def items(self):
        return [(key, self.__getitem__(key)) for key in self.keys()]

    def iteritems(self):
        return [(key, self.__getitem__(key)) for key in self.keys()]

    def itervalues(self):
        return [self.__getitem__(key) for key in self.keys()]

    def get(self, k, d=None):
        return self.__getitem__(k) if k in self else d

    def __getitem__(self, key):
        item = super(LazyDict, self).__getitem__(key)
        try:
            self[key] = item = item()
        except TypeError:
            pass
        return item

    def __repr__(self):
        # make dictionaries evaluate
        self.values()
        return super(LazyDict, self).__repr__()

    def __eq__(self, other):
        # make dictionaries evaluate
        self.values()
        if isinstance(other, LazyDict):
            other.values()

        return super(LazyDict, self).__eq__(other)
