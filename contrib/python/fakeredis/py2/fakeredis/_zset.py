import sortedcontainers


class ZSet(object):
    def __init__(self):
        self._bylex = {}     # Maps value to score
        self._byscore = sortedcontainers.SortedList()

    def __contains__(self, value):
        return value in self._bylex

    def add(self, value, score):
        """Update the item and return whether it modified the zset"""
        old_score = self._bylex.get(value, None)
        if old_score is not None:
            if score == old_score:
                return False
            self._byscore.remove((old_score, value))
        self._bylex[value] = score
        self._byscore.add((score, value))
        return True

    def __setitem__(self, value, score):
        self.add(value, score)

    def __getitem__(self, key):
        return self._bylex[key]

    def get(self, key, default=None):
        return self._bylex.get(key, default)

    def __len__(self):
        return len(self._bylex)

    def __iter__(self):
        def gen():
            for score, value in self._byscore:
                yield value

        return gen()

    def discard(self, key):
        try:
            score = self._bylex.pop(key)
        except KeyError:
            return
        else:
            self._byscore.remove((score, key))

    def zcount(self, min_, max_):
        pos1 = self._byscore.bisect_left(min_)
        pos2 = self._byscore.bisect_left(max_)
        return max(0, pos2 - pos1)

    def zlexcount(self, min_value, min_exclusive, max_value, max_exclusive):
        if not self._byscore:
            return 0
        score = self._byscore[0][0]
        if min_exclusive:
            pos1 = self._byscore.bisect_right((score, min_value))
        else:
            pos1 = self._byscore.bisect_left((score, min_value))
        if max_exclusive:
            pos2 = self._byscore.bisect_left((score, max_value))
        else:
            pos2 = self._byscore.bisect_right((score, max_value))
        return max(0, pos2 - pos1)

    def islice_score(self, start, stop, reverse=False):
        return self._byscore.islice(start, stop, reverse)

    def irange_lex(self, start, stop, inclusive=(True, True), reverse=False):
        if not self._byscore:
            return iter([])
        score = self._byscore[0][0]
        it = self._byscore.irange((score, start), (score, stop),
                                  inclusive=inclusive, reverse=reverse)
        return (item[1] for item in it)

    def irange_score(self, start, stop, reverse=False):
        return self._byscore.irange(start, stop, reverse=reverse)

    def rank(self, member):
        return self._byscore.index((self._bylex[member], member))

    def items(self):
        return self._bylex.items()
