"""Column-major Fortran iterator of indices across multipe dimensions.

:copyright: Copyright 2015 Marshall Ward, see AUTHORS for details.
:license: Apache License, Version 2.0, see LICENSE for details.
"""


class FIndex(object):
    """Column-major multidimensional index iterator."""

    def __init__(self, bounds, first=None):
        """Initialise the index iterator."""
        self.start = [1 if b[0] is None else b[0] for b in bounds]
        self.end = [b[1] for b in bounds]
        self.step = [1 if b[2] is None else b[2] for b in bounds]

        self.current = self.start[:]

        # Default global starting index
        if first is not None:
            self.first = [min(first, s) for s in self.start]
        else:
            self.first = [b[0] for b in bounds]

    def __iter__(self):
        """Declare object as iterator."""
        return self

    def next(self):
        """Python 2 interface to Python 3 iterator."""
        return self.__next__()

    def __next__(self):
        """Iterate to next contiguous index tuple."""
        if self.end[-1] and self.current[-1] >= self.end[-1]:
            raise StopIteration

        state = self.current[:]
        # Allow the final index to exceed self.end[-1] as a finalisation check
        for rank, idx in enumerate(self.current):
            if ((self.end[rank] is None or idx < (self.end[rank] - 1)) or
                    rank == (len(self.current) - 1)):
                self.current[rank] = idx + self.step[rank]
                break
            else:
                self.current[rank] = self.start[rank]

        return state
