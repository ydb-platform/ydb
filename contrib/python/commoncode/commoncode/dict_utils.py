
# Copyright (c) 2003-2012 Raymond Hettinger
# SPDX-License-Identifier: Python-2.0


def sparsify(d):
    """
    http://code.activestate.com/recipes/198157-improve-dictionary-lookup-performance/
    Created by Raymond Hettinger on Sun, 4 May 2003
    Reduce average dictionary lookup time by making the internal tables more sparse.
    Improve dictionary sparsity.

    The dict.update() method makes space for non-overlapping keys.
    Giving it a dictionary with 100% overlap will build the same
    dictionary in the larger space.  The resulting dictionary will
    be no more that 1/3 full.  As a result, lookups require less
    than 1.5 probes on average.

    Example:
    >>> sparsify({1: 3, 4: 5})
    {1: 3, 4: 5}
    """
    e = d.copy()
    d.update(e)
    return d
