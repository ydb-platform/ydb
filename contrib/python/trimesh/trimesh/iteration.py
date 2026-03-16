from math import log2

from .typed import Any, Callable, Iterable, List, NDArray, Sequence, Union


def reduce_cascade(operation: Callable, items: Union[Sequence, NDArray]):
    """
    Call an operation function in a cascaded pairwise way against a
    flat list of items.

    This should produce the same result as `functools.reduce`
    if `operation` is commutable like addition or multiplication.
    This may be faster for an `operation` that runs with a speed
    proportional to its largest input, which mesh booleans appear to.

    The union of a large number of small meshes appears to be
    "much faster" using this method.

    This only differs from `functools.reduce` for commutative `operation`
    in that it returns `None` on empty inputs rather than `functools.reduce`
    which raises a `TypeError`.

    For example on `a b c d e f g` this function would run and return:
        a b
        c d
        e f
        ab cd
        ef g
        abcd efg
     -> abcdefg

    Where `functools.reduce` would run and return:
        a b
        ab c
        abc d
        abcd e
        abcde f
        abcdef g
     -> abcdefg

    Parameters
    ----------
    operation
      The function to call on pairs of items.
    items
      The flat list of items to apply operation against.
    """
    if len(items) == 0:
        return None
    elif len(items) == 1:
        # skip the loop overhead for a single item
        return items[0]
    elif len(items) == 2:
        # skip the loop overhead for a single pair
        return operation(items[0], items[1])

    for _ in range(int(1 + log2(len(items)))):
        results = []

        # loop over pairs of items.
        items_mod = len(items) % 2
        for i in range(0, len(items) - items_mod, 2):
            results.append(operation(items[i], items[i + 1]))

        # if we had a non-even number of items it will have been
        # skipped by the loop so append it to our list
        if items_mod != 0:
            results.append(items[-1])

        items = results

    # logic should have reduced to a single item
    assert len(results) == 1

    return results[0]


def chain(*args: Union[Iterable[Any], Any, None]) -> List[Any]:
    """
    A less principled version of `list(itertools.chain(*args))` that
    accepts non-iterable values, filters `None`, and returns a list
    rather than yielding values.

    If all passed values are iterables this will return identical
    results to `list(itertools.chain(*args))`.


    Examples
    ----------

    In [1]: list(itertools.chain([1,2], [3]))
    Out[1]: [1, 2, 3]

    In [2]: trimesh.util.chain([1,2], [3])
    Out[2]: [1, 2, 3]

    In [3]: trimesh.util.chain([1,2], [3], 4)
    Out[3]: [1, 2, 3, 4]

    In [4]: list(itertools.chain([1,2], [3], 4))
      ----> 1 list(itertools.chain([1,2], [3], 4))
      TypeError: 'int' object is not iterable

    In [5]: trimesh.util.chain([1,2], None, 3, None, [4], [], [], 5, [])
    Out[5]: [1, 2, 3, 4, 5]


    Parameters
    -----------
    args
      Will be individually checked to see if they're iterable
      before either being appended or extended to a flat list.


    Returns
    ----------
    chained
      The values in a flat list.
    """
    # collect values to a flat list
    chained = []
    # extend if it's a sequence, otherwise append
    [
        chained.extend(a)
        if (hasattr(a, "__iter__") and not isinstance(a, (str, bytes)))
        else chained.append(a)
        for a in args
        if a is not None
    ]
    return chained
