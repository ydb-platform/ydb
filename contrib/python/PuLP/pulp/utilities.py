# Utility functions
import collections
import itertools
from itertools import combinations as combination
from itertools import permutations as permutation


def resource_clock():
    import resource

    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_utime


def isNumber(x):
    """Returns true if x is an int or a float"""
    return isinstance(x, (int, float))


def value(x):
    """Returns the value of the variable/expression x, or x if it is a number"""
    if isNumber(x):
        return x
    else:
        return x.value()


def valueOrDefault(x):
    """Returns the value of the variable/expression x, or x if it is a number
    Variable without value (None) are affected a possible value (within their
    bounds)."""
    if isNumber(x):
        return x
    else:
        return x.valueOrDefault()


def allpermutations(orgset, k):
    """
    returns all permutations of orgset with up to k items

    :param orgset: the list to be iterated
    :param k: the maxcardinality of the subsets

    :return: an iterator of the subsets

    example:

    >>> c = allpermutations([1,2,3,4],2)
    >>> for s in c:
    ...     print(s)
    (1,)
    (2,)
    (3,)
    (4,)
    (1, 2)
    (1, 3)
    (1, 4)
    (2, 1)
    (2, 3)
    (2, 4)
    (3, 1)
    (3, 2)
    (3, 4)
    (4, 1)
    (4, 2)
    (4, 3)
    """
    return itertools.chain(*[permutation(orgset, i) for i in range(1, k + 1)])


def allcombinations(orgset, k):
    """
    returns all combinations of orgset with up to k items

    :param orgset: the list to be iterated
    :param k: the maxcardinality of the subsets

    :return: an iterator of the subsets

    example:

    >>> c = allcombinations([1,2,3,4],2)
    >>> for s in c:
    ...     print(s)
    (1,)
    (2,)
    (3,)
    (4,)
    (1, 2)
    (1, 3)
    (1, 4)
    (2, 3)
    (2, 4)
    (3, 4)
    """
    return itertools.chain(*[combination(orgset, i) for i in range(1, k + 1)])


def makeDict(headers, array, default=None):
    """
    makes a list into a dictionary with the headings given in headings
    headers is a list of header lists
    array is a list with the data
    """
    result, defdict = __makeDict(headers, array, default)
    return result


def __makeDict(headers, array, default=None):
    # this is a recursive function so end the recursion as follows
    result = {}
    returndefaultvalue = None
    if len(headers) == 1:
        result.update(dict(zip(headers[0], array)))
        defaultvalue = default
    else:
        for i, h in enumerate(headers[0]):
            result[h], defaultvalue = __makeDict(headers[1:], array[i], default)
    if default is not None:
        f = lambda: defaultvalue
        defresult = collections.defaultdict(f)
        defresult.update(result)
        result = defresult
        returndefaultvalue = collections.defaultdict(f)
    return result, returndefaultvalue


def splitDict(data):
    """
    Split a dictionary with lists as the data, into smaller dictionaries

    :param dict data: A dictionary with lists as the values

    :return: A tuple of dictionaries each containing the data separately,
            with the same dictionary keys
    """
    # find the maximum number of items in the dictionary
    maxitems = max([len(values) for values in data.values()])
    output = [dict() for _ in range(maxitems)]
    for key, values in data.items():
        for i, val in enumerate(values):
            output[i][key] = val

    return tuple(output)


def read_table(data, coerce_type, transpose=False):
    """
    Reads in data from a simple table and forces it to be a particular type

    This is a helper function that allows data to be easily constained in a
    simple script
    ::return: a dictionary of with the keys being a tuple of the strings
       in the first row and colum of the table
    :param str data: the multiline string containing the table data
    :param coerce_type: the type that the table data is converted to
    :param bool transpose: reverses the data if needed

    Example:
    >>> table_data = '''
    ...         L1      L2      L3      L4      L5      L6
    ... C1      6736    42658   70414   45170   184679  111569
    ... C2      217266  227190  249640  203029  153531  117487
    ... C3      35936   28768   126316  2498    130317  74034
    ... C4      73446   52077   108368  75011   49827   62850
    ... C5      174664  177461  151589  153300  59916   135162
    ... C6      186302  189099  147026  164938  149836  286307
    ... '''
    >>> table = read_table(table_data, int)
    >>> table[("C1","L1")]
    6736
    >>> table[("C6","L5")]
    149836
    """
    lines = data.splitlines()
    headings = lines[1].split()
    result = {}
    for row in lines[2:]:
        items = row.split()
        for i, item in enumerate(items[1:]):
            if transpose:
                key = (headings[i], items[0])
            else:
                key = (items[0], headings[i])
            result[key] = coerce_type(item)
    return result
