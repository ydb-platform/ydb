from builtins import all as _all, any as _any
from copy import copy
from operator import itemgetter, methodcaller, attrgetter
from itertools import chain, tee
from collections import defaultdict
from collections.abc import Mapping, Set, Iterable, Iterator

from .primitives import EMPTY
from .funcs import partial, compose
from .funcmakers import make_func, make_pred
from .seqs import take, map as xmap, filter as xfilter


__all__ = ['empty', 'iteritems', 'itervalues',
           'join', 'merge', 'join_with', 'merge_with',
           'walk', 'walk_keys', 'walk_values', 'select', 'select_keys', 'select_values', 'compact',
           'is_distinct', 'all', 'any', 'none', 'one', 'some',
           'zipdict', 'flip', 'project', 'omit', 'zip_values', 'zip_dicts',
           'where', 'pluck', 'pluck_attr', 'invoke', 'lwhere', 'lpluck', 'lpluck_attr', 'linvoke',
           'get_in', 'get_lax', 'set_in', 'update_in', 'del_in', 'has_path']


### Generic ops
FACTORY_REPLACE = {
    type(object.__dict__): dict,
    type({}.keys()): list,
    type({}.values()): list,
    type({}.items()): list,
}

def _factory(coll, mapper=None):
    coll_type = type(coll)
    # Hack for defaultdicts overridden constructor
    if isinstance(coll, defaultdict):
        item_factory = compose(mapper, coll.default_factory) if mapper and coll.default_factory \
                       else coll.default_factory
        return partial(defaultdict, item_factory)
    elif isinstance(coll, Iterator):
        return iter
    elif isinstance(coll, (bytes, str)):
        return coll_type().join
    elif coll_type in FACTORY_REPLACE:
        return FACTORY_REPLACE[coll_type]
    else:
        return coll_type

def empty(coll):
    """Creates an empty collection of the same type."""
    if isinstance(coll, Iterator):
        return iter([])
    return _factory(coll)()

def iteritems(coll):
    return coll.items() if hasattr(coll, 'items') else coll

def itervalues(coll):
    return coll.values() if hasattr(coll, 'values') else coll

iteritems.__doc__ = "Yields (key, value) pairs of the given collection."
itervalues.__doc__ = "Yields values of the given collection."


def join(colls):
    """Joins several collections of same type into one."""
    colls, colls_copy = tee(colls)
    it = iter(colls_copy)
    try:
        dest = next(it)
    except StopIteration:
        return None
    cls = dest.__class__

    if isinstance(dest, (bytes, str)):
        return ''.join(colls)
    elif isinstance(dest, Mapping):
        result = dest.copy()
        for d in it:
            result.update(d)
        return result
    elif isinstance(dest, Set):
        return dest.union(*it)
    elif isinstance(dest, (Iterator, range)):
        return chain.from_iterable(colls)
    elif isinstance(dest, Iterable):
        # NOTE: this could be reduce(concat, ...),
        #       more effective for low count
        return cls(chain.from_iterable(colls))
    else:
        raise TypeError("Don't know how to join %s" % cls.__name__)

def merge(*colls):
    """Merges several collections of same type into one.

    Works with dicts, sets, lists, tuples, iterators and strings.
    For dicts later values take precedence."""
    return join(colls)


def join_with(f, dicts, strict=False):
    """Joins several dicts, combining values with given function."""
    dicts = list(dicts)
    if not dicts:
        return {}
    elif not strict and len(dicts) == 1:
        return dicts[0]

    lists = {}
    for c in dicts:
        for k, v in iteritems(c):
            if k in lists:
                lists[k].append(v)
            else:
                lists[k] = [v]

    if f is not list:
        # kind of walk_values() inplace
        for k, v in iteritems(lists):
            lists[k] = f(v)

    return lists

def merge_with(f, *dicts):
    """Merges several dicts, combining values with given function."""
    return join_with(f, dicts)


def walk(f, coll):
    """Walks the collection transforming its elements with f.
       Same as map, but preserves coll type."""
    return _factory(coll)(xmap(f, iteritems(coll)))

def walk_keys(f, coll):
    """Walks keys of the collection, mapping them with f."""
    f = make_func(f)
    # NOTE: we use this awkward construct instead of lambda to be Python 3 compatible
    def pair_f(pair):
        k, v = pair
        return f(k), v

    return walk(pair_f, coll)

def walk_values(f, coll):
    """Walks values of the collection, mapping them with f."""
    f = make_func(f)
    # NOTE: we use this awkward construct instead of lambda to be Python 3 compatible
    def pair_f(pair):
        k, v = pair
        return k, f(v)

    return _factory(coll, mapper=f)(xmap(pair_f, iteritems(coll)))

# TODO: prewalk, postwalk and friends

def select(pred, coll):
    """Same as filter but preserves coll type."""
    return _factory(coll)(xfilter(pred, iteritems(coll)))

def select_keys(pred, coll):
    """Select part of the collection with keys passing pred."""
    pred = make_pred(pred)
    return select(lambda pair: pred(pair[0]), coll)

def select_values(pred, coll):
    """Select part of the collection with values passing pred."""
    pred = make_pred(pred)
    return select(lambda pair: pred(pair[1]), coll)


def compact(coll):
    """Removes falsy values from the collection."""
    if isinstance(coll, Mapping):
        return select_values(bool, coll)
    else:
        return select(bool, coll)


### Content tests

def is_distinct(coll, key=EMPTY):
    """Checks if all elements in the collection are different."""
    if key is EMPTY:
        return len(coll) == len(set(coll))
    else:
        return len(coll) == len(set(xmap(key, coll)))


def all(pred, seq=EMPTY):
    """Checks if all items in seq pass pred (or are truthy)."""
    if seq is EMPTY:
        return _all(pred)
    return _all(xmap(pred, seq))

def any(pred, seq=EMPTY):
    """Checks if any item in seq passes pred (or is truthy)."""
    if seq is EMPTY:
        return _any(pred)
    return _any(xmap(pred, seq))

def none(pred, seq=EMPTY):
    """"Checks if none of the items in seq pass pred (or are truthy)."""
    return not any(pred, seq)

def one(pred, seq=EMPTY):
    """Checks whether exactly one item in seq passes pred (or is truthy)."""
    if seq is EMPTY:
        return one(bool, pred)
    return len(take(2, xfilter(pred, seq))) == 1

# Not same as in clojure! returns value found not pred(value)
def some(pred, seq=EMPTY):
    """Finds first item in seq passing pred or first that is truthy."""
    if seq is EMPTY:
        return some(bool, pred)
    return next(xfilter(pred, seq), None)

# TODO: a variant of some that returns mapped value,
#       one can use some(map(f, seq)) or first(keep(f, seq)) for now.

# TODO: vector comparison tests - ascending, descending and such
# def chain_test(compare, seq):
#     return all(compare, zip(seq, rest(seq))

def zipdict(keys, vals):
    """Creates a dict with keys mapped to the corresponding vals."""
    return dict(zip(keys, vals))

def flip(mapping):
    """Flip passed dict or collection of pairs swapping its keys and values."""
    def flip_pair(pair):
        k, v = pair
        return v, k
    return walk(flip_pair, mapping)

def project(mapping, keys):
    """Leaves only given keys in mapping."""
    return _factory(mapping)((k, mapping[k]) for k in keys if k in mapping)

def omit(mapping, keys):
    """Removes given keys from mapping."""
    return _factory(mapping)((k, v) for k, v in iteritems(mapping) if k not in keys)

def zip_values(*dicts):
    """Yields tuples of corresponding values of several dicts."""
    if len(dicts) < 1:
        raise TypeError('zip_values expects at least one argument')
    keys = set.intersection(*map(set, dicts))
    for key in keys:
        yield tuple(d[key] for d in dicts)

def zip_dicts(*dicts):
    """Yields tuples like (key, (val1, val2, ...))
       for each common key in all given dicts."""
    if len(dicts) < 1:
        raise TypeError('zip_dicts expects at least one argument')
    keys = set.intersection(*map(set, dicts))
    for key in keys:
        yield key, tuple(d[key] for d in dicts)

def get_in(coll, path, default=None):
    """Returns a value at path in the given nested collection."""
    for key in path:
        try:
            coll = coll[key]
        except (KeyError, IndexError):
            return default
    return coll

def get_lax(coll, path, default=None):
    """Returns a value at path in the given nested collection.
       Does not raise on a wrong collection type along the way, but removes default.
    """
    for key in path:
        try:
            coll = coll[key]
        except (KeyError, IndexError, TypeError):
            return default
    return coll

def set_in(coll, path, value):
    """Creates a copy of coll with the value set at path."""
    return update_in(coll, path, lambda _: value)

def update_in(coll, path, update, default=None):
    """Creates a copy of coll with a value updated at path."""
    if not path:
        return update(coll)
    elif isinstance(coll, list):
        copy = coll[:]
        # NOTE: there is no auto-vivication for lists
        copy[path[0]] = update_in(copy[path[0]], path[1:], update, default)
        return copy
    else:
        copy = coll.copy()
        current_default = {} if len(path) > 1 else default
        copy[path[0]] = update_in(copy.get(path[0], current_default), path[1:], update, default)
        return copy


def del_in(coll, path):
    """Creates a copy of coll with a nested key or index deleted."""
    if not path:
        return coll
    try:
        next_coll = coll[path[0]]
    except (KeyError, IndexError):
        return coll

    coll_copy = copy(coll)
    if len(path) == 1:
        del coll_copy[path[0]]
    else:
        coll_copy[path[0]] = del_in(next_coll, path[1:])
    return coll_copy


def has_path(coll, path):
    """Checks if path exists in the given nested collection."""
    for p in path:
        try:
            coll = coll[p]
        except (KeyError, IndexError):
            return False
    return True

def lwhere(mappings, **cond):
    """Selects mappings containing all pairs in cond."""
    return list(where(mappings, **cond))

def lpluck(key, mappings):
    """Lists values for key in each mapping."""
    return list(pluck(key, mappings))

def lpluck_attr(attr, objects):
    """Lists values of given attribute of each object."""
    return list(pluck_attr(attr, objects))

def linvoke(objects, name, *args, **kwargs):
    """Makes a list of results of the obj.name(*args, **kwargs)
       for each object in objects."""
    return list(invoke(objects, name, *args, **kwargs))


# Iterator versions for python 3 interface

def where(mappings, **cond):
    """Iterates over mappings containing all pairs in cond."""
    items = cond.items()
    match = lambda m: all(k in m and m[k] == v for k, v in items)
    return filter(match, mappings)

def pluck(key, mappings):
    """Iterates over values for key in mappings."""
    return map(itemgetter(key), mappings)

def pluck_attr(attr, objects):
    """Iterates over values of given attribute of given objects."""
    return map(attrgetter(attr), objects)

def invoke(objects, name, *args, **kwargs):
    """Yields results of the obj.name(*args, **kwargs)
       for each object in objects."""
    return map(methodcaller(name, *args, **kwargs), objects)
