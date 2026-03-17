"""Advanced utilities for traversing nesting/hierarchical Dimensioned
objects either to inspect the structure of their declared dimensions
or mutate the matching elements.

"""

from collections import defaultdict
from operator import itemgetter

from .dimension import Dimension
from .util import merge_dimensions


def create_ndkey(length, indexes, values):
    key = [None] * length
    for i, v in zip(indexes, values, strict=None):
        key[i] = v
    return tuple(key)

def uniform(obj):
    """Finds all common dimension keys in the object including subsets of
    dimensions. If there are is no common subset of dimensions, None
    is returned.

    """
    from .spaces import HoloMap
    dim_groups = obj.traverse(lambda x: tuple(x.kdims),
                              (HoloMap,))
    if dim_groups:
        dgroups = [frozenset(d.name for d in dg) for dg in dim_groups]
        return all(g1 <= g2 or g1 >= g2 for g1 in dgroups for g2 in dgroups)
    return True


def unique_dimkeys(obj, default_dim='Frame'):
    """Finds all common dimension keys in the object including subsets of
    dimensions. If there are is no common subset of dimensions, None
    is returned.

    Returns the list of dimensions followed by the list of unique
    keys.

    """
    from .ndmapping import NdMapping, item_check
    from .spaces import HoloMap
    key_dims = obj.traverse(lambda x: (tuple(x.kdims),
                                       list(x.data.keys())), (HoloMap,))
    if not key_dims:
        return [Dimension(default_dim)], [(0,)]
    dim_groups, keys = zip(*sorted(key_dims, key=lambda x: -len(x[0])), strict=None)
    dgroups = [frozenset(d.name for d in dg) for dg in dim_groups]
    subset = all(g1 <= g2 or g1 >= g2 for g1 in dgroups for g2 in dgroups)
    # Find unique keys
    if subset:
        dims = merge_dimensions(dim_groups)
        all_dims = sorted(dims, key=lambda x: dim_groups[0].index(x))
    else:
        # Handle condition when HoloMap/DynamicMap dimensions do not overlap
        hmaps = obj.traverse(lambda x: x, ['HoloMap'])
        if hmaps:
            raise ValueError('When combining HoloMaps into a composite plot '
                             'their dimensions must be subsets of each other.')
        dimensions = merge_dimensions(dim_groups)
        dim_keys = {}
        for dims, keys in key_dims:
            for key in keys:
                for d, k in zip(dims, key, strict=None):
                    dim_keys[d.name] = k
        if dim_keys:
            keys = [tuple(dim_keys.get(dim.name) for dim in dimensions)]
        else:
            keys = []
        return merge_dimensions(dim_groups), keys

    ndims = len(all_dims)
    unique_keys = []
    for group, subkeys in zip(dim_groups, keys, strict=None):
        dim_idxs = [all_dims.index(dim) for dim in group]
        for key in subkeys:
            padded_key = create_ndkey(ndims, dim_idxs, key)
            matches = [item for item in unique_keys
                       if padded_key == tuple(k if k is None else i
                                              for i, k in zip(item, padded_key, strict=None))]
            if not matches:
                unique_keys.append(padded_key)

    with item_check(False):
        sorted_keys = NdMapping({key: None for key in unique_keys},
                                kdims=all_dims).data.keys()
    return all_dims, list(sorted_keys)


def bijective(keys):
    ndims = len(keys[0])
    if ndims <= 1:
        return True
    for idx in range(ndims):
        getter = itemgetter(*(i for i in range(ndims) if i != idx))
        store = []
        for key in keys:
            subkey = getter(key)
            if subkey in store:
                return False
            store.append(subkey)
    return True


def hierarchical(keys):
    """Iterates over dimension values in keys, taking two sets
    of dimension values at a time to determine whether two
    consecutive dimensions have a one-to-many relationship.
    If they do a mapping between the first and second dimension
    values is returned. Returns a list of n-1 mappings, between
    consecutive dimensions.

    """
    ndims = len(keys[0])
    if ndims <= 1:
        return True
    dim_vals = list(zip(*keys, strict=None))
    combinations = (zip(*dim_vals[i:i+2], strict=None)
                    for i in range(ndims-1))
    hierarchies = []
    for combination in combinations:
        hierarchy = True
        store1 = defaultdict(list)
        store2 = defaultdict(list)
        for v1, v2 in combination:
            if v2 not in store2[v1]:
                store2[v1].append(v2)
            previous = store1[v2]
            if previous and previous[0] != v1:
                hierarchy = False
                break
            if v1 not in store1[v2]:
                store1[v2].append(v1)
        hierarchies.append(store2 if hierarchy else {})
    return hierarchies
