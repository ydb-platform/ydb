import dpath.path
import dpath.exceptions
import traceback
try:
    #python3, especially 3.8
    from collections.abc import MutableSequence
    from collections.abc import MutableMapping
except ImportError:
    #python2
    from collections import MutableSequence
    from collections import MutableMapping

MERGE_REPLACE=(1 << 1)
MERGE_ADDITIVE=(1 << 2)
MERGE_TYPESAFE=(1 << 3)
MERGE_LOOSEDICT=(1 << 4)

def __safe_path__(path, separator):
    """
    Given a path and separator, return a list of path components. If path
    is already a list, return it.

    Note that a string path with the separator at index[0] will have the
    separator stripped off. If you pass a list path, the separator is
    ignored, and is assumed to be part of each key glob. It will not be
    stripped.
    """
    if issubclass(path.__class__, (MutableSequence)):
        return path
    path = path.lstrip(separator).split(separator)
    validated = []
    for elem in path:
        key = elem[0]
        strkey = str(key)
        if (separator and (separator in strkey)):
            raise dpath.exceptions.InvalidKeyName("{0} at {1} contains the separator {2}"
                                                  "".format(strkey,
                                                            separator.join(validated),
                                                            separator))
        validated.append(strkey)
    return path

def new(obj, path, value, separator="/", creator=None):
    """
    Set the element at the terminus of path to value, and create
    it if it does not exist (as opposed to 'set' that can only
    change existing keys).

    path will NOT be treated like a glob. If it has globbing
    characters in it, they will become part of the resulting
    keys

    creator allows you to pass in a creator method that is 
    responsible for creating missing keys at arbitrary levels of
    the path (see the help for dpath.path.set)
    """
    pathlist = __safe_path__(path, separator)
    pathobj = dpath.path.path_types(obj, pathlist)
    return dpath.path.set(obj, pathobj, value, creator=creator)

def delete(obj, glob, separator="/", afilter=None):
    """
    Given a path glob, delete all elements that match the glob.

    Returns the number of deleted objects. Raises PathNotFound if no paths are
    found to delete.
    """
    deleted = 0
    paths = []
    globlist = __safe_path__(glob, separator)
    for path in _inner_search(obj, globlist, separator):
        # These are yielded back, don't mess up the dict.
        paths.append(path)

    paths.reverse()
    for path in paths:
        cur = obj
        prev = None
        for item in path:
            prev = cur
            try:
                cur = cur[item[0]]
            except AttributeError as e:
                # This only happens when we delete X/Y and the next
                # item in the paths is X/Y/Z
                pass
        if (not afilter) or (afilter and afilter(prev[item[0]])):
            prev.pop(item[0])
        deleted += 1
    if not deleted:
        raise dpath.exceptions.PathNotFound("Could not find {0} to delete it".format(glob))
    return deleted

def set(obj, glob, value, separator="/", afilter=None):
    """
    Given a path glob, set all existing elements in the document
    to the given value. Returns the number of elements changed.
    """
    changed = 0
    globlist = __safe_path__(glob, separator)
    for path in _inner_search(obj, globlist, separator):
        changed += 1
        dpath.path.set(
            obj,
            path,
            value,
            afilter=afilter,
            creator=dpath.path.creator_error_on_missing)
    return changed

def get(obj, glob, separator="/"):
    """
    Given an object which contains only one possible match for the given glob,
    return the value for the leaf matching the given glob.

    If more than one leaf matches the glob, ValueError is raised. If the glob is
    not found, KeyError is raised.
    """
    ret = None
    matches = list(search(obj, glob, yielded=True, separator=separator))
    if len(matches) == 0:
        raise KeyError(glob)
    elif len(matches) > 1:
        raise ValueError("dpath.util.get() globs must match only one leaf : %s" % glob)
    return matches[0][1]

def values(obj, glob, separator="/", afilter=None, dirs=True):
    """
    Given an object and a path glob, return an array of all values which match
    the glob. The arguments to this function are identical to those of search(),
    and it is primarily a shorthand for a list comprehension over a yielded
    search call.
    """
    return [x[1] for x in dpath.util.search(obj, glob, yielded=True, separator=separator, afilter=afilter, dirs=dirs)]

def search(obj, glob, yielded=False, separator="/", afilter=None, dirs = True):
    """
    Given a path glob, return a dictionary containing all keys
    that matched the given glob.

    If 'yielded' is true, then a dictionary will not be returned.
    Instead tuples will be yielded in the form of (path, value) for
    every element in the document that matched the glob.
    """

    def _search_view(obj, glob, separator, afilter, dirs):
        view = {}
        globlist = __safe_path__(glob, separator)
        for path in _inner_search(obj, globlist, separator, dirs=dirs):
            try:
                val = dpath.path.get(obj, path, afilter=afilter, view=True)
                merge(view, val)
            except dpath.exceptions.FilteredValue:
                pass
        return view

    def _search_yielded(obj, glob, separator, afilter, dirs):
        globlist = __safe_path__(glob, separator)
        for path in _inner_search(obj, globlist, separator, dirs=dirs):
            try:
                val = dpath.path.get(obj, path, view=False, afilter=afilter)
                yield (separator.join(map(str, dpath.path.paths_only(path))), val)
            except dpath.exceptions.FilteredValue:
                pass

    if afilter is not None:
        dirs = False
    if yielded:
        return _search_yielded(obj, glob, separator, afilter, dirs)
    return _search_view(obj, glob, separator, afilter, dirs)

def _inner_search(obj, glob, separator, dirs=True, leaves=False):
    """Search the object paths that match the glob."""
    for path in dpath.path.paths(obj, dirs, leaves, skip=True):
        if dpath.path.match(path, glob):
            yield path

def merge(dst, src, separator="/", afilter=None, flags=MERGE_ADDITIVE, _path=""):
    """Merge source into destination. Like dict.update() but performs
    deep merging.

    NOTE that merge() does NOT copy objects - it REFERENCES. If you merge
    take these two dictionaries:

    >>> a = {'a': [0] }
    >>> b = {'a': [1] }
    
    ... and you merge them into an empty dictionary, like so:

    >>> d = {}
    >>> dpath.util.merge(d, a)
    >>> dpath.util.merge(d, b)

    ... you might be surprised to find that a['a'] now contains [0, 1].
    This is because merge() says (d['a'] = a['a']), and thus creates a reference.
    This reference is then modified when b is merged, causing both d and
    a to have ['a'][0, 1]. To avoid this, make your own deep copies of source
    objects that you intend to merge. For further notes see
    https://github.com/akesterson/dpath-python/issues/58

    flags is an OR'ed combination of MERGE_ADDITIVE, MERGE_REPLACE,
    MERGE_TYPESAFE, or MERGE_LOOSEDICT.
        * MERGE_ADDITIVE : List objects are combined onto one long
          list (NOT a set). This is the default flag.
        * MERGE_REPLACE : Instead of combining list objects, when
          2 list objects are at an equal depth of merge, replace
          the destination with the source.
        * MERGE_TYPESAFE : When 2 keys at equal levels are of different
          types, raise a TypeError exception. By default, the source
          replaces the destination in this situation.
        * MERGE_LOOSEDICT : When 2 keys at equal levels are instances
          of MutableMapping, continue with recursive merge even if they
          are not the exact same type. By default the destination type
          is not changed to the source type.

    Note that MERGE_LOOSEDICT and MERGE_TYPESAFE are mutually exclusive,
    setting them both will result in ValueError being thrown.
    """

    if ( (flags & MERGE_LOOSEDICT == MERGE_LOOSEDICT ) and
         (flags & MERGE_TYPESAFE == MERGE_TYPESAFE) ):
        raise ValueError(
            "MERGE_LOOSEDICT and MERGE_TYPESAFE "
            "are mutually exclusive"
        )
    
    if afilter:
        # Having merge do its own afiltering is dumb, let search do the
        # heavy lifting for us.
        src = search(src, '**', afilter=afilter)
        return merge(dst, src)

    def _check_typesafe(obj1, obj2, key, path):
        if not key in obj1:
            return
        elif ( (flags & MERGE_LOOSEDICT == MERGE_LOOSEDICT) and (isinstance(obj1[key], MutableMapping)) and (isinstance(obj2[key], MutableMapping))):
            return
        elif ( (flags & MERGE_TYPESAFE == MERGE_TYPESAFE) and (type(obj1[key]) != type(obj2[key]))):
            raise TypeError("Cannot merge objects of type {0} and {1} at {2}"
                            "".format(type(obj1[key]), type(obj2[key]), path))
        elif ( (flags & MERGE_TYPESAFE != MERGE_TYPESAFE) and (type(obj1[key]) != type(obj2[key]))):
            obj1.pop(key)

    if isinstance(src, MutableMapping):
        for (i, v) in enumerate(src):
            _check_typesafe(dst, src, v, separator.join([_path, str(v)]))

            if not v in dst:
                dst[v] = src[v]
            else:
                if (
                        (flags & MERGE_REPLACE == MERGE_REPLACE) or
                        (not isinstance(src[v], (MutableMapping, MutableSequence)))
                ):
                    dst[v] = src[v]
                else:
                    merge(dst[v], src[v], afilter=afilter, flags=flags,
                          _path=separator.join([_path, str(v)]), separator=separator)
    elif isinstance(src, MutableSequence):
        for (i, v) in enumerate(src):
            _check_typesafe(dst, src, i, separator.join([_path, str(i)]))
            dsti = i
            if ( flags & MERGE_ADDITIVE):
                dsti = len(dst)
            if dsti >= len(dst):
                dst += [None] * (dsti - (len(dst) - 1))
            if dst[dsti] == None:
                dst[dsti] = src[i]
            else:
                if (
                        (flags & MERGE_REPLACE == MERGE_REPLACE) or 
                        (not isinstance(src[i], (MutableMapping, MutableSequence)))
                ):
                    dst[dsti] = src[i]
                else:
                    merge(dst[i], src[i], afilter=afilter, flags=flags,
                          _path=separator.join([_path, str(i)]), separator=separator)
