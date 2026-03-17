from libc cimport stdlib
from cython.view cimport array
from .h3lib cimport H3int, H3str, h3IsValid, h3UnidirectionalEdgeIsValid

cimport h3lib


cdef h3lib.GeoCoord deg2coord(double lat, double lng) nogil:
    cdef:
        h3lib.GeoCoord c

    c.lat = h3lib.degsToRads(lat)
    c.lng = h3lib.degsToRads(lng)

    return c


cdef (double, double) coord2deg(h3lib.GeoCoord c) nogil:
    return (
        h3lib.radsToDegs(c.lat),
        h3lib.radsToDegs(c.lng)
    )


cpdef basestring c_version():
    v = (
        h3lib.H3_VERSION_MAJOR,
        h3lib.H3_VERSION_MINOR,
        h3lib.H3_VERSION_PATCH,
    )

    return '{}.{}.{}'.format(*v)


cpdef H3int hex2int(H3str h) except? 0:
    return int(h, 16)


cpdef H3str int2hex(H3int x):
    """ Convert H3 integer to hex string representation

    Need to be careful in Python 2 because `hex(x)` may return a string
    with a trailing `L` character (denoting a "large" integer).
    The formatting approach below avoids this.

    Also need to be careful about unicode/str differences.
    """
    return '{:x}'.format(x)


class H3ValueError(ValueError):
    pass

class H3CellError(H3ValueError):
    pass

class H3EdgeError(H3ValueError):
    pass

class H3ResolutionError(H3ValueError):
    pass

class H3DistanceError(H3ValueError):
    pass

cdef check_cell(H3int h):
    """ Check if valid H3 "cell" (hexagon or pentagon).

    Does not check if a valid H3 edge, for example.

    Since this function is used by multiple interfaces (int or str),
    we want the error message to be informative to the user
    in either case.

    We use the builtin `hex` function instead of `int2hex` to
    prepend `0x` to indicate that this **integer** representation
    is incorrect, but in a format that is easily compared to
    `str` inputs.
    """
    if h3IsValid(h) == 0:
        raise H3CellError('Integer is not a valid H3 cell: {}'.format(hex(h)))

cdef check_edge(H3int e):
    if h3UnidirectionalEdgeIsValid(e) == 0:
        raise H3EdgeError('Integer is not a valid H3 edge: {}'.format(hex(e)))

cdef check_res(int res):
    if (res < 0) or (res > 15):
        raise H3ResolutionError(res)

cdef check_distance(int k):
    if k < 0:
        raise H3DistanceError(
            'Grid distances must be nonnegative. Received: {}'.format(k)
        )


## todo: can i turn these two into a context manager?
cdef H3int* create_ptr(size_t n) except? NULL:
    cdef H3int* ptr = <H3int*> stdlib.calloc(n, sizeof(H3int))
    if (n > 0) and (not ptr):
        raise MemoryError()

    return ptr


cdef H3int[:] create_mv(H3int* ptr, size_t n):
    cdef:
        array x

    n = move_nonzeros(ptr, n)
    if n <= 0:
        stdlib.free(ptr)
        return empty_memory_view()

    ptr = <H3int*> stdlib.realloc(ptr, n*sizeof(H3int))

    if ptr is NULL:
        raise MemoryError()

    x = <H3int[:n]> ptr
    x.callback_free_data = stdlib.free

    return x


cpdef H3int[:] from_iter(hexes):
    """ hexes needs to be an iterable that knows its size...
    or should we have it match the np.fromiter function, which infers if not available?
    """
    cdef:
        array x
        size_t n
    n = len(hexes)

    if n == 0:
        return empty_memory_view()

    x = <H3int[:n]> create_ptr(n)
    x.callback_free_data = stdlib.free

    for i,h in enumerate(hexes):
        x[i] = h

    return x


cdef size_t move_nonzeros(H3int* a, size_t n):
    """ Move nonzero elements to front of array `a` of length `n`.
    Return the number of nonzero elements.

    Loop invariant: Everything *before* `i` or *after* `j` is "done".
    Move `i` and `j` inwards until they equal, and exit.
    You can move `i` forward until there's a zero in front of it.
    You can move `j` backward until there's a nonzero to the left of it.
    Anything to the right of `j` is "junk" that can be reallocated.

    | a | b | 0 | c | d | ... |
            ^           ^
            i           j


    | a | b | d | c | d | ... |
            ^       ^
            i       j
    """
    cdef:
        size_t i = 0
        size_t j = n

    while i < j:
        if a[j-1] == 0:
            j -= 1
            continue

        if a[i] != 0:
            i += 1
            continue

        # if we're here, we know:
        # a[i] == 0
        # a[j-1] != 0
        # i < j
        # so we can swap! (actually, move a[j-1] -> a[i])
        a[i] = a[j-1]
        j -= 1

    return i


cdef H3int[:] empty_memory_view():
    # there's gotta be a better way to do this...
    # create an empty cython.view.array?
    cdef:
        H3int a[1]

    return (<H3int[:]>a)[:0]
