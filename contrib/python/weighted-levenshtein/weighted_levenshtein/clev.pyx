#!python
# cython: language_level=3, boundscheck=False, wraparound=False, embedsignature=True, linetrace=True, c_string_type=str, c_string_encoding=ascii
# distutils: define_macros=CYTHON_TRACE_NOGIL=1

from libc.stdlib cimport malloc, free
from cython.view cimport array as cvarray
from .clev cimport DTYPE_t, DTYPE_MAX, ALPHABET_SIZE


cyarr = cvarray(shape=(ALPHABET_SIZE,), itemsize=sizeof(double), format="d")
cdef DTYPE_t[::1] unit_array = cyarr
unit_array[:] = 1

cymatrix = cvarray(shape=(ALPHABET_SIZE, ALPHABET_SIZE), itemsize=sizeof(double), format="d")
cdef DTYPE_t[:,::1] unit_matrix = cymatrix
unit_matrix[:, :] = 1


# Begin helper functions

# Begin Array2D

# Struct that represents a 2D array
ctypedef struct Array2D:
    DTYPE_t* mem
    Py_ssize_t num_rows
    Py_ssize_t num_cols


cdef inline void Array2D_init(
    Array2D* array2d,
    Py_ssize_t num_rows,
    Py_ssize_t num_cols) nogil:
    """
    Initializes an Array2D struct with the given number of rows and columns
    """
    array2d.num_rows = num_rows
    array2d.num_cols = num_cols
    array2d.mem = <DTYPE_t*> malloc(num_rows * num_cols * sizeof(DTYPE_t))


cdef inline void Array2D_del(
    Array2D array2d) nogil:
    """
    Destroys an Array2D struct
    """
    free(array2d.mem)


cdef inline DTYPE_t Array2D_n1_get(
    Array2D array2d,
    Py_ssize_t row,
    Py_ssize_t col) nogil:
    """
    Takes the row and column index of a (-1)-indexed matrix
    and returns the value at that location
    """
    row += 1
    col += 1
    return array2d.mem[row * array2d.num_cols + col]


cdef inline DTYPE_t* Array2D_n1_at(
    Array2D array2d,
    Py_ssize_t row,
    Py_ssize_t col) nogil:
    """
    Takes the row and column index of a (-1)-indexed matrix
    and returns a pointer to that location
    """
    row += 1
    col += 1
    return array2d.mem + row * array2d.num_cols + col


cdef inline DTYPE_t Array2D_0_get(
    Array2D array2d,
    Py_ssize_t row,
    Py_ssize_t col) nogil:
    """
    Takes the row and column index of a 0-indexed matrix
    and returns the value at that location
    """
    return array2d.mem[row * array2d.num_cols + col]


cdef inline DTYPE_t* Array2D_0_at(
    Array2D array2d,
    Py_ssize_t row,
    Py_ssize_t col) nogil:
    """
    Takes the row and column index of a 0-indexed matrix
    and returns a pointer to that location
    """
    return array2d.mem + row * array2d.num_cols + col


cdef inline DTYPE_t col_delete_range_cost(
    Array2D d,
    Py_ssize_t start,
    Py_ssize_t end) nogil:
    """
    Calculates the cost incurred by deleting
    characters 'start' to 'end' (inclusive) from 'str1',
    assuming that 'str1' is 1-indexed.

    Works since column 0 of 'd' is the cumulative sums
    of the deletion costs of the characters in str1.

    This function computes the range sum by computing the difference
    between the cumulative sums at each end of the range.
    """
    return Array2D_n1_get(d, end, 0) - Array2D_n1_get(d, start - 1, 0)


cdef inline DTYPE_t row_insert_range_cost(
    Array2D d,
    Py_ssize_t start,
    Py_ssize_t end) nogil:
    """
    Calculates the cost incurred by inserting
    characters 'start' to 'end' (inclusive) from 'str2',
    assuming that 'str2' is 1-indexed.

    Works since row 0 of 'd' is the cumulative sums
    of the insertion costs of the characters in str2.

    This function computes the range sum by computing the difference
    between the cumulative sums at each end of the range.
    """
    return Array2D_n1_get(d, 0, end) - Array2D_n1_get(d, 0, start - 1)

# End Array2D


cdef inline unsigned char str_1_get(unsigned char* s, Py_ssize_t i) nogil:
    """
    Takes an index of a 1-indexed string
    and returns that character
    """
    return s[i - 1]

# End helper functions


def damerau_levenshtein(
    unsigned char* str1,
    unsigned char* str2,
    DTYPE_t[::1] insert_costs=None,
    DTYPE_t[::1] delete_costs=None,
    DTYPE_t[:,::1] substitute_costs=None,
    DTYPE_t[:,::1] transpose_costs=None):
    """
    Calculates the Damerau-Levenshtein distance between str1 and str2,
    provided the costs of inserting, deleting, substituting, and transposing characters.
    The costs default to 1 if not provided.

    For convenience, this function is aliased as clev.dam_lev().

    :param str str1: first string
    :param str str2: second string
    :param np.ndarray insert_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where insert_costs[i] is the cost of inserting ASCII character i
    :param np.ndarray delete_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where delete_costs[i] is the cost of deleting ASCII character i
    :param np.ndarray substitute_costs: a 2D numpy array of np.float64 (C doubles) of dimensions (128, 128),
        where substitute_costs[i, j] is the cost of substituting ASCII character i with
        ASCII character j
    :param np.ndarray transpose_costs: a 2D numpy array of np.float64 (C doubles) of dimensions (128, 128),
        where transpose_costs[i, j] is the cost of transposing ASCII character i with
        ASCII character j, where character i is followed by character j in the string
    """
    if insert_costs is None:
        insert_costs = unit_array
    if delete_costs is None:
        delete_costs = unit_array
    if substitute_costs is None:
        substitute_costs = unit_matrix
    if transpose_costs is None:
        transpose_costs = unit_matrix

    # s1 = str(str1).encode()
    # s2 = str(str2).encode()

    return c_damerau_levenshtein(
        str1, len(str1),
        str2, len(str2),
        insert_costs,
        delete_costs,
        substitute_costs,
        transpose_costs
    )

dam_lev = damerau_levenshtein


cdef DTYPE_t c_damerau_levenshtein(
    unsigned char* str1, Py_ssize_t len1,
    unsigned char* str2, Py_ssize_t len2,
    DTYPE_t[::1] insert_costs,
    DTYPE_t[::1] delete_costs,
    DTYPE_t[:,::1] substitute_costs,
    DTYPE_t[:,::1] transpose_costs) nogil:
    """
    https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Distance_with_adjacent_transpositions
    """
    cdef:
        Py_ssize_t[ALPHABET_SIZE] da

        Py_ssize_t i, j
        unsigned char char_i, char_j
        DTYPE_t cost, ret_val
        Py_ssize_t db, k, l

        Array2D d

    Array2D_init(&d, len1 + 2, len2 + 2)

    # initialize 'da' to all 0
    for i in range(ALPHABET_SIZE):
        da[i] = 0

    # fill row (-1) and column (-1) with 'DTYPE_MAX'
    Array2D_n1_at(d, -1, -1)[0] = DTYPE_MAX
    for i in range(0, len1 + 1):
        Array2D_n1_at(d, i, -1)[0] = DTYPE_MAX
    for j in range(0, len2 + 1):
        Array2D_n1_at(d, -1, j)[0] = DTYPE_MAX

    # fill row 0 and column 0 with insertion and deletion costs
    Array2D_n1_at(d, 0, 0)[0] = 0
    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)
        cost = delete_costs[char_i]
        Array2D_n1_at(d, i, 0)[0] = Array2D_n1_get(d, i - 1, 0) + cost
    for j in range(1, len2 + 1):
        char_j = str_1_get(str2, j)
        cost = insert_costs[char_j]
        Array2D_n1_at(d, 0, j)[0] = Array2D_n1_get(d, 0, j - 1) + cost

    # fill DP array
    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)

        db = 0
        for j in range(1, len2 + 1):
            char_j = str_1_get(str2, j)

            k = da[char_j]
            l = db
            if char_i == char_j:
                cost = 0
                db = j
            else:
                cost = substitute_costs[char_i, char_j]

            Array2D_n1_at(d, i, j)[0] = min(
                Array2D_n1_get(d, i - 1, j - 1) + cost,                  # equal/substitute
                Array2D_n1_get(d, i, j - 1) + insert_costs[char_j],    # insert
                Array2D_n1_get(d, i - 1, j) + delete_costs[char_i],    # delete
                Array2D_n1_get(d, k - 1, l - 1) +                        # transpose
                    col_delete_range_cost(d, k + 1, i - 1) +                      # delete chars in between
                    transpose_costs[str_1_get(str1, k), str_1_get(str1, i)] +   # transpose chars
                    row_insert_range_cost(d, l + 1, j - 1)                        # insert chars in between
            )

        da[char_i] = i

    ret_val = Array2D_n1_get(d, len1, len2)
    Array2D_del(d)
    return ret_val


def optimal_string_alignment(
    unsigned char* str1,
    unsigned char* str2,
    DTYPE_t[::1] insert_costs=None,
    DTYPE_t[::1] delete_costs=None,
    DTYPE_t[:,::1] substitute_costs=None,
    DTYPE_t[:,::1] transpose_costs=None):
    """
    Calculates the Optimal String Alignment distance between str1 and str2,
    provided the costs of inserting, deleting, and substituting characters.
    The costs default to 1 if not provided.

    For convenience, this function is aliased as clev.osa().

    :param str str1: first string
    :param str str2: second string
    :param np.ndarray insert_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where insert_costs[i] is the cost of inserting ASCII character i
    :param np.ndarray delete_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where delete_costs[i] is the cost of deleting ASCII character i
    :param np.ndarray substitute_costs: a 2D numpy array of np.float64 (C doubles) of dimensions (128, 128),
        where substitute_costs[i, j] is the cost of substituting ASCII character i with
        ASCII character j
    :param np.ndarray transpose_costs: a 2D numpy array of np.float64 (C doubles) of dimensions (128, 128),
        where transpose_costs[i, j] is the cost of transposing ASCII character i with
        ASCII character j, where character i is followed by character j in the string
    """
    if insert_costs is None:
        insert_costs = unit_array
    if delete_costs is None:
        delete_costs = unit_array
    if substitute_costs is None:
        substitute_costs = unit_matrix
    if transpose_costs is None:
        transpose_costs = unit_matrix

    s1 = str(str1).encode()  
    s2 = str(str2).encode()   

    return c_optimal_string_alignment(
        s1, len(s1),
        s2, len(s2),
        insert_costs,
        delete_costs,
        substitute_costs,
        transpose_costs
    )

osa = optimal_string_alignment


cdef DTYPE_t c_optimal_string_alignment(
    unsigned char* str1, Py_ssize_t len1,
    unsigned char* str2, Py_ssize_t len2,
    DTYPE_t[::1] insert_costs,
    DTYPE_t[::1] delete_costs,
    DTYPE_t[:,::1] substitute_costs,
    DTYPE_t[:,::1] transpose_costs) nogil:
    """
    https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance
    """
    cdef:
        Py_ssize_t i, j
        unsigned char char_i, char_j, prev_char_i, prev_char_j
        DTYPE_t ret_val
        Array2D d

    Array2D_init(&d, len1 + 1, len2 + 1)

    # fill row 0 and column 0 with insertion and deletion costs
    Array2D_0_at(d, 0, 0)[0] = 0
    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)
        Array2D_0_at(d, i, 0)[0] = Array2D_0_get(d, i - 1, 0) + delete_costs[char_i]
    for j in range(1, len2 + 1):
        char_j = str_1_get(str2, j)
        Array2D_0_at(d, 0, j)[0] = Array2D_0_get(d, 0, j - 1) + insert_costs[char_j]

    # fill DP array
    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)
        for j in range(1, len2 + 1):
            char_j = str_1_get(str2, j)
            if char_i == char_j:  # match
                Array2D_0_at(d, i, j)[0] = Array2D_0_get(d, i - 1, j - 1)
            else:
                Array2D_0_at(d, i, j)[0] = min(
                    Array2D_0_get(d, i - 1, j) + delete_costs[char_i],  # deletion
                    Array2D_0_get(d, i, j - 1) + insert_costs[char_j],  # insertion
                    Array2D_0_get(d, i - 1, j - 1) + substitute_costs[char_i, char_j]  # substitution
                )

            if i > 1 and j > 1:
                prev_char_i = str_1_get(str1, i - 1)
                prev_char_j = str_1_get(str2, j - 1)
                if char_i == prev_char_j and prev_char_i == char_j:  # transpose
                    Array2D_0_at(d, i, j)[0] = min(
                        Array2D_0_get(d, i, j),
                        Array2D_0_get(d, i - 2, j - 2) + transpose_costs[prev_char_i, char_i]
                    )

    ret_val = Array2D_0_get(d, len1, len2)
    Array2D_del(d)
    return ret_val


def levenshtein(
    unsigned char* str1,
    unsigned char* str2,
    DTYPE_t[::1] insert_costs=None,
    DTYPE_t[::1] delete_costs=None,
    DTYPE_t[:,::1] substitute_costs=None):
    """
    Calculates the Levenshtein distance between str1 and str2,
    provided the costs of inserting, deleting, and substituting characters.
    The costs default to 1 if not provided.

    For convenience, this function is aliased as clev.lev().

    :param str str1: first string
    :param str str2: second string
    :param np.ndarray insert_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where insert_costs[i] is the cost of inserting ASCII character i
    :param np.ndarray delete_costs: a numpy array of np.float64 (C doubles) of length 128 (0..127),
        where delete_costs[i] is the cost of deleting ASCII character i
    :param np.ndarray substitute_costs: a 2D numpy array of np.float64 (C doubles) of dimensions (128, 128),
        where substitute_costs[i, j] is the cost of substituting ASCII character i with
        ASCII character j

    """
    if insert_costs is None:
        insert_costs = unit_array
    if delete_costs is None:
        delete_costs = unit_array
    if substitute_costs is None:
        substitute_costs = unit_matrix

    s1 = str(str1).encode()
    s2 = str(str2).encode()  

    return c_levenshtein(
        s1, len(s1),
        s2, len(s2),
        insert_costs,
        delete_costs,
        substitute_costs
    )

lev = levenshtein


cdef DTYPE_t c_levenshtein(
    unsigned char* str1, Py_ssize_t len1,
    unsigned char* str2, Py_ssize_t len2,
    DTYPE_t[::1] insert_costs,
    DTYPE_t[::1] delete_costs,
    DTYPE_t[:,::1] substitute_costs) nogil:
    """
    https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer_algorithm
    """
    cdef:
        Py_ssize_t i, j
        unsigned char char_i, char_j
        DTYPE_t ret_val
        Array2D d

    Array2D_init(&d, len1 + 1, len2 + 1)

    Array2D_0_at(d, 0, 0)[0] = 0
    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)
        Array2D_0_at(d, i, 0)[0] = Array2D_0_get(d, i - 1, 0) + delete_costs[char_i]
    for j in range(1, len2 + 1):
        char_j = str_1_get(str2, j)
        Array2D_0_at(d, 0, j)[0] = Array2D_0_get(d, 0, j - 1) + insert_costs[char_j]

    for i in range(1, len1 + 1):
        char_i = str_1_get(str1, i)
        for j in range(1, len2 + 1):
            char_j = str_1_get(str2, j)
            if char_i == char_j:  # match
                Array2D_0_at(d, i, j)[0] = Array2D_0_get(d, i - 1, j - 1)
            else:
                Array2D_0_at(d, i, j)[0] = min(
                    Array2D_0_get(d, i - 1, j) + delete_costs[char_i],
                    Array2D_0_get(d, i, j - 1) + insert_costs[char_j],
                    Array2D_0_get(d, i - 1, j - 1) + substitute_costs[char_i, char_j]
                )

    ret_val = Array2D_0_get(d, len1, len2)
    Array2D_del(d)
    return ret_val
