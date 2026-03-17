"""Count occurrences of uint64-valued keys."""
from __future__ import division
cimport cython
from libc.math cimport log, exp, sqrt


cdef class PreshCounter:
    def __init__(self, initial_size=8):
        assert initial_size != 0
        assert initial_size & (initial_size - 1) == 0
        self.mem = Pool()
        self.c_map = <MapStruct*>self.mem.alloc(1, sizeof(MapStruct))
        map_init(self.mem, self.c_map, initial_size)
        self.smoother = None
        self.total = 0

    property length:
        def __get__(self):
            return self.c_map.length

    def __len__(self):
        return self.c_map.length

    def __iter__(self):
        cdef int i = 0
        cdef key_t key
        cdef void* value
        while map_iter(self.c_map, &i, &key, &value):
            yield key, <size_t>value

    def __getitem__(self, key_t key):
        return <count_t>map_get(self.c_map, key)

    cpdef int inc(self, key_t key, count_t inc) except -1:
        cdef count_t c = <count_t>map_get(self.c_map, key)
        c += inc
        map_set(self.mem, self.c_map, key, <void*>c)
        self.total += inc
        return c

    def prob(self, key_t key):
        cdef GaleSmoother smoother
        cdef void* value = map_get(self.c_map, key)
        if self.smoother is not None:
            smoother = self.smoother
            r_star = self.smoother(<count_t>value)
            return r_star / self.smoother.total
        elif value == NULL:
            return 0
        else:
            return <count_t>value / self.total

    def smooth(self):
        self.smoother = GaleSmoother(self)
       

cdef class GaleSmoother:
    cdef Pool mem
    cdef count_t* Nr
    cdef double gradient
    cdef double intercept
    cdef readonly count_t cutoff
    cdef count_t Nr0
    cdef readonly double total

    def __init__(self, PreshCounter counts):
        count_counts = PreshCounter()
        cdef double total = 0
        for _, count in counts:
            count_counts.inc(count, 1)
            total += count
        # If we have no items seen 1 or 2 times, this doesn't work. But, this
        # won't be true in real data...
        assert count_counts[1] != 0 and count_counts[2] != 0, "Cannot smooth your weird data"
        # Extrapolate Nr0 from Nr1 and Nr2.
        self.Nr0 = count_counts[1] + (count_counts[1] - count_counts[2])
        self.mem = Pool()

        cdef double[2] mb

        cdef int n_counts = 0
        for _ in count_counts:
            n_counts += 1
        sorted_r = <count_t*>count_counts.mem.alloc(n_counts, sizeof(count_t))
        self.Nr = <count_t*>self.mem.alloc(n_counts, sizeof(count_t))
        for i, (count, count_count) in enumerate(sorted(count_counts)):
            sorted_r[i] = count
            self.Nr[i] = count_count

        _fit_loglinear_model(mb, sorted_r, self.Nr, n_counts)
        
        self.cutoff = _find_when_to_switch(sorted_r, self.Nr, mb[0], mb[1],
                                           n_counts)
        self.gradient = mb[0]
        self.intercept = mb[1]
        self.total = self(0) * self.Nr0
        for count, count_count in count_counts:
            self.total += self(count) * count_count

    def __call__(self, count_t r):
        if r == 0:
            return self.Nr[1] / self.Nr0
        elif r < self.cutoff:
            return turing_estimate_of_r(<double>r, <double>self.Nr[r-1], <double>self.Nr[r])
        else:
            return gale_estimate_of_r(<double>r, self.gradient, self.intercept)

    def count_count(self, count_t r):
        if r == 0:
            return self.Nr0
        else:
            return self.Nr[r-1]


@cython.cdivision(True)
cdef double turing_estimate_of_r(double r, double Nr, double Nr1) except -1:
    return ((r + 1) * Nr1) / Nr


@cython.cdivision(True)
cdef double gale_estimate_of_r(double r, double gradient, double intercept) except -1:
    cdef double e_nr  = exp(gradient * log(r) + intercept)
    cdef double e_nr1 = exp(gradient * log(r+1) + intercept)
    return (r + 1) * (e_nr1 / e_nr)


@cython.cdivision(True)
cdef void _fit_loglinear_model(double* output, count_t* sorted_r, count_t* Nr,
                               int length) except *:
    cdef double x_mean = 0.0
    cdef double y_mean = 0.0

    cdef Pool mem = Pool()
    x = <double*>mem.alloc(length, sizeof(double))
    y = <double*>mem.alloc(length, sizeof(double))

    cdef int i
    for i in range(length):
        r = sorted_r[i]
        x[i] = log(<double>r)
        y[i] = log(<double>_get_zr(i, sorted_r, Nr[i], length))
        x_mean += x[i]
        y_mean += y[i]
    
    x_mean /= length
    y_mean /= length

    cdef double ss_xy = 0.0
    cdef double ss_xx = 0.0
   
    for i in range(length):
        x_dist = x[i] - x_mean
        y_dist = y[i] - y_mean
        # SS_xy = sum the product of the distances from the mean
        ss_xy += x_dist * y_dist
        # SS_xx = sum the squares of the x distance
        ss_xx  += x_dist * x_dist
    # Gradient
    output[0]  = ss_xy / ss_xx
    # Intercept
    output[1] = y_mean - output[0] * x_mean


@cython.cdivision(True)
cdef double _get_zr(int j, count_t* sorted_r, count_t Nr_j, int n_counts) except -1:
    cdef double r_i = sorted_r[j-1] if j >= 1 else 0
    cdef double r_j = sorted_r[j]
    cdef double r_k = sorted_r[j+1] if (j+1) < n_counts else (2 * r_i - 1)
    return 2 * Nr_j / (r_k - r_i)


@cython.cdivision(True)
cdef double _variance(double r, double Nr, double Nr1) nogil:
    return 1.96 * sqrt((r+1)**2 * (Nr1 / Nr**2) * (1.0 + (Nr1 / Nr)))


@cython.cdivision(True)
cdef count_t _find_when_to_switch(count_t* sorted_r, count_t* Nr, double m, double b,
                                  int length) except -1:
    cdef int i
    cdef count_t r
    for i in range(length-1):
        r = sorted_r[i]
        if sorted_r[i+1] != r+1:
            return r
        g_r = gale_estimate_of_r(r, m, b)
        t_r = turing_estimate_of_r(<double>r, <double>Nr[i], <double>Nr[i+1])
        if abs(t_r - g_r) <= _variance(<double>r, <double>Nr[i], <double>Nr[i+1]):
            return r
    else:
        return length - 1
