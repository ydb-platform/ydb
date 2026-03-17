# cython: infer_types=True
# cython: cdivision=True

cimport cython
from cymem.cymem cimport Pool
from libc.stdint cimport int32_t
from libc.string cimport memcpy, memset

ctypedef float weight_t

DEF USE_BLAS = False
DEF EPS = 1e-5


IF USE_BLAS:
    cimport blis.cy

cdef extern from "math.h" nogil:
    weight_t exp(weight_t x)
    weight_t sqrt(weight_t x)


cdef class Matrix:
    cdef readonly Pool mem
    cdef weight_t* data
    cdef readonly int32_t nr_row
    cdef readonly int32_t nr_col


cdef class Vec:
    @staticmethod    
    cdef inline int arg_max(const weight_t* scores, const int n_classes) nogil:
        if n_classes == 2:
            return 0 if scores[0] > scores[1] else 1
        cdef int i
        cdef int best = 0
        cdef weight_t mode = scores[0]
        for i in range(1, n_classes):
            if scores[i] > mode:
                mode = scores[i]
                best = i
        return best

    @staticmethod
    cdef inline weight_t max(const weight_t* x, int32_t nr) nogil:
        if nr == 0:
            return 0
        cdef int i
        cdef weight_t mode = x[0]
        for i in range(1, nr):
            if x[i] > mode:
                mode = x[i]
        return mode

    @staticmethod
    cdef inline weight_t sum(const weight_t* vec, int32_t nr) nogil:
        cdef int i
        cdef weight_t total = 0
        for i in range(nr):
            total += vec[i]
        return total

    @staticmethod
    cdef inline weight_t norm(const weight_t* vec, int32_t nr) nogil:
        cdef weight_t total = 0
        for i in range(nr):
            total += vec[i] ** 2
        return sqrt(total)

    @staticmethod
    cdef inline void add(weight_t* output, const weight_t* x,
            weight_t inc, int32_t nr) nogil:
        memcpy(output, x, sizeof(output[0]) * nr)
        Vec.add_i(output, inc, nr)

    @staticmethod
    cdef inline void add_i(weight_t* vec, weight_t inc, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            vec[i] += inc

    @staticmethod
    cdef inline void mul(weight_t* output, const weight_t* vec, weight_t scal,
            int32_t nr) nogil:
        memcpy(output, vec, sizeof(output[0]) * nr)
        Vec.mul_i(output, scal, nr)

    @staticmethod
    cdef inline void mul_i(weight_t* vec, weight_t scal, int32_t nr) nogil:
        cdef int i
        IF USE_BLAS:
            blis.cy.scalv(BLIS_NO_CONJUGATE, nr, scal, vec, 1)
        ELSE:
            for i in range(nr):
                vec[i] *= scal

    @staticmethod
    cdef inline void pow(weight_t* output, const weight_t* vec, weight_t scal,
            int32_t nr) nogil:
        memcpy(output, vec, sizeof(output[0]) * nr)
        Vec.pow_i(output, scal, nr)

    @staticmethod
    cdef inline void pow_i(weight_t* vec, const weight_t scal, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            vec[i] **= scal

    @staticmethod
    @cython.cdivision(True)
    cdef inline void div(weight_t* output, const weight_t* vec, weight_t scal,
            int32_t nr) nogil:
        memcpy(output, vec, sizeof(output[0]) * nr)
        Vec.div_i(output, scal, nr)

    @staticmethod
    @cython.cdivision(True)
    cdef inline void div_i(weight_t* vec, const weight_t scal, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            vec[i] /= scal

    @staticmethod
    cdef inline void exp(weight_t* output, const weight_t* vec, int32_t nr) nogil:
        memcpy(output, vec, sizeof(output[0]) * nr)
        Vec.exp_i(output, nr)

    @staticmethod
    cdef inline void exp_i(weight_t* vec, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            vec[i] = exp(vec[i])

    @staticmethod
    cdef inline void reciprocal_i(weight_t* vec, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            vec[i] = 1.0 / vec[i]

    @staticmethod
    cdef inline weight_t mean(const weight_t* X, int32_t nr_dim) nogil:
        cdef weight_t mean = 0.
        for x in X[:nr_dim]:
            mean += x
        return mean / nr_dim

    @staticmethod
    cdef inline weight_t variance(const weight_t* X, int32_t nr_dim) nogil:
        # See https://www.johndcook.com/blog/standard_deviation/
        cdef double m = X[0]
        cdef double v = 0.
        for i in range(1, nr_dim):
            diff = X[i]-m
            m += diff / (i+1)
            v += diff * (X[i] - m)
        return v / nr_dim


cdef class VecVec:
    @staticmethod
    cdef inline void add(weight_t* output,
                         const weight_t* x, 
                         const weight_t* y,
                         weight_t scale,
                         int32_t nr) nogil:
        memcpy(output, x, sizeof(output[0]) * nr)
        VecVec.add_i(output, y, scale, nr)
   
    @staticmethod
    cdef inline void add_i(weight_t* x, 
                           const weight_t* y,
                           weight_t scale,
                           int32_t nr) nogil:
        cdef int i
        IF USE_BLAS:
            blis.cy.axpyv(BLIS_NO_CONJUGATE, nr, scale, y, 1, x, 1)
        ELSE:
            for i in range(nr):
                x[i] += y[i] * scale
    
    @staticmethod
    cdef inline void batch_add_i(weight_t* x, 
                           const weight_t* y,
                           weight_t scale,
                           int32_t nr, int32_t nr_batch) nogil:
        # For fixed x, matrix of y
        cdef int i, _
        for _ in range(nr_batch):
            VecVec.add_i(x,
                y, scale, nr)
            y += nr
 
    @staticmethod
    cdef inline void add_pow(weight_t* output,
            const weight_t* x, const weight_t* y, weight_t power, int32_t nr) nogil:
        memcpy(output, x, sizeof(output[0]) * nr)
        VecVec.add_pow_i(output, y, power, nr)

   
    @staticmethod
    cdef inline void add_pow_i(weight_t* x, 
            const weight_t* y, weight_t power, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            x[i] += y[i] ** power
 
    @staticmethod
    cdef inline void mul(weight_t* output,
            const weight_t* x, const weight_t* y, int32_t nr) nogil:
        memcpy(output, x, sizeof(output[0]) * nr)
        VecVec.mul_i(output, y, nr)
   
    @staticmethod
    cdef inline void mul_i(weight_t* x, 
            const weight_t* y, int32_t nr) nogil:
        cdef int i
        for i in range(nr):
            x[i] *= y[i]

    @staticmethod
    cdef inline weight_t dot(
            const weight_t* x, const weight_t* y, int32_t nr) nogil:
        cdef int i
        cdef weight_t total = 0
        for i in range(nr):
            total += x[i] * y[i]
        return total
 
    @staticmethod
    cdef inline int arg_max_if_true(
            const weight_t* scores, const int* is_valid, const int n_classes) nogil:
        cdef int i
        cdef int best = -1
        for i in range(n_classes):
            if is_valid[i] and (best == -1 or scores[i] > scores[best]):
                best = i
        return best

    @staticmethod
    cdef inline int arg_max_if_zero(
            const weight_t* scores, const weight_t* costs, const int n_classes) nogil:
        cdef int i
        cdef int best = -1
        for i in range(n_classes):
            if costs[i] == 0 and (best == -1 or scores[i] > scores[best]):
                best = i
        return best


cdef class Mat:
    @staticmethod
    cdef inline void mean_row(weight_t* Ex,
            const weight_t* mat, int32_t nr_row, int32_t nr_col) nogil:
        memset(Ex, 0, sizeof(Ex[0]) * nr_col)
        for i in range(nr_row):
            VecVec.add_i(Ex, &mat[i * nr_col], 1.0, nr_col)
        Vec.mul_i(Ex, 1.0 / nr_row, nr_col)

    @staticmethod
    cdef inline void var_row(weight_t* Vx,
            const weight_t* mat, const weight_t* Ex,
            int32_t nr_row, int32_t nr_col, weight_t eps) nogil:
        # From https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        if nr_row == 0 or nr_col == 0:
            return
        cdef weight_t sum_, sum2
        for i in range(nr_col):
            sum_ = 0.0
            sum2 = 0.0
            for j in range(nr_row):
                x = mat[j * nr_col + i]
                sum2 += (x - Ex[i]) ** 2
                sum_ += x - Ex[i]
            Vx[i] = (sum2 - sum_**2 / nr_row) / nr_row
            Vx[i] += eps
