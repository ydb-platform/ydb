# Copyright ExplsionAI GmbH, released under BSD.
from cython cimport view
from libc.stdint cimport int64_t


ctypedef float[::1] float1d_t
ctypedef double[::1] double1d_t
ctypedef float[:, ::1] float2d_t
ctypedef double[:, ::1] double2d_t
ctypedef float* floats_t
ctypedef double* doubles_t
ctypedef const float[::1] const_float1d_t
ctypedef const double[::1] const_double1d_t
ctypedef const float[:, ::1] const_float2d_t
ctypedef const double[:, ::1] const_double2d_t
ctypedef const float* const_floats_t
ctypedef const double* const_doubles_t



cdef fused reals_ft:
    floats_t
    doubles_t
    float1d_t
    double1d_t

cdef fused const_reals_ft:
    const_floats_t
    const_doubles_t
    const_float1d_t
    const_double1d_t


cdef fused reals1d_ft:
    float1d_t
    double1d_t

cdef fused const_reals1d_ft:
    const_float1d_t
    const_double1d_t


cdef fused reals2d_ft:
    float2d_t
    double2d_t


cdef fused const_reals2d_ft:
    const_float2d_t
    const_double2d_t


cdef fused real_ft:
    float
    double


ctypedef int64_t dim_t
ctypedef int64_t inc_t
ctypedef int64_t doff_t


# Sucks to set these from magic numbers, but it's better than dragging
# the header into our header.
# We get some piece of mind from checking the values on init.
cpdef enum trans_t:
    NO_TRANSPOSE = 0
    TRANSPOSE = 8
    CONJ_NO_TRANSPOSE = 16
    CONJ_TRANSPOSE = 24


cpdef enum conj_t:
    NO_CONJUGATE = 0
    CONJUGATE = 16


cpdef enum side_t:
    LEFT = 0
    RIGHT = 1


cpdef enum uplo_t:
    LOWER = 192
    UPPER = 96
    DENSE = 224


cpdef enum diag_t:
    NONUNIT_DIAG = 0
    UNIT_DIAG = 256


cdef void gemm(
    trans_t transa,
    trans_t transb,
    dim_t   m,
    dim_t   n,
    dim_t   k,
    double  alpha,
    reals_ft  a, inc_t rsa, inc_t csa,
    reals_ft  b, inc_t rsb, inc_t csb,
    double  beta,
    reals_ft  c, inc_t rsc, inc_t csc,
) noexcept nogil


cdef void ger(
    conj_t  conjx,
    conj_t  conjy,
    dim_t   m,
    dim_t   n,
    double  alpha,
    reals_ft  x, inc_t incx,
    reals_ft  y, inc_t incy,
    reals_ft  a, inc_t rsa, inc_t csa
) noexcept nogil


cdef void gemv(
    trans_t transa,
    conj_t  conjx,
    dim_t   m,
    dim_t   n,
    real_ft  alpha,
    reals_ft  a, inc_t rsa, inc_t csa,
    reals_ft  x, inc_t incx,
    real_ft  beta,
    reals_ft  y, inc_t incy
) noexcept nogil


cdef void axpyv(
    conj_t  conjx,
    dim_t   m,
    real_ft  alpha,
    reals_ft  x, inc_t incx,
    reals_ft  y, inc_t incy
) noexcept nogil


cdef void scalv(
    conj_t  conjalpha,
    dim_t   m,
    real_ft  alpha,
    reals_ft  x, inc_t incx
) noexcept nogil


cdef double dotv(
    conj_t  conjx,
    conj_t  conjy,
    dim_t   m,
    reals_ft x,
    reals_ft y,
    inc_t incx,
    inc_t incy,
) noexcept nogil


cdef double norm_L1(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil


cdef double norm_L2(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil


cdef double norm_inf(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil


cdef void randv(
    dim_t m,
    reals_ft x, inc_t incx
) noexcept nogil


cdef void dgemm(bint transA, bint transB, int M, int N, int K,
                double alpha, const double* A, int lda, const double* B,
                int ldb, double beta, double* C, int ldc) noexcept nogil


cdef void sgemm(bint transA, bint transB, int M, int N, int K,
                float alpha, const float* A, int lda, const float* B,
                int ldb, float beta, float* C, int ldc) noexcept nogil


cdef void daxpy(int N, double alpha, const double* X, int incX,
                double* Y, int incY) noexcept nogil


cdef void saxpy(int N, float alpha, const float* X, int incX,
                float* Y, int incY) noexcept nogil
