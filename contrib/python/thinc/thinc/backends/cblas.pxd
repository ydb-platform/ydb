from libcpp.memory cimport shared_ptr

ctypedef void (*sgemm_ptr)(bint transA, bint transB, int M, int N, int K,
                           float alpha, const float* A, int lda, const float *B,
                           int ldb, float beta, float* C, int ldc) noexcept nogil


ctypedef void (*saxpy_ptr)(int N, float alpha, const float* X, int incX,
                           float *Y, int incY) noexcept nogil


ctypedef void (*daxpy_ptr)(int N, double alpha, const double* X, int incX,
                           double *Y, int incY) noexcept nogil


# Forward-declaration of the BlasFuncs struct. This struct must be opaque, so
# that consumers of the CBlas class cannot become dependent on its size or
# ordering.
cdef struct BlasFuncs


cdef class CBlas:
    cdef shared_ptr[BlasFuncs] ptr


# Note: the following functions are intentionally standalone. If we make them
# methods of CBlas, Cython will generate and use a vtable. This makes it
# impossible to add new BLAS functions later without breaking the ABI.
#
# See https://github.com/explosion/thinc/pull/700 for more information.

cdef daxpy_ptr daxpy(CBlas cblas) noexcept nogil
cdef saxpy_ptr saxpy(CBlas cblas) noexcept nogil
cdef sgemm_ptr sgemm(CBlas cblas) noexcept nogil
cdef void set_daxpy(CBlas cblas, daxpy_ptr daxpy) noexcept nogil
cdef void set_saxpy(CBlas cblas, saxpy_ptr saxpy) noexcept nogil
cdef void set_sgemm(CBlas cblas, sgemm_ptr sgemm) noexcept nogil
