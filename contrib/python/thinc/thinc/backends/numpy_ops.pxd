from .cblas cimport saxpy_ptr

ctypedef double[:, ::1] double2d_t
ctypedef double[:, :, ::1] double3d_t
ctypedef float[:, ::1] float2d_t
ctypedef float[:, :, ::1] float3d_t
ctypedef int[:, ::1] int2d_t
ctypedef unsigned int[:, ::1] uint2d_t

cdef fused ints2d_ft:
    int2d_t
    uint2d_t

cdef fused reals2d_ft:
    float2d_t
    double2d_t

cdef fused reals3d_ft:
    float3d_t
    double3d_t


cdef extern from "cpu_kernels.hh":
    cdef cppclass axpy[T]:
        ctypedef void (*ptr)(int N, T alpha, const T* X, int incX, T *Y, int incY);

    void cpu_maxout[A, L](A* best__bo, L* which__bo, const A* cands_bop,
        L B, L O, L P)
    void cpu_backprop_maxout[A, L](A* dX__bop, const A* dX__bo, const L* which__bo,
        L B, L O, L P) except +
    void cpu_reduce_max[A, L](A* maxes__bo, L* which_bo, const A* X__to,
        const L* lengths__b, L B, L T, L O) except +

    void cpu_backprop_reduce_max[A, L](A* dX__to, const A* d_maxes__bo, const L* which__bo,
        const L* lengths__b, L B, L T, L O) except +
    void cpu_reduce_mean[A, L](A* means__bo, const A* X__to, const L* lengths__b,
        L B, L T, L O) except +
    void cpu_backprop_reduce_mean[A, L](A* dX__to, const A* d_means__bo, const L* lengths__b,
        L B, L T, L O)
    void cpu_mish[A, L](A* Y, L N, A threshold)
    void cpu_backprop_mish[A, L](A* dX, const A* X, L N, A threshold)
    void cpu_reduce_sum[A, L](A* sums__bo, const A* X__to, const L* lengths__b,
        L B, L T, L O) except +
    void cpu_backprop_reduce_sum[A, L](A* dX__to, const A* d_sums__bo, const L* lengths__b,
        L B, L T, L O)
    void cpu_relu[A, L](A* X, L N)
    void backprop_seq2col[A, L](A* d_seqs, const A* d_cols, const L* lengths, L B, L I, L nW, L nL)
    void seq2col[A, L](A* output, const A* X, const L* lengths, L nW, L B, L I, L nL)
    void cpu_gather_add[F, I, L](axpy[F].ptr axpy, F* out_bo, const F* table_to, const I* indices_bk,
                                 L T, L O, L B, L K) except +
