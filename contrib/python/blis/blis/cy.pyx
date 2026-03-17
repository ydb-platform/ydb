# cython: infer_types=True
# cython: boundscheck=False
# Copyright ExplsionAI GmbH, released under BSD.

import atexit


cdef extern from "blis.h" nogil:
    enum blis_err_t "err_t":
        pass


    cdef struct blis_cntx_t "cntx_t":
        pass

    cdef struct blis_rntm_t "rntm_s":
        pass


    ctypedef enum blis_trans_t "trans_t":
        BLIS_NO_TRANSPOSE
        BLIS_TRANSPOSE
        BLIS_CONJ_NO_TRANSPOSE
        BLIS_CONJ_TRANSPOSE

    ctypedef enum blis_conj_t "conj_t":
        BLIS_NO_CONJUGATE
        BLIS_CONJUGATE

    ctypedef enum blis_side_t "side_t":
        BLIS_LEFT
        BLIS_RIGHT

    ctypedef enum blis_uplo_t "uplo_t":
        BLIS_LOWER
        BLIS_UPPER
        BLIS_DENSE

    ctypedef enum blis_diag_t "diag_t":
        BLIS_NONUNIT_DIAG
        BLIS_UNIT_DIAG

    char* bli_info_get_int_type_size_str()

    blis_err_t bli_init()
    blis_err_t bli_finalize()

    blis_err_t bli_rntm_init(blis_rntm_t* rntm);

    # BLAS level 3 routines
    void bli_dgemm_ex(
       blis_trans_t transa,
       blis_trans_t transb,
       dim_t   m,
       dim_t   n,
       dim_t   k,
       double*  alpha,
       double*  a, inc_t rsa, inc_t csa,
       double*  b, inc_t rsb, inc_t csb,
       double*  beta,
       double*  c, inc_t rsc, inc_t csc,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil
    # BLAS level 3 routines
    void bli_sgemm_ex(
       blis_trans_t transa,
       blis_trans_t transb,
       dim_t   m,
       dim_t   n,
       dim_t   k,
       float*  alpha,
       float*  a, inc_t rsa, inc_t csa,
       float*  b, inc_t rsb, inc_t csb,
       float*  beta,
       float*  c, inc_t rsc, inc_t csc,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dger_ex(
       blis_conj_t  conjx,
       blis_conj_t  conjy,
       dim_t   m,
       dim_t   n,
       double*  alpha,
       double*  x, inc_t incx,
       double*  y, inc_t incy,
       double*  a, inc_t rsa, inc_t csa,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_sger_ex(
       blis_conj_t  conjx,
       blis_conj_t  conjy,
       dim_t   m,
       dim_t   n,
       float*  alpha,
       float*  x, inc_t incx,
       float*  y, inc_t incy,
       float*  a, inc_t rsa, inc_t csa,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dgemv_ex(
       blis_trans_t transa,
       blis_conj_t  conjx,
       dim_t   m,
       dim_t   n,
       double*  alpha,
       double*  a, inc_t rsa, inc_t csa,
       double*  x, inc_t incx,
       double*  beta,
       double*  y, inc_t incy,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
     ) noexcept nogil

    void bli_sgemv_ex(
       blis_trans_t transa,
       blis_conj_t  conjx,
       dim_t   m,
       dim_t   n,
       float*  alpha,
       float*  a, inc_t rsa, inc_t csa,
       float*  x, inc_t incx,
       float*  beta,
       float*  y, inc_t incy,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
     ) noexcept nogil

    void bli_daxpyv_ex(
       blis_conj_t  conjx,
       dim_t   m,
       double*  alpha,
       double*  x, inc_t incx,
       double*  y, inc_t incy,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
     ) noexcept nogil

    void bli_saxpyv_ex(
       blis_conj_t  conjx,
       dim_t   m,
       float*  alpha,
       float*  x, inc_t incx,
       float*  y, inc_t incy,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
     ) noexcept nogil

    void bli_dscalv_ex(
       blis_conj_t  conjalpha,
       dim_t   m,
       double*  alpha,
       double*  x, inc_t incx,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_sscalv_ex(
       blis_conj_t  conjalpha,
       dim_t   m,
       float*  alpha,
       float*  x, inc_t incx,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_ddotv_ex(
       blis_conj_t  conjx,
       blis_conj_t  conjy,
       dim_t   m,
       double*  x, inc_t incx,
       double*  y, inc_t incy,
       double*  rho,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_sdotv_ex(
       blis_conj_t  conjx,
       blis_conj_t  conjy,
       dim_t   m,
       float*  x, inc_t incx,
       float*  y, inc_t incy,
       float*  rho,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_snorm1v_ex(
       dim_t n,
       float* x, inc_t incx,
       float* norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dnorm1v_ex(
       dim_t   n,
       double* x, inc_t incx,
       double* norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_snormfv_ex(
       dim_t   n,
       float*  x, inc_t incx,
       float*  norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dnormfv_ex(
       dim_t   n,
       double*  x, inc_t incx,
       double*  norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_snormiv_ex(
       dim_t   n,
       float*  x, inc_t incx,
       float*  norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dnormiv_ex(
       dim_t   n,
       double*  x, inc_t incx,
       double*  norm,
       blis_cntx_t* cntx,
       blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_srandv_ex(
        dim_t   m,
        float*  x, inc_t incx,
        blis_cntx_t* cntx,
        blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_drandv_ex(
        dim_t   m,
        double*  x, inc_t incx,
        blis_cntx_t* cntx,
        blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_ssumsqv_ex(
        dim_t   m,
        float*  x, inc_t incx,
        float*  scale,
        float*  sumsq,
        blis_cntx_t* cntx,
        blis_rntm_t* rntm,
    ) noexcept nogil

    void bli_dsumsqv_ex(
        dim_t   m,
        double*  x, inc_t incx,
        double*  scale,
        double*  sumsq,
        blis_cntx_t* cntx,
        blis_rntm_t* rntm,
    ) noexcept nogil



bli_init()
cdef blis_rntm_t rntm;

def init():
    bli_init()
    bli_rntm_init(&rntm);
    assert BLIS_NO_TRANSPOSE == <blis_trans_t>NO_TRANSPOSE
    assert BLIS_TRANSPOSE == <blis_trans_t>TRANSPOSE
    assert BLIS_CONJ_NO_TRANSPOSE == <blis_trans_t>CONJ_NO_TRANSPOSE
    assert BLIS_CONJ_TRANSPOSE == <blis_trans_t>CONJ_TRANSPOSE
    assert BLIS_NO_CONJUGATE == <blis_conj_t>NO_CONJUGATE
    assert BLIS_CONJUGATE == <blis_conj_t>CONJUGATE
    assert BLIS_LEFT == <blis_side_t>LEFT
    assert BLIS_RIGHT == <blis_side_t>RIGHT
    assert BLIS_LOWER == <blis_uplo_t>LOWER
    assert BLIS_UPPER == <blis_uplo_t>UPPER
    assert BLIS_DENSE == <blis_uplo_t>DENSE
    assert BLIS_NONUNIT_DIAG == <blis_diag_t>NONUNIT_DIAG
    assert BLIS_UNIT_DIAG == <blis_diag_t>UNIT_DIAG


def get_int_type_size():
    cdef char* int_size = bli_info_get_int_type_size_str()
    return '%d' % int_size[0]


# BLAS level 3 routines
cdef void gemm(
    trans_t trans_a,
    trans_t trans_b,
    dim_t   m,
    dim_t   n,
    dim_t   k,
    double  alpha,
    reals_ft  a, inc_t rsa, inc_t csa,
    reals_ft  b, inc_t rsb, inc_t csb,
    double  beta,
    reals_ft  c, inc_t rsc, inc_t csc
) noexcept nogil:
    cdef float alpha_f = alpha
    cdef float beta_f = beta
    cdef double alpha_d = alpha
    cdef double beta_d = beta
    if reals_ft is floats_t:
        bli_sgemm_ex(
            <blis_trans_t>trans_a, <blis_trans_t>trans_b,
            m, n, k,
            &alpha_f, a, rsa, csa, b, rsb, csb, &beta_f, c, rsc, csc, NULL, &rntm)
    elif reals_ft is doubles_t:
        bli_dgemm_ex(
            <blis_trans_t>trans_a, <blis_trans_t>trans_b,
            m, n, k,
            &alpha_d, a, rsa, csa, b, rsb, csb, &beta_d, c, rsc, csc, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_sgemm_ex(
            <blis_trans_t>trans_a, <blis_trans_t>trans_b,
            m, n, k,
            &alpha_f, &a[0], rsa, csa, &b[0], rsb, csb, &beta_f, &c[0],
            rsc, csc, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_dgemm_ex(
            <blis_trans_t>trans_a, <blis_trans_t>trans_b,
            m, n, k,
            &alpha_d, &a[0], rsa, csa, &b[0], rsb, csb, &beta_d, &c[0],
            rsc, csc, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass


cdef void ger(
    conj_t  conjx,
    conj_t  conjy,
    dim_t   m,
    dim_t   n,
    double  alpha,
    reals_ft  x, inc_t incx,
    reals_ft  y, inc_t incy,
    reals_ft  a, inc_t rsa, inc_t csa
) noexcept nogil:
    cdef float alpha_f = alpha
    cdef double alpha_d = alpha
    if reals_ft is floats_t:
        bli_sger_ex(
            <blis_conj_t>conjx, <blis_conj_t>conjy,
            m, n,
            &alpha_f,
            x, incx, y, incy, a, rsa, csa, NULL, &rntm)
    elif reals_ft is doubles_t:
        bli_dger_ex(
            <blis_conj_t>conjx, <blis_conj_t>conjy,
            m, n,
            &alpha_d,
            x, incx, y, incy, a, rsa, csa, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_sger_ex(
            <blis_conj_t>conjx, <blis_conj_t>conjy,
            m, n,
            &alpha_f,
            &x[0], incx, &y[0], incy, &a[0], rsa, csa, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_dger_ex(
            <blis_conj_t>conjx, <blis_conj_t>conjy,
            m, n,
            &alpha_d,
            &x[0], incx, &y[0], incy, &a[0], rsa, csa, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass


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
) noexcept nogil:
    cdef float alpha_f = alpha
    cdef double alpha_d = alpha
    cdef float beta_f = alpha
    cdef double beta_d = alpha
    if reals_ft is floats_t:
        bli_sgemv_ex(
            <blis_trans_t>transa, <blis_conj_t>conjx,
            m, n,
            &alpha_f, a, rsa, csa,
            x, incx, &beta_f,
            y, incy, NULL, &rntm)
    elif reals_ft is doubles_t:
        bli_dgemv_ex(
            <blis_trans_t>transa, <blis_conj_t>conjx,
            m, n,
            &alpha_d, a, rsa, csa,
            x, incx, &beta_d,
            y, incy, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_sgemv_ex(
            <blis_trans_t>transa, <blis_conj_t>conjx,
            m, n,
            &alpha_f, &a[0], rsa, csa,
            &x[0], incx, &beta_f,
            &y[0], incy, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_dgemv_ex(
            <blis_trans_t>transa, <blis_conj_t>conjx,
            m, n,
            &alpha_d, &a[0], rsa, csa,
            &x[0], incx, &beta_d,
            &y[0], incy, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass


cdef void axpyv(
    conj_t  conjx,
    dim_t   m,
    real_ft  alpha,
    reals_ft  x, inc_t incx,
    reals_ft  y, inc_t incy
) noexcept nogil:
    cdef float alpha_f = alpha
    cdef double alpha_d = alpha
    if reals_ft is floats_t:
        bli_saxpyv_ex(<blis_conj_t>conjx, m,  &alpha_f, x, incx, y, incy, NULL, &rntm)
    elif reals_ft is doubles_t:
        bli_daxpyv_ex(<blis_conj_t>conjx, m,  &alpha_d, x, incx, y, incy, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_saxpyv_ex(<blis_conj_t>conjx, m,  &alpha_f, &x[0], incx, &y[0], incy, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_daxpyv_ex(<blis_conj_t>conjx, m,  &alpha_d, &x[0], incx, &y[0], incy, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass


cdef void scalv(
    conj_t  conjalpha,
    dim_t   m,
    real_ft  alpha,
    reals_ft  x, inc_t incx
) noexcept nogil:
    cdef float alpha_f = alpha
    cdef double alpha_d = alpha
    if reals_ft is floats_t:
        bli_sscalv_ex(<blis_conj_t>conjalpha, m, &alpha_f, x, incx, NULL, &rntm)
    elif reals_ft is doubles_t:
        bli_dscalv_ex(<blis_conj_t>conjalpha, m, &alpha_d, x, incx, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_sscalv_ex(<blis_conj_t>conjalpha, m, &alpha_f, &x[0], incx, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_dscalv_ex(<blis_conj_t>conjalpha, m, &alpha_d, &x[0], incx, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass


cdef double norm_L1(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil:
    cdef double dnorm = 0
    cdef float snorm = 0
    if reals_ft is floats_t:
        bli_snorm1v_ex(n, x, incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is doubles_t:
        bli_dnorm1v_ex(n, x, incx, &dnorm, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_snorm1v_ex(n, &x[0], incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is double1d_t:
        bli_dnorm1v_ex(n, &x[0], incx, &dnorm, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass
    return dnorm


cdef double norm_L2(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil:
    cdef double dnorm = 0
    cdef float snorm = 0
    if reals_ft is floats_t:
        bli_snormfv_ex(n, x, incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is doubles_t:
        bli_dnormfv_ex(n, x, incx, &dnorm, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_snormfv_ex(n, &x[0], incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is double1d_t:
        bli_dnormfv_ex(n, &x[0], incx, &dnorm, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass
    return dnorm


cdef double norm_inf(
    dim_t n,
    reals_ft x, inc_t incx
) noexcept nogil:
    cdef double dnorm = 0
    cdef float snorm = 0
    if reals_ft is floats_t:
        bli_snormiv_ex(n, x, incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is doubles_t:
        bli_dnormiv_ex(n, x, incx, &dnorm, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_snormiv_ex(n, &x[0], incx, &snorm, NULL, &rntm)
        dnorm = snorm
    elif reals_ft is double1d_t:
        bli_dnormiv_ex(n, &x[0], incx, &dnorm, NULL, &rntm)
    else:
        # Impossible --- panic?
        pass
    return dnorm


cdef double dotv(
    conj_t  conjx,
    conj_t  conjy,
    dim_t   m,
    reals_ft x,
    reals_ft y,
    inc_t incx,
    inc_t incy,
) noexcept nogil:
    cdef double rho_d = 0.0
    cdef float rho_f = 0.0
    if reals_ft is floats_t:
        bli_sdotv_ex(<blis_conj_t>conjx, <blis_conj_t>conjy, m, x, incx, y, incy, &rho_f, NULL, &rntm)
        return rho_f
    elif reals_ft is doubles_t:
        bli_ddotv_ex(<blis_conj_t>conjx, <blis_conj_t>conjy, m, x, incx, y, incy, &rho_d, NULL, &rntm)
        return rho_d
    elif reals_ft is float1d_t:
        bli_sdotv_ex(<blis_conj_t>conjx, <blis_conj_t>conjy, m, &x[0], incx, &y[0], incy,
                  &rho_f, NULL, &rntm)
        return rho_f
    elif reals_ft is double1d_t:
        bli_ddotv_ex(<blis_conj_t>conjx, <blis_conj_t>conjy, m, &x[0], incx, &y[0], incy,
                  &rho_d, NULL, &rntm)
        return rho_d
    else:
        raise ValueError("Unhandled fused type")


cdef void randv(dim_t m, reals_ft x, inc_t incx) noexcept nogil:
    if reals_ft is floats_t:
        bli_srandv_ex(m, x, incx, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_srandv_ex(m, &x[0], incx, NULL, &rntm)
    if reals_ft is doubles_t:
        bli_drandv_ex(m, x, incx, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_drandv_ex(m, &x[0], incx, NULL, &rntm)
    else:
        with gil:
            raise ValueError("Unhandled fused type")


cdef void sumsqv(dim_t   m, reals_ft  x, inc_t incx,
        reals_ft scale, reals_ft sumsq) noexcept nogil:
    if reals_ft is floats_t:
        bli_ssumsqv_ex(m, &x[0], incx, scale, sumsq, NULL, &rntm)
    elif reals_ft is float1d_t:
        bli_ssumsqv_ex(m, &x[0], incx, &scale[0], &sumsq[0], NULL, &rntm)
    if reals_ft is doubles_t:
        bli_dsumsqv_ex(m, x, incx, scale, sumsq, NULL, &rntm)
    elif reals_ft is double1d_t:
        bli_dsumsqv_ex(m, &x[0], incx, &scale[0], &sumsq[0], NULL, &rntm)
    else:
        with gil:
            raise ValueError("Unhandled fused type")


cdef void dgemm(bint transA, bint transB, int M, int N, int K,
                double alpha, const double* A, int lda, const double* B,
                int ldb, double beta, double* C, int ldc) noexcept nogil:
    gemm(
        TRANSPOSE if transA else NO_TRANSPOSE,
        TRANSPOSE if transB else NO_TRANSPOSE,
        M, N, K,
        alpha, A, lda, 1,
        B, ldb, 1,
        beta, C, ldc, 1
    )


cdef void sgemm(bint transA, bint transB, int M, int N, int K,
                float alpha, const float* A, int lda, const float* B,
                int ldb, float beta, float* C, int ldc) noexcept nogil:
    gemm(
        TRANSPOSE if transA else NO_TRANSPOSE,
        TRANSPOSE if transB else NO_TRANSPOSE,
        M, N, K,
        alpha, A, lda, 1,
        B, ldb, 1,
        beta, C, ldc, 1
    )


cdef void saxpy(int N, float alpha, const float* X, int incX,
                float* Y, int incY) noexcept nogil:
    axpyv(NO_CONJUGATE, N, alpha, X, incX, Y, incY)


cdef void daxpy(int N, double alpha, const double* X, int incX,
                double* Y, int incY) noexcept nogil:
    axpyv(NO_CONJUGATE, N, alpha, X, incX, Y, incY)


@atexit.register
def finalize():
    bli_finalize()
