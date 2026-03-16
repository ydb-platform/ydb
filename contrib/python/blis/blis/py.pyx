# cython: boundscheck=False
# Copyright ExplsionAI GmbH, released under BSD.
cimport numpy as np
from . cimport cy
from .cy cimport reals1d_ft, reals2d_ft, float1d_t, float2d_t
from .cy cimport const_reals1d_ft, const_reals2d_ft, const_float1d_t, const_float2d_t
from .cy cimport const_double1d_t, const_double2d_t

import numpy


def axpy(const_reals1d_ft A, double scale=1., np.ndarray out=None):
    if const_reals1d_ft is const_float1d_t:
        if out is None:
            out = numpy.zeros((A.shape[0],), dtype='f')
        B = <float*>out.data
        return out
    elif const_reals1d_ft is const_double1d_t:
        if out is None:
            out = numpy.zeros((A.shape[0],), dtype='d')
        B = <double*>out.data
        with nogil:
            cy.axpyv(cy.NO_CONJUGATE, A.shape[0], scale, &A[0], 1, B, 1)
        return out
    else:
        B = NULL
        raise TypeError("Unhandled fused type")


def batch_axpy(reals2d_ft A, reals1d_ft B, np.ndarray out=None):
    pass


def ger(const_reals2d_ft A, const_reals1d_ft B, double scale=1., np.ndarray out=None):
    if const_reals2d_ft is const_float2d_t and const_reals1d_ft is const_float1d_t:
        if out is None:
            out = numpy.zeros((A.shape[0], B.shape[0]), dtype='f')
        with nogil:
            cy.ger(
                cy.NO_CONJUGATE, cy.NO_CONJUGATE,
                A.shape[0], B.shape[0],
                scale,
                &A[0,0], 1,
                &B[0], 1,
                <float*>out.data, out.shape[1], 1)
        return out
    elif const_reals2d_ft is const_double2d_t and const_reals1d_ft is const_double1d_t:
        if out is None:
            out = numpy.zeros((A.shape[0], B.shape[0]), dtype='d')
        with nogil:
            cy.ger(
                cy.NO_CONJUGATE, cy.NO_CONJUGATE,
                A.shape[0], B.shape[0],
                scale,
                &A[0,0], 1,
                &B[0], 1,
                <double*>out.data, out.shape[1], 1)
        return out
    else:
        C = NULL
        raise TypeError("Unhandled fused type")


def gemm(const_reals2d_ft A, const_reals2d_ft B,
         np.ndarray out=None, bint trans1=False, bint trans2=False,
         double alpha=1., double beta=1.):
    cdef cy.dim_t nM = A.shape[0] if not trans1 else A.shape[1]
    cdef cy.dim_t nK = A.shape[1] if not trans1 else A.shape[0]
    cdef cy.dim_t nK_b = B.shape[0] if not trans2 else B.shape[1]
    cdef cy.dim_t nN = B.shape[1] if not trans2 else B.shape[0]

    if nK != nK_b:
        msg = "Shape mismatch for blis.gemm: (%d, %d), (%d, %d)"
        raise ValueError(msg % (nM, nK, nK_b, nN))

    if const_reals2d_ft is const_float2d_t:
        if out is None:
            if beta == 0.:
                out = numpy.empty((nM, nN), dtype='f')
            else:
                out = numpy.zeros((nM, nN), dtype='f')
        C = <float*>out.data
        with nogil:
            cy.gemm(
                cy.TRANSPOSE if trans1 else cy.NO_TRANSPOSE,
                cy.TRANSPOSE if trans2 else cy.NO_TRANSPOSE,
                nM, nN, nK,
                alpha,
                &A[0,0], A.shape[1], 1,
                &B[0,0], B.shape[1], 1,
                beta,
                C, out.shape[1], 1)
        return out
    elif const_reals2d_ft is const_double2d_t:
        if out is None:
            out = numpy.zeros((A.shape[0], B.shape[1]), dtype='d')
        C = <double*>out.data
        with nogil:
            cy.gemm(
                cy.TRANSPOSE if trans1 else cy.NO_TRANSPOSE,
                cy.TRANSPOSE if trans2 else cy.NO_TRANSPOSE,
                A.shape[0], B.shape[1], A.shape[1],
                alpha,
                &A[0,0], A.shape[1], 1,
                &B[0,0], B.shape[1], 1,
                beta,
                C, out.shape[1], 1)
        return out
    else:
        C = NULL
        raise TypeError("Unhandled fused type")

def gemv(const_reals2d_ft A, const_reals1d_ft B,
        bint trans1=False, double alpha=1., double beta=1.,
        np.ndarray out=None):
    if const_reals1d_ft is const_float1d_t and const_reals2d_ft is const_float2d_t:
        if out is None:
            out = numpy.zeros((A.shape[0],), dtype='f')
        with nogil:
            cy.gemv(
                cy.TRANSPOSE if trans1 else cy.NO_TRANSPOSE,
                cy.NO_CONJUGATE,
                A.shape[0], A.shape[1],
                alpha,
                &A[0,0], A.shape[1], 1,
                &B[0], 1,
                beta,
                <float*>out.data, 1)
        return out
    elif const_reals1d_ft is const_double1d_t and const_reals2d_ft is const_double2d_t:
        if out is None:
            out = numpy.zeros((A.shape[0],), dtype='d')
        with nogil:
            cy.gemv(
                cy.TRANSPOSE if trans1 else cy.NO_TRANSPOSE,
                cy.NO_CONJUGATE,
                A.shape[0], A.shape[1],
                alpha,
                &A[0,0], A.shape[1], 1,
                &B[0], 1,
                beta,
                <double*>out.data, 1)
        return out
    else:
        raise TypeError("Unhandled fused type")


def dotv(const_reals1d_ft X, const_reals1d_ft Y, bint conjX=False, bint conjY=False):
    if X.shape[0] != Y.shape[0]:
        msg = "Shape mismatch for blis.dotv: (%d,), (%d,)"
        raise ValueError(msg % (X.shape[0], Y.shape[0]))
    return cy.dotv(
        cy.CONJUGATE if conjX else cy.NO_CONJUGATE,
        cy.CONJUGATE if conjY else cy.NO_CONJUGATE,
        X.shape[0], &X[0], &Y[0], 1, 1
    )


def einsum(todo, A, B, out=None):
    if todo == 'a,a->a':
        return axpy(A, B, out=out)
    elif todo == 'a,b->ab':
        return ger(A, B, out=out)
    elif todo == 'a,b->ba':
        return ger(B, A, out=out)
    elif todo == 'ab,a->ab':
        return batch_axpy(A, B, out=out)
    elif todo == 'ab,a->ba':
        return batch_axpy(A, B, trans1=True, out=out)
    elif todo == 'ab,b->a':
        return gemv(A, B, out=out)
    elif todo == 'ab,a->b':
        return gemv(A, B, trans1=True, out=out)
    # The rule here is, look at the first dimension of the output. That must
    # occur in arg1. Set trans1 if it's dimension 2.
    # E.g. bc is output, b occurs in ab, so that must be arg1. So we need
    # trans1=True, to make ba,ac->bc
    elif todo == 'ab,ac->bc':
        return gemm(A, B, trans1=True, trans2=False, out=out)
    elif todo == 'ab,ac->cb':
        return gemm(B, A, out=out, trans1=True, trans2=True)
    elif todo == 'ab,bc->ac':
        return gemm(A, B, out=out, trans1=False, trans2=False)
    elif todo == 'ab,bc->ca':
        return gemm(B, A, out=out, trans1=True, trans2=True)
    elif todo == 'ab,ca->bc':
        return gemm(A, B, out=out, trans1=True, trans2=True)
    elif todo == 'ab,ca->cb':
        return gemm(B, A, out=out, trans1=False, trans2=False)
    elif todo == 'ab,cb->ac':
        return gemm(A, B, out=out, trans1=False, trans2=True)
    elif todo == 'ab,cb->ca':
        return gemm(B, A, out=out, trans1=False, trans2=True)
    else:
        raise ValueError("Invalid einsum: %s" % todo)
