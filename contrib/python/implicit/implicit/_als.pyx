import numpy as np
import cython
from cython cimport floating, integral
from cython.parallel import parallel, prange
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset

# requires scipy v0.16
cimport scipy.linalg.cython_lapack as cython_lapack
cimport scipy.linalg.cython_blas as cython_blas

# lapack/blas wrappers for cython fused types
cdef inline void axpy(int * n, floating * da, floating * dx, int * incx, floating * dy,
                      int * incy) nogil:
    if floating is double:
        cython_blas.daxpy(n, da, dx, incx, dy, incy)
    else:
        cython_blas.saxpy(n, da, dx, incx, dy, incy)

cdef inline void symv(char *uplo, int *n, floating *alpha, floating *a, int *lda, floating *x,
                      int *incx, floating *beta, floating *y, int *incy) nogil:
    if floating is double:
        cython_blas.dsymv(uplo, n, alpha, a, lda, x, incx, beta, y, incy)
    else:
        cython_blas.ssymv(uplo, n, alpha, a, lda, x, incx, beta, y, incy)

cdef inline floating dot(int *n, floating *sx, int *incx, floating *sy, int *incy) nogil:
    if floating is double:
        return cython_blas.ddot(n, sx, incx, sy, incy)
    else:
        return cython_blas.sdot(n, sx, incx, sy, incy)

cdef inline void scal(int *n, floating *sa, floating *sx, int *incx) nogil:
    if floating is double:
        cython_blas.dscal(n, sa, sx, incx)
    else:
        cython_blas.sscal(n, sa, sx, incx)

cdef inline void posv(char * u, int * n, int * nrhs, floating * a, int * lda, floating * b,
                      int * ldb, int * info) nogil:
    if floating is double:
        cython_lapack.dposv(u, n, nrhs, a, lda, b, ldb, info)
    else:
        cython_lapack.sposv(u, n, nrhs, a, lda, b, ldb, info)

cdef inline void gesv(int * n, int * nrhs, floating * a, int * lda, int * piv, floating * b,
                      int * ldb, int * info) nogil:
    if floating is double:
        cython_lapack.dgesv(n, nrhs, a, lda, piv, b, ldb, info)
    else:
        cython_lapack.sgesv(n, nrhs, a, lda, piv, b, ldb, info)


def least_squares(Cui, X, Y, regularization, num_threads=0):
    _least_squares(Cui.indptr, Cui.indices, Cui.data.astype('float32'),
                   X, Y, regularization, num_threads)


@cython.boundscheck(False)
def _least_squares(integral[:] indptr, integral[:] indices, float[:] data,
                   floating[:, :] X, floating[:, :] Y, double regularization,
                   int num_threads=0):
    dtype = np.float64 if floating is double else np.float32

    cdef int users = X.shape[0], factors = X.shape[1], u, i, j, index, err, one = 1
    cdef floating confidence, temp

    YtY = np.dot(np.transpose(Y), Y)

    cdef floating[:, :] initialA = YtY + regularization * np.eye(factors, dtype=dtype)
    cdef floating[:] initialB = np.zeros(factors, dtype=dtype)

    cdef floating * A
    cdef floating * b

    with nogil, parallel(num_threads=num_threads):
        # allocate temp memory for each thread
        A = <floating *> malloc(sizeof(floating) * factors * factors)
        b = <floating *> malloc(sizeof(floating) * factors)
        try:
            for u in prange(users, schedule='guided'):
                # if we have no items for this user, skip and set to zero
                if indptr[u] == indptr[u+1]:
                    memset(&X[u, 0], 0, sizeof(floating) * factors)
                    continue

                # For each user u calculate
                # Xu = (YtCuY + regularization*I)i^-1 * YtYCuPu

                # Build up A = YtCuY + reg * I and b = YtCuPu
                memcpy(A, &initialA[0, 0], sizeof(floating) * factors * factors)
                memcpy(b, &initialB[0], sizeof(floating) * factors)

                for index in range(indptr[u], indptr[u+1]):
                    i = indices[index]
                    confidence = data[index]

                    # b += Yi Cui Pui
                    # Pui is implicit, its defined to be 1 for non-zero entries
                    if confidence > 0:
                        axpy(&factors, &confidence, &Y[i, 0], &one, b, &one)
                    else:
                        confidence = -1 * confidence

                    # A += Yi^T Cui Yi
                    # Since we've already added in YtY, we subtract 1 from confidence
                    for j in range(factors):
                        temp = (confidence - 1) * Y[i, j]
                        axpy(&factors, &temp, &Y[i, 0], &one, A + j * factors, &one)

                err = 0
                posv("U", &factors, &one, A, &factors, b, &factors, &err)

                if not err:
                    memcpy(&X[u, 0], b, sizeof(floating) * factors)
                else:
                    # I believe the only time this should happen is on trivial problems with no
                    # regularization being used (posv requires a positive semi-definite matrix,
                    # since a= Yt(Cu + regularization I)Y a is always positive. its also non
                    # zero if the regularization factor > 0.
                    with gil:
                        raise ValueError("cython_lapack.posv failed (err=%i) on row %i. Try "
                                         "increasing the regularization parameter." % (err, u))

        finally:
            free(A)
            free(b)


def least_squares_cg(Cui, X, Y, regularization, num_threads=0, cg_steps=3):
    return _least_squares_cg(Cui.indptr, Cui.indices, Cui.data.astype('float32'),
                             X, Y, regularization, num_threads, cg_steps)


@cython.cdivision(True)
@cython.boundscheck(False)
def _least_squares_cg(integral[:] indptr, integral[:] indices, float[:] data,
                      floating[:, :] X, floating[:, :] Y, float regularization,
                      int num_threads=0, int cg_steps=3):
    dtype = np.float64 if floating is double else np.float32

    cdef int users = X.shape[0], N = X.shape[1], u, i, index, one = 1, it
    cdef floating confidence, temp, alpha, rsnew, rsold
    cdef floating zero = 0.

    cdef floating[:, :] YtY = np.dot(np.transpose(Y), Y) + regularization * np.eye(N, dtype=dtype)

    cdef floating * x
    cdef floating * p
    cdef floating * r
    cdef floating * Ap

    with nogil, parallel(num_threads=num_threads):

        # allocate temp memory for each thread
        Ap = <floating *> malloc(sizeof(floating) * N)
        p = <floating *> malloc(sizeof(floating) * N)
        r = <floating *> malloc(sizeof(floating) * N)
        try:
            for u in prange(users, schedule='guided'):
                # start from previous iteration
                x = &X[u, 0]

                # if we have no items for this user, skip and set to zero
                if indptr[u] == indptr[u+1]:
                    memset(x, 0, sizeof(floating) * N)
                    continue

                # calculate residual r = (YtCuPu - (YtCuY.dot(Xu)
                temp = -1.0
                symv("U", &N, &temp, &YtY[0, 0], &N, x, &one, &zero, r, &one)

                for index in range(indptr[u], indptr[u + 1]):
                    i = indices[index]
                    confidence = data[index]

                    if confidence > 0:
                        temp = confidence
                    else:
                        temp = 0
                        confidence = -1 * confidence

                    temp = temp - (confidence - 1) * dot(&N, &Y[i, 0], &one, x, &one)
                    axpy(&N, &temp, &Y[i, 0], &one, r, &one)

                memcpy(p, r, sizeof(floating) * N)
                rsold = dot(&N, r, &one, r, &one)

                if rsold < 1e-20:
                    continue

                for it in range(cg_steps):
                    # calculate Ap = YtCuYp - without actually calculating YtCuY
                    temp = 1.0
                    symv("U", &N, &temp, &YtY[0, 0], &N, p, &one, &zero, Ap, &one)

                    for index in range(indptr[u], indptr[u + 1]):
                        i = indices[index]
                        confidence = data[index]

                        if confidence < 0:
                            confidence = -1 * confidence

                        temp = (confidence - 1) * dot(&N, &Y[i, 0], &one, p, &one)
                        axpy(&N, &temp, &Y[i, 0], &one, Ap, &one)

                    # alpha = rsold / p.dot(Ap);
                    alpha = rsold / dot(&N, p, &one, Ap, &one)

                    # x += alpha * p
                    axpy(&N, &alpha, p, &one, x, &one)

                    # r -= alpha * Ap
                    temp = alpha * -1
                    axpy(&N, &temp, Ap, &one, r, &one)

                    rsnew = dot(&N, r, &one, r, &one)
                    if rsnew < 1e-20:
                        break

                    # p = r + (rsnew/rsold) * p
                    temp = rsnew / rsold
                    scal(&N, &temp, p, &one)
                    temp = 1.0
                    axpy(&N, &temp, r, &one, p, &one)

                    rsold = rsnew
        finally:
            free(p)
            free(r)
            free(Ap)


def calculate_loss(Cui, X, Y, regularization, num_threads=0):
    return _calculate_loss(Cui, Cui.indptr, Cui.indices, Cui.data.astype('float32'),
                           X, Y, regularization, num_threads)


@cython.cdivision(True)
@cython.boundscheck(False)
def _calculate_loss(Cui, integral[:] indptr, integral[:] indices, float[:] data,
                    floating[:, :] X, floating[:, :] Y, float regularization,
                    int num_threads=0):
    dtype = np.float64 if floating is double else np.float32
    cdef int users = X.shape[0], N = X.shape[1], items = Y.shape[0], u, i, index, one = 1
    cdef floating confidence, temp
    cdef floating zero = 0.

    cdef floating[:, :] YtY = np.dot(np.transpose(Y), Y)

    cdef floating * r

    cdef double loss = 0, total_confidence = 0, item_norm = 0, user_norm = 0

    with nogil, parallel(num_threads=num_threads):
        r = <floating *> malloc(sizeof(floating) * N)
        try:
            for u in prange(users, schedule='guided'):
                # calculates (A.dot(Xu) - 2 * b).dot(Xu), without calculating A
                temp = 1.0
                symv("U", &N, &temp, &YtY[0, 0], &N, &X[u, 0], &one, &zero, r, &one)

                for index in range(indptr[u], indptr[u + 1]):
                    i = indices[index]
                    confidence = data[index]

                    if confidence > 0:
                        temp = -2 * confidence
                    else:
                        temp = 0
                        confidence = -1 * confidence

                    temp = temp + (confidence - 1) * dot(&N, &Y[i, 0], &one, &X[u, 0], &one)
                    axpy(&N, &temp, &Y[i, 0], &one, r, &one)

                    total_confidence += confidence
                    loss += confidence

                loss += dot(&N, r, &one, &X[u, 0], &one)
                user_norm += dot(&N, &X[u, 0], &one, &X[u, 0], &one)

            for i in prange(items, schedule='guided'):
                item_norm += dot(&N, &Y[i, 0], &one, &Y[i, 0], &one)

        finally:
            free(r)

    loss += regularization * (item_norm + user_norm)
    return loss / (total_confidence + Cui.shape[0] * Cui.shape[1] - Cui.nnz)
