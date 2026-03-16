#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Model - Cython tools

Author: Chad Fulton  
License: Simplified-BSD
"""

# Typical imports
cimport numpy as np
cimport cython
import numpy as np

np.import_array()

from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack

cdef FORTRAN = 1

# Array shape validation
cdef validate_matrix_shape(str name, Py_ssize_t *shape, int nrows, int ncols, nobs=None):
    if not shape[0] == nrows:
        raise ValueError('Invalid shape for %s matrix: requires %d rows,'
                         ' got %d' % (name, nrows, shape[0]))
    if not shape[1] == ncols:
        raise ValueError('Invalid shape for %s matrix: requires %d columns,'
                         'got %d' % (name, shape[1], shape[1]))
    if nobs is not None and shape[2] not in [1, nobs]:
        raise ValueError('Invalid time-varying dimension for %s matrix:'
                         ' requires 1 or %d, got %d' % (name, nobs, shape[2]))

cdef validate_vector_shape(str name, Py_ssize_t *shape, int nrows, nobs = None):
    if not shape[0] == nrows:
        raise ValueError('Invalid shape for %s vector: requires %d rows,'
                         ' got %d' % (name, nrows, shape[0]))
    if nobs is not None and not shape[1] in [1, nobs]:
        raise ValueError('Invalid time-varying dimension for %s vector:'
                         ' requires 1 or %d got %d' % (name, nobs, shape[1]))


cdef bint _sselect1(np.float32_t * a):
    return 0

cdef bint _sselect2(np.float32_t * a, np.float32_t * b):
    return 0

cdef int _ssolve_discrete_lyapunov(np.float32_t * a, np.float32_t * q, int n, int complex_step=False) except *:
    # Note: some of this code (esp. the Sylvester solving part) cribbed from
    # https://raw.githubusercontent.com/scipy/scipy/master/scipy/linalg/_solvers.py

    # Solve an equation of the form $A'XA-X=-Q$
    # a: input / output
    # q: input / output
    cdef:
        int i, j
        int info
        int inc = 1
        int n2 = n**2
        np.float32_t scale = 0.0
        np.float32_t tmp = 0.0
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t delta = -2.0
        char trans
    cdef np.npy_intp dim[2]
    cdef np.float32_t [::1,:] apI, capI, u, v
    cdef int [::1,:] ipiv
    # Dummy selection function, won't actually be referenced since we don't
    # need to order the eigenvalues in the ?gees call.
    cdef:
        int sdim
        int lwork = 3*n
        bint bwork
    cdef np.npy_intp dim1[1]
    cdef np.float32_t [::1,:] work
    cdef np.float32_t [:] wr
    cdef np.float32_t [:] wi

    # Initialize arrays
    dim[0] = n; dim[1] = n;
    apI = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)
    capI = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)
    u = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)
    v = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)
    ipiv = np.PyArray_ZEROS(2, dim, np.NPY_INT32, FORTRAN)

    dim1[0] = n;
    wr = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT32, FORTRAN)
    wi = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT32, FORTRAN)
    #vs = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)
    dim[0] = lwork; dim[1] = lwork;
    work = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT32, FORTRAN)

    # - Solve for b.conj().transpose() --------

    # Get apI = a + I (stored in apI)
    # = (a + eye)
    # For: c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    blas.scopy(&n2, a, &inc, &apI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) + I (stored in capI)
    # a^H + I -> capI
    # For: aHI_inv = inv(aH + eye)
    blas.scopy(&n2, a, &inc, &capI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) - I (stored in a)
    # a^H - I -> a
    # For: b = np.dot(aH - eye, aHI_inv)
    # (for loop below subtracts the identity)

    # Add / subtract identity matrix
    for i in range(n):
        apI[i,i] = apI[i,i] + 1 # apI -> a + eye
        capI[i,i] = capI[i,i] + 1 # aH + eye
        a[i + i*n] = a[i + i*n] - 1 # a - eye

    # Solve [conj(a) + I] b' = [conj(a) - I] (result stored in a)
    # For: b = np.dot(aH - eye, aHI_inv)
    # Where: aHI_inv = inv(aH + eye)
    # where b = (a^H - eye) (a^H + eye)^{-1}
    # or b^H = (a + eye)^{-1} (a - eye)
    # or (a + eye) b^H = (a - eye)
    lapack.sgetrf(&n, &n, &capI[0,0], &n, &ipiv[0,0], &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU decomposition error.')

    lapack.sgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                               a, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Now we have b^H; we could take the conjugate transpose to get b, except
    # that the input to the continuous Lyapunov equation is exactly
    # b^H, so we already have the quantity we need.

    # - Solve for (-c) --------

    # where c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    # = 2*(a + eye)^{-1} q (a^H + eye)^{-1}
    # and with q Hermitian
    # consider x = (a + eye)^{-1} q (a^H + eye)^{-1}
    # this can be done in two linear solving steps:
    # 1. consider y = q (a^H + eye)^{-1}
    #    or y (a^H + eye) = q
    #    or (a^H + eye)^H y^H = q^H
    #    or (a + eye) y^H = q
    # 2. Then consider x = (a + eye)^{-1} y
    #    or (a + eye) x = y

    # Solve [conj(a) + I] tmp' = q (result stored in q)
    # For: y = q (a^H + eye)^{-1} => (a + eye) y^H = q
    lapack.sgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Replace the result (stored in q) with its (conjugate) transpose
    for j in range(1, n):
        for i in range(j):
            tmp = q[i + j*n]
            q[i + j*n] = q[j + i*n]
            q[j + i*n] = tmp


    lapack.sgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # q -> -2.0 * q
    blas.sscal(&n2, &delta, q, &inc)

    # - Solve continuous time Lyapunov --------

    # Now solve the continuous time Lyapunov equation (AX + XA^H = Q), on the
    # transformed inputs ...

    # ... which requires solving the continuous time Sylvester equation
    # (AX + XB = Q) where B = A^H

    # Compute the real Schur decomposition of a (unordered)
    # TODO compute the optimal lwork rather than always using 3*n
    # a is now the Schur form of A; (r)
    # u is now the unitary Schur transformation matrix for A; (u)
    # In the usual case, we will also have:
    # r = s, so s is also stored in a
    # u = v, so v is also stored in u
    # In the complex-step case, we will instead have:
    # r = s.conj()
    # u = v.conj()
    lapack.sgees("V", "N", <lapack.sselect2 *> &_sselect2, &n,
                          a, &n,
                          &sdim,
                          &wr[0], &wi[0],
                          &u[0,0], &n,
                          &work[0,0], &lwork,
                          &bwork, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('Schur decomposition solver error.')

    # Get v (so that in the complex step case we can take the conjugate)
    blas.scopy(&n2, &u[0,0], &inc, &v[0,0], &inc)
    # If complex step, take the conjugate

    # Construct f = u^H*q*u (result overwrites q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.sgemm("N", "N", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.sgemm("C", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)

    # DTRYSL Solve op(A)*X + X*op(B) = scale*C which is here:
    # r*X + X*r = scale*q
    # results overwrite q
    blas.scopy(&n2, a, &inc, &apI[0,0], &inc)
    lapack.strsyl("N", "C", &inc, &n, &n,
                           a, &n,
                           &apI[0,0], &n,
                           q, &n,
                           &scale, &info)

    # Scale q by scale
    if not scale == 1.0:
        blas.sscal(&n2, <np.float32_t*> &scale, q, &inc)

    # Calculate the solution: u * q * v^H (results overwrite q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.sgemm("N", "C", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.sgemm("N", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)


cpdef _scompute_coefficients_from_multivariate_pacf(np.float32_t [::1,:] partial_autocorrelations,
                                                             np.float32_t [::1,:] error_variance,
                                                             int transform_variance,
                                                             int order, int k_endog):
    """
    Notes
    -----

    This uses the ?trmm BLAS functions which are not available in
    Scipy v0.11.0
    """
    # Constants
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
        int k_endog2 = k_endog**2
        int k_endog_order = k_endog * order
        int k_endog_order1 = k_endog * (order+1)
        int info, s, k
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.float32_t [::1, :] initial_variance
        np.float32_t [::1, :] forward_variance
        np.float32_t [::1, :] backward_variance
        np.float32_t [::1, :] autocovariances
        np.float32_t [::1, :] forwards1
        np.float32_t [::1, :] forwards2
        np.float32_t [::1, :] backwards1
        np.float32_t [::1, :] backwards2
        np.float32_t [::1, :] forward_factors
        np.float32_t [::1, :] backward_factors
        np.float32_t [::1, :] tmp
        np.float32_t [::1, :] tmp2
    # Pointers
    cdef:
        np.float32_t * forwards
        np.float32_t * prev_forwards
        np.float32_t * backwards
        np.float32_t * prev_backwards
    # ?trmm
    # cdef strmm_t *strmm = <strmm_t*>Capsule_AsVoidPtr(blas.strmm._cpointer)

    # dim2[0] = self.k_endog; dim2[1] = storage;
    # self.forecast = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    # If we want to keep the provided variance but with the constrained
    # coefficient matrices, we need to make a copy here, and then after the
    # main loop we will transform the coefficients to match the passed variance
    if not transform_variance:
        initial_variance = np.asfortranarray(error_variance.copy())
        # Need to make the input variance large enough that the recursions
        # don't lead to zero-matrices due to roundoff error, which would case
        # exceptions from the Cholesky decompositions.
        # Note that this will still not always ensure positive definiteness,
        # and for k_endog, order large enough an exception may still be raised
        error_variance = np.asfortranarray(np.eye(k_endog, dtype=np.float32) * (order + k_endog)**10)

    # Initialize matrices
    dim2[0] = k_endog; dim2[1] = k_endog;
    forward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    backward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    forward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    backward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    tmp2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order;
    # \phi_{s,k}, s = 1, ..., p
    #             k = 1, ..., s+1
    forwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    forwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    # \phi_{s,k}^*
    backwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    backwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order1;
    autocovariances = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    blas.scopy(&k_endog2, &error_variance[0,0], &inc, &forward_variance[0,0], &inc)   # \Sigma_s
    blas.scopy(&k_endog2, &error_variance[0,0], &inc, &backward_variance[0,0], &inc)  # \Sigma_s^*,  s = 0, ..., p
    blas.scopy(&k_endog2, &error_variance[0,0], &inc, &autocovariances[0,0], &inc)  # \Gamma_s

    # error_variance_factor = linalg.cholesky(error_variance, lower=True)
    blas.scopy(&k_endog2, &error_variance[0,0], &inc, &forward_factors[0,0], &inc)
    lapack.spotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
    blas.scopy(&k_endog2, &forward_factors[0,0], &inc, &backward_factors[0,0], &inc)

    # We fill in the entries as follows:
    # [1,1]
    # [2,2], [2,1]
    # [3,3], [3,1], [3,2]
    # ...
    # [p,p], [p,1], ..., [p,p-1]
    # the last row, correctly ordered, is then used as the coefficients
    for s in range(order):  # s = 0, ..., p-1
        if s % 2 == 0:
            forwards = &forwards1[0, 0]
            prev_forwards = &forwards2[0, 0]
            backwards = &backwards1[0, 0]
            prev_backwards = &backwards2[0, 0]
        else:
            forwards = &forwards2[0, 0]
            prev_forwards = &forwards1[0, 0]
            backwards = &backwards2[0, 0]
            prev_backwards = &backwards1[0, 0]

        # Create the "last" (k = s+1) matrix
        # Note: this is for k = s+1. However, below we then have to fill
        # in for k = 1, ..., s in order.
        # P L*^{-1} = x
        # x L* = P
        # L*' x' = P'
        # forwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     forward_factors,
        #     linalg.solve_triangular(
        #         backward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog].T,
        #         lower=True, trans='T').T
        # )
        for k in range(k_endog):
            blas.scopy(&k_endog, &partial_autocorrelations[k,s*k_endog], &k_endog, &tmp[0, k], &inc)
        lapack.strtrs("L", "T", "N", &k_endog, &k_endog, &backward_factors[0,0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # sgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &forward_factors[0,0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &forwards[s*k_endog2], &k_endog)
        blas.strmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &forward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.scopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s*k_endog2 + k*k_endog], &inc)

        # P' L^{-1} = x
        # x L = P'
        # L' x' = P
        # backwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     backward_factors,
        #     linalg.solve_triangular(
        #         forward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog],
        #         lower=True, trans='T').T
        # )
        blas.scopy(&k_endog2, &partial_autocorrelations[0, s*k_endog], &inc, &tmp[0, 0], &inc)
        lapack.strtrs("L", "T", "N", &k_endog, &k_endog, &forward_factors[0, 0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # sgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &backward_factors[0, 0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &backwards[s * k_endog2], &k_endog)
        blas.strmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &backward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.scopy(&k_endog, &tmp[k,0], &k_endog, &backwards[s*k_endog2 + k*k_endog], &inc)

        # Update the variance
        # Note: if s >= 1, this will be further updated in the for loop
        # below
        # Also, this calculation will be re-used in the forward variance
        # tmp = np.dot(forwards[:, s*k_endog:(s+1)*k_endog], backward_variance)
        # tmpT = np.dot(backward_variance.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        blas.sgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &beta, &tmp[0, 0], &k_endog)
        # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] = tmp.copy().T
        blas.scopy(&k_endog2, &tmp[0, 0], &inc, &autocovariances[0, (s+1)*k_endog], &inc)

        # Create the remaining k = 1, ..., s matrices,
        # only has an effect if s >= 1
        for k in range(s):
            # forwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_forwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         forwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_backwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.scopy(&k_endog2, &prev_forwards[k * k_endog2], &inc, &forwards[k * k_endog2], &inc)
            blas.sgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &forwards[s * k_endog2], &k_endog,
                      &prev_backwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &forwards[k * k_endog2], &k_endog)

            # backwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_backwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         backwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.scopy(&k_endog2, &prev_backwards[k * k_endog2], &inc, &backwards[k * k_endog2], &inc)
            blas.sgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &backwards[s * k_endog2], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &backwards[k * k_endog2], &k_endog)

            # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] += np.dot(
            #     autocovariances[:, (k+1)*k_endog:(k+2)*k_endog],
            #     prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog].T
            # )
            blas.sgemm("N", "T", &k_endog, &k_endog, &k_endog,
              &alpha, &autocovariances[0, (k+1)*k_endog], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &autocovariances[0, (s+1)*k_endog], &k_endog)

        # Create forward and backwards variances
        # backward_variance = (
        #     backward_variance -
        #     np.dot(
        #         np.dot(backwards[:, s*k_endog:(s+1)*k_endog], forward_variance),
        #         backwards[:, s*k_endog:(s+1)*k_endog].T
        #     )
        # )
        blas.sgemm("N", "N", &k_endog, &k_endog, &k_endog,
          &alpha, &backwards[s * k_endog2], &k_endog,
                  &forward_variance[0, 0], &k_endog,
          &beta, &tmp2[0, 0], &k_endog)
        blas.sgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp2[0, 0], &k_endog,
                  &backwards[s * k_endog2], &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog)
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmp, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmpT.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        blas.sgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &alpha, &forward_variance[0, 0], &k_endog)

        # Cholesky factors
        # forward_factors = linalg.cholesky(forward_variance, lower=True)
        # backward_factors =  linalg.cholesky(backward_variance, lower=True)
        blas.scopy(&k_endog2, &forward_variance[0,0], &inc, &forward_factors[0,0], &inc)
        lapack.spotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
        blas.scopy(&k_endog2, &backward_variance[0,0], &inc, &backward_factors[0,0], &inc)
        lapack.spotrf("L", &k_endog, &backward_factors[0,0], &k_endog, &info)


    # If we do not want to use the transformed variance, we need to
    # adjust the constrained matrices, as presented in Lemma 2.3, see above
    if not transform_variance:
        if order % 2 == 0:
            forwards = &forwards2[0,0]
        else:
            forwards = &forwards1[0,0]

        # Here, we need to construct T such that:
        # variance = T * initial_variance * T'
        # To do that, consider the Cholesky of variance (L) and
        # input_variance (M) to get:
        # L L' = T M M' T' = (TM) (TM)'
        # => L = T M
        # => L M^{-1} = T
        # initial_variance_factor = np.linalg.cholesky(initial_variance)
        # L'
        lapack.spotrf("U", &k_endog, &initial_variance[0,0], &k_endog, &info)
        # transformed_variance_factor = np.linalg.cholesky(variance)
        # M'
        blas.scopy(&k_endog2, &forward_variance[0,0], &inc, &tmp[0,0], &inc)
        lapack.spotrf("U", &k_endog, &tmp[0,0], &k_endog, &info)
        # spotri("L", &k_endog, &tmp[0,0], &k_endog, &info)

        # We need to zero out the lower triangle of L', because ?trtrs only
        # knows that M' is upper triangular
        for s in range(k_endog - 1):          # column
            for k in range(s+1, k_endog):     # row
                initial_variance[k, s] = 0

        # Note that T is lower triangular
        # L M^{-1} = T
        # M' T' = L'
        # transform = np.dot(initial_variance_factor,
        #                    np.linalg.inv(transformed_variance_factor))
        lapack.strtrs("U", "N", "N", &k_endog, &k_endog, &tmp[0,0], &k_endog,
                                                           &initial_variance[0, 0], &k_endog, &info)
        # Now:
        # initial_variance = T'

        for s in range(order):
            # forwards[:, s*k_endog:(s+1)*k_endog] = (
            #     np.dot(
            #         np.dot(transform, forwards[:, s*k_endog:(s+1)*k_endog]),
            #         inv_transform
            #     )
            # )
            # TF T^{-1} = x
            # TF = x T
            # (TF)' = T' x'

            # Get TF
            blas.scopy(&k_endog2, &forwards[s * k_endog2], &inc, &tmp2[0,0], &inc)
            blas.strmm("L", "U", "T", "N", &k_endog, &k_endog,
              &alpha, &initial_variance[0, 0], &k_endog,
                      &tmp2[0, 0], &k_endog)
            for k in range(k_endog):
                blas.scopy(&k_endog, &tmp2[k,0], &k_endog, &tmp[0, k], &inc)
            # Get x'
            lapack.strtrs("U", "N", "N", &k_endog, &k_endog, &initial_variance[0,0], &k_endog,
                                                               &tmp[0, 0], &k_endog, &info)
            # Get x
            for k in range(k_endog):
                blas.scopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s * k_endog2 + k*k_endog], &inc)


    if order % 2 == 0:
        return forwards2, forward_variance
    else:
        return forwards1, forward_variance

cpdef _sconstrain_sv_less_than_one(np.float32_t [::1,:] unconstrained, int order, int k_endog):
    """
    Transform arbitrary matrices to matrices with singular values less than
    one.

    Corresponds to Lemma 2.2 in Ansley and Kohn (1986). See
    `constrain_stationary_multivariate` for more details.
    """
    # Constants
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        int k_endog2 = k_endog**2
        int info, i
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.float32_t [::1, :] constrained
        np.float32_t [::1, :] tmp
        np.float32_t [::1, :] eye

    dim2[0] = k_endog; dim2[1] = k_endog * order;
    constrained = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    dim2[0] = k_endog; dim2[1] = k_endog;
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
    eye = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    eye = np.asfortranarray(np.eye(k_endog, dtype=np.float32))
    for i in range(order):
        blas.scopy(&k_endog2, &eye[0, 0], &inc, &tmp[0, 0], &inc)
        blas.sgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &unconstrained[0, i*k_endog], &k_endog,
                  &unconstrained[0, i*k_endog], &k_endog,
          &alpha, &tmp[0, 0], &k_endog)
        lapack.spotrf("L", &k_endog, &tmp[0, 0], &k_endog, &info)

        blas.scopy(&k_endog2, &unconstrained[0, i*k_endog], &inc, &constrained[0, i*k_endog], &inc)
        # constrained.append(linalg.solve_triangular(B, A, lower=lower))
        lapack.strtrs("L", "N", "N", &k_endog, &k_endog, &tmp[0, 0], &k_endog,
                                                           &constrained[0, i*k_endog], &k_endog, &info)
    return constrained

cdef int _sldl(np.float32_t * A, int n) except *:
    # See Golub and Van Loan, Algorithm 4.1.2
    cdef:
        int info = 0
        int j, i, k
        cdef np.npy_intp dim[1]
        np.float64_t tol = 1e-15
        np.float32_t [:] v

    dim[0] = n
    v = np.PyArray_ZEROS(1, dim, np.NPY_FLOAT32, FORTRAN)

    for j in range(n):
        # Compute v(1:j)
        v[j] = A[j + j*n]

        # Positive definite element: use Golub and Van Loan algorithm
        if v[j].real < -tol:
            info = -j
            break
        elif v[j].real > tol:
            for i in range(j):
                v[i] = A[j + i*n] * A[i + i*n]
                v[j] = v[j] - A[j + i*n] * v[i]

            # Store d(j) and compute L(j+1:n,j)
            A[j + j*n] = v[j]
            for i in range(j+1, n):
                for k in range(j):
                    A[i + j*n] = A[i + j*n] - A[i + k*n] * v[k]
                A[i + j*n] = A[i + j*n] / v[j]
        # Positive semi-definite element: zero the appropriate column
        else:
            info = 1
            for i in range(j, n):
                A[i + j*n]

    return info

cpdef int sldl(np.float32_t [::1, :] A) except *:
    _sldl(&A[0,0], A.shape[0])

cdef int _sreorder_missing_diagonal(np.float32_t * a, int * missing, int n):
    """
    a is a pointer to an n x n diagonal array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            a[i + i*n] = a[k + k*n]
            k = k - 1
        else:
            a[i + i*n] = 0

cdef int _sreorder_missing_submatrix(np.float32_t * a, int * missing, int n):
    """
    a is a pointer to an n x n array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    _sreorder_missing_rows(a, missing, n, n)
    _sreorder_missing_cols(a, missing, n, n)

cdef int _sreorder_missing_rows(np.float32_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            blas.sswap(&m, &a[i], &n, &a[k], &n)
            k = k - 1


cdef int _sreorder_missing_cols(np.float32_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(m-1,-1,-1):
        if not missing[i]:
            blas.sswap(&n, &a[i*n], &inc, &a[k*n], &inc)
            k = k - 1

cpdef int sreorder_missing_matrix(np.float32_t [::1, :, :] A, int [::1, :] missing, int reorder_rows, int reorder_cols, int diagonal) except *:
    cdef int n, m, T, t

    n, m, T = A.shape[0:3]

    if reorder_rows and reorder_cols:
        if not n == m:
            raise RuntimeError('Reordering a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                _sreorder_missing_diagonal(&A[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                _sreorder_missing_submatrix(&A[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with reordering a submatrix')
    elif reorder_rows:
        for t in range(T):
            _sreorder_missing_rows(&A[0, 0, t], &missing[0, t], n, m)
    elif reorder_cols:
        for t in range(T):
            _sreorder_missing_cols(&A[0, 0, t], &missing[0, t], n, m)


cpdef int sreorder_missing_vector(np.float32_t [::1, :] A, int [::1, :] missing) except *:
    cdef int i, k, t, n, T, nobs

    n, T = A.shape[0:2]

    for t in range(T):
        _sreorder_missing_rows(&A[0, t], &missing[0, t], n, 1)


cdef int _scopy_missing_diagonal(np.float32_t * a, np.float32_t * b, int * missing, int n):
    """
    Copy the non-missing block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(nobs):
        b[i + i*n] = a[i + i*n]


cdef int _scopy_missing_submatrix(np.float32_t * a, np.float32_t * b, int * missing, int n):
    """
    Copy the non-missing submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.scopy(&nobs, &a[i*n], &inc, &b[i*n], &inc)


cdef int _scopy_missing_rows(np.float32_t * a, np.float32_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.scopy(&m, &a[i], &n, &b[i], &n)


cdef int _scopy_missing_cols(np.float32_t * a, np.float32_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.scopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int scopy_missing_matrix(np.float32_t [::1, :, :] A, np.float32_t [::1, :, :] B, int [::1, :] missing, int missing_rows, int missing_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if missing_rows and missing_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _scopy_missing_diagonal(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _scopy_missing_submatrix(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif missing_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _scopy_missing_rows(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    elif missing_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _scopy_missing_cols(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    pass


cpdef int scopy_missing_vector(np.float32_t [::1, :] A, np.float32_t [::1, :] B, int [::1, :] missing) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _scopy_missing_rows(&A[0, A_t], &B[0, t], &missing[0, t], n, 1)

cdef int _scopy_index_diagonal(np.float32_t * a, np.float32_t * b, int * index, int n):
    """
    Copy the non-index block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            b[i + i*n] = a[i + i*n]


cdef int _scopy_index_submatrix(np.float32_t * a, np.float32_t * b, int * index, int n):
    """
    Copy the non-index submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    _scopy_index_rows(a, b, index, n, n)
    _scopy_index_cols(a, b, index, n, n)


cdef int _scopy_index_rows(np.float32_t * a, np.float32_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            blas.scopy(&m, &a[i], &n, &b[i], &n)


cdef int _scopy_index_cols(np.float32_t * a, np.float32_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    # Perform replacement
    for i in range(m):
        if index[i]:
            blas.scopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int scopy_index_matrix(np.float32_t [::1, :, :] A, np.float32_t [::1, :, :] B, int [::1, :] index, int index_rows, int index_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if index_rows and index_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _scopy_index_diagonal(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _scopy_index_submatrix(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif index_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _scopy_index_rows(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)
    elif index_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _scopy_index_cols(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)


cpdef int scopy_index_vector(np.float32_t [::1, :] A, np.float32_t [::1, :] B, int [::1, :] index) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _scopy_index_rows(&A[0, A_t], &B[0, t], &index[0, t], n, 1)

cdef int _sselect_cov(int k_states, int k_posdef, int k_states_total,
                               np.float32_t * tmp,
                               np.float32_t * selection,
                               np.float32_t * cov,
                               np.float32_t * selected_cov):
    cdef:
        int i, k_states2 = k_states**2
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    # Only need to do something if there is a covariance matrix
    # (i.e k_posdof == 0)
    if k_posdef > 0:

        # #### Calculate selected state covariance matrix  
        # $Q_t^* = R_t Q_t R_t'$
        # 
        # Combine a selection matrix and a covariance matrix to get
        # a simplified (but possibly singular) "selected" covariance
        # matrix (see e.g. Durbin and Koopman p. 43)

        # `tmp0` array used here, dimension $(m \times r)$  

        # $\\#_0 = 1.0 * R_t Q_t$  
        # $(m \times r) = (m \times r) (r \times r)$
        blas.sgemm("N", "N", &k_states, &k_posdef, &k_posdef,
              &alpha, selection, &k_states_total,
                      cov, &k_posdef,
              &beta, tmp, &k_states)
        # $Q_t^* = 1.0 * \\#_0 R_t'$  
        # $(m \times m) = (m \times r) (m \times r)'$
        blas.sgemm("N", "T", &k_states, &k_states, &k_posdef,
              &alpha, tmp, &k_states,
                      selection, &k_states_total,
              &beta, selected_cov, &k_states)
    else:
        for i in range(k_states2):
            selected_cov[i] = 0


cdef bint _dselect1(np.float64_t * a):
    return 0

cdef bint _dselect2(np.float64_t * a, np.float64_t * b):
    return 0

cdef int _dsolve_discrete_lyapunov(np.float64_t * a, np.float64_t * q, int n, int complex_step=False) except *:
    # Note: some of this code (esp. the Sylvester solving part) cribbed from
    # https://raw.githubusercontent.com/scipy/scipy/master/scipy/linalg/_solvers.py

    # Solve an equation of the form $A'XA-X=-Q$
    # a: input / output
    # q: input / output
    cdef:
        int i, j
        int info
        int inc = 1
        int n2 = n**2
        np.float64_t scale = 0.0
        np.float64_t tmp = 0.0
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t delta = -2.0
        char trans
    cdef np.npy_intp dim[2]
    cdef np.float64_t [::1,:] apI, capI, u, v
    cdef int [::1,:] ipiv
    # Dummy selection function, won't actually be referenced since we don't
    # need to order the eigenvalues in the ?gees call.
    cdef:
        int sdim
        int lwork = 3*n
        bint bwork
    cdef np.npy_intp dim1[1]
    cdef np.float64_t [::1,:] work
    cdef np.float64_t [:] wr
    cdef np.float64_t [:] wi

    # Initialize arrays
    dim[0] = n; dim[1] = n;
    apI = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)
    capI = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)
    u = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)
    v = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)
    ipiv = np.PyArray_ZEROS(2, dim, np.NPY_INT32, FORTRAN)

    dim1[0] = n;
    wr = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)
    wi = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)
    #vs = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)
    dim[0] = lwork; dim[1] = lwork;
    work = np.PyArray_ZEROS(2, dim, np.NPY_FLOAT64, FORTRAN)

    # - Solve for b.conj().transpose() --------

    # Get apI = a + I (stored in apI)
    # = (a + eye)
    # For: c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    blas.dcopy(&n2, a, &inc, &apI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) + I (stored in capI)
    # a^H + I -> capI
    # For: aHI_inv = inv(aH + eye)
    blas.dcopy(&n2, a, &inc, &capI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) - I (stored in a)
    # a^H - I -> a
    # For: b = np.dot(aH - eye, aHI_inv)
    # (for loop below subtracts the identity)

    # Add / subtract identity matrix
    for i in range(n):
        apI[i,i] = apI[i,i] + 1 # apI -> a + eye
        capI[i,i] = capI[i,i] + 1 # aH + eye
        a[i + i*n] = a[i + i*n] - 1 # a - eye

    # Solve [conj(a) + I] b' = [conj(a) - I] (result stored in a)
    # For: b = np.dot(aH - eye, aHI_inv)
    # Where: aHI_inv = inv(aH + eye)
    # where b = (a^H - eye) (a^H + eye)^{-1}
    # or b^H = (a + eye)^{-1} (a - eye)
    # or (a + eye) b^H = (a - eye)
    lapack.dgetrf(&n, &n, &capI[0,0], &n, &ipiv[0,0], &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU decomposition error.')

    lapack.dgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                               a, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Now we have b^H; we could take the conjugate transpose to get b, except
    # that the input to the continuous Lyapunov equation is exactly
    # b^H, so we already have the quantity we need.

    # - Solve for (-c) --------

    # where c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    # = 2*(a + eye)^{-1} q (a^H + eye)^{-1}
    # and with q Hermitian
    # consider x = (a + eye)^{-1} q (a^H + eye)^{-1}
    # this can be done in two linear solving steps:
    # 1. consider y = q (a^H + eye)^{-1}
    #    or y (a^H + eye) = q
    #    or (a^H + eye)^H y^H = q^H
    #    or (a + eye) y^H = q
    # 2. Then consider x = (a + eye)^{-1} y
    #    or (a + eye) x = y

    # Solve [conj(a) + I] tmp' = q (result stored in q)
    # For: y = q (a^H + eye)^{-1} => (a + eye) y^H = q
    lapack.dgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Replace the result (stored in q) with its (conjugate) transpose
    for j in range(1, n):
        for i in range(j):
            tmp = q[i + j*n]
            q[i + j*n] = q[j + i*n]
            q[j + i*n] = tmp


    lapack.dgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # q -> -2.0 * q
    blas.dscal(&n2, &delta, q, &inc)

    # - Solve continuous time Lyapunov --------

    # Now solve the continuous time Lyapunov equation (AX + XA^H = Q), on the
    # transformed inputs ...

    # ... which requires solving the continuous time Sylvester equation
    # (AX + XB = Q) where B = A^H

    # Compute the real Schur decomposition of a (unordered)
    # TODO compute the optimal lwork rather than always using 3*n
    # a is now the Schur form of A; (r)
    # u is now the unitary Schur transformation matrix for A; (u)
    # In the usual case, we will also have:
    # r = s, so s is also stored in a
    # u = v, so v is also stored in u
    # In the complex-step case, we will instead have:
    # r = s.conj()
    # u = v.conj()
    lapack.dgees("V", "N", <lapack.dselect2 *> &_dselect2, &n,
                          a, &n,
                          &sdim,
                          &wr[0], &wi[0],
                          &u[0,0], &n,
                          &work[0,0], &lwork,
                          &bwork, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('Schur decomposition solver error.')

    # Get v (so that in the complex step case we can take the conjugate)
    blas.dcopy(&n2, &u[0,0], &inc, &v[0,0], &inc)
    # If complex step, take the conjugate

    # Construct f = u^H*q*u (result overwrites q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.dgemm("N", "N", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.dgemm("C", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)

    # DTRYSL Solve op(A)*X + X*op(B) = scale*C which is here:
    # r*X + X*r = scale*q
    # results overwrite q
    blas.dcopy(&n2, a, &inc, &apI[0,0], &inc)
    lapack.dtrsyl("N", "C", &inc, &n, &n,
                           a, &n,
                           &apI[0,0], &n,
                           q, &n,
                           &scale, &info)

    # Scale q by scale
    if not scale == 1.0:
        blas.dscal(&n2, <np.float64_t*> &scale, q, &inc)

    # Calculate the solution: u * q * v^H (results overwrite q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.dgemm("N", "C", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.dgemm("N", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)


cpdef _dcompute_coefficients_from_multivariate_pacf(np.float64_t [::1,:] partial_autocorrelations,
                                                             np.float64_t [::1,:] error_variance,
                                                             int transform_variance,
                                                             int order, int k_endog):
    """
    Notes
    -----

    This uses the ?trmm BLAS functions which are not available in
    Scipy v0.11.0
    """
    # Constants
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
        int k_endog2 = k_endog**2
        int k_endog_order = k_endog * order
        int k_endog_order1 = k_endog * (order+1)
        int info, s, k
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.float64_t [::1, :] initial_variance
        np.float64_t [::1, :] forward_variance
        np.float64_t [::1, :] backward_variance
        np.float64_t [::1, :] autocovariances
        np.float64_t [::1, :] forwards1
        np.float64_t [::1, :] forwards2
        np.float64_t [::1, :] backwards1
        np.float64_t [::1, :] backwards2
        np.float64_t [::1, :] forward_factors
        np.float64_t [::1, :] backward_factors
        np.float64_t [::1, :] tmp
        np.float64_t [::1, :] tmp2
    # Pointers
    cdef:
        np.float64_t * forwards
        np.float64_t * prev_forwards
        np.float64_t * backwards
        np.float64_t * prev_backwards
    # ?trmm
    # cdef dtrmm_t *dtrmm = <dtrmm_t*>Capsule_AsVoidPtr(blas.dtrmm._cpointer)

    # dim2[0] = self.k_endog; dim2[1] = storage;
    # self.forecast = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    # If we want to keep the provided variance but with the constrained
    # coefficient matrices, we need to make a copy here, and then after the
    # main loop we will transform the coefficients to match the passed variance
    if not transform_variance:
        initial_variance = np.asfortranarray(error_variance.copy())
        # Need to make the input variance large enough that the recursions
        # don't lead to zero-matrices due to roundoff error, which would case
        # exceptions from the Cholesky decompositions.
        # Note that this will still not always ensure positive definiteness,
        # and for k_endog, order large enough an exception may still be raised
        error_variance = np.asfortranarray(np.eye(k_endog, dtype=float) * (order + k_endog)**10)

    # Initialize matrices
    dim2[0] = k_endog; dim2[1] = k_endog;
    forward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    backward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    forward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    backward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    tmp2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order;
    # \phi_{s,k}, s = 1, ..., p
    #             k = 1, ..., s+1
    forwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    forwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    # \phi_{s,k}^*
    backwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    backwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order1;
    autocovariances = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    blas.dcopy(&k_endog2, &error_variance[0,0], &inc, &forward_variance[0,0], &inc)   # \Sigma_s
    blas.dcopy(&k_endog2, &error_variance[0,0], &inc, &backward_variance[0,0], &inc)  # \Sigma_s^*,  s = 0, ..., p
    blas.dcopy(&k_endog2, &error_variance[0,0], &inc, &autocovariances[0,0], &inc)  # \Gamma_s

    # error_variance_factor = linalg.cholesky(error_variance, lower=True)
    blas.dcopy(&k_endog2, &error_variance[0,0], &inc, &forward_factors[0,0], &inc)
    lapack.dpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
    blas.dcopy(&k_endog2, &forward_factors[0,0], &inc, &backward_factors[0,0], &inc)

    # We fill in the entries as follows:
    # [1,1]
    # [2,2], [2,1]
    # [3,3], [3,1], [3,2]
    # ...
    # [p,p], [p,1], ..., [p,p-1]
    # the last row, correctly ordered, is then used as the coefficients
    for s in range(order):  # s = 0, ..., p-1
        if s % 2 == 0:
            forwards = &forwards1[0, 0]
            prev_forwards = &forwards2[0, 0]
            backwards = &backwards1[0, 0]
            prev_backwards = &backwards2[0, 0]
        else:
            forwards = &forwards2[0, 0]
            prev_forwards = &forwards1[0, 0]
            backwards = &backwards2[0, 0]
            prev_backwards = &backwards1[0, 0]

        # Create the "last" (k = s+1) matrix
        # Note: this is for k = s+1. However, below we then have to fill
        # in for k = 1, ..., s in order.
        # P L*^{-1} = x
        # x L* = P
        # L*' x' = P'
        # forwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     forward_factors,
        #     linalg.solve_triangular(
        #         backward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog].T,
        #         lower=True, trans='T').T
        # )
        for k in range(k_endog):
            blas.dcopy(&k_endog, &partial_autocorrelations[k,s*k_endog], &k_endog, &tmp[0, k], &inc)
        lapack.dtrtrs("L", "T", "N", &k_endog, &k_endog, &backward_factors[0,0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # dgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &forward_factors[0,0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &forwards[s*k_endog2], &k_endog)
        blas.dtrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &forward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.dcopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s*k_endog2 + k*k_endog], &inc)

        # P' L^{-1} = x
        # x L = P'
        # L' x' = P
        # backwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     backward_factors,
        #     linalg.solve_triangular(
        #         forward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog],
        #         lower=True, trans='T').T
        # )
        blas.dcopy(&k_endog2, &partial_autocorrelations[0, s*k_endog], &inc, &tmp[0, 0], &inc)
        lapack.dtrtrs("L", "T", "N", &k_endog, &k_endog, &forward_factors[0, 0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # dgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &backward_factors[0, 0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &backwards[s * k_endog2], &k_endog)
        blas.dtrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &backward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.dcopy(&k_endog, &tmp[k,0], &k_endog, &backwards[s*k_endog2 + k*k_endog], &inc)

        # Update the variance
        # Note: if s >= 1, this will be further updated in the for loop
        # below
        # Also, this calculation will be re-used in the forward variance
        # tmp = np.dot(forwards[:, s*k_endog:(s+1)*k_endog], backward_variance)
        # tmpT = np.dot(backward_variance.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        blas.dgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &beta, &tmp[0, 0], &k_endog)
        # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] = tmp.copy().T
        blas.dcopy(&k_endog2, &tmp[0, 0], &inc, &autocovariances[0, (s+1)*k_endog], &inc)

        # Create the remaining k = 1, ..., s matrices,
        # only has an effect if s >= 1
        for k in range(s):
            # forwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_forwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         forwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_backwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.dcopy(&k_endog2, &prev_forwards[k * k_endog2], &inc, &forwards[k * k_endog2], &inc)
            blas.dgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &forwards[s * k_endog2], &k_endog,
                      &prev_backwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &forwards[k * k_endog2], &k_endog)

            # backwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_backwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         backwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.dcopy(&k_endog2, &prev_backwards[k * k_endog2], &inc, &backwards[k * k_endog2], &inc)
            blas.dgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &backwards[s * k_endog2], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &backwards[k * k_endog2], &k_endog)

            # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] += np.dot(
            #     autocovariances[:, (k+1)*k_endog:(k+2)*k_endog],
            #     prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog].T
            # )
            blas.dgemm("N", "T", &k_endog, &k_endog, &k_endog,
              &alpha, &autocovariances[0, (k+1)*k_endog], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &autocovariances[0, (s+1)*k_endog], &k_endog)

        # Create forward and backwards variances
        # backward_variance = (
        #     backward_variance -
        #     np.dot(
        #         np.dot(backwards[:, s*k_endog:(s+1)*k_endog], forward_variance),
        #         backwards[:, s*k_endog:(s+1)*k_endog].T
        #     )
        # )
        blas.dgemm("N", "N", &k_endog, &k_endog, &k_endog,
          &alpha, &backwards[s * k_endog2], &k_endog,
                  &forward_variance[0, 0], &k_endog,
          &beta, &tmp2[0, 0], &k_endog)
        blas.dgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp2[0, 0], &k_endog,
                  &backwards[s * k_endog2], &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog)
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmp, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmpT.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        blas.dgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &alpha, &forward_variance[0, 0], &k_endog)

        # Cholesky factors
        # forward_factors = linalg.cholesky(forward_variance, lower=True)
        # backward_factors =  linalg.cholesky(backward_variance, lower=True)
        blas.dcopy(&k_endog2, &forward_variance[0,0], &inc, &forward_factors[0,0], &inc)
        lapack.dpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
        blas.dcopy(&k_endog2, &backward_variance[0,0], &inc, &backward_factors[0,0], &inc)
        lapack.dpotrf("L", &k_endog, &backward_factors[0,0], &k_endog, &info)


    # If we do not want to use the transformed variance, we need to
    # adjust the constrained matrices, as presented in Lemma 2.3, see above
    if not transform_variance:
        if order % 2 == 0:
            forwards = &forwards2[0,0]
        else:
            forwards = &forwards1[0,0]

        # Here, we need to construct T such that:
        # variance = T * initial_variance * T'
        # To do that, consider the Cholesky of variance (L) and
        # input_variance (M) to get:
        # L L' = T M M' T' = (TM) (TM)'
        # => L = T M
        # => L M^{-1} = T
        # initial_variance_factor = np.linalg.cholesky(initial_variance)
        # L'
        lapack.dpotrf("U", &k_endog, &initial_variance[0,0], &k_endog, &info)
        # transformed_variance_factor = np.linalg.cholesky(variance)
        # M'
        blas.dcopy(&k_endog2, &forward_variance[0,0], &inc, &tmp[0,0], &inc)
        lapack.dpotrf("U", &k_endog, &tmp[0,0], &k_endog, &info)
        # dpotri("L", &k_endog, &tmp[0,0], &k_endog, &info)

        # We need to zero out the lower triangle of L', because ?trtrs only
        # knows that M' is upper triangular
        for s in range(k_endog - 1):          # column
            for k in range(s+1, k_endog):     # row
                initial_variance[k, s] = 0

        # Note that T is lower triangular
        # L M^{-1} = T
        # M' T' = L'
        # transform = np.dot(initial_variance_factor,
        #                    np.linalg.inv(transformed_variance_factor))
        lapack.dtrtrs("U", "N", "N", &k_endog, &k_endog, &tmp[0,0], &k_endog,
                                                           &initial_variance[0, 0], &k_endog, &info)
        # Now:
        # initial_variance = T'

        for s in range(order):
            # forwards[:, s*k_endog:(s+1)*k_endog] = (
            #     np.dot(
            #         np.dot(transform, forwards[:, s*k_endog:(s+1)*k_endog]),
            #         inv_transform
            #     )
            # )
            # TF T^{-1} = x
            # TF = x T
            # (TF)' = T' x'

            # Get TF
            blas.dcopy(&k_endog2, &forwards[s * k_endog2], &inc, &tmp2[0,0], &inc)
            blas.dtrmm("L", "U", "T", "N", &k_endog, &k_endog,
              &alpha, &initial_variance[0, 0], &k_endog,
                      &tmp2[0, 0], &k_endog)
            for k in range(k_endog):
                blas.dcopy(&k_endog, &tmp2[k,0], &k_endog, &tmp[0, k], &inc)
            # Get x'
            lapack.dtrtrs("U", "N", "N", &k_endog, &k_endog, &initial_variance[0,0], &k_endog,
                                                               &tmp[0, 0], &k_endog, &info)
            # Get x
            for k in range(k_endog):
                blas.dcopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s * k_endog2 + k*k_endog], &inc)


    if order % 2 == 0:
        return forwards2, forward_variance
    else:
        return forwards1, forward_variance

cpdef _dconstrain_sv_less_than_one(np.float64_t [::1,:] unconstrained, int order, int k_endog):
    """
    Transform arbitrary matrices to matrices with singular values less than
    one.

    Corresponds to Lemma 2.2 in Ansley and Kohn (1986). See
    `constrain_stationary_multivariate` for more details.
    """
    # Constants
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        int k_endog2 = k_endog**2
        int info, i
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.float64_t [::1, :] constrained
        np.float64_t [::1, :] tmp
        np.float64_t [::1, :] eye

    dim2[0] = k_endog; dim2[1] = k_endog * order;
    constrained = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    dim2[0] = k_endog; dim2[1] = k_endog;
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
    eye = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    eye = np.asfortranarray(np.eye(k_endog, dtype=float))
    for i in range(order):
        blas.dcopy(&k_endog2, &eye[0, 0], &inc, &tmp[0, 0], &inc)
        blas.dgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &unconstrained[0, i*k_endog], &k_endog,
                  &unconstrained[0, i*k_endog], &k_endog,
          &alpha, &tmp[0, 0], &k_endog)
        lapack.dpotrf("L", &k_endog, &tmp[0, 0], &k_endog, &info)

        blas.dcopy(&k_endog2, &unconstrained[0, i*k_endog], &inc, &constrained[0, i*k_endog], &inc)
        # constrained.append(linalg.solve_triangular(B, A, lower=lower))
        lapack.dtrtrs("L", "N", "N", &k_endog, &k_endog, &tmp[0, 0], &k_endog,
                                                           &constrained[0, i*k_endog], &k_endog, &info)
    return constrained

cdef int _dldl(np.float64_t * A, int n) except *:
    # See Golub and Van Loan, Algorithm 4.1.2
    cdef:
        int info = 0
        int j, i, k
        cdef np.npy_intp dim[1]
        np.float64_t tol = 1e-15
        np.float64_t [:] v

    dim[0] = n
    v = np.PyArray_ZEROS(1, dim, np.NPY_FLOAT64, FORTRAN)

    for j in range(n):
        # Compute v(1:j)
        v[j] = A[j + j*n]

        # Positive definite element: use Golub and Van Loan algorithm
        if v[j].real < -tol:
            info = -j
            break
        elif v[j].real > tol:
            for i in range(j):
                v[i] = A[j + i*n] * A[i + i*n]
                v[j] = v[j] - A[j + i*n] * v[i]

            # Store d(j) and compute L(j+1:n,j)
            A[j + j*n] = v[j]
            for i in range(j+1, n):
                for k in range(j):
                    A[i + j*n] = A[i + j*n] - A[i + k*n] * v[k]
                A[i + j*n] = A[i + j*n] / v[j]
        # Positive semi-definite element: zero the appropriate column
        else:
            info = 1
            for i in range(j, n):
                A[i + j*n]

    return info

cpdef int dldl(np.float64_t [::1, :] A) except *:
    _dldl(&A[0,0], A.shape[0])

cdef int _dreorder_missing_diagonal(np.float64_t * a, int * missing, int n):
    """
    a is a pointer to an n x n diagonal array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            a[i + i*n] = a[k + k*n]
            k = k - 1
        else:
            a[i + i*n] = 0

cdef int _dreorder_missing_submatrix(np.float64_t * a, int * missing, int n):
    """
    a is a pointer to an n x n array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    _dreorder_missing_rows(a, missing, n, n)
    _dreorder_missing_cols(a, missing, n, n)

cdef int _dreorder_missing_rows(np.float64_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            blas.dswap(&m, &a[i], &n, &a[k], &n)
            k = k - 1


cdef int _dreorder_missing_cols(np.float64_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(m-1,-1,-1):
        if not missing[i]:
            blas.dswap(&n, &a[i*n], &inc, &a[k*n], &inc)
            k = k - 1

cpdef int dreorder_missing_matrix(np.float64_t [::1, :, :] A, int [::1, :] missing, int reorder_rows, int reorder_cols, int diagonal) except *:
    cdef int n, m, T, t

    n, m, T = A.shape[0:3]

    if reorder_rows and reorder_cols:
        if not n == m:
            raise RuntimeError('Reordering a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                _dreorder_missing_diagonal(&A[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                _dreorder_missing_submatrix(&A[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with reordering a submatrix')
    elif reorder_rows:
        for t in range(T):
            _dreorder_missing_rows(&A[0, 0, t], &missing[0, t], n, m)
    elif reorder_cols:
        for t in range(T):
            _dreorder_missing_cols(&A[0, 0, t], &missing[0, t], n, m)


cpdef int dreorder_missing_vector(np.float64_t [::1, :] A, int [::1, :] missing) except *:
    cdef int i, k, t, n, T, nobs

    n, T = A.shape[0:2]

    for t in range(T):
        _dreorder_missing_rows(&A[0, t], &missing[0, t], n, 1)


cdef int _dcopy_missing_diagonal(np.float64_t * a, np.float64_t * b, int * missing, int n):
    """
    Copy the non-missing block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(nobs):
        b[i + i*n] = a[i + i*n]


cdef int _dcopy_missing_submatrix(np.float64_t * a, np.float64_t * b, int * missing, int n):
    """
    Copy the non-missing submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.dcopy(&nobs, &a[i*n], &inc, &b[i*n], &inc)


cdef int _dcopy_missing_rows(np.float64_t * a, np.float64_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.dcopy(&m, &a[i], &n, &b[i], &n)


cdef int _dcopy_missing_cols(np.float64_t * a, np.float64_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.dcopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int dcopy_missing_matrix(np.float64_t [::1, :, :] A, np.float64_t [::1, :, :] B, int [::1, :] missing, int missing_rows, int missing_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if missing_rows and missing_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _dcopy_missing_diagonal(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _dcopy_missing_submatrix(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif missing_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _dcopy_missing_rows(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    elif missing_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _dcopy_missing_cols(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    pass


cpdef int dcopy_missing_vector(np.float64_t [::1, :] A, np.float64_t [::1, :] B, int [::1, :] missing) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _dcopy_missing_rows(&A[0, A_t], &B[0, t], &missing[0, t], n, 1)

cdef int _dcopy_index_diagonal(np.float64_t * a, np.float64_t * b, int * index, int n):
    """
    Copy the non-index block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            b[i + i*n] = a[i + i*n]


cdef int _dcopy_index_submatrix(np.float64_t * a, np.float64_t * b, int * index, int n):
    """
    Copy the non-index submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    _dcopy_index_rows(a, b, index, n, n)
    _dcopy_index_cols(a, b, index, n, n)


cdef int _dcopy_index_rows(np.float64_t * a, np.float64_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            blas.dcopy(&m, &a[i], &n, &b[i], &n)


cdef int _dcopy_index_cols(np.float64_t * a, np.float64_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    # Perform replacement
    for i in range(m):
        if index[i]:
            blas.dcopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int dcopy_index_matrix(np.float64_t [::1, :, :] A, np.float64_t [::1, :, :] B, int [::1, :] index, int index_rows, int index_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if index_rows and index_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _dcopy_index_diagonal(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _dcopy_index_submatrix(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif index_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _dcopy_index_rows(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)
    elif index_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _dcopy_index_cols(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)


cpdef int dcopy_index_vector(np.float64_t [::1, :] A, np.float64_t [::1, :] B, int [::1, :] index) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _dcopy_index_rows(&A[0, A_t], &B[0, t], &index[0, t], n, 1)

cdef int _dselect_cov(int k_states, int k_posdef, int k_states_total,
                               np.float64_t * tmp,
                               np.float64_t * selection,
                               np.float64_t * cov,
                               np.float64_t * selected_cov):
    cdef:
        int i, k_states2 = k_states**2
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    # Only need to do something if there is a covariance matrix
    # (i.e k_posdof == 0)
    if k_posdef > 0:

        # #### Calculate selected state covariance matrix  
        # $Q_t^* = R_t Q_t R_t'$
        # 
        # Combine a selection matrix and a covariance matrix to get
        # a simplified (but possibly singular) "selected" covariance
        # matrix (see e.g. Durbin and Koopman p. 43)

        # `tmp0` array used here, dimension $(m \times r)$  

        # $\\#_0 = 1.0 * R_t Q_t$  
        # $(m \times r) = (m \times r) (r \times r)$
        blas.dgemm("N", "N", &k_states, &k_posdef, &k_posdef,
              &alpha, selection, &k_states_total,
                      cov, &k_posdef,
              &beta, tmp, &k_states)
        # $Q_t^* = 1.0 * \\#_0 R_t'$  
        # $(m \times m) = (m \times r) (m \times r)'$
        blas.dgemm("N", "T", &k_states, &k_states, &k_posdef,
              &alpha, tmp, &k_states,
                      selection, &k_states_total,
              &beta, selected_cov, &k_states)
    else:
        for i in range(k_states2):
            selected_cov[i] = 0


cdef bint _cselect1(np.complex64_t * a):
    return 0

cdef bint _cselect2(np.complex64_t * a, np.complex64_t * b):
    return 0

cdef int _csolve_discrete_lyapunov(np.complex64_t * a, np.complex64_t * q, int n, int complex_step=False) except *:
    # Note: some of this code (esp. the Sylvester solving part) cribbed from
    # https://raw.githubusercontent.com/scipy/scipy/master/scipy/linalg/_solvers.py

    # Solve an equation of the form $A'XA-X=-Q$
    # a: input / output
    # q: input / output
    cdef:
        int i, j
        int info
        int inc = 1
        int n2 = n**2
        np.float32_t scale = 0.0
        np.complex64_t tmp = 0.0
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t delta = -2.0
        char trans
    cdef np.npy_intp dim[2]
    cdef np.complex64_t [::1,:] apI, capI, u, v
    cdef int [::1,:] ipiv
    # Dummy selection function, won't actually be referenced since we don't
    # need to order the eigenvalues in the ?gees call.
    cdef:
        int sdim
        int lwork = 3*n
        bint bwork
    cdef np.npy_intp dim1[1]
    cdef np.complex64_t [::1,:] work
    cdef np.complex64_t [:] wr
    cdef np.float32_t [:] wi

    # Initialize arrays
    dim[0] = n; dim[1] = n;
    apI = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)
    capI = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)
    u = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)
    v = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)
    ipiv = np.PyArray_ZEROS(2, dim, np.NPY_INT32, FORTRAN)

    dim1[0] = n;
    wr = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX64, FORTRAN)
    wi = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)
    #vs = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)
    dim[0] = lwork; dim[1] = lwork;
    work = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX64, FORTRAN)

    # - Solve for b.conj().transpose() --------

    # Get apI = a + I (stored in apI)
    # = (a + eye)
    # For: c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    blas.ccopy(&n2, a, &inc, &apI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) + I (stored in capI)
    # a^H + I -> capI
    # For: aHI_inv = inv(aH + eye)
    blas.ccopy(&n2, a, &inc, &capI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) - I (stored in a)
    # a^H - I -> a
    # For: b = np.dot(aH - eye, aHI_inv)
    # (for loop below subtracts the identity)

    # Add / subtract identity matrix
    for i in range(n):
        apI[i,i] = apI[i,i] + 1 # apI -> a + eye
        capI[i,i] = capI[i,i] + 1 # aH + eye
        a[i + i*n] = a[i + i*n] - 1 # a - eye

    # Solve [conj(a) + I] b' = [conj(a) - I] (result stored in a)
    # For: b = np.dot(aH - eye, aHI_inv)
    # Where: aHI_inv = inv(aH + eye)
    # where b = (a^H - eye) (a^H + eye)^{-1}
    # or b^H = (a + eye)^{-1} (a - eye)
    # or (a + eye) b^H = (a - eye)
    lapack.cgetrf(&n, &n, &capI[0,0], &n, &ipiv[0,0], &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU decomposition error.')

    lapack.cgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                               a, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Now we have b^H; we could take the conjugate transpose to get b, except
    # that the input to the continuous Lyapunov equation is exactly
    # b^H, so we already have the quantity we need.

    # - Solve for (-c) --------

    # where c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    # = 2*(a + eye)^{-1} q (a^H + eye)^{-1}
    # and with q Hermitian
    # consider x = (a + eye)^{-1} q (a^H + eye)^{-1}
    # this can be done in two linear solving steps:
    # 1. consider y = q (a^H + eye)^{-1}
    #    or y (a^H + eye) = q
    #    or (a^H + eye)^H y^H = q^H
    #    or (a + eye) y^H = q
    # 2. Then consider x = (a + eye)^{-1} y
    #    or (a + eye) x = y

    # Solve [conj(a) + I] tmp' = q (result stored in q)
    # For: y = q (a^H + eye)^{-1} => (a + eye) y^H = q
    lapack.cgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Replace the result (stored in q) with its (conjugate) transpose
    for j in range(1, n):
        for i in range(j):
            tmp = q[i + j*n]
            q[i + j*n] = q[j + i*n]
            q[j + i*n] = tmp

    if not complex_step:
        for i in range(n2):
            q[i] = q[i] - q[i].imag * 2.0j

    lapack.cgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # q -> -2.0 * q
    blas.cscal(&n2, &delta, q, &inc)

    # - Solve continuous time Lyapunov --------

    # Now solve the continuous time Lyapunov equation (AX + XA^H = Q), on the
    # transformed inputs ...

    # ... which requires solving the continuous time Sylvester equation
    # (AX + XB = Q) where B = A^H

    # Compute the real Schur decomposition of a (unordered)
    # TODO compute the optimal lwork rather than always using 3*n
    lapack.cgees("V", "N", <lapack.cselect1 *> &_cselect1, &n,
                          a, &n,
                          &sdim,
                          &wr[0],
                          &u[0,0], &n,
                          &work[0,0], &lwork,
                          &wi[0],
                          &bwork, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('Schur decomposition solver error.')

    # Get v (so that in the complex step case we can take the conjugate)
    blas.ccopy(&n2, &u[0,0], &inc, &v[0,0], &inc)
    # If complex step, take the conjugate
    if complex_step:
        for i in range(n):
            for j in range(n):
                v[i,j] = v[i,j] - v[i,j].imag * 2.0j

    # Construct f = u^H*q*u (result overwrites q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.cgemm("N", "N", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.cgemm("C", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)

    # DTRYSL Solve op(A)*X + X*op(B) = scale*C which is here:
    # r*X + X*r = scale*q
    # results overwrite q
    blas.ccopy(&n2, a, &inc, &apI[0,0], &inc)
    if complex_step:
        for i in range(n):
            for j in range(n):
                apI[j,i] = apI[j,i] - apI[j,i].imag * 2.0j
    lapack.ctrsyl("N", "C", &inc, &n, &n,
                           a, &n,
                           &apI[0,0], &n,
                           q, &n,
                           &scale, &info)

    # Scale q by scale
    if not scale == 1.0:
        blas.cscal(&n2, <np.complex64_t*> &scale, q, &inc)

    # Calculate the solution: u * q * v^H (results overwrite q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.cgemm("N", "C", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.cgemm("N", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)


cpdef _ccompute_coefficients_from_multivariate_pacf(np.complex64_t [::1,:] partial_autocorrelations,
                                                             np.complex64_t [::1,:] error_variance,
                                                             int transform_variance,
                                                             int order, int k_endog):
    """
    Notes
    -----

    This uses the ?trmm BLAS functions which are not available in
    Scipy v0.11.0
    """
    # Constants
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
        int k_endog2 = k_endog**2
        int k_endog_order = k_endog * order
        int k_endog_order1 = k_endog * (order+1)
        int info, s, k
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.complex64_t [::1, :] initial_variance
        np.complex64_t [::1, :] forward_variance
        np.complex64_t [::1, :] backward_variance
        np.complex64_t [::1, :] autocovariances
        np.complex64_t [::1, :] forwards1
        np.complex64_t [::1, :] forwards2
        np.complex64_t [::1, :] backwards1
        np.complex64_t [::1, :] backwards2
        np.complex64_t [::1, :] forward_factors
        np.complex64_t [::1, :] backward_factors
        np.complex64_t [::1, :] tmp
        np.complex64_t [::1, :] tmp2
    # Pointers
    cdef:
        np.complex64_t * forwards
        np.complex64_t * prev_forwards
        np.complex64_t * backwards
        np.complex64_t * prev_backwards
    # ?trmm
    # cdef ctrmm_t *ctrmm = <ctrmm_t*>Capsule_AsVoidPtr(blas.ctrmm._cpointer)

    # dim2[0] = self.k_endog; dim2[1] = storage;
    # self.forecast = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    # If we want to keep the provided variance but with the constrained
    # coefficient matrices, we need to make a copy here, and then after the
    # main loop we will transform the coefficients to match the passed variance
    if not transform_variance:
        initial_variance = np.asfortranarray(error_variance.copy())
        # Need to make the input variance large enough that the recursions
        # don't lead to zero-matrices due to roundoff error, which would case
        # exceptions from the Cholesky decompositions.
        # Note that this will still not always ensure positive definiteness,
        # and for k_endog, order large enough an exception may still be raised
        error_variance = np.asfortranarray(np.eye(k_endog, dtype=np.complex64) * (order + k_endog)**10)

    # Initialize matrices
    dim2[0] = k_endog; dim2[1] = k_endog;
    forward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    backward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    forward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    backward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    tmp2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order;
    # \phi_{s,k}, s = 1, ..., p
    #             k = 1, ..., s+1
    forwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    forwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    # \phi_{s,k}^*
    backwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    backwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order1;
    autocovariances = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    blas.ccopy(&k_endog2, &error_variance[0,0], &inc, &forward_variance[0,0], &inc)   # \Sigma_s
    blas.ccopy(&k_endog2, &error_variance[0,0], &inc, &backward_variance[0,0], &inc)  # \Sigma_s^*,  s = 0, ..., p
    blas.ccopy(&k_endog2, &error_variance[0,0], &inc, &autocovariances[0,0], &inc)  # \Gamma_s

    # error_variance_factor = linalg.cholesky(error_variance, lower=True)
    blas.ccopy(&k_endog2, &error_variance[0,0], &inc, &forward_factors[0,0], &inc)
    lapack.cpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
    blas.ccopy(&k_endog2, &forward_factors[0,0], &inc, &backward_factors[0,0], &inc)

    # We fill in the entries as follows:
    # [1,1]
    # [2,2], [2,1]
    # [3,3], [3,1], [3,2]
    # ...
    # [p,p], [p,1], ..., [p,p-1]
    # the last row, correctly ordered, is then used as the coefficients
    for s in range(order):  # s = 0, ..., p-1
        if s % 2 == 0:
            forwards = &forwards1[0, 0]
            prev_forwards = &forwards2[0, 0]
            backwards = &backwards1[0, 0]
            prev_backwards = &backwards2[0, 0]
        else:
            forwards = &forwards2[0, 0]
            prev_forwards = &forwards1[0, 0]
            backwards = &backwards2[0, 0]
            prev_backwards = &backwards1[0, 0]

        # Create the "last" (k = s+1) matrix
        # Note: this is for k = s+1. However, below we then have to fill
        # in for k = 1, ..., s in order.
        # P L*^{-1} = x
        # x L* = P
        # L*' x' = P'
        # forwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     forward_factors,
        #     linalg.solve_triangular(
        #         backward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog].T,
        #         lower=True, trans='T').T
        # )
        for k in range(k_endog):
            blas.ccopy(&k_endog, &partial_autocorrelations[k,s*k_endog], &k_endog, &tmp[0, k], &inc)
        lapack.ctrtrs("L", "T", "N", &k_endog, &k_endog, &backward_factors[0,0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # cgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &forward_factors[0,0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &forwards[s*k_endog2], &k_endog)
        blas.ctrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &forward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.ccopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s*k_endog2 + k*k_endog], &inc)

        # P' L^{-1} = x
        # x L = P'
        # L' x' = P
        # backwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     backward_factors,
        #     linalg.solve_triangular(
        #         forward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog],
        #         lower=True, trans='T').T
        # )
        blas.ccopy(&k_endog2, &partial_autocorrelations[0, s*k_endog], &inc, &tmp[0, 0], &inc)
        lapack.ctrtrs("L", "T", "N", &k_endog, &k_endog, &forward_factors[0, 0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # cgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &backward_factors[0, 0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &backwards[s * k_endog2], &k_endog)
        blas.ctrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &backward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.ccopy(&k_endog, &tmp[k,0], &k_endog, &backwards[s*k_endog2 + k*k_endog], &inc)

        # Update the variance
        # Note: if s >= 1, this will be further updated in the for loop
        # below
        # Also, this calculation will be re-used in the forward variance
        # tmp = np.dot(forwards[:, s*k_endog:(s+1)*k_endog], backward_variance)
        # tmpT = np.dot(backward_variance.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        blas.cgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &beta, &tmp[0, 0], &k_endog)
        # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] = tmp.copy().T
        blas.ccopy(&k_endog2, &tmp[0, 0], &inc, &autocovariances[0, (s+1)*k_endog], &inc)

        # Create the remaining k = 1, ..., s matrices,
        # only has an effect if s >= 1
        for k in range(s):
            # forwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_forwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         forwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_backwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.ccopy(&k_endog2, &prev_forwards[k * k_endog2], &inc, &forwards[k * k_endog2], &inc)
            blas.cgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &forwards[s * k_endog2], &k_endog,
                      &prev_backwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &forwards[k * k_endog2], &k_endog)

            # backwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_backwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         backwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.ccopy(&k_endog2, &prev_backwards[k * k_endog2], &inc, &backwards[k * k_endog2], &inc)
            blas.cgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &backwards[s * k_endog2], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &backwards[k * k_endog2], &k_endog)

            # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] += np.dot(
            #     autocovariances[:, (k+1)*k_endog:(k+2)*k_endog],
            #     prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog].T
            # )
            blas.cgemm("N", "T", &k_endog, &k_endog, &k_endog,
              &alpha, &autocovariances[0, (k+1)*k_endog], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &autocovariances[0, (s+1)*k_endog], &k_endog)

        # Create forward and backwards variances
        # backward_variance = (
        #     backward_variance -
        #     np.dot(
        #         np.dot(backwards[:, s*k_endog:(s+1)*k_endog], forward_variance),
        #         backwards[:, s*k_endog:(s+1)*k_endog].T
        #     )
        # )
        blas.cgemm("N", "N", &k_endog, &k_endog, &k_endog,
          &alpha, &backwards[s * k_endog2], &k_endog,
                  &forward_variance[0, 0], &k_endog,
          &beta, &tmp2[0, 0], &k_endog)
        blas.cgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp2[0, 0], &k_endog,
                  &backwards[s * k_endog2], &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog)
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmp, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmpT.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        blas.cgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &alpha, &forward_variance[0, 0], &k_endog)

        # Cholesky factors
        # forward_factors = linalg.cholesky(forward_variance, lower=True)
        # backward_factors =  linalg.cholesky(backward_variance, lower=True)
        blas.ccopy(&k_endog2, &forward_variance[0,0], &inc, &forward_factors[0,0], &inc)
        lapack.cpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
        blas.ccopy(&k_endog2, &backward_variance[0,0], &inc, &backward_factors[0,0], &inc)
        lapack.cpotrf("L", &k_endog, &backward_factors[0,0], &k_endog, &info)


    # If we do not want to use the transformed variance, we need to
    # adjust the constrained matrices, as presented in Lemma 2.3, see above
    if not transform_variance:
        if order % 2 == 0:
            forwards = &forwards2[0,0]
        else:
            forwards = &forwards1[0,0]

        # Here, we need to construct T such that:
        # variance = T * initial_variance * T'
        # To do that, consider the Cholesky of variance (L) and
        # input_variance (M) to get:
        # L L' = T M M' T' = (TM) (TM)'
        # => L = T M
        # => L M^{-1} = T
        # initial_variance_factor = np.linalg.cholesky(initial_variance)
        # L'
        lapack.cpotrf("U", &k_endog, &initial_variance[0,0], &k_endog, &info)
        # transformed_variance_factor = np.linalg.cholesky(variance)
        # M'
        blas.ccopy(&k_endog2, &forward_variance[0,0], &inc, &tmp[0,0], &inc)
        lapack.cpotrf("U", &k_endog, &tmp[0,0], &k_endog, &info)
        # cpotri("L", &k_endog, &tmp[0,0], &k_endog, &info)

        # We need to zero out the lower triangle of L', because ?trtrs only
        # knows that M' is upper triangular
        for s in range(k_endog - 1):          # column
            for k in range(s+1, k_endog):     # row
                initial_variance[k, s] = 0

        # Note that T is lower triangular
        # L M^{-1} = T
        # M' T' = L'
        # transform = np.dot(initial_variance_factor,
        #                    np.linalg.inv(transformed_variance_factor))
        lapack.ctrtrs("U", "N", "N", &k_endog, &k_endog, &tmp[0,0], &k_endog,
                                                           &initial_variance[0, 0], &k_endog, &info)
        # Now:
        # initial_variance = T'

        for s in range(order):
            # forwards[:, s*k_endog:(s+1)*k_endog] = (
            #     np.dot(
            #         np.dot(transform, forwards[:, s*k_endog:(s+1)*k_endog]),
            #         inv_transform
            #     )
            # )
            # TF T^{-1} = x
            # TF = x T
            # (TF)' = T' x'

            # Get TF
            blas.ccopy(&k_endog2, &forwards[s * k_endog2], &inc, &tmp2[0,0], &inc)
            blas.ctrmm("L", "U", "T", "N", &k_endog, &k_endog,
              &alpha, &initial_variance[0, 0], &k_endog,
                      &tmp2[0, 0], &k_endog)
            for k in range(k_endog):
                blas.ccopy(&k_endog, &tmp2[k,0], &k_endog, &tmp[0, k], &inc)
            # Get x'
            lapack.ctrtrs("U", "N", "N", &k_endog, &k_endog, &initial_variance[0,0], &k_endog,
                                                               &tmp[0, 0], &k_endog, &info)
            # Get x
            for k in range(k_endog):
                blas.ccopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s * k_endog2 + k*k_endog], &inc)


    if order % 2 == 0:
        return forwards2, forward_variance
    else:
        return forwards1, forward_variance

cpdef _cconstrain_sv_less_than_one(np.complex64_t [::1,:] unconstrained, int order, int k_endog):
    """
    Transform arbitrary matrices to matrices with singular values less than
    one.

    Corresponds to Lemma 2.2 in Ansley and Kohn (1986). See
    `constrain_stationary_multivariate` for more details.
    """
    # Constants
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        int k_endog2 = k_endog**2
        int info, i
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.complex64_t [::1, :] constrained
        np.complex64_t [::1, :] tmp
        np.complex64_t [::1, :] eye

    dim2[0] = k_endog; dim2[1] = k_endog * order;
    constrained = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    dim2[0] = k_endog; dim2[1] = k_endog;
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
    eye = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    eye = np.asfortranarray(np.eye(k_endog, dtype=np.complex64))
    for i in range(order):
        blas.ccopy(&k_endog2, &eye[0, 0], &inc, &tmp[0, 0], &inc)
        blas.cgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &unconstrained[0, i*k_endog], &k_endog,
                  &unconstrained[0, i*k_endog], &k_endog,
          &alpha, &tmp[0, 0], &k_endog)
        lapack.cpotrf("L", &k_endog, &tmp[0, 0], &k_endog, &info)

        blas.ccopy(&k_endog2, &unconstrained[0, i*k_endog], &inc, &constrained[0, i*k_endog], &inc)
        # constrained.append(linalg.solve_triangular(B, A, lower=lower))
        lapack.ctrtrs("L", "N", "N", &k_endog, &k_endog, &tmp[0, 0], &k_endog,
                                                           &constrained[0, i*k_endog], &k_endog, &info)
    return constrained

cdef int _cldl(np.complex64_t * A, int n) except *:
    # See Golub and Van Loan, Algorithm 4.1.2
    cdef:
        int info = 0
        int j, i, k
        cdef np.npy_intp dim[1]
        np.float64_t tol = 1e-15
        np.complex64_t [:] v

    dim[0] = n
    v = np.PyArray_ZEROS(1, dim, np.NPY_COMPLEX64, FORTRAN)

    for j in range(n):
        # Compute v(1:j)
        v[j] = A[j + j*n]

        # Positive definite element: use Golub and Van Loan algorithm
        if v[j].real < -tol:
            info = -j
            break
        elif v[j].real > tol:
            for i in range(j):
                v[i] = A[j + i*n] * A[i + i*n]
                v[j] = v[j] - A[j + i*n] * v[i]

            # Store d(j) and compute L(j+1:n,j)
            A[j + j*n] = v[j]
            for i in range(j+1, n):
                for k in range(j):
                    A[i + j*n] = A[i + j*n] - A[i + k*n] * v[k]
                A[i + j*n] = A[i + j*n] / v[j]
        # Positive semi-definite element: zero the appropriate column
        else:
            info = 1
            for i in range(j, n):
                A[i + j*n]

    return info

cpdef int cldl(np.complex64_t [::1, :] A) except *:
    _cldl(&A[0,0], A.shape[0])

cdef int _creorder_missing_diagonal(np.complex64_t * a, int * missing, int n):
    """
    a is a pointer to an n x n diagonal array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            a[i + i*n] = a[k + k*n]
            k = k - 1
        else:
            a[i + i*n] = 0

cdef int _creorder_missing_submatrix(np.complex64_t * a, int * missing, int n):
    """
    a is a pointer to an n x n array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    _creorder_missing_rows(a, missing, n, n)
    _creorder_missing_cols(a, missing, n, n)

cdef int _creorder_missing_rows(np.complex64_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            blas.cswap(&m, &a[i], &n, &a[k], &n)
            k = k - 1


cdef int _creorder_missing_cols(np.complex64_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(m-1,-1,-1):
        if not missing[i]:
            blas.cswap(&n, &a[i*n], &inc, &a[k*n], &inc)
            k = k - 1

cpdef int creorder_missing_matrix(np.complex64_t [::1, :, :] A, int [::1, :] missing, int reorder_rows, int reorder_cols, int diagonal) except *:
    cdef int n, m, T, t

    n, m, T = A.shape[0:3]

    if reorder_rows and reorder_cols:
        if not n == m:
            raise RuntimeError('Reordering a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                _creorder_missing_diagonal(&A[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                _creorder_missing_submatrix(&A[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with reordering a submatrix')
    elif reorder_rows:
        for t in range(T):
            _creorder_missing_rows(&A[0, 0, t], &missing[0, t], n, m)
    elif reorder_cols:
        for t in range(T):
            _creorder_missing_cols(&A[0, 0, t], &missing[0, t], n, m)


cpdef int creorder_missing_vector(np.complex64_t [::1, :] A, int [::1, :] missing) except *:
    cdef int i, k, t, n, T, nobs

    n, T = A.shape[0:2]

    for t in range(T):
        _creorder_missing_rows(&A[0, t], &missing[0, t], n, 1)


cdef int _ccopy_missing_diagonal(np.complex64_t * a, np.complex64_t * b, int * missing, int n):
    """
    Copy the non-missing block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(nobs):
        b[i + i*n] = a[i + i*n]


cdef int _ccopy_missing_submatrix(np.complex64_t * a, np.complex64_t * b, int * missing, int n):
    """
    Copy the non-missing submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.ccopy(&nobs, &a[i*n], &inc, &b[i*n], &inc)


cdef int _ccopy_missing_rows(np.complex64_t * a, np.complex64_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.ccopy(&m, &a[i], &n, &b[i], &n)


cdef int _ccopy_missing_cols(np.complex64_t * a, np.complex64_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.ccopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int ccopy_missing_matrix(np.complex64_t [::1, :, :] A, np.complex64_t [::1, :, :] B, int [::1, :] missing, int missing_rows, int missing_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if missing_rows and missing_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _ccopy_missing_diagonal(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _ccopy_missing_submatrix(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif missing_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _ccopy_missing_rows(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    elif missing_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _ccopy_missing_cols(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    pass


cpdef int ccopy_missing_vector(np.complex64_t [::1, :] A, np.complex64_t [::1, :] B, int [::1, :] missing) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _ccopy_missing_rows(&A[0, A_t], &B[0, t], &missing[0, t], n, 1)

cdef int _ccopy_index_diagonal(np.complex64_t * a, np.complex64_t * b, int * index, int n):
    """
    Copy the non-index block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            b[i + i*n] = a[i + i*n]


cdef int _ccopy_index_submatrix(np.complex64_t * a, np.complex64_t * b, int * index, int n):
    """
    Copy the non-index submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    _ccopy_index_rows(a, b, index, n, n)
    _ccopy_index_cols(a, b, index, n, n)


cdef int _ccopy_index_rows(np.complex64_t * a, np.complex64_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            blas.ccopy(&m, &a[i], &n, &b[i], &n)


cdef int _ccopy_index_cols(np.complex64_t * a, np.complex64_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    # Perform replacement
    for i in range(m):
        if index[i]:
            blas.ccopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int ccopy_index_matrix(np.complex64_t [::1, :, :] A, np.complex64_t [::1, :, :] B, int [::1, :] index, int index_rows, int index_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if index_rows and index_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _ccopy_index_diagonal(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _ccopy_index_submatrix(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif index_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _ccopy_index_rows(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)
    elif index_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _ccopy_index_cols(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)


cpdef int ccopy_index_vector(np.complex64_t [::1, :] A, np.complex64_t [::1, :] B, int [::1, :] index) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _ccopy_index_rows(&A[0, A_t], &B[0, t], &index[0, t], n, 1)

cdef int _cselect_cov(int k_states, int k_posdef, int k_states_total,
                               np.complex64_t * tmp,
                               np.complex64_t * selection,
                               np.complex64_t * cov,
                               np.complex64_t * selected_cov):
    cdef:
        int i, k_states2 = k_states**2
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    # Only need to do something if there is a covariance matrix
    # (i.e k_posdof == 0)
    if k_posdef > 0:

        # #### Calculate selected state covariance matrix  
        # $Q_t^* = R_t Q_t R_t'$
        # 
        # Combine a selection matrix and a covariance matrix to get
        # a simplified (but possibly singular) "selected" covariance
        # matrix (see e.g. Durbin and Koopman p. 43)

        # `tmp0` array used here, dimension $(m \times r)$  

        # $\\#_0 = 1.0 * R_t Q_t$  
        # $(m \times r) = (m \times r) (r \times r)$
        blas.cgemm("N", "N", &k_states, &k_posdef, &k_posdef,
              &alpha, selection, &k_states_total,
                      cov, &k_posdef,
              &beta, tmp, &k_states)
        # $Q_t^* = 1.0 * \\#_0 R_t'$  
        # $(m \times m) = (m \times r) (m \times r)'$
        blas.cgemm("N", "T", &k_states, &k_states, &k_posdef,
              &alpha, tmp, &k_states,
                      selection, &k_states_total,
              &beta, selected_cov, &k_states)
    else:
        for i in range(k_states2):
            selected_cov[i] = 0


cdef bint _zselect1(np.complex128_t * a):
    return 0

cdef bint _zselect2(np.complex128_t * a, np.complex128_t * b):
    return 0

cdef int _zsolve_discrete_lyapunov(np.complex128_t * a, np.complex128_t * q, int n, int complex_step=False) except *:
    # Note: some of this code (esp. the Sylvester solving part) cribbed from
    # https://raw.githubusercontent.com/scipy/scipy/master/scipy/linalg/_solvers.py

    # Solve an equation of the form $A'XA-X=-Q$
    # a: input / output
    # q: input / output
    cdef:
        int i, j
        int info
        int inc = 1
        int n2 = n**2
        np.float64_t scale = 0.0
        np.complex128_t tmp = 0.0
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t delta = -2.0
        char trans
    cdef np.npy_intp dim[2]
    cdef np.complex128_t [::1,:] apI, capI, u, v
    cdef int [::1,:] ipiv
    # Dummy selection function, won't actually be referenced since we don't
    # need to order the eigenvalues in the ?gees call.
    cdef:
        int sdim
        int lwork = 3*n
        bint bwork
    cdef np.npy_intp dim1[1]
    cdef np.complex128_t [::1,:] work
    cdef np.complex128_t [:] wr
    cdef np.float64_t [:] wi

    # Initialize arrays
    dim[0] = n; dim[1] = n;
    apI = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)
    capI = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)
    u = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)
    v = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)
    ipiv = np.PyArray_ZEROS(2, dim, np.NPY_INT32, FORTRAN)

    dim1[0] = n;
    wr = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX128, FORTRAN)
    wi = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)
    #vs = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)
    dim[0] = lwork; dim[1] = lwork;
    work = np.PyArray_ZEROS(2, dim, np.NPY_COMPLEX128, FORTRAN)

    # - Solve for b.conj().transpose() --------

    # Get apI = a + I (stored in apI)
    # = (a + eye)
    # For: c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    blas.zcopy(&n2, a, &inc, &apI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) + I (stored in capI)
    # a^H + I -> capI
    # For: aHI_inv = inv(aH + eye)
    blas.zcopy(&n2, a, &inc, &capI[0,0], &inc)
    # (for loop below adds the identity)

    # Get conj(a) - I (stored in a)
    # a^H - I -> a
    # For: b = np.dot(aH - eye, aHI_inv)
    # (for loop below subtracts the identity)

    # Add / subtract identity matrix
    for i in range(n):
        apI[i,i] = apI[i,i] + 1 # apI -> a + eye
        capI[i,i] = capI[i,i] + 1 # aH + eye
        a[i + i*n] = a[i + i*n] - 1 # a - eye

    # Solve [conj(a) + I] b' = [conj(a) - I] (result stored in a)
    # For: b = np.dot(aH - eye, aHI_inv)
    # Where: aHI_inv = inv(aH + eye)
    # where b = (a^H - eye) (a^H + eye)^{-1}
    # or b^H = (a + eye)^{-1} (a - eye)
    # or (a + eye) b^H = (a - eye)
    lapack.zgetrf(&n, &n, &capI[0,0], &n, &ipiv[0,0], &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU decomposition error.')

    lapack.zgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                               a, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Now we have b^H; we could take the conjugate transpose to get b, except
    # that the input to the continuous Lyapunov equation is exactly
    # b^H, so we already have the quantity we need.

    # - Solve for (-c) --------

    # where c = 2*np.dot(np.dot(inv(a + eye), q), aHI_inv)
    # = 2*(a + eye)^{-1} q (a^H + eye)^{-1}
    # and with q Hermitian
    # consider x = (a + eye)^{-1} q (a^H + eye)^{-1}
    # this can be done in two linear solving steps:
    # 1. consider y = q (a^H + eye)^{-1}
    #    or y (a^H + eye) = q
    #    or (a^H + eye)^H y^H = q^H
    #    or (a + eye) y^H = q
    # 2. Then consider x = (a + eye)^{-1} y
    #    or (a + eye) x = y

    # Solve [conj(a) + I] tmp' = q (result stored in q)
    # For: y = q (a^H + eye)^{-1} => (a + eye) y^H = q
    lapack.zgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # Replace the result (stored in q) with its (conjugate) transpose
    for j in range(1, n):
        for i in range(j):
            tmp = q[i + j*n]
            q[i + j*n] = q[j + i*n]
            q[j + i*n] = tmp

    if not complex_step:
        for i in range(n2):
            q[i] = q[i] - q[i].imag * 2.0j

    lapack.zgetrs("N", &n, &n, &capI[0,0], &n, &ipiv[0,0],
                                        q, &n, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('LU solver error.')

    # q -> -2.0 * q
    blas.zscal(&n2, &delta, q, &inc)

    # - Solve continuous time Lyapunov --------

    # Now solve the continuous time Lyapunov equation (AX + XA^H = Q), on the
    # transformed inputs ...

    # ... which requires solving the continuous time Sylvester equation
    # (AX + XB = Q) where B = A^H

    # Compute the real Schur decomposition of a (unordered)
    # TODO compute the optimal lwork rather than always using 3*n
    lapack.zgees("V", "N", <lapack.zselect1 *> &_zselect1, &n,
                          a, &n,
                          &sdim,
                          &wr[0],
                          &u[0,0], &n,
                          &work[0,0], &lwork,
                          &wi[0],
                          &bwork, &info)

    if not info == 0:
        raise np.linalg.LinAlgError('Schur decomposition solver error.')

    # Get v (so that in the complex step case we can take the conjugate)
    blas.zcopy(&n2, &u[0,0], &inc, &v[0,0], &inc)
    # If complex step, take the conjugate
    if complex_step:
        for i in range(n):
            for j in range(n):
                v[i,j] = v[i,j] - v[i,j].imag * 2.0j

    # Construct f = u^H*q*u (result overwrites q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.zgemm("N", "N", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.zgemm("C", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)

    # DTRYSL Solve op(A)*X + X*op(B) = scale*C which is here:
    # r*X + X*r = scale*q
    # results overwrite q
    blas.zcopy(&n2, a, &inc, &apI[0,0], &inc)
    if complex_step:
        for i in range(n):
            for j in range(n):
                apI[j,i] = apI[j,i] - apI[j,i].imag * 2.0j
    lapack.ztrsyl("N", "C", &inc, &n, &n,
                           a, &n,
                           &apI[0,0], &n,
                           q, &n,
                           &scale, &info)

    # Scale q by scale
    if not scale == 1.0:
        blas.zscal(&n2, <np.complex128_t*> &scale, q, &inc)

    # Calculate the solution: u * q * v^H (results overwrite q)
    # In the usual case, v = u
    # In the complex step case, v = u.conj()
    blas.zgemm("N", "C", &n, &n, &n,
                        &alpha, q, &n,
                                &v[0,0], &n,
                        &beta, &capI[0,0], &n)
    blas.zgemm("N", "N", &n, &n, &n,
                        &alpha, &u[0,0], &n,
                                &capI[0,0], &n,
                        &beta, q, &n)


cpdef _zcompute_coefficients_from_multivariate_pacf(np.complex128_t [::1,:] partial_autocorrelations,
                                                             np.complex128_t [::1,:] error_variance,
                                                             int transform_variance,
                                                             int order, int k_endog):
    """
    Notes
    -----

    This uses the ?trmm BLAS functions which are not available in
    Scipy v0.11.0
    """
    # Constants
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
        int k_endog2 = k_endog**2
        int k_endog_order = k_endog * order
        int k_endog_order1 = k_endog * (order+1)
        int info, s, k
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.complex128_t [::1, :] initial_variance
        np.complex128_t [::1, :] forward_variance
        np.complex128_t [::1, :] backward_variance
        np.complex128_t [::1, :] autocovariances
        np.complex128_t [::1, :] forwards1
        np.complex128_t [::1, :] forwards2
        np.complex128_t [::1, :] backwards1
        np.complex128_t [::1, :] backwards2
        np.complex128_t [::1, :] forward_factors
        np.complex128_t [::1, :] backward_factors
        np.complex128_t [::1, :] tmp
        np.complex128_t [::1, :] tmp2
    # Pointers
    cdef:
        np.complex128_t * forwards
        np.complex128_t * prev_forwards
        np.complex128_t * backwards
        np.complex128_t * prev_backwards
    # ?trmm
    # cdef ztrmm_t *ztrmm = <ztrmm_t*>Capsule_AsVoidPtr(blas.ztrmm._cpointer)

    # dim2[0] = self.k_endog; dim2[1] = storage;
    # self.forecast = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    # If we want to keep the provided variance but with the constrained
    # coefficient matrices, we need to make a copy here, and then after the
    # main loop we will transform the coefficients to match the passed variance
    if not transform_variance:
        initial_variance = np.asfortranarray(error_variance.copy())
        # Need to make the input variance large enough that the recursions
        # don't lead to zero-matrices due to roundoff error, which would case
        # exceptions from the Cholesky decompositions.
        # Note that this will still not always ensure positive definiteness,
        # and for k_endog, order large enough an exception may still be raised
        error_variance = np.asfortranarray(np.eye(k_endog, dtype=complex) * (order + k_endog)**10)

    # Initialize matrices
    dim2[0] = k_endog; dim2[1] = k_endog;
    forward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    backward_variance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    forward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    backward_factors = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    tmp2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order;
    # \phi_{s,k}, s = 1, ..., p
    #             k = 1, ..., s+1
    forwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    forwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    # \phi_{s,k}^*
    backwards1 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    backwards2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    dim2[0] = k_endog; dim2[1] = k_endog_order1;
    autocovariances = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    blas.zcopy(&k_endog2, &error_variance[0,0], &inc, &forward_variance[0,0], &inc)   # \Sigma_s
    blas.zcopy(&k_endog2, &error_variance[0,0], &inc, &backward_variance[0,0], &inc)  # \Sigma_s^*,  s = 0, ..., p
    blas.zcopy(&k_endog2, &error_variance[0,0], &inc, &autocovariances[0,0], &inc)  # \Gamma_s

    # error_variance_factor = linalg.cholesky(error_variance, lower=True)
    blas.zcopy(&k_endog2, &error_variance[0,0], &inc, &forward_factors[0,0], &inc)
    lapack.zpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
    blas.zcopy(&k_endog2, &forward_factors[0,0], &inc, &backward_factors[0,0], &inc)

    # We fill in the entries as follows:
    # [1,1]
    # [2,2], [2,1]
    # [3,3], [3,1], [3,2]
    # ...
    # [p,p], [p,1], ..., [p,p-1]
    # the last row, correctly ordered, is then used as the coefficients
    for s in range(order):  # s = 0, ..., p-1
        if s % 2 == 0:
            forwards = &forwards1[0, 0]
            prev_forwards = &forwards2[0, 0]
            backwards = &backwards1[0, 0]
            prev_backwards = &backwards2[0, 0]
        else:
            forwards = &forwards2[0, 0]
            prev_forwards = &forwards1[0, 0]
            backwards = &backwards2[0, 0]
            prev_backwards = &backwards1[0, 0]

        # Create the "last" (k = s+1) matrix
        # Note: this is for k = s+1. However, below we then have to fill
        # in for k = 1, ..., s in order.
        # P L*^{-1} = x
        # x L* = P
        # L*' x' = P'
        # forwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     forward_factors,
        #     linalg.solve_triangular(
        #         backward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog].T,
        #         lower=True, trans='T').T
        # )
        for k in range(k_endog):
            blas.zcopy(&k_endog, &partial_autocorrelations[k,s*k_endog], &k_endog, &tmp[0, k], &inc)
        lapack.ztrtrs("L", "T", "N", &k_endog, &k_endog, &backward_factors[0,0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # zgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &forward_factors[0,0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &forwards[s*k_endog2], &k_endog)
        blas.ztrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &forward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.zcopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s*k_endog2 + k*k_endog], &inc)

        # P' L^{-1} = x
        # x L = P'
        # L' x' = P
        # backwards[:, s*k_endog:(s+1)*k_endog] = np.dot(
        #     backward_factors,
        #     linalg.solve_triangular(
        #         forward_factors, partial_autocorrelations[:, s*k_endog:(s+1)*k_endog],
        #         lower=True, trans='T').T
        # )
        blas.zcopy(&k_endog2, &partial_autocorrelations[0, s*k_endog], &inc, &tmp[0, 0], &inc)
        lapack.ztrtrs("L", "T", "N", &k_endog, &k_endog, &forward_factors[0, 0], &k_endog,
                                                           &tmp[0, 0], &k_endog, &info)
        # zgemm("N", "T", &k_endog, &k_endog, &k_endog,
        #   &alpha, &backward_factors[0, 0], &k_endog,
        #           &tmp[0, 0], &k_endog,
        #   &beta, &backwards[s * k_endog2], &k_endog)
        blas.ztrmm("R", "L", "T", "N", &k_endog, &k_endog,
          &alpha, &backward_factors[0,0], &k_endog,
                  &tmp[0, 0], &k_endog)
        for k in range(k_endog):
            blas.zcopy(&k_endog, &tmp[k,0], &k_endog, &backwards[s*k_endog2 + k*k_endog], &inc)

        # Update the variance
        # Note: if s >= 1, this will be further updated in the for loop
        # below
        # Also, this calculation will be re-used in the forward variance
        # tmp = np.dot(forwards[:, s*k_endog:(s+1)*k_endog], backward_variance)
        # tmpT = np.dot(backward_variance.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        blas.zgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &beta, &tmp[0, 0], &k_endog)
        # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] = tmp.copy().T
        blas.zcopy(&k_endog2, &tmp[0, 0], &inc, &autocovariances[0, (s+1)*k_endog], &inc)

        # Create the remaining k = 1, ..., s matrices,
        # only has an effect if s >= 1
        for k in range(s):
            # forwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_forwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         forwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_backwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.zcopy(&k_endog2, &prev_forwards[k * k_endog2], &inc, &forwards[k * k_endog2], &inc)
            blas.zgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &forwards[s * k_endog2], &k_endog,
                      &prev_backwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &forwards[k * k_endog2], &k_endog)

            # backwards[:, k*k_endog:(k+1)*k_endog] = (
            #     prev_backwards[:, k*k_endog:(k+1)*k_endog] -
            #     np.dot(
            #         backwards[:, s*k_endog:(s+1)*k_endog],
            #         prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog]
            #     )
            # )
            blas.zcopy(&k_endog2, &prev_backwards[k * k_endog2], &inc, &backwards[k * k_endog2], &inc)
            blas.zgemm("N", "N", &k_endog, &k_endog, &k_endog,
              &gamma, &backwards[s * k_endog2], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &backwards[k * k_endog2], &k_endog)

            # autocovariances[:, (s+1)*k_endog:(s+2)*k_endog] += np.dot(
            #     autocovariances[:, (k+1)*k_endog:(k+2)*k_endog],
            #     prev_forwards[:, (s-k-1)*k_endog:(s-k)*k_endog].T
            # )
            blas.zgemm("N", "T", &k_endog, &k_endog, &k_endog,
              &alpha, &autocovariances[0, (k+1)*k_endog], &k_endog,
                      &prev_forwards[(s - k - 1) * k_endog2], &k_endog,
              &alpha, &autocovariances[0, (s+1)*k_endog], &k_endog)

        # Create forward and backwards variances
        # backward_variance = (
        #     backward_variance -
        #     np.dot(
        #         np.dot(backwards[:, s*k_endog:(s+1)*k_endog], forward_variance),
        #         backwards[:, s*k_endog:(s+1)*k_endog].T
        #     )
        # )
        blas.zgemm("N", "N", &k_endog, &k_endog, &k_endog,
          &alpha, &backwards[s * k_endog2], &k_endog,
                  &forward_variance[0, 0], &k_endog,
          &beta, &tmp2[0, 0], &k_endog)
        blas.zgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp2[0, 0], &k_endog,
                  &backwards[s * k_endog2], &k_endog,
          &alpha, &backward_variance[0, 0], &k_endog)
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmp, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        # forward_variance = (
        #     forward_variance -
        #     np.dot(tmpT.T, forwards[:, s*k_endog:(s+1)*k_endog].T)
        # )
        blas.zgemm("T", "T", &k_endog, &k_endog, &k_endog,
          &gamma, &tmp[0, 0], &k_endog,
                  &forwards[s * k_endog2], &k_endog,
          &alpha, &forward_variance[0, 0], &k_endog)

        # Cholesky factors
        # forward_factors = linalg.cholesky(forward_variance, lower=True)
        # backward_factors =  linalg.cholesky(backward_variance, lower=True)
        blas.zcopy(&k_endog2, &forward_variance[0,0], &inc, &forward_factors[0,0], &inc)
        lapack.zpotrf("L", &k_endog, &forward_factors[0,0], &k_endog, &info)
        blas.zcopy(&k_endog2, &backward_variance[0,0], &inc, &backward_factors[0,0], &inc)
        lapack.zpotrf("L", &k_endog, &backward_factors[0,0], &k_endog, &info)


    # If we do not want to use the transformed variance, we need to
    # adjust the constrained matrices, as presented in Lemma 2.3, see above
    if not transform_variance:
        if order % 2 == 0:
            forwards = &forwards2[0,0]
        else:
            forwards = &forwards1[0,0]

        # Here, we need to construct T such that:
        # variance = T * initial_variance * T'
        # To do that, consider the Cholesky of variance (L) and
        # input_variance (M) to get:
        # L L' = T M M' T' = (TM) (TM)'
        # => L = T M
        # => L M^{-1} = T
        # initial_variance_factor = np.linalg.cholesky(initial_variance)
        # L'
        lapack.zpotrf("U", &k_endog, &initial_variance[0,0], &k_endog, &info)
        # transformed_variance_factor = np.linalg.cholesky(variance)
        # M'
        blas.zcopy(&k_endog2, &forward_variance[0,0], &inc, &tmp[0,0], &inc)
        lapack.zpotrf("U", &k_endog, &tmp[0,0], &k_endog, &info)
        # zpotri("L", &k_endog, &tmp[0,0], &k_endog, &info)

        # We need to zero out the lower triangle of L', because ?trtrs only
        # knows that M' is upper triangular
        for s in range(k_endog - 1):          # column
            for k in range(s+1, k_endog):     # row
                initial_variance[k, s] = 0

        # Note that T is lower triangular
        # L M^{-1} = T
        # M' T' = L'
        # transform = np.dot(initial_variance_factor,
        #                    np.linalg.inv(transformed_variance_factor))
        lapack.ztrtrs("U", "N", "N", &k_endog, &k_endog, &tmp[0,0], &k_endog,
                                                           &initial_variance[0, 0], &k_endog, &info)
        # Now:
        # initial_variance = T'

        for s in range(order):
            # forwards[:, s*k_endog:(s+1)*k_endog] = (
            #     np.dot(
            #         np.dot(transform, forwards[:, s*k_endog:(s+1)*k_endog]),
            #         inv_transform
            #     )
            # )
            # TF T^{-1} = x
            # TF = x T
            # (TF)' = T' x'

            # Get TF
            blas.zcopy(&k_endog2, &forwards[s * k_endog2], &inc, &tmp2[0,0], &inc)
            blas.ztrmm("L", "U", "T", "N", &k_endog, &k_endog,
              &alpha, &initial_variance[0, 0], &k_endog,
                      &tmp2[0, 0], &k_endog)
            for k in range(k_endog):
                blas.zcopy(&k_endog, &tmp2[k,0], &k_endog, &tmp[0, k], &inc)
            # Get x'
            lapack.ztrtrs("U", "N", "N", &k_endog, &k_endog, &initial_variance[0,0], &k_endog,
                                                               &tmp[0, 0], &k_endog, &info)
            # Get x
            for k in range(k_endog):
                blas.zcopy(&k_endog, &tmp[k,0], &k_endog, &forwards[s * k_endog2 + k*k_endog], &inc)


    if order % 2 == 0:
        return forwards2, forward_variance
    else:
        return forwards1, forward_variance

cpdef _zconstrain_sv_less_than_one(np.complex128_t [::1,:] unconstrained, int order, int k_endog):
    """
    Transform arbitrary matrices to matrices with singular values less than
    one.

    Corresponds to Lemma 2.2 in Ansley and Kohn (1986). See
    `constrain_stationary_multivariate` for more details.
    """
    # Constants
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        int k_endog2 = k_endog**2
        int info, i
    # Local variables
    cdef:
        np.npy_intp dim2[2]
        np.complex128_t [::1, :] constrained
        np.complex128_t [::1, :] tmp
        np.complex128_t [::1, :] eye

    dim2[0] = k_endog; dim2[1] = k_endog * order;
    constrained = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    dim2[0] = k_endog; dim2[1] = k_endog;
    tmp = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
    eye = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    eye = np.asfortranarray(np.eye(k_endog, dtype=complex))
    for i in range(order):
        blas.zcopy(&k_endog2, &eye[0, 0], &inc, &tmp[0, 0], &inc)
        blas.zgemm("N", "T", &k_endog, &k_endog, &k_endog,
          &alpha, &unconstrained[0, i*k_endog], &k_endog,
                  &unconstrained[0, i*k_endog], &k_endog,
          &alpha, &tmp[0, 0], &k_endog)
        lapack.zpotrf("L", &k_endog, &tmp[0, 0], &k_endog, &info)

        blas.zcopy(&k_endog2, &unconstrained[0, i*k_endog], &inc, &constrained[0, i*k_endog], &inc)
        # constrained.append(linalg.solve_triangular(B, A, lower=lower))
        lapack.ztrtrs("L", "N", "N", &k_endog, &k_endog, &tmp[0, 0], &k_endog,
                                                           &constrained[0, i*k_endog], &k_endog, &info)
    return constrained

cdef int _zldl(np.complex128_t * A, int n) except *:
    # See Golub and Van Loan, Algorithm 4.1.2
    cdef:
        int info = 0
        int j, i, k
        cdef np.npy_intp dim[1]
        np.float64_t tol = 1e-15
        np.complex128_t [:] v

    dim[0] = n
    v = np.PyArray_ZEROS(1, dim, np.NPY_COMPLEX128, FORTRAN)

    for j in range(n):
        # Compute v(1:j)
        v[j] = A[j + j*n]

        # Positive definite element: use Golub and Van Loan algorithm
        if v[j].real < -tol:
            info = -j
            break
        elif v[j].real > tol:
            for i in range(j):
                v[i] = A[j + i*n] * A[i + i*n]
                v[j] = v[j] - A[j + i*n] * v[i]

            # Store d(j) and compute L(j+1:n,j)
            A[j + j*n] = v[j]
            for i in range(j+1, n):
                for k in range(j):
                    A[i + j*n] = A[i + j*n] - A[i + k*n] * v[k]
                A[i + j*n] = A[i + j*n] / v[j]
        # Positive semi-definite element: zero the appropriate column
        else:
            info = 1
            for i in range(j, n):
                A[i + j*n]

    return info

cpdef int zldl(np.complex128_t [::1, :] A) except *:
    _zldl(&A[0,0], A.shape[0])

cdef int _zreorder_missing_diagonal(np.complex128_t * a, int * missing, int n):
    """
    a is a pointer to an n x n diagonal array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            a[i + i*n] = a[k + k*n]
            k = k - 1
        else:
            a[i + i*n] = 0

cdef int _zreorder_missing_submatrix(np.complex128_t * a, int * missing, int n):
    """
    a is a pointer to an n x n array A
    missing is a pointer to an n x 1 array
    n is the dimension of A
    """
    cdef int i, j, k, nobs

    _zreorder_missing_rows(a, missing, n, n)
    _zreorder_missing_cols(a, missing, n, n)

cdef int _zreorder_missing_rows(np.complex128_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(n-1,-1,-1):
        if not missing[i]:
            blas.zswap(&m, &a[i], &n, &a[k], &n)
            k = k - 1


cdef int _zreorder_missing_cols(np.complex128_t * a, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(m-1,-1,-1):
        if not missing[i]:
            blas.zswap(&n, &a[i*n], &inc, &a[k*n], &inc)
            k = k - 1

cpdef int zreorder_missing_matrix(np.complex128_t [::1, :, :] A, int [::1, :] missing, int reorder_rows, int reorder_cols, int diagonal) except *:
    cdef int n, m, T, t

    n, m, T = A.shape[0:3]

    if reorder_rows and reorder_cols:
        if not n == m:
            raise RuntimeError('Reordering a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                _zreorder_missing_diagonal(&A[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                _zreorder_missing_submatrix(&A[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with reordering a submatrix')
    elif reorder_rows:
        for t in range(T):
            _zreorder_missing_rows(&A[0, 0, t], &missing[0, t], n, m)
    elif reorder_cols:
        for t in range(T):
            _zreorder_missing_cols(&A[0, 0, t], &missing[0, t], n, m)


cpdef int zreorder_missing_vector(np.complex128_t [::1, :] A, int [::1, :] missing) except *:
    cdef int i, k, t, n, T, nobs

    n, T = A.shape[0:2]

    for t in range(T):
        _zreorder_missing_rows(&A[0, t], &missing[0, t], n, 1)


cdef int _zcopy_missing_diagonal(np.complex128_t * a, np.complex128_t * b, int * missing, int n):
    """
    Copy the non-missing block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    k = nobs-1
    for i in range(nobs):
        b[i + i*n] = a[i + i*n]


cdef int _zcopy_missing_submatrix(np.complex128_t * a, np.complex128_t * b, int * missing, int n):
    """
    Copy the non-missing submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.zcopy(&nobs, &a[i*n], &inc, &b[i*n], &inc)


cdef int _zcopy_missing_rows(np.complex128_t * a, np.complex128_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    nobs = n
    # Construct the non-missing index
    for i in range(n):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.zcopy(&m, &a[i], &n, &b[i], &n)


cdef int _zcopy_missing_cols(np.complex128_t * a, np.complex128_t * b, int * missing, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    missing is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    nobs = m
    # Construct the non-missing index
    for i in range(m):
        nobs = nobs - missing[i]

    # Perform replacement
    for i in range(nobs):
        blas.zcopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int zcopy_missing_matrix(np.complex128_t [::1, :, :] A, np.complex128_t [::1, :, :] B, int [::1, :] missing, int missing_rows, int missing_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if missing_rows and missing_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _zcopy_missing_diagonal(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _zcopy_missing_submatrix(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif missing_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _zcopy_missing_rows(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    elif missing_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _zcopy_missing_cols(&A[0, 0, A_t], &B[0, 0, t], &missing[0, t], n, m)
    pass


cpdef int zcopy_missing_vector(np.complex128_t [::1, :] A, np.complex128_t [::1, :] B, int [::1, :] missing) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _zcopy_missing_rows(&A[0, A_t], &B[0, t], &missing[0, t], n, 1)

cdef int _zcopy_index_diagonal(np.complex128_t * a, np.complex128_t * b, int * index, int n):
    """
    Copy the non-index block of diagonal entries

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            b[i + i*n] = a[i + i*n]


cdef int _zcopy_index_submatrix(np.complex128_t * a, np.complex128_t * b, int * index, int n):
    """
    Copy the non-index submatrix

    a is a pointer to an n x n diagonal array A (copy from)
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the dimension of A, B
    """
    cdef int i, j, k, nobs, inc = 1

    _zcopy_index_rows(a, b, index, n, n)
    _zcopy_index_cols(a, b, index, n, n)


cdef int _zcopy_index_rows(np.complex128_t * a, np.complex128_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an n x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs

    # Perform replacement
    for i in range(n):
        if index[i]:
            blas.zcopy(&m, &a[i], &n, &b[i], &n)


cdef int _zcopy_index_cols(np.complex128_t * a, np.complex128_t * b, int * index, int n, int m):
    """
    a is a pointer to an n x m array A
    b is a pointer to an n x n diagonal array B (copy to)
    index is a pointer to an m x 1 array
    n is the number of rows of A
    m is the number of columns of A
    """
    cdef int i, j, k, nobs, inc = 1

    # Perform replacement
    for i in range(m):
        if index[i]:
            blas.zcopy(&n, &a[i*n], &inc, &b[i*n], &inc)


cpdef int zcopy_index_matrix(np.complex128_t [::1, :, :] A, np.complex128_t [::1, :, :] B, int [::1, :] index, int index_rows, int index_cols, int diagonal) except *:
    cdef int n, m, T, t, A_T, A_t = 0, time_varying

    n, m, T = B.shape[0:3]
    A_T = A.shape[2]
    time_varying = (A_T == T)

    if index_rows and index_cols:
        if not n == m:
            raise RuntimeError('Copying a submatrix requires n = m')
        if diagonal:
            for t in range(T):
                if time_varying:
                    A_t = t
                _zcopy_index_diagonal(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
        else:
            for t in range(T):
                if time_varying:
                    A_t = t
                _zcopy_index_submatrix(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n)
    elif diagonal:
        raise RuntimeError('`diagonal` argument only valid with copying a submatrix')
    elif index_rows:
        for t in range(T):
            if time_varying:
                    A_t = t
            _zcopy_index_rows(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)
    elif index_cols:
        for t in range(T):
            if time_varying:
                    A_t = t
            _zcopy_index_cols(&A[0, 0, A_t], &B[0, 0, t], &index[0, t], n, m)


cpdef int zcopy_index_vector(np.complex128_t [::1, :] A, np.complex128_t [::1, :] B, int [::1, :] index) except *:
    cdef int n, t, T, A_t = 0, A_T

    n, T = B.shape[0:2]
    A_T = A.shape[1]
    time_varying = (A_T == T)

    for t in range(T):
        if time_varying:
            A_t = t
        _zcopy_index_rows(&A[0, A_t], &B[0, t], &index[0, t], n, 1)

cdef int _zselect_cov(int k_states, int k_posdef, int k_states_total,
                               np.complex128_t * tmp,
                               np.complex128_t * selection,
                               np.complex128_t * cov,
                               np.complex128_t * selected_cov):
    cdef:
        int i, k_states2 = k_states**2
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    # Only need to do something if there is a covariance matrix
    # (i.e k_posdof == 0)
    if k_posdef > 0:

        # #### Calculate selected state covariance matrix  
        # $Q_t^* = R_t Q_t R_t'$
        # 
        # Combine a selection matrix and a covariance matrix to get
        # a simplified (but possibly singular) "selected" covariance
        # matrix (see e.g. Durbin and Koopman p. 43)

        # `tmp0` array used here, dimension $(m \times r)$  

        # $\\#_0 = 1.0 * R_t Q_t$  
        # $(m \times r) = (m \times r) (r \times r)$
        blas.zgemm("N", "N", &k_states, &k_posdef, &k_posdef,
              &alpha, selection, &k_states_total,
                      cov, &k_posdef,
              &beta, tmp, &k_states)
        # $Q_t^* = 1.0 * \\#_0 R_t'$  
        # $(m \times m) = (m \times r) (m \times r)'$
        blas.zgemm("N", "T", &k_states, &k_states, &k_posdef,
              &alpha, tmp, &k_states,
                      selection, &k_states_total,
              &beta, selected_cov, &k_states)
    else:
        for i in range(k_states2):
            selected_cov[i] = 0
