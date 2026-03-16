#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
Innovations algorithm

Author: Chad Fulton
License: Simplified-BSD
"""

# Typical imports
import numpy as np
import warnings
from statsmodels.tsa import arima_process
from statsmodels.src.math cimport NPY_PI, dlog, zlog
cimport numpy as cnp
cimport cython

cnp.import_array()

cdef int C = 0



cdef stoeplitz(int n, int offset0, int offset1,
                        cnp.float32_t [:] in_column,
                        cnp.float32_t [:, :] out_matrix):
    """
    toeplitz(int n, int o0, int o1, cnp.float32_t [:] in_column, cnp.float32_t [:, :] out_matrix
    
    Construct a Toeplitz block in a matrix in place

    Parameters
    ----------
    n : int
        The number of entries of `in_column` to use.
    offset0 : int
        The row offset for `out_matrix` at which to begin writing the Toeplitz
        block.
    offset1 : int
        The column offset for `out_matrix` at which to begin writing the
        Toeplitz block.
    in_column : ndarray
        The column used to construct the Toeplitz block.
    out_matrix : ndarray
        The matrix in which to write a Toeplitz block

    Notes
    -----
    This function constructs the Toeplitz block in-place and does not return
    any output.

    """
    cdef Py_ssize_t i, j

    for i in range(n):
        for j in range(i + 1):
            out_matrix[offset0 + i, offset1 + j] = in_column[i - j]
            if i != j:
              # Note: scipy by default does complex conjugate, but not
              # necessary here since we're only dealing with covariances,
              # which will be real (except in the case of complex-step
              # differentiation, but in that case we do not want to apply
              # the conjugation anyway)
              out_matrix[offset0 + j, offset1 + i] = in_column[i - j]


cpdef sarma_transformed_acovf_fast(cnp.float32_t [:] ar,
                                            cnp.float32_t [:] ma,
                                            cnp.float32_t [:] arma_acovf):
    """
    arma_transformed_acovf_fast(cnp.float32_t [:] ar, cnp.float32_t [:] ma, cnp.float32_t [:] arma_acovf)
    
    Quickly construct the autocovariance matrix for a transformed process.

    Using the autocovariance function for an ARMA process, constructs the
    autocovariances associated with the transformed process described
    in equation (3.3.1) of _[1] in a memory efficient, and so fast, way.

    Parameters
    ----------
    ar : ndarray
        Autoregressive coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the left-hand-side of the ARMA definition (i.e. they have the opposite
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    ma : ndarray
        Moving average coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the right-hand-side of the ARMA definition (i.e. they have the same
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    arma_acovf : ndarray
        The vector of autocovariances of the ARMA process.

    Returns
    -------
    acovf : ndarray
        A matrix containing the autocovariances of the portion of the
        transformed process with time-varying autocovariances. Its dimension
        is equal to `min(m * 2, n)` where `m = max(len(ar) - 1, len(ma) - 1)`
        and `n` is the length of the input array `arma_acovf`. It is important
        to note that only the values in the first `m` columns or `m` rows are
        valid. In particular, the entries in the block `acovf[m:, m:]` should
        not be used in any case (and in fact will always be equal to zeros).
    acovf2 : ndarray
        An array containing the autocovariance function of the portion of the
        transformed process with time-invariant autocovariances. Its dimension
        is equal to `max(n - m, 0)` where `n` is the length of the input
        array `arma_acovf`.

    Notes
    -----
    The definition of this autocovariance matrix is from _[1] equation 3.3.3.

    This function assumes that the variance of the ARMA innovation term is
    equal to one. If this is not true, then the calling program should replace
    `arma_acovf` with `arma_acovf / sigma2`, where sigma2 is that variance.

    This function is relatively fast even when `arma_acovf` is large, since
    it only constructs the full autocovariance matrix for a generally small
    subset of observations. The trade-off is that the output of this function
    is somewhat more difficult to use.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, p, q, m, m2, n, i, j, r, tmp_ix
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.float32_t [:, :] acovf
    cdef cnp.float32_t [:] acovf2

    nobs = arma_acovf.shape[0]
    p = len(ar) - 1
    q = len(ma) - 1
    m = max(p, q)
    m2 = 2 * m
    n = min(m2, nobs)

    dim2[0] = m2;
    dim2[1] = m2;
    acovf = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_FLOAT32, C)
    dim1[0] = max(nobs - m, 0);
    acovf2 = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)

    # Set i, j = 1, ..., m   (which is then done)
    stoeplitz(m, 0, 0, arma_acovf, acovf)

    # Set i = 1, ..., m;         j = m + 1, ..., 2 m
    # and i = m + 1, ..., 2 m;   j = 1, ..., m
    if nobs > m:
        for j in range(m):
            for i in range(m, m2):
                acovf[i, j] = arma_acovf[i - j]
                for r in range(1, p + 1):
                    tmp_ix = abs(r - (i - j))
                    acovf[i, j] = acovf[i, j] - (-ar[r] * arma_acovf[tmp_ix])
        acovf[:m, m:m2] = acovf[m:m2, :m].T

    # Set values for |i - j| <= q, min(i, j) = m + 1, and max(i, j) <= nobs
    if nobs > m:
        for i in range(nobs - m):
            for r in range(q + 1 - i):
                acovf2[i] = acovf2[i] + ma[r] * ma[r + i]

    return acovf[:n, :n], acovf2


cpdef sarma_innovations_algo_fast(int nobs,
                                           cnp.float32_t [:] ar_params,
                                           cnp.float32_t [:] ma_params,
                                           cnp.float32_t [:, :] acovf,
                                           cnp.float32_t [:] acovf2):
    """
    arma_innovations_algo_fast(int nobs, cnp.float32_t [:] ar_params, cnp.float32_t [:] ma_params, cnp.float32_t [:, :] acovf, cnp.float32_t [:] acovf2)
    
    Quickly apply innovations algorithm for an ARMA process.

    Parameters
    ----------
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    acovf : ndarray
        An `m * 2` x `m * 2` autocovariance matrix at least the first `m`
        columns filled in, where `m = max(len(ar_params), ma_params)`
        (see `arma_transformed_acovf_fast`).
    acovf2 : ndarray
        A `max(0, nobs - m)` length vector containing the autocovariance
        function associated with the final `nobs - m` observations.

    Returns
    -------
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm.
    v : ndarray
        The vector of mean squared errors.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    This function is relatively fast even with a large number of observations
    since we can exploit a number of known zeros in the theta array.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t i, j, k, n, _n, m, m2, p, q, start, start2
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.float32_t [:, :] theta
    cdef cnp.float32_t [:] v

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)
    m2 = 2 * m

    dim1[0] = nobs;
    v = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)
    dim2[0] = nobs;
    dim2[1] = m + 1;
    theta = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_FLOAT32, C)

    if m > 0:
        v[0] = acovf[0, 0]
    else:
        # (handle the corner case where p = q = 0)
        v[0] = acovf2[0]

    for n in range(nobs - 1):
        _n = n + 1

        start = 0 if n < m else n + 1 - q
        for k in range(start, n + 1):
            # See Brockwell and Davis, p. 100-101
            # (here we have weak rather than strict inequality due to Python's
            # zero indexing)
            if n >= m and n - k >= q:
                continue

            if n + 1 < m2 and k < m:
                theta[_n, n - k] = acovf[n + 1, k]
            else:
                theta[_n, n - k] = acovf2[n + 1 - k]

            start2 = 0 if n < m else n - m
            for j in range(start2, k):
                if n - j < m + 1:
                    theta[_n, n - k] = theta[_n, n - k] - theta[k - 1 + 1, k - j - 1] * theta[_n, n - j] * v[j]
            theta[_n, n - k] = theta[_n, n - k] / v[k]

        if n + 1 < m:
            v[n + 1] = acovf[n + 1, n + 1]
        else:
            v[n + 1] = acovf2[0]
        start = max(0, n - (m + 1) + 2)
        for i in range(start, n + 1):
            v[n + 1] = v[n + 1] - theta[_n, n - i]**2 * v[i]

    return theta, v


cpdef sarma_innovations_filter(cnp.float32_t [:] endog,
                                        cnp.float32_t [:] ar_params,
                                        cnp.float32_t [:] ma_params,
                                        cnp.float32_t [:, :] theta):
    """
    arma_innovations_filter(cnp.float32_t [:] endog, cnp.float32_t [:] ar_params, cnp.float32_t [:] ma_params, cnp.float32_t [:, :] theta):
    
    Innovations filter for an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm (see `arma_innovations_algo` or `arma_innovations_algo_fast`)

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, i, k, j, m, p, q
    cdef cnp.npy_intp dim1[1]
    cdef cnp.float32_t [:] u
    cdef cnp.float32_t hat

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)

    nobs = theta.shape[0]
    k = theta.shape[1]

    dim1[0] = nobs;
    u = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)
    u[0] = endog[0]

    for i in range(1, nobs):
        hat = 0
        if i < m:
            for j in range(i):
                hat = hat + theta[i, j] * u[i - j - 1]
        else:
            for j in range(p):
                hat = hat + ar_params[j] * endog[i - j - 1]
            for j in range(q):
                hat = hat + theta[i, j] * u[i - j - 1]
        u[i] = endog[i] - hat

    return u


cpdef sarma_innovations(cnp.float32_t [:] endog,
                                      cnp.float32_t [:] ar_params,
                                      cnp.float32_t [:] ma_params,
                                      cnp.float32_t sigma2):
    """
    arma_innovations(cnp.float32_t [:] endog, cnp.float32_t [:] ar_params, cnp.float32_t [:] ma_params):
    
    Compute innovations and variances based on an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.
    v : ndarray
        The vector of innovation variances.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.float32_t const
    cdef cnp.float32_t [:] ar, ma, arma_acovf, llf_obs, acovf2, u, v
    cdef cnp.float32_t [:, :] acovf
    cdef cnp.npy_intp dim1[1]

    dim1[0] = len(ar_params) + 1;
    ar = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)
    dim1[0] = len(ma_params) + 1;
    ma = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)

    ar[0] = 1
    for i in range(1, len(ar_params) + 1):
        ar[i] = -1 * ar_params[i - 1]
    ma[0] = 1
    ma[1:] = ma_params

    arma_acovf = arima_process.arma_acovf(ar, ma, nobs, sigma2, dtype=np.float32) / sigma2
    acovf, acovf2 = sarma_transformed_acovf_fast(ar, ma, arma_acovf)
    theta, v = sarma_innovations_algo_fast(nobs, ar_params, ma_params, acovf, acovf2)
    u = sarma_innovations_filter(endog, ar_params, ma_params, theta)

    return u, v


cpdef sarma_loglikeobs_fast(cnp.float32_t [:] endog,
                                     cnp.float32_t [:] ar_params,
                                     cnp.float32_t [:] ma_params,
                                     cnp.float32_t sigma2):
    """
    sarma_loglikeobs_fast(cnp.float32_t [:] endog, cnp.float32_t [:] ar_params, cnp.float32_t [:] ma_params, cnp.float32_t sigma2)

    Quickly calculate the loglikelihood of each observation for an ARMA process

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    loglike : ndarray of float
        Array of loglikelihood values for each observation.

    Notes
    -----
    Details related to computing the loglikelihood associated with an ARMA
    process using the innovations algorithm are given in _[1] section 5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """

    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.float32_t const
    cdef cnp.float32_t [:] llf_obs, u, v
    cdef cnp.npy_intp dim1[1]

    u, v = sarma_innovations(endog, ar_params, ma_params, sigma2)

    dim1[0] = nobs;
    llf_obs = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT32, C)

    const = dlog(2*NPY_PI)
    for i in range(nobs):
        llf_obs[i] = -0.5 * u[i]**2 / (sigma2 * v[i]) - 0.5 * (const + dlog(sigma2 * v[i]))

    return np.array(llf_obs, dtype=np.float32)


cdef dtoeplitz(int n, int offset0, int offset1,
                        cnp.float64_t [:] in_column,
                        cnp.float64_t [:, :] out_matrix):
    """
    toeplitz(int n, int o0, int o1, cnp.float64_t [:] in_column, cnp.float64_t [:, :] out_matrix
    
    Construct a Toeplitz block in a matrix in place

    Parameters
    ----------
    n : int
        The number of entries of `in_column` to use.
    offset0 : int
        The row offset for `out_matrix` at which to begin writing the Toeplitz
        block.
    offset1 : int
        The column offset for `out_matrix` at which to begin writing the
        Toeplitz block.
    in_column : ndarray
        The column used to construct the Toeplitz block.
    out_matrix : ndarray
        The matrix in which to write a Toeplitz block

    Notes
    -----
    This function constructs the Toeplitz block in-place and does not return
    any output.

    """
    cdef Py_ssize_t i, j

    for i in range(n):
        for j in range(i + 1):
            out_matrix[offset0 + i, offset1 + j] = in_column[i - j]
            if i != j:
              # Note: scipy by default does complex conjugate, but not
              # necessary here since we're only dealing with covariances,
              # which will be real (except in the case of complex-step
              # differentiation, but in that case we do not want to apply
              # the conjugation anyway)
              out_matrix[offset0 + j, offset1 + i] = in_column[i - j]


cpdef darma_transformed_acovf_fast(cnp.float64_t [:] ar,
                                            cnp.float64_t [:] ma,
                                            cnp.float64_t [:] arma_acovf):
    """
    arma_transformed_acovf_fast(cnp.float64_t [:] ar, cnp.float64_t [:] ma, cnp.float64_t [:] arma_acovf)
    
    Quickly construct the autocovariance matrix for a transformed process.

    Using the autocovariance function for an ARMA process, constructs the
    autocovariances associated with the transformed process described
    in equation (3.3.1) of _[1] in a memory efficient, and so fast, way.

    Parameters
    ----------
    ar : ndarray
        Autoregressive coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the left-hand-side of the ARMA definition (i.e. they have the opposite
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    ma : ndarray
        Moving average coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the right-hand-side of the ARMA definition (i.e. they have the same
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    arma_acovf : ndarray
        The vector of autocovariances of the ARMA process.

    Returns
    -------
    acovf : ndarray
        A matrix containing the autocovariances of the portion of the
        transformed process with time-varying autocovariances. Its dimension
        is equal to `min(m * 2, n)` where `m = max(len(ar) - 1, len(ma) - 1)`
        and `n` is the length of the input array `arma_acovf`. It is important
        to note that only the values in the first `m` columns or `m` rows are
        valid. In particular, the entries in the block `acovf[m:, m:]` should
        not be used in any case (and in fact will always be equal to zeros).
    acovf2 : ndarray
        An array containing the autocovariance function of the portion of the
        transformed process with time-invariant autocovariances. Its dimension
        is equal to `max(n - m, 0)` where `n` is the length of the input
        array `arma_acovf`.

    Notes
    -----
    The definition of this autocovariance matrix is from _[1] equation 3.3.3.

    This function assumes that the variance of the ARMA innovation term is
    equal to one. If this is not true, then the calling program should replace
    `arma_acovf` with `arma_acovf / sigma2`, where sigma2 is that variance.

    This function is relatively fast even when `arma_acovf` is large, since
    it only constructs the full autocovariance matrix for a generally small
    subset of observations. The trade-off is that the output of this function
    is somewhat more difficult to use.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, p, q, m, m2, n, i, j, r, tmp_ix
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.float64_t [:, :] acovf
    cdef cnp.float64_t [:] acovf2

    nobs = arma_acovf.shape[0]
    p = len(ar) - 1
    q = len(ma) - 1
    m = max(p, q)
    m2 = 2 * m
    n = min(m2, nobs)

    dim2[0] = m2;
    dim2[1] = m2;
    acovf = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_FLOAT64, C)
    dim1[0] = max(nobs - m, 0);
    acovf2 = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)

    # Set i, j = 1, ..., m   (which is then done)
    dtoeplitz(m, 0, 0, arma_acovf, acovf)

    # Set i = 1, ..., m;         j = m + 1, ..., 2 m
    # and i = m + 1, ..., 2 m;   j = 1, ..., m
    if nobs > m:
        for j in range(m):
            for i in range(m, m2):
                acovf[i, j] = arma_acovf[i - j]
                for r in range(1, p + 1):
                    tmp_ix = abs(r - (i - j))
                    acovf[i, j] = acovf[i, j] - (-ar[r] * arma_acovf[tmp_ix])
        acovf[:m, m:m2] = acovf[m:m2, :m].T

    # Set values for |i - j| <= q, min(i, j) = m + 1, and max(i, j) <= nobs
    if nobs > m:
        for i in range(nobs - m):
            for r in range(q + 1 - i):
                acovf2[i] = acovf2[i] + ma[r] * ma[r + i]

    return acovf[:n, :n], acovf2


cpdef darma_innovations_algo_fast(int nobs,
                                           cnp.float64_t [:] ar_params,
                                           cnp.float64_t [:] ma_params,
                                           cnp.float64_t [:, :] acovf,
                                           cnp.float64_t [:] acovf2):
    """
    arma_innovations_algo_fast(int nobs, cnp.float64_t [:] ar_params, cnp.float64_t [:] ma_params, cnp.float64_t [:, :] acovf, cnp.float64_t [:] acovf2)
    
    Quickly apply innovations algorithm for an ARMA process.

    Parameters
    ----------
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    acovf : ndarray
        An `m * 2` x `m * 2` autocovariance matrix at least the first `m`
        columns filled in, where `m = max(len(ar_params), ma_params)`
        (see `arma_transformed_acovf_fast`).
    acovf2 : ndarray
        A `max(0, nobs - m)` length vector containing the autocovariance
        function associated with the final `nobs - m` observations.

    Returns
    -------
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm.
    v : ndarray
        The vector of mean squared errors.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    This function is relatively fast even with a large number of observations
    since we can exploit a number of known zeros in the theta array.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t i, j, k, n, _n, m, m2, p, q, start, start2
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.float64_t [:, :] theta
    cdef cnp.float64_t [:] v

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)
    m2 = 2 * m

    dim1[0] = nobs;
    v = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)
    dim2[0] = nobs;
    dim2[1] = m + 1;
    theta = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_FLOAT64, C)

    if m > 0:
        v[0] = acovf[0, 0]
    else:
        # (handle the corner case where p = q = 0)
        v[0] = acovf2[0]

    for n in range(nobs - 1):
        _n = n + 1

        start = 0 if n < m else n + 1 - q
        for k in range(start, n + 1):
            # See Brockwell and Davis, p. 100-101
            # (here we have weak rather than strict inequality due to Python's
            # zero indexing)
            if n >= m and n - k >= q:
                continue

            if n + 1 < m2 and k < m:
                theta[_n, n - k] = acovf[n + 1, k]
            else:
                theta[_n, n - k] = acovf2[n + 1 - k]

            start2 = 0 if n < m else n - m
            for j in range(start2, k):
                if n - j < m + 1:
                    theta[_n, n - k] = theta[_n, n - k] - theta[k - 1 + 1, k - j - 1] * theta[_n, n - j] * v[j]
            theta[_n, n - k] = theta[_n, n - k] / v[k]

        if n + 1 < m:
            v[n + 1] = acovf[n + 1, n + 1]
        else:
            v[n + 1] = acovf2[0]
        start = max(0, n - (m + 1) + 2)
        for i in range(start, n + 1):
            v[n + 1] = v[n + 1] - theta[_n, n - i]**2 * v[i]

    return theta, v


cpdef darma_innovations_filter(cnp.float64_t [:] endog,
                                        cnp.float64_t [:] ar_params,
                                        cnp.float64_t [:] ma_params,
                                        cnp.float64_t [:, :] theta):
    """
    arma_innovations_filter(cnp.float64_t [:] endog, cnp.float64_t [:] ar_params, cnp.float64_t [:] ma_params, cnp.float64_t [:, :] theta):
    
    Innovations filter for an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm (see `arma_innovations_algo` or `arma_innovations_algo_fast`)

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, i, k, j, m, p, q
    cdef cnp.npy_intp dim1[1]
    cdef cnp.float64_t [:] u
    cdef cnp.float64_t hat

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)

    nobs = theta.shape[0]
    k = theta.shape[1]

    dim1[0] = nobs;
    u = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)
    u[0] = endog[0]

    for i in range(1, nobs):
        hat = 0
        if i < m:
            for j in range(i):
                hat = hat + theta[i, j] * u[i - j - 1]
        else:
            for j in range(p):
                hat = hat + ar_params[j] * endog[i - j - 1]
            for j in range(q):
                hat = hat + theta[i, j] * u[i - j - 1]
        u[i] = endog[i] - hat

    return u


cpdef darma_innovations(cnp.float64_t [:] endog,
                                      cnp.float64_t [:] ar_params,
                                      cnp.float64_t [:] ma_params,
                                      cnp.float64_t sigma2):
    """
    arma_innovations(cnp.float64_t [:] endog, cnp.float64_t [:] ar_params, cnp.float64_t [:] ma_params):
    
    Compute innovations and variances based on an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.
    v : ndarray
        The vector of innovation variances.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.float64_t const
    cdef cnp.float64_t [:] ar, ma, arma_acovf, llf_obs, acovf2, u, v
    cdef cnp.float64_t [:, :] acovf
    cdef cnp.npy_intp dim1[1]

    dim1[0] = len(ar_params) + 1;
    ar = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)
    dim1[0] = len(ma_params) + 1;
    ma = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)

    ar[0] = 1
    for i in range(1, len(ar_params) + 1):
        ar[i] = -1 * ar_params[i - 1]
    ma[0] = 1
    ma[1:] = ma_params

    arma_acovf = arima_process.arma_acovf(ar, ma, nobs, sigma2, dtype=float) / sigma2
    acovf, acovf2 = darma_transformed_acovf_fast(ar, ma, arma_acovf)
    theta, v = darma_innovations_algo_fast(nobs, ar_params, ma_params, acovf, acovf2)
    u = darma_innovations_filter(endog, ar_params, ma_params, theta)

    return u, v


cpdef darma_loglikeobs_fast(cnp.float64_t [:] endog,
                                     cnp.float64_t [:] ar_params,
                                     cnp.float64_t [:] ma_params,
                                     cnp.float64_t sigma2):
    """
    darma_loglikeobs_fast(cnp.float64_t [:] endog, cnp.float64_t [:] ar_params, cnp.float64_t [:] ma_params, cnp.float64_t sigma2)

    Quickly calculate the loglikelihood of each observation for an ARMA process

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    loglike : ndarray of float
        Array of loglikelihood values for each observation.

    Notes
    -----
    Details related to computing the loglikelihood associated with an ARMA
    process using the innovations algorithm are given in _[1] section 5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """

    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.float64_t const
    cdef cnp.float64_t [:] llf_obs, u, v
    cdef cnp.npy_intp dim1[1]

    u, v = darma_innovations(endog, ar_params, ma_params, sigma2)

    dim1[0] = nobs;
    llf_obs = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_FLOAT64, C)

    const = dlog(2*NPY_PI)
    for i in range(nobs):
        llf_obs[i] = -0.5 * u[i]**2 / (sigma2 * v[i]) - 0.5 * (const + dlog(sigma2 * v[i]))

    return np.array(llf_obs, dtype=float)


cdef ctoeplitz(int n, int offset0, int offset1,
                        cnp.complex64_t [:] in_column,
                        cnp.complex64_t [:, :] out_matrix):
    """
    toeplitz(int n, int o0, int o1, cnp.complex64_t [:] in_column, cnp.complex64_t [:, :] out_matrix
    
    Construct a Toeplitz block in a matrix in place

    Parameters
    ----------
    n : int
        The number of entries of `in_column` to use.
    offset0 : int
        The row offset for `out_matrix` at which to begin writing the Toeplitz
        block.
    offset1 : int
        The column offset for `out_matrix` at which to begin writing the
        Toeplitz block.
    in_column : ndarray
        The column used to construct the Toeplitz block.
    out_matrix : ndarray
        The matrix in which to write a Toeplitz block

    Notes
    -----
    This function constructs the Toeplitz block in-place and does not return
    any output.

    """
    cdef Py_ssize_t i, j

    for i in range(n):
        for j in range(i + 1):
            out_matrix[offset0 + i, offset1 + j] = in_column[i - j]
            if i != j:
              # Note: scipy by default does complex conjugate, but not
              # necessary here since we're only dealing with covariances,
              # which will be real (except in the case of complex-step
              # differentiation, but in that case we do not want to apply
              # the conjugation anyway)
              out_matrix[offset0 + j, offset1 + i] = in_column[i - j]


cpdef carma_transformed_acovf_fast(cnp.complex64_t [:] ar,
                                            cnp.complex64_t [:] ma,
                                            cnp.complex64_t [:] arma_acovf):
    """
    arma_transformed_acovf_fast(cnp.complex64_t [:] ar, cnp.complex64_t [:] ma, cnp.complex64_t [:] arma_acovf)
    
    Quickly construct the autocovariance matrix for a transformed process.

    Using the autocovariance function for an ARMA process, constructs the
    autocovariances associated with the transformed process described
    in equation (3.3.1) of _[1] in a memory efficient, and so fast, way.

    Parameters
    ----------
    ar : ndarray
        Autoregressive coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the left-hand-side of the ARMA definition (i.e. they have the opposite
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    ma : ndarray
        Moving average coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the right-hand-side of the ARMA definition (i.e. they have the same
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    arma_acovf : ndarray
        The vector of autocovariances of the ARMA process.

    Returns
    -------
    acovf : ndarray
        A matrix containing the autocovariances of the portion of the
        transformed process with time-varying autocovariances. Its dimension
        is equal to `min(m * 2, n)` where `m = max(len(ar) - 1, len(ma) - 1)`
        and `n` is the length of the input array `arma_acovf`. It is important
        to note that only the values in the first `m` columns or `m` rows are
        valid. In particular, the entries in the block `acovf[m:, m:]` should
        not be used in any case (and in fact will always be equal to zeros).
    acovf2 : ndarray
        An array containing the autocovariance function of the portion of the
        transformed process with time-invariant autocovariances. Its dimension
        is equal to `max(n - m, 0)` where `n` is the length of the input
        array `arma_acovf`.

    Notes
    -----
    The definition of this autocovariance matrix is from _[1] equation 3.3.3.

    This function assumes that the variance of the ARMA innovation term is
    equal to one. If this is not true, then the calling program should replace
    `arma_acovf` with `arma_acovf / sigma2`, where sigma2 is that variance.

    This function is relatively fast even when `arma_acovf` is large, since
    it only constructs the full autocovariance matrix for a generally small
    subset of observations. The trade-off is that the output of this function
    is somewhat more difficult to use.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, p, q, m, m2, n, i, j, r, tmp_ix
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.complex64_t [:, :] acovf
    cdef cnp.complex64_t [:] acovf2

    nobs = arma_acovf.shape[0]
    p = len(ar) - 1
    q = len(ma) - 1
    m = max(p, q)
    m2 = 2 * m
    n = min(m2, nobs)

    dim2[0] = m2;
    dim2[1] = m2;
    acovf = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_COMPLEX64, C)
    dim1[0] = max(nobs - m, 0);
    acovf2 = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)

    # Set i, j = 1, ..., m   (which is then done)
    ctoeplitz(m, 0, 0, arma_acovf, acovf)

    # Set i = 1, ..., m;         j = m + 1, ..., 2 m
    # and i = m + 1, ..., 2 m;   j = 1, ..., m
    if nobs > m:
        for j in range(m):
            for i in range(m, m2):
                acovf[i, j] = arma_acovf[i - j]
                for r in range(1, p + 1):
                    tmp_ix = abs(r - (i - j))
                    acovf[i, j] = acovf[i, j] - (-ar[r] * arma_acovf[tmp_ix])
        acovf[:m, m:m2] = acovf[m:m2, :m].T

    # Set values for |i - j| <= q, min(i, j) = m + 1, and max(i, j) <= nobs
    if nobs > m:
        for i in range(nobs - m):
            for r in range(q + 1 - i):
                acovf2[i] = acovf2[i] + ma[r] * ma[r + i]

    return acovf[:n, :n], acovf2


cpdef carma_innovations_algo_fast(int nobs,
                                           cnp.complex64_t [:] ar_params,
                                           cnp.complex64_t [:] ma_params,
                                           cnp.complex64_t [:, :] acovf,
                                           cnp.complex64_t [:] acovf2):
    """
    arma_innovations_algo_fast(int nobs, cnp.complex64_t [:] ar_params, cnp.complex64_t [:] ma_params, cnp.complex64_t [:, :] acovf, cnp.complex64_t [:] acovf2)
    
    Quickly apply innovations algorithm for an ARMA process.

    Parameters
    ----------
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    acovf : ndarray
        An `m * 2` x `m * 2` autocovariance matrix at least the first `m`
        columns filled in, where `m = max(len(ar_params), ma_params)`
        (see `arma_transformed_acovf_fast`).
    acovf2 : ndarray
        A `max(0, nobs - m)` length vector containing the autocovariance
        function associated with the final `nobs - m` observations.

    Returns
    -------
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm.
    v : ndarray
        The vector of mean squared errors.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    This function is relatively fast even with a large number of observations
    since we can exploit a number of known zeros in the theta array.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t i, j, k, n, _n, m, m2, p, q, start, start2
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.complex64_t [:, :] theta
    cdef cnp.complex64_t [:] v

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)
    m2 = 2 * m

    dim1[0] = nobs;
    v = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)
    dim2[0] = nobs;
    dim2[1] = m + 1;
    theta = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_COMPLEX64, C)

    if m > 0:
        v[0] = acovf[0, 0]
    else:
        # (handle the corner case where p = q = 0)
        v[0] = acovf2[0]

    for n in range(nobs - 1):
        _n = n + 1

        start = 0 if n < m else n + 1 - q
        for k in range(start, n + 1):
            # See Brockwell and Davis, p. 100-101
            # (here we have weak rather than strict inequality due to Python's
            # zero indexing)
            if n >= m and n - k >= q:
                continue

            if n + 1 < m2 and k < m:
                theta[_n, n - k] = acovf[n + 1, k]
            else:
                theta[_n, n - k] = acovf2[n + 1 - k]

            start2 = 0 if n < m else n - m
            for j in range(start2, k):
                if n - j < m + 1:
                    theta[_n, n - k] = theta[_n, n - k] - theta[k - 1 + 1, k - j - 1] * theta[_n, n - j] * v[j]
            theta[_n, n - k] = theta[_n, n - k] / v[k]

        if n + 1 < m:
            v[n + 1] = acovf[n + 1, n + 1]
        else:
            v[n + 1] = acovf2[0]
        start = max(0, n - (m + 1) + 2)
        for i in range(start, n + 1):
            v[n + 1] = v[n + 1] - theta[_n, n - i]**2 * v[i]

    return theta, v


cpdef carma_innovations_filter(cnp.complex64_t [:] endog,
                                        cnp.complex64_t [:] ar_params,
                                        cnp.complex64_t [:] ma_params,
                                        cnp.complex64_t [:, :] theta):
    """
    arma_innovations_filter(cnp.complex64_t [:] endog, cnp.complex64_t [:] ar_params, cnp.complex64_t [:] ma_params, cnp.complex64_t [:, :] theta):
    
    Innovations filter for an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm (see `arma_innovations_algo` or `arma_innovations_algo_fast`)

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, i, k, j, m, p, q
    cdef cnp.npy_intp dim1[1]
    cdef cnp.complex64_t [:] u
    cdef cnp.complex64_t hat

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)

    nobs = theta.shape[0]
    k = theta.shape[1]

    dim1[0] = nobs;
    u = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)
    u[0] = endog[0]

    for i in range(1, nobs):
        hat = 0
        if i < m:
            for j in range(i):
                hat = hat + theta[i, j] * u[i - j - 1]
        else:
            for j in range(p):
                hat = hat + ar_params[j] * endog[i - j - 1]
            for j in range(q):
                hat = hat + theta[i, j] * u[i - j - 1]
        u[i] = endog[i] - hat

    return u


cpdef carma_innovations(cnp.complex64_t [:] endog,
                                      cnp.complex64_t [:] ar_params,
                                      cnp.complex64_t [:] ma_params,
                                      cnp.complex64_t sigma2):
    """
    arma_innovations(cnp.complex64_t [:] endog, cnp.complex64_t [:] ar_params, cnp.complex64_t [:] ma_params):
    
    Compute innovations and variances based on an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.
    v : ndarray
        The vector of innovation variances.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.complex64_t const
    cdef cnp.complex64_t [:] ar, ma, arma_acovf, llf_obs, acovf2, u, v
    cdef cnp.complex64_t [:, :] acovf
    cdef cnp.npy_intp dim1[1]

    dim1[0] = len(ar_params) + 1;
    ar = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)
    dim1[0] = len(ma_params) + 1;
    ma = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)

    ar[0] = 1
    for i in range(1, len(ar_params) + 1):
        ar[i] = -1 * ar_params[i - 1]
    ma[0] = 1
    ma[1:] = ma_params

    arma_acovf = arima_process.arma_acovf(ar, ma, nobs, sigma2, dtype=np.complex64) / sigma2
    acovf, acovf2 = carma_transformed_acovf_fast(ar, ma, arma_acovf)
    theta, v = carma_innovations_algo_fast(nobs, ar_params, ma_params, acovf, acovf2)
    u = carma_innovations_filter(endog, ar_params, ma_params, theta)

    return u, v


cpdef carma_loglikeobs_fast(cnp.complex64_t [:] endog,
                                     cnp.complex64_t [:] ar_params,
                                     cnp.complex64_t [:] ma_params,
                                     cnp.complex64_t sigma2):
    """
    carma_loglikeobs_fast(cnp.complex64_t [:] endog, cnp.complex64_t [:] ar_params, cnp.complex64_t [:] ma_params, cnp.complex64_t sigma2)

    Quickly calculate the loglikelihood of each observation for an ARMA process

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    loglike : ndarray of float
        Array of loglikelihood values for each observation.

    Notes
    -----
    Details related to computing the loglikelihood associated with an ARMA
    process using the innovations algorithm are given in _[1] section 5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """

    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.complex64_t const
    cdef cnp.complex64_t [:] llf_obs, u, v
    cdef cnp.npy_intp dim1[1]

    u, v = carma_innovations(endog, ar_params, ma_params, sigma2)

    dim1[0] = nobs;
    llf_obs = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX64, C)

    const = zlog(2*NPY_PI)
    for i in range(nobs):
        llf_obs[i] = -0.5 * u[i]**2 / (sigma2 * v[i]) - 0.5 * (const + zlog(sigma2 * v[i]))

    return np.array(llf_obs, dtype=np.complex64)


cdef ztoeplitz(int n, int offset0, int offset1,
                        cnp.complex128_t [:] in_column,
                        cnp.complex128_t [:, :] out_matrix):
    """
    toeplitz(int n, int o0, int o1, cnp.complex128_t [:] in_column, cnp.complex128_t [:, :] out_matrix
    
    Construct a Toeplitz block in a matrix in place

    Parameters
    ----------
    n : int
        The number of entries of `in_column` to use.
    offset0 : int
        The row offset for `out_matrix` at which to begin writing the Toeplitz
        block.
    offset1 : int
        The column offset for `out_matrix` at which to begin writing the
        Toeplitz block.
    in_column : ndarray
        The column used to construct the Toeplitz block.
    out_matrix : ndarray
        The matrix in which to write a Toeplitz block

    Notes
    -----
    This function constructs the Toeplitz block in-place and does not return
    any output.

    """
    cdef Py_ssize_t i, j

    for i in range(n):
        for j in range(i + 1):
            out_matrix[offset0 + i, offset1 + j] = in_column[i - j]
            if i != j:
              # Note: scipy by default does complex conjugate, but not
              # necessary here since we're only dealing with covariances,
              # which will be real (except in the case of complex-step
              # differentiation, but in that case we do not want to apply
              # the conjugation anyway)
              out_matrix[offset0 + j, offset1 + i] = in_column[i - j]


cpdef zarma_transformed_acovf_fast(cnp.complex128_t [:] ar,
                                            cnp.complex128_t [:] ma,
                                            cnp.complex128_t [:] arma_acovf):
    """
    arma_transformed_acovf_fast(cnp.complex128_t [:] ar, cnp.complex128_t [:] ma, cnp.complex128_t [:] arma_acovf)
    
    Quickly construct the autocovariance matrix for a transformed process.

    Using the autocovariance function for an ARMA process, constructs the
    autocovariances associated with the transformed process described
    in equation (3.3.1) of _[1] in a memory efficient, and so fast, way.

    Parameters
    ----------
    ar : ndarray
        Autoregressive coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the left-hand-side of the ARMA definition (i.e. they have the opposite
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    ma : ndarray
        Moving average coefficients, including the zero lag, where the sign
        convention assumes the coefficients are part of the lag polynomial on
        the right-hand-side of the ARMA definition (i.e. they have the same
        sign from the usual econometrics convention in which the coefficients
        are on the right-hand-side of the ARMA definition).
    arma_acovf : ndarray
        The vector of autocovariances of the ARMA process.

    Returns
    -------
    acovf : ndarray
        A matrix containing the autocovariances of the portion of the
        transformed process with time-varying autocovariances. Its dimension
        is equal to `min(m * 2, n)` where `m = max(len(ar) - 1, len(ma) - 1)`
        and `n` is the length of the input array `arma_acovf`. It is important
        to note that only the values in the first `m` columns or `m` rows are
        valid. In particular, the entries in the block `acovf[m:, m:]` should
        not be used in any case (and in fact will always be equal to zeros).
    acovf2 : ndarray
        An array containing the autocovariance function of the portion of the
        transformed process with time-invariant autocovariances. Its dimension
        is equal to `max(n - m, 0)` where `n` is the length of the input
        array `arma_acovf`.

    Notes
    -----
    The definition of this autocovariance matrix is from _[1] equation 3.3.3.

    This function assumes that the variance of the ARMA innovation term is
    equal to one. If this is not true, then the calling program should replace
    `arma_acovf` with `arma_acovf / sigma2`, where sigma2 is that variance.

    This function is relatively fast even when `arma_acovf` is large, since
    it only constructs the full autocovariance matrix for a generally small
    subset of observations. The trade-off is that the output of this function
    is somewhat more difficult to use.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, p, q, m, m2, n, i, j, r, tmp_ix
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.complex128_t [:, :] acovf
    cdef cnp.complex128_t [:] acovf2

    nobs = arma_acovf.shape[0]
    p = len(ar) - 1
    q = len(ma) - 1
    m = max(p, q)
    m2 = 2 * m
    n = min(m2, nobs)

    dim2[0] = m2;
    dim2[1] = m2;
    acovf = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_COMPLEX128, C)
    dim1[0] = max(nobs - m, 0);
    acovf2 = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)

    # Set i, j = 1, ..., m   (which is then done)
    ztoeplitz(m, 0, 0, arma_acovf, acovf)

    # Set i = 1, ..., m;         j = m + 1, ..., 2 m
    # and i = m + 1, ..., 2 m;   j = 1, ..., m
    if nobs > m:
        for j in range(m):
            for i in range(m, m2):
                acovf[i, j] = arma_acovf[i - j]
                for r in range(1, p + 1):
                    tmp_ix = abs(r - (i - j))
                    acovf[i, j] = acovf[i, j] - (-ar[r] * arma_acovf[tmp_ix])
        acovf[:m, m:m2] = acovf[m:m2, :m].T

    # Set values for |i - j| <= q, min(i, j) = m + 1, and max(i, j) <= nobs
    if nobs > m:
        for i in range(nobs - m):
            for r in range(q + 1 - i):
                acovf2[i] = acovf2[i] + ma[r] * ma[r + i]

    return acovf[:n, :n], acovf2


cpdef zarma_innovations_algo_fast(int nobs,
                                           cnp.complex128_t [:] ar_params,
                                           cnp.complex128_t [:] ma_params,
                                           cnp.complex128_t [:, :] acovf,
                                           cnp.complex128_t [:] acovf2):
    """
    arma_innovations_algo_fast(int nobs, cnp.complex128_t [:] ar_params, cnp.complex128_t [:] ma_params, cnp.complex128_t [:, :] acovf, cnp.complex128_t [:] acovf2)
    
    Quickly apply innovations algorithm for an ARMA process.

    Parameters
    ----------
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    acovf : ndarray
        An `m * 2` x `m * 2` autocovariance matrix at least the first `m`
        columns filled in, where `m = max(len(ar_params), ma_params)`
        (see `arma_transformed_acovf_fast`).
    acovf2 : ndarray
        A `max(0, nobs - m)` length vector containing the autocovariance
        function associated with the final `nobs - m` observations.

    Returns
    -------
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm.
    v : ndarray
        The vector of mean squared errors.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    This function is relatively fast even with a large number of observations
    since we can exploit a number of known zeros in the theta array.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t i, j, k, n, _n, m, m2, p, q, start, start2
    cdef cnp.npy_intp dim1[1]
    cdef cnp.npy_intp dim2[2]
    cdef cnp.complex128_t [:, :] theta
    cdef cnp.complex128_t [:] v

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)
    m2 = 2 * m

    dim1[0] = nobs;
    v = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)
    dim2[0] = nobs;
    dim2[1] = m + 1;
    theta = cnp.PyArray_ZEROS(2, dim2, cnp.NPY_COMPLEX128, C)

    if m > 0:
        v[0] = acovf[0, 0]
    else:
        # (handle the corner case where p = q = 0)
        v[0] = acovf2[0]

    for n in range(nobs - 1):
        _n = n + 1

        start = 0 if n < m else n + 1 - q
        for k in range(start, n + 1):
            # See Brockwell and Davis, p. 100-101
            # (here we have weak rather than strict inequality due to Python's
            # zero indexing)
            if n >= m and n - k >= q:
                continue

            if n + 1 < m2 and k < m:
                theta[_n, n - k] = acovf[n + 1, k]
            else:
                theta[_n, n - k] = acovf2[n + 1 - k]

            start2 = 0 if n < m else n - m
            for j in range(start2, k):
                if n - j < m + 1:
                    theta[_n, n - k] = theta[_n, n - k] - theta[k - 1 + 1, k - j - 1] * theta[_n, n - j] * v[j]
            theta[_n, n - k] = theta[_n, n - k] / v[k]

        if n + 1 < m:
            v[n + 1] = acovf[n + 1, n + 1]
        else:
            v[n + 1] = acovf2[0]
        start = max(0, n - (m + 1) + 2)
        for i in range(start, n + 1):
            v[n + 1] = v[n + 1] - theta[_n, n - i]**2 * v[i]

    return theta, v


cpdef zarma_innovations_filter(cnp.complex128_t [:] endog,
                                        cnp.complex128_t [:] ar_params,
                                        cnp.complex128_t [:] ma_params,
                                        cnp.complex128_t [:, :] theta):
    """
    arma_innovations_filter(cnp.complex128_t [:] endog, cnp.complex128_t [:] ar_params, cnp.complex128_t [:] ma_params, cnp.complex128_t [:, :] theta):
    
    Innovations filter for an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    theta : ndarray
        The matrix of moving average coefficients from the innovations
        algorithm (see `arma_innovations_algo` or `arma_innovations_algo_fast`)

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    Details of the innovations algorithm applied to ARMA processes is given
    in _[1] section 3.3 and in section 5.2.7.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs, i, k, j, m, p, q
    cdef cnp.npy_intp dim1[1]
    cdef cnp.complex128_t [:] u
    cdef cnp.complex128_t hat

    p = len(ar_params)
    q = len(ma_params)
    m = max(p, q)

    nobs = theta.shape[0]
    k = theta.shape[1]

    dim1[0] = nobs;
    u = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)
    u[0] = endog[0]

    for i in range(1, nobs):
        hat = 0
        if i < m:
            for j in range(i):
                hat = hat + theta[i, j] * u[i - j - 1]
        else:
            for j in range(p):
                hat = hat + ar_params[j] * endog[i - j - 1]
            for j in range(q):
                hat = hat + theta[i, j] * u[i - j - 1]
        u[i] = endog[i] - hat

    return u


cpdef zarma_innovations(cnp.complex128_t [:] endog,
                                      cnp.complex128_t [:] ar_params,
                                      cnp.complex128_t [:] ma_params,
                                      cnp.complex128_t sigma2):
    """
    arma_innovations(cnp.complex128_t [:] endog, cnp.complex128_t [:] ar_params, cnp.complex128_t [:] ma_params):
    
    Compute innovations and variances based on an ARMA process.

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    u : ndarray
        The vector of innovations: the one-step-ahead prediction errors from
        applying the innovations algorithm.
    v : ndarray
        The vector of innovation variances.

    Notes
    -----
    The innovations algorithm is presented in _[1], section 2.5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """
    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.complex128_t const
    cdef cnp.complex128_t [:] ar, ma, arma_acovf, llf_obs, acovf2, u, v
    cdef cnp.complex128_t [:, :] acovf
    cdef cnp.npy_intp dim1[1]

    dim1[0] = len(ar_params) + 1;
    ar = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)
    dim1[0] = len(ma_params) + 1;
    ma = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)

    ar[0] = 1
    for i in range(1, len(ar_params) + 1):
        ar[i] = -1 * ar_params[i - 1]
    ma[0] = 1
    ma[1:] = ma_params

    arma_acovf = arima_process.arma_acovf(ar, ma, nobs, sigma2, dtype=complex) / sigma2
    acovf, acovf2 = zarma_transformed_acovf_fast(ar, ma, arma_acovf)
    theta, v = zarma_innovations_algo_fast(nobs, ar_params, ma_params, acovf, acovf2)
    u = zarma_innovations_filter(endog, ar_params, ma_params, theta)

    return u, v


cpdef zarma_loglikeobs_fast(cnp.complex128_t [:] endog,
                                     cnp.complex128_t [:] ar_params,
                                     cnp.complex128_t [:] ma_params,
                                     cnp.complex128_t sigma2):
    """
    zarma_loglikeobs_fast(cnp.complex128_t [:] endog, cnp.complex128_t [:] ar_params, cnp.complex128_t [:] ma_params, cnp.complex128_t sigma2)

    Quickly calculate the loglikelihood of each observation for an ARMA process

    Parameters
    ----------
    endog : ndarray
        The observed time-series process.
    ar_params : ndarray
        Autoregressive parameters.
    ma_params : ndarray
        Moving average parameters.
    sigma2 : ndarray
        The ARMA innovation variance.

    Returns
    -------
    loglike : ndarray of float
        Array of loglikelihood values for each observation.

    Notes
    -----
    Details related to computing the loglikelihood associated with an ARMA
    process using the innovations algorithm are given in _[1] section 5.2.

    References
    ----------
    .. [1] Brockwell, Peter J., and Richard A. Davis. 2009.
       Time Series: Theory and Methods. 2nd ed. 1991.
       New York, NY: Springer.

    """

    cdef Py_ssize_t nobs = len(endog), i
    cdef cnp.complex128_t const
    cdef cnp.complex128_t [:] llf_obs, u, v
    cdef cnp.npy_intp dim1[1]

    u, v = zarma_innovations(endog, ar_params, ma_params, sigma2)

    dim1[0] = nobs;
    llf_obs = cnp.PyArray_ZEROS(1, dim1, cnp.NPY_COMPLEX128, C)

    const = zlog(2*NPY_PI)
    for i in range(nobs):
        llf_obs[i] = -0.5 * u[i]**2 / (sigma2 * v[i]) - 0.5 * (const + zlog(sigma2 * v[i]))

    return np.array(llf_obs, dtype=complex)
