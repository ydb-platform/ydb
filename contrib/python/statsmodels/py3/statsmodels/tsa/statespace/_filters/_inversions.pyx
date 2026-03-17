#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
#cython: cpow=True
"""
State Space Models

Author: Chad Fulton  
License: Simplified-BSD
"""

cimport numpy as np
import numpy as np
from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack

from statsmodels.tsa.statespace._kalman_filter cimport (
    MEMORY_NO_SMOOTHING, MEMORY_NO_STD_FORECAST)

# ## Forecast error covariance inversion
#
# The following are routines that can calculate the inverse of the forecast
# error covariance matrix (defined in `forecast_<filter type>`).
#
# These routines are aware of the possibility that the Kalman filter may have
# converged to a steady state, in which case they do not need to perform the
# inversion or calculate the determinant.

cdef np.float32_t sinverse_univariate(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using simple division
    in the case that the observations are univariate.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    # TODO it's not a noop if converged, but the docstring says it is...

    # #### Intermediate values
    cdef:
        int inc = 1
        np.float32_t scalar

    # Take the inverse of the forecast error covariance matrix
    if not kfilter.converged:
        determinant = dlog(kfilter._forecast_error_cov[0])
    try:
        # If we're close to singular, switch to univariate version even if the
        # following doesn't trigger a ZeroDivisionError
        if kfilter._forecast_error_cov[0] < 1e-12:
            raise Exception

        scalar = 1.0 / kfilter._forecast_error_cov[0]
    except:
        raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                    ' covariance matrix encountered at'
                                    ' period %d' % kfilter.t)
    kfilter._tmp2[0] = scalar * kfilter._forecast_error[0]
    blas.scopy(&model._k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    blas.sscal(&model._k_endogstates, &scalar, kfilter._tmp3, &inc)

    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        kfilter._standardized_forecast_error[0] = kfilter._forecast_error[0] * scalar**0.5

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        kfilter._tmp4[0] = scalar * model._obs_cov[0]

    return determinant

cdef np.float32_t sfactorize_cholesky(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using a Cholesky
    decomposition. Called by either of the `solve_cholesky` or
    `invert_cholesky` routines.

    Requires a positive definite matrix, but is faster than an LU
    decomposition.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.scopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        lapack.spotrf("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                       ' covariance matrix encountered at'
                                       ' period %d' % kfilter.t)

        # Calculate the determinant (just the squared product of the
        # diagonals, in the Cholesky decomposition case)
        determinant = 0.0
        for i in range(model._k_endog):
            determinant = determinant + dlog(kfilter.forecast_error_fac[i, i])
        determinant = 2 * determinant

    return determinant

cdef np.float32_t sfactorize_lu(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using an LU
    decomposition. Called by either of the `solve_lu` or `invert_lu`
    routines.

    Is slower than a Cholesky decomposition, but does not require a
    positive definite matrix.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged:
        # Perform LU decomposition into `forecast_error_fac`
        blas.scopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        
        lapack.sgetrf(&model._k_endog, &model._k_endog,
                        kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Singular forecast error covariance'
                                        ' matrix encountered at period %d' %
                                        kfilter.t)

        # Calculate the determinant (product of the diagonals, but with
        # sign modifications according to the permutation matrix)    
        determinant = 1
        for i in range(model._k_endog):
            if not kfilter._forecast_error_ipiv[i] == i+1:
                determinant *= -1*kfilter.forecast_error_fac[i, i]
            else:
                determinant *= kfilter.forecast_error_fac[i, i]
        determinant = dlog(determinant)

    return determinant

cdef np.float32_t sinverse_cholesky(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        int i, j
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        # Perform the Cholesky decomposition and get the determinant
        determinant = sfactorize_cholesky(kfilter, model, determinant)

        # Use the Cholesky factor to get standardized forecast errors, if desired
        # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
        # we want to solve the equation L_t' v_t^s = v_t)
        if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            blas.scopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
            lapack.strtrs("U", "T", "N", &model._k_endog, &inc,
                                   kfilter._forecast_error_fac, &kfilter.k_endog,
                                   kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

            if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

        # Continue taking the inverse
        lapack.spotri("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        # ?potri only fills in the upper triangle of the symmetric array, and
        # since the ?symm and ?symv routines are not available as of scipy
        # 0.11.0, we cannot use them, so we must fill in the lower triangle
        # by hand
        for i in range(model._k_endog): # columns
            for j in range(i): # rows
                kfilter.forecast_error_fac[i,j] = kfilter.forecast_error_fac[j,i]


    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    #blas.ssymv("U", &model._k_endog, &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #               kfilter._forecast_error, &inc, &beta, kfilter._tmp2, &inc)
    blas.sgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    #blas.ssymm("L", "U", &kfilter.k_endog, &kfilter.k_states,
    #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #                       kfilter._design, &kfilter.k_endog,
    #               &beta, kfilter._tmp3, &kfilter.k_endog)
    blas.sgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        #blas.ssymm("L", "U", &kfilter.k_endog, &kfilter.k_endog,
        #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
        #                       kfilter._obs_cov, &kfilter.k_endog,
        #               &beta, kfilter._tmp4, &kfilter.k_endog)
        blas.sgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.float32_t sinverse_lu(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = sfactorize_lu(kfilter, model, determinant)

        # Continue taking the inverse
        lapack.sgetri(&model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)

    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    blas.sgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    blas.sgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        blas.sgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.float32_t ssolve_cholesky(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    solve_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info, i, j
        int inc = 1

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = sfactorize_cholesky(kfilter, model, determinant)

    # Use the Cholesky factor to get standardized forecast errors, if desired
    # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
    # we want to solve the equation L_t' v_t^s = v_t)
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.scopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
        lapack.strtrs("U", "T", "N", &model._k_endog, &inc,
                               kfilter._forecast_error_fac, &kfilter.k_endog,
                               kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

        if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.scopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.spotrs("U", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.scopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.spotrs("U", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.scopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.spotrs("U", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

cdef np.float32_t ssolve_lu(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = sfactorize_lu(kfilter, model, determinant)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.scopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.sgetrs("N", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.scopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.sgetrs("N", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.scopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.sgetrs("N", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

# ## Forecast error covariance inversion
#
# The following are routines that can calculate the inverse of the forecast
# error covariance matrix (defined in `forecast_<filter type>`).
#
# These routines are aware of the possibility that the Kalman filter may have
# converged to a steady state, in which case they do not need to perform the
# inversion or calculate the determinant.

cdef np.float64_t dinverse_univariate(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using simple division
    in the case that the observations are univariate.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    # TODO it's not a noop if converged, but the docstring says it is...

    # #### Intermediate values
    cdef:
        int inc = 1
        np.float64_t scalar

    # Take the inverse of the forecast error covariance matrix
    if not kfilter.converged:
        determinant = dlog(kfilter._forecast_error_cov[0])
    try:
        # If we're close to singular, switch to univariate version even if the
        # following doesn't trigger a ZeroDivisionError
        if kfilter._forecast_error_cov[0] < 1e-12:
            raise Exception

        scalar = 1.0 / kfilter._forecast_error_cov[0]
    except:
        raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                    ' covariance matrix encountered at'
                                    ' period %d' % kfilter.t)
    kfilter._tmp2[0] = scalar * kfilter._forecast_error[0]
    blas.dcopy(&model._k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    blas.dscal(&model._k_endogstates, &scalar, kfilter._tmp3, &inc)

    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        kfilter._standardized_forecast_error[0] = kfilter._forecast_error[0] * scalar**0.5

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        kfilter._tmp4[0] = scalar * model._obs_cov[0]

    return determinant

cdef np.float64_t dfactorize_cholesky(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using a Cholesky
    decomposition. Called by either of the `solve_cholesky` or
    `invert_cholesky` routines.

    Requires a positive definite matrix, but is faster than an LU
    decomposition.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.dcopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        lapack.dpotrf("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                       ' covariance matrix encountered at'
                                       ' period %d' % kfilter.t)

        # Calculate the determinant (just the squared product of the
        # diagonals, in the Cholesky decomposition case)
        determinant = 0.0
        for i in range(model._k_endog):
            determinant = determinant + dlog(kfilter.forecast_error_fac[i, i])
        determinant = 2 * determinant

    return determinant

cdef np.float64_t dfactorize_lu(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using an LU
    decomposition. Called by either of the `solve_lu` or `invert_lu`
    routines.

    Is slower than a Cholesky decomposition, but does not require a
    positive definite matrix.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged:
        # Perform LU decomposition into `forecast_error_fac`
        blas.dcopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        
        lapack.dgetrf(&model._k_endog, &model._k_endog,
                        kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Singular forecast error covariance'
                                        ' matrix encountered at period %d' %
                                        kfilter.t)

        # Calculate the determinant (product of the diagonals, but with
        # sign modifications according to the permutation matrix)    
        determinant = 1
        for i in range(model._k_endog):
            if not kfilter._forecast_error_ipiv[i] == i+1:
                determinant *= -1*kfilter.forecast_error_fac[i, i]
            else:
                determinant *= kfilter.forecast_error_fac[i, i]
        determinant = dlog(determinant)

    return determinant

cdef np.float64_t dinverse_cholesky(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        int i, j
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        # Perform the Cholesky decomposition and get the determinant
        determinant = dfactorize_cholesky(kfilter, model, determinant)

        # Use the Cholesky factor to get standardized forecast errors, if desired
        # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
        # we want to solve the equation L_t' v_t^s = v_t)
        if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            blas.dcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
            lapack.dtrtrs("U", "T", "N", &model._k_endog, &inc,
                                   kfilter._forecast_error_fac, &kfilter.k_endog,
                                   kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

            if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

        # Continue taking the inverse
        lapack.dpotri("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        # ?potri only fills in the upper triangle of the symmetric array, and
        # since the ?symm and ?symv routines are not available as of scipy
        # 0.11.0, we cannot use them, so we must fill in the lower triangle
        # by hand
        for i in range(model._k_endog): # columns
            for j in range(i): # rows
                kfilter.forecast_error_fac[i,j] = kfilter.forecast_error_fac[j,i]


    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    #blas.dsymv("U", &model._k_endog, &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #               kfilter._forecast_error, &inc, &beta, kfilter._tmp2, &inc)
    blas.dgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    #blas.dsymm("L", "U", &kfilter.k_endog, &kfilter.k_states,
    #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #                       kfilter._design, &kfilter.k_endog,
    #               &beta, kfilter._tmp3, &kfilter.k_endog)
    blas.dgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        #blas.dsymm("L", "U", &kfilter.k_endog, &kfilter.k_endog,
        #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
        #                       kfilter._obs_cov, &kfilter.k_endog,
        #               &beta, kfilter._tmp4, &kfilter.k_endog)
        blas.dgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.float64_t dinverse_lu(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = dfactorize_lu(kfilter, model, determinant)

        # Continue taking the inverse
        lapack.dgetri(&model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)

    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    blas.dgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    blas.dgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        blas.dgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.float64_t dsolve_cholesky(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    solve_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info, i, j
        int inc = 1

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = dfactorize_cholesky(kfilter, model, determinant)

    # Use the Cholesky factor to get standardized forecast errors, if desired
    # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
    # we want to solve the equation L_t' v_t^s = v_t)
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.dcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
        lapack.dtrtrs("U", "T", "N", &model._k_endog, &inc,
                               kfilter._forecast_error_fac, &kfilter.k_endog,
                               kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

        if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.dcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.dpotrs("U", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.dcopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.dpotrs("U", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.dcopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.dpotrs("U", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

cdef np.float64_t dsolve_lu(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = dfactorize_lu(kfilter, model, determinant)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.dcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.dgetrs("N", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.dcopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.dgetrs("N", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.dcopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.dgetrs("N", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

# ## Forecast error covariance inversion
#
# The following are routines that can calculate the inverse of the forecast
# error covariance matrix (defined in `forecast_<filter type>`).
#
# These routines are aware of the possibility that the Kalman filter may have
# converged to a steady state, in which case they do not need to perform the
# inversion or calculate the determinant.

cdef np.complex64_t cinverse_univariate(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using simple division
    in the case that the observations are univariate.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    # TODO it's not a noop if converged, but the docstring says it is...

    # #### Intermediate values
    cdef:
        int inc = 1
        np.complex64_t scalar

    # Take the inverse of the forecast error covariance matrix
    if not kfilter.converged:
        determinant = zlog(kfilter._forecast_error_cov[0])
    try:
        # If we're close to singular, switch to univariate version even if the
        # following doesn't trigger a ZeroDivisionError
        if kfilter._forecast_error_cov[0].real < 1e-12:
            raise Exception

        scalar = 1.0 / kfilter._forecast_error_cov[0]
    except:
        raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                    ' covariance matrix encountered at'
                                    ' period %d' % kfilter.t)
    kfilter._tmp2[0] = scalar * kfilter._forecast_error[0]
    blas.ccopy(&model._k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    blas.cscal(&model._k_endogstates, &scalar, kfilter._tmp3, &inc)

    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        kfilter._standardized_forecast_error[0] = kfilter._forecast_error[0] * scalar**0.5

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        kfilter._tmp4[0] = scalar * model._obs_cov[0]

    return determinant

cdef np.complex64_t cfactorize_cholesky(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using a Cholesky
    decomposition. Called by either of the `solve_cholesky` or
    `invert_cholesky` routines.

    Requires a positive definite matrix, but is faster than an LU
    decomposition.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.ccopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        lapack.cpotrf("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                       ' covariance matrix encountered at'
                                       ' period %d' % kfilter.t)

        # Calculate the determinant (just the squared product of the
        # diagonals, in the Cholesky decomposition case)
        determinant = 0.0
        for i in range(model._k_endog):
            determinant = determinant + zlog(kfilter.forecast_error_fac[i, i])
        determinant = 2 * determinant

    return determinant

cdef np.complex64_t cfactorize_lu(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using an LU
    decomposition. Called by either of the `solve_lu` or `invert_lu`
    routines.

    Is slower than a Cholesky decomposition, but does not require a
    positive definite matrix.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged:
        # Perform LU decomposition into `forecast_error_fac`
        blas.ccopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        
        lapack.cgetrf(&model._k_endog, &model._k_endog,
                        kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Singular forecast error covariance'
                                        ' matrix encountered at period %d' %
                                        kfilter.t)

        # Calculate the determinant (product of the diagonals, but with
        # sign modifications according to the permutation matrix)    
        determinant = 1
        for i in range(model._k_endog):
            if not kfilter._forecast_error_ipiv[i] == i+1:
                determinant *= -1*kfilter.forecast_error_fac[i, i]
            else:
                determinant *= kfilter.forecast_error_fac[i, i]
        determinant = zlog(determinant)

    return determinant

cdef np.complex64_t cinverse_cholesky(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        int i, j
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        # Perform the Cholesky decomposition and get the determinant
        determinant = cfactorize_cholesky(kfilter, model, determinant)

        # Use the Cholesky factor to get standardized forecast errors, if desired
        # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
        # we want to solve the equation L_t' v_t^s = v_t)
        if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            blas.ccopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
            lapack.ctrtrs("U", "T", "N", &model._k_endog, &inc,
                                   kfilter._forecast_error_fac, &kfilter.k_endog,
                                   kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

            if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

        # Continue taking the inverse
        lapack.cpotri("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        # ?potri only fills in the upper triangle of the symmetric array, and
        # since the ?symm and ?symv routines are not available as of scipy
        # 0.11.0, we cannot use them, so we must fill in the lower triangle
        # by hand
        for i in range(model._k_endog): # columns
            for j in range(i): # rows
                kfilter.forecast_error_fac[i,j] = kfilter.forecast_error_fac[j,i]


    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    #blas.csymv("U", &model._k_endog, &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #               kfilter._forecast_error, &inc, &beta, kfilter._tmp2, &inc)
    blas.cgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    #blas.csymm("L", "U", &kfilter.k_endog, &kfilter.k_states,
    #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #                       kfilter._design, &kfilter.k_endog,
    #               &beta, kfilter._tmp3, &kfilter.k_endog)
    blas.cgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        #blas.csymm("L", "U", &kfilter.k_endog, &kfilter.k_endog,
        #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
        #                       kfilter._obs_cov, &kfilter.k_endog,
        #               &beta, kfilter._tmp4, &kfilter.k_endog)
        blas.cgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.complex64_t cinverse_lu(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = cfactorize_lu(kfilter, model, determinant)

        # Continue taking the inverse
        lapack.cgetri(&model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)

    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    blas.cgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    blas.cgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        blas.cgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.complex64_t csolve_cholesky(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    solve_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info, i, j
        int inc = 1

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = cfactorize_cholesky(kfilter, model, determinant)

    # Use the Cholesky factor to get standardized forecast errors, if desired
    # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
    # we want to solve the equation L_t' v_t^s = v_t)
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.ccopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
        lapack.ctrtrs("U", "T", "N", &model._k_endog, &inc,
                               kfilter._forecast_error_fac, &kfilter.k_endog,
                               kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

        if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.ccopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.cpotrs("U", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.ccopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.cpotrs("U", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.ccopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.cpotrs("U", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

cdef np.complex64_t csolve_lu(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = cfactorize_lu(kfilter, model, determinant)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.ccopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.cgetrs("N", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.ccopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.cgetrs("N", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.ccopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.cgetrs("N", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

# ## Forecast error covariance inversion
#
# The following are routines that can calculate the inverse of the forecast
# error covariance matrix (defined in `forecast_<filter type>`).
#
# These routines are aware of the possibility that the Kalman filter may have
# converged to a steady state, in which case they do not need to perform the
# inversion or calculate the determinant.

cdef np.complex128_t zinverse_univariate(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using simple division
    in the case that the observations are univariate.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    # TODO it's not a noop if converged, but the docstring says it is...

    # #### Intermediate values
    cdef:
        int inc = 1
        np.complex128_t scalar

    # Take the inverse of the forecast error covariance matrix
    if not kfilter.converged:
        determinant = zlog(kfilter._forecast_error_cov[0])
    try:
        # If we're close to singular, switch to univariate version even if the
        # following doesn't trigger a ZeroDivisionError
        if kfilter._forecast_error_cov[0].real < 1e-12:
            raise Exception

        scalar = 1.0 / kfilter._forecast_error_cov[0]
    except:
        raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                    ' covariance matrix encountered at'
                                    ' period %d' % kfilter.t)
    kfilter._tmp2[0] = scalar * kfilter._forecast_error[0]
    blas.zcopy(&model._k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    blas.zscal(&model._k_endogstates, &scalar, kfilter._tmp3, &inc)

    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        kfilter._standardized_forecast_error[0] = kfilter._forecast_error[0] * scalar**0.5

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        kfilter._tmp4[0] = scalar * model._obs_cov[0]

    return determinant

cdef np.complex128_t zfactorize_cholesky(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using a Cholesky
    decomposition. Called by either of the `solve_cholesky` or
    `invert_cholesky` routines.

    Requires a positive definite matrix, but is faster than an LU
    decomposition.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.zcopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        lapack.zpotrf("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite forecast error'
                                       ' covariance matrix encountered at'
                                       ' period %d' % kfilter.t)

        # Calculate the determinant (just the squared product of the
        # diagonals, in the Cholesky decomposition case)
        determinant = 0.0
        for i in range(model._k_endog):
            determinant = determinant + zlog(kfilter.forecast_error_fac[i, i])
        determinant = 2 * determinant

    return determinant

cdef np.complex128_t zfactorize_lu(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    Factorize the forecast error covariance matrix using an LU
    decomposition. Called by either of the `solve_lu` or `invert_lu`
    routines.

    Is slower than a Cholesky decomposition, but does not require a
    positive definite matrix.

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int inc = 1
        int info
        int i

    if not kfilter.converged:
        # Perform LU decomposition into `forecast_error_fac`
        blas.zcopy(&kfilter.k_endog2, kfilter._forecast_error_cov, &inc, kfilter._forecast_error_fac, &inc)
        
        lapack.zgetrf(&model._k_endog, &model._k_endog,
                        kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)

        if info < 0:
            raise np.linalg.LinAlgError('Illegal value in forecast error'
                                        ' covariance matrix encountered at'
                                        ' period %d' % kfilter.t)
        if info > 0:
            raise np.linalg.LinAlgError('Singular forecast error covariance'
                                        ' matrix encountered at period %d' %
                                        kfilter.t)

        # Calculate the determinant (product of the diagonals, but with
        # sign modifications according to the permutation matrix)    
        determinant = 1
        for i in range(model._k_endog):
            if not kfilter._forecast_error_ipiv[i] == i+1:
                determinant *= -1*kfilter.forecast_error_fac[i, i]
            else:
                determinant *= kfilter.forecast_error_fac[i, i]
        determinant = zlog(determinant)

    return determinant

cdef np.complex128_t zinverse_cholesky(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        int i, j
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    if not kfilter.converged or not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        # Perform the Cholesky decomposition and get the determinant
        determinant = zfactorize_cholesky(kfilter, model, determinant)

        # Use the Cholesky factor to get standardized forecast errors, if desired
        # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
        # we want to solve the equation L_t' v_t^s = v_t)
        if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            blas.zcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
            lapack.ztrtrs("U", "T", "N", &model._k_endog, &inc,
                                   kfilter._forecast_error_fac, &kfilter.k_endog,
                                   kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

            if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

        # Continue taking the inverse
        lapack.zpotri("U", &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, &info)

        # ?potri only fills in the upper triangle of the symmetric array, and
        # since the ?symm and ?symv routines are not available as of scipy
        # 0.11.0, we cannot use them, so we must fill in the lower triangle
        # by hand
        for i in range(model._k_endog): # columns
            for j in range(i): # rows
                kfilter.forecast_error_fac[i,j] = kfilter.forecast_error_fac[j,i]


    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    #blas.zsymv("U", &model._k_endog, &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #               kfilter._forecast_error, &inc, &beta, kfilter._tmp2, &inc)
    blas.zgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    #blas.zsymm("L", "U", &kfilter.k_endog, &kfilter.k_states,
    #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
    #                       kfilter._design, &kfilter.k_endog,
    #               &beta, kfilter._tmp3, &kfilter.k_endog)
    blas.zgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        #blas.zsymm("L", "U", &kfilter.k_endog, &kfilter.k_endog,
        #               &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
        #                       kfilter._obs_cov, &kfilter.k_endog,
        #               &beta, kfilter._tmp4, &kfilter.k_endog)
        blas.zgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.complex128_t zinverse_lu(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = zfactorize_lu(kfilter, model, determinant)

        # Continue taking the inverse
        lapack.zgetri(&model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)

    # Get `tmp2` and `tmp3` via matrix multiplications

    # `tmp2` array used here, dimension $(p \times 1)$  
    # $\\#_2 = F_t^{-1} v_t$  
    blas.zgemv("N", &model._k_endog, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           kfilter._forecast_error, &inc,
                   &beta, kfilter._tmp2, &inc)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $\\#_3 = F_t^{-1} Z_t$
    blas.zgemm("N", "N", &model._k_endog, &model._k_states, &model._k_endog,
                   &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                           model._design, &model._k_endog,
                   &beta, kfilter._tmp3, &kfilter.k_endog)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $\\#_4 = F_t^{-1} H_t$
        blas.zgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                       &alpha, kfilter._forecast_error_fac, &kfilter.k_endog,
                               model._obs_cov, &model._k_endog,
                       &beta, kfilter._tmp4, &kfilter.k_endog)

    return determinant

cdef np.complex128_t zsolve_cholesky(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    solve_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info, i, j
        int inc = 1

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = zfactorize_cholesky(kfilter, model, determinant)

    # Use the Cholesky factor to get standardized forecast errors, if desired
    # (in the notation of DK, kfilter._forecast_error_fac holds L_t', and
    # we want to solve the equation L_t' v_t^s = v_t)
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
        blas.zcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._standardized_forecast_error, &inc)
        lapack.ztrtrs("U", "T", "N", &model._k_endog, &inc,
                               kfilter._forecast_error_fac, &kfilter.k_endog,
                               kfilter._standardized_forecast_error, &kfilter.k_endog, &info)

        if info != 0:
                raise np.linalg.LinAlgError('Error computing standardized'
                                            ' forecast error at period %d'
                                            % kfilter.t)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.zcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.zpotrs("U", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.zcopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.zpotrs("U", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.zcopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.zpotrs("U", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant

cdef np.complex128_t zsolve_lu(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    """
    inverse_cholesky(self, determinant)

    If the model has converged to a steady-state, this is a NOOP and simply
    returns the determinant that was passed in.
    """
    cdef:
        int info
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    if not kfilter.converged:
        # Perform the Cholesky decomposition and get the determinant
        determinant = zfactorize_lu(kfilter, model, determinant)

    # Solve the linear systems  
    # `tmp2` array used here, dimension $(p \times 1)$  
    # $F_t \\#_2 = v_t$  
    blas.zcopy(&kfilter.k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)
    lapack.zgetrs("N", &model._k_endog, &inc, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp2, &kfilter.k_endog, &info)

    # `tmp3` array used here, dimension $(p \times m)$  
    # $F_t \\#_3 = Z_t$
    if model._k_states == model.k_states and model._k_endog == model.k_endog:
        blas.zcopy(&kfilter.k_endogstates, model._design, &inc, kfilter._tmp3, &inc)
    else:
        for i in range(model._k_states): # columns
            for j in range(model._k_endog): # rows
                kfilter._tmp3[j + i*kfilter.k_endog] = model._design[j + i*model._k_endog]
    lapack.zgetrs("N", &model._k_endog, &model._k_states, kfilter._forecast_error_fac, &kfilter.k_endog,
                    kfilter._forecast_error_ipiv, kfilter._tmp3, &kfilter.k_endog, &info)

    if not (kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0):
        # `tmp4` array used here, dimension $(p \times p)$  
        # $F_t \\#_4 = H_t$
        if model._k_states == model.k_states and model._k_endog == model.k_endog:
            blas.zcopy(&kfilter.k_endog2, model._obs_cov, &inc, kfilter._tmp4, &inc)
        else:
            for i in range(model._k_endog): # columns
                for j in range(model._k_endog): # rows
                    kfilter._tmp4[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]
        lapack.zgetrs("N", &model._k_endog, &model._k_endog, kfilter._forecast_error_fac, &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, kfilter._tmp4, &kfilter.k_endog, &info)

    return determinant
