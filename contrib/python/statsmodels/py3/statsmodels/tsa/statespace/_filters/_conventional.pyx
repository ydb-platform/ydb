#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Models

Author: Chad Fulton  
License: Simplified-BSD
"""

# Typical imports
import numpy as np
cimport numpy as np
from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack

from statsmodels.tsa.statespace._kalman_filter cimport (
    MEMORY_NO_FORECAST_COV, FILTER_CONCENTRATED, FILTER_CHANDRASEKHAR)


# ### Missing Observation Conventional Kalman filter
#
# See Durbin and Koopman (2012) Chapter 4.10
#
# Here k_endog is the same as usual, but the design matrix and observation
# covariance matrix are enforced to be zero matrices, and the loglikelihood
# is defined to be zero.

cdef int sforecast_missing_conventional(sKalmanFilter kfilter, sStatespace model):
    cdef int i, j
    cdef int inc = 1, design_t = 0
    cdef np.float32_t alpha = 1

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # Just set to zeros, see below (this means if forecasts are required for
    # this part, they must be done in the wrappe)

    # #### Forecast error for time t  
    # It is undefined here, since obs is nan
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zero
    for i in range(kfilter.k_endog):
        kfilter._forecast[i] = 0
        kfilter._forecast_error[i] = 0

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv 0$
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zeros
    for i in range(kfilter.k_endog): # columns
        for j in range(kfilter.k_endog): # rows
            kfilter._forecast_error_cov[j + i*kfilter.k_endog] = 0

cdef int supdating_missing_conventional(sKalmanFilter kfilter, sStatespace model):
    cdef int inc = 1

    # Simply copy over the input arrays ($a_t, P_t$) to the filtered arrays
    # ($a_{t|t}, P_{t|t}$)
    # Note: use kfilter dimensions since we just want to copy whole arrays;
    blas.scopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.scopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

cdef np.float32_t sinverse_missing_conventional(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant)  except *:
    # Since the inverse of the forecast error covariance matrix is not
    # stored, we do not need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return -np.inf

cdef np.float32_t sloglikelihood_missing_conventional(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant):
    return 0.0

cdef np.float32_t sscale_missing_conventional(sKalmanFilter kfilter, sStatespace model):
    return 0.0

# ### Conventional Kalman filter
#
# The following are the above routines as defined in the conventional Kalman
# filter.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int sforecast_conventional(sKalmanFilter kfilter, sStatespace model):

    # Constants
    cdef:
        int inc = 1, ld, i, j
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # 
    # *Note*: $a_t$ is given from the initialization (for $t = 0$) or
    # from the previous iteration of the filter (for $t > 0$).

    # $\\# = d_t$
    blas.scopy(&model._k_endog, model._obs_intercept, &inc, kfilter._forecast, &inc)
    # `forecast` $= 1.0 * Z_t a_t + 1.0 * \\#$  
    # $(p \times 1) = (p \times m) (m \times 1) + (p \times 1)$
    blas.sgemv("N", &model._k_endog, &model._k_states,
          &alpha, model._design, &model._k_endog,
                  kfilter._input_state, &inc,
          &alpha, kfilter._forecast, &inc)

    # #### Forecast error for time t  
    # `forecast_error` $\equiv v_t = y_t -$ `forecast`

    # $\\# = y_t$
    blas.scopy(&model._k_endog, model._obs, &inc, kfilter._forecast_error, &inc)
    # $v_t = -1.0 * $ `forecast` $ + \\#$
    # $(p \times 1) = (p \times 1) + (p \times 1)$
    blas.saxpy(&model._k_endog, &gamma, kfilter._forecast, &inc, kfilter._forecast_error, &inc)

    # *Intermediate calculation* (used just below and then once more)  
    # `tmp1` array used here, dimension $(m \times p)$  
    # $\\#_1 = P_t Z_t'$  
    # $(m \times p) = (m \times m) (p \times m)'$
    blas.sgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
          &alpha, kfilter._input_state_cov, &kfilter.k_states,
                  model._design, &model._k_endog,
          &beta, kfilter._tmp1, &kfilter.k_states)

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv Z_t P_t Z_t' + H_t$
    # 
    # *Note*: this and does nothing at all to `forecast_error_cov` if
    # converged == True
    # TODO make sure when converged, the copies are made correctly
    if not kfilter.converged:
        # $\\# = H_t$
        # blas.scopy(&kfilter.k_endog2, kfilter._obs_cov, &inc, kfilter._forecast_error_cov, &inc)
        for i in range(model._k_endog): # columns
            for j in range(model._k_endog): # rows
                kfilter._forecast_error_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]

        # $F_t = 1.0 * Z_t \\#_1 + 1.0 * \\#$
        blas.sgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_states,
              &alpha, model._design, &model._k_endog,
                     kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._forecast_error_cov, &kfilter.k_endog)

    return 0

cdef int schandrasekhar_recursion(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int info
        int inc = 1
        int j, k
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # Chandrasekhar recursion
    # Initialization of the recursions
    if kfilter.t == 0:
        # Initialize M
        blas.scopy(&model._k_endog2, kfilter._forecast_error_cov, &inc, &kfilter.CM[0, 0], &inc)
        lapack.sgetrf(&model._k_endog, &model._k_endog,
                        &kfilter.CM[0, 0], &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)
        lapack.sgetri(&model._k_endog, &kfilter.CM[0, 0], &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)
        blas.sscal(&model._k_endog2, &gamma, &kfilter.CM[0, 0], &inc)

        # Initialize W
        # Our Kalman gain is T P Z' F^{-1} but we need to initialize
        # W = T P Z', so we need to multiply by F
        # (not particularly efficient way to do this, but it's only
        # done for one time step so it's probably okay)
        blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
          &alpha, kfilter._kalman_gain, &kfilter.k_states,
                  kfilter._forecast_error_cov, &kfilter.k_endog,
          &beta, &kfilter.CW[0, 0], &kfilter.k_states)
    else:
        # Compute M = M + M @ W.T @ Z.T @ Fi_prev @ Z @ W @ M
        #           = M + (M @ W.T) @ Z.T @ (Fi_prev @ Z) @ (W @ M)

        # M @ W.T: (p x p) (p x m) -> (p x m)
        blas.sgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)

        # (Fi_prev @ Z) @ (W @ M): (p x m) (m x p) -> (p x p)
        blas.sgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CprevFiZ[0, 0], &kfilter.k_endog,
                  &kfilter.CMW[0, 0], &kfilter.k_endog,
          &beta, &kfilter.CtmpM[0, 0], &kfilter.k_endog)

        # (M @ W.T) @ Z.T: (p x m) (m x p) -> (p x p)
        blas.sgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  model._design, &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &kfilter.k_endog)

        # M + (M @ W.T @ Z.T) (Fi_prev @ Z @ W @ M) : (p x p) (p x p) -> (p x p)
        blas.sgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
          &alpha, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
                  &kfilter.CtmpM[0, 0], &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Compute W = (T - Kg @ Z) @ W
        # W -> tmpW
        blas.scopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        # T -> tmp00
        blas.scopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)

        # T - K @ Z: (m x p) (p x m) -> (m x m)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
            &gamma, kfilter._kalman_gain, &kfilter.k_states,
                    model._design, &model._k_endog,
            &alpha, kfilter._tmp00, &kfilter.k_states)

        # (T - T @ K @ Z[i]) @ tmpW -> W
        blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      &kfilter.CtmpW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CW[0, 0], &kfilter.k_states)

    # Save F^{-1} Z (this period's value used in next iteration)
    blas.scopy(&model._k_endogstates, kfilter._tmp3, &inc, &kfilter.CprevFiZ[0, 0], &inc)

cdef int supdating_conventional(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # #### Filtered state for time t
    # $a_{t|t} = a_t + P_t Z_t' F_t^{-1} v_t$  
    # $a_{t|t} = 1.0 * \\#_1 \\#_2 + 1.0 a_t$
    blas.scopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.sgemv("N", &model._k_states, &model._k_endog,
          &alpha, kfilter._tmp1, &kfilter.k_states,
                  kfilter._tmp2, &inc,
          &alpha, kfilter._filtered_state, &inc)

    # #### Filtered state covariance for time t
    # Temporary computation used below
    if not kfilter.converged:
        # P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        blas.sgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._input_state_cov, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, &kfilter.CtmpW[0, 0], &kfilter.k_states)

    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.scopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times p) (p \times m) + (m \times m)$
        blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, &kfilter.CtmpW[0, 0], &kfilter.k_states,
                      kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # T_t P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        if model.identity_transition:
            blas.scopy(&model._k_endogstates, &kfilter.CtmpW[0, 0], &inc, kfilter._kalman_gain, &inc)
        else:
            blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                &alpha, model._transition, &kfilter.k_states,
                        &kfilter.CtmpW[0, 0], &kfilter.k_states,
                &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int sprediction_conventional(sKalmanFilter kfilter, sStatespace model):

    # Constants
    cdef:
        int inc = 1
        int forecast_cov_t = kfilter.t
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.scopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    if model.identity_transition:
        blas.saxpy(&model._k_states, &alpha, kfilter._filtered_state, &inc, kfilter._predicted_state, &inc)
    else:
        blas.sgemv("N", &model._k_states, &model._k_states,
            &alpha, model._transition, &model._k_states,
                    kfilter._filtered_state, &inc,
            &alpha, kfilter._predicted_state, &inc)

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t|t} T_t' + Q_t^*$
    #
    # *Note*: this and does nothing at all to `predicted_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.scopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
        # `tmp0` array used here, dimension $(m \times m)$  

        if kfilter.filter_method & FILTER_CHANDRASEKHAR:
            # Chandrasekhar_recursion
            schandrasekhar_recursion(kfilter, model)

            # Chandrasekhar-based prediction step
            blas.scopy(&model._k_states2, kfilter._input_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            # M @ W.T. (p x p) (p x m)
            blas.sgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                      &kfilter.CW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
            # P = P + W M W.T
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CW[0, 0], &kfilter.k_states,
                      &kfilter.CMW[0, 0], &kfilter.k_endog,
              &alpha, kfilter._predicted_state_cov, &kfilter.k_states)
        else:
            # Kalman filter prediction step
            # $\\#_0 = T_t P_{t|t} $

            if model.identity_transition:
                blas.saxpy(&model._k_states2, &alpha, kfilter._filtered_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            else:
                # $(m \times m) = (m \times m) (m \times m)$
                blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, model._transition, &model._k_states,
                            kfilter._filtered_state_cov, &kfilter.k_states,
                    &beta, kfilter._tmp0, &kfilter.k_states)
                # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
                # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
                blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, kfilter._tmp0, &kfilter.k_states,
                            model._transition, &model._k_states,
                    &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

    return 0


cdef np.float32_t sloglikelihood_conventional(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant):
    # Constants
    cdef:
        np.float32_t loglikelihood
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    loglikelihood = -0.5*(model._k_endog*dlog(2*M_PI) + determinant)

    if not kfilter.filter_method & FILTER_CONCENTRATED:
        loglikelihood = loglikelihood - 0.5*blas.sdot(&model._k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)

    return loglikelihood

cdef np.float32_t sscale_conventional(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        np.float32_t scale
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    scale = blas.sdot(&model._k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)

    return scale

# ### Missing Observation Conventional Kalman filter
#
# See Durbin and Koopman (2012) Chapter 4.10
#
# Here k_endog is the same as usual, but the design matrix and observation
# covariance matrix are enforced to be zero matrices, and the loglikelihood
# is defined to be zero.

cdef int dforecast_missing_conventional(dKalmanFilter kfilter, dStatespace model):
    cdef int i, j
    cdef int inc = 1, design_t = 0
    cdef np.float64_t alpha = 1

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # Just set to zeros, see below (this means if forecasts are required for
    # this part, they must be done in the wrappe)

    # #### Forecast error for time t  
    # It is undefined here, since obs is nan
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zero
    for i in range(kfilter.k_endog):
        kfilter._forecast[i] = 0
        kfilter._forecast_error[i] = 0

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv 0$
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zeros
    for i in range(kfilter.k_endog): # columns
        for j in range(kfilter.k_endog): # rows
            kfilter._forecast_error_cov[j + i*kfilter.k_endog] = 0

cdef int dupdating_missing_conventional(dKalmanFilter kfilter, dStatespace model):
    cdef int inc = 1

    # Simply copy over the input arrays ($a_t, P_t$) to the filtered arrays
    # ($a_{t|t}, P_{t|t}$)
    # Note: use kfilter dimensions since we just want to copy whole arrays;
    blas.dcopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.dcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

cdef np.float64_t dinverse_missing_conventional(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant)  except *:
    # Since the inverse of the forecast error covariance matrix is not
    # stored, we do not need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return -np.inf

cdef np.float64_t dloglikelihood_missing_conventional(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant):
    return 0.0

cdef np.float64_t dscale_missing_conventional(dKalmanFilter kfilter, dStatespace model):
    return 0.0

# ### Conventional Kalman filter
#
# The following are the above routines as defined in the conventional Kalman
# filter.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int dforecast_conventional(dKalmanFilter kfilter, dStatespace model):

    # Constants
    cdef:
        int inc = 1, ld, i, j
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # 
    # *Note*: $a_t$ is given from the initialization (for $t = 0$) or
    # from the previous iteration of the filter (for $t > 0$).

    # $\\# = d_t$
    blas.dcopy(&model._k_endog, model._obs_intercept, &inc, kfilter._forecast, &inc)
    # `forecast` $= 1.0 * Z_t a_t + 1.0 * \\#$  
    # $(p \times 1) = (p \times m) (m \times 1) + (p \times 1)$
    blas.dgemv("N", &model._k_endog, &model._k_states,
          &alpha, model._design, &model._k_endog,
                  kfilter._input_state, &inc,
          &alpha, kfilter._forecast, &inc)

    # #### Forecast error for time t  
    # `forecast_error` $\equiv v_t = y_t -$ `forecast`

    # $\\# = y_t$
    blas.dcopy(&model._k_endog, model._obs, &inc, kfilter._forecast_error, &inc)
    # $v_t = -1.0 * $ `forecast` $ + \\#$
    # $(p \times 1) = (p \times 1) + (p \times 1)$
    blas.daxpy(&model._k_endog, &gamma, kfilter._forecast, &inc, kfilter._forecast_error, &inc)

    # *Intermediate calculation* (used just below and then once more)  
    # `tmp1` array used here, dimension $(m \times p)$  
    # $\\#_1 = P_t Z_t'$  
    # $(m \times p) = (m \times m) (p \times m)'$
    blas.dgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
          &alpha, kfilter._input_state_cov, &kfilter.k_states,
                  model._design, &model._k_endog,
          &beta, kfilter._tmp1, &kfilter.k_states)

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv Z_t P_t Z_t' + H_t$
    # 
    # *Note*: this and does nothing at all to `forecast_error_cov` if
    # converged == True
    # TODO make sure when converged, the copies are made correctly
    if not kfilter.converged:
        # $\\# = H_t$
        # blas.dcopy(&kfilter.k_endog2, kfilter._obs_cov, &inc, kfilter._forecast_error_cov, &inc)
        for i in range(model._k_endog): # columns
            for j in range(model._k_endog): # rows
                kfilter._forecast_error_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]

        # $F_t = 1.0 * Z_t \\#_1 + 1.0 * \\#$
        blas.dgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_states,
              &alpha, model._design, &model._k_endog,
                     kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._forecast_error_cov, &kfilter.k_endog)

    return 0

cdef int dchandrasekhar_recursion(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int info
        int inc = 1
        int j, k
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # Chandrasekhar recursion
    # Initialization of the recursions
    if kfilter.t == 0:
        # Initialize M
        blas.dcopy(&model._k_endog2, kfilter._forecast_error_cov, &inc, &kfilter.CM[0, 0], &inc)
        lapack.dgetrf(&model._k_endog, &model._k_endog,
                        &kfilter.CM[0, 0], &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)
        lapack.dgetri(&model._k_endog, &kfilter.CM[0, 0], &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)
        blas.dscal(&model._k_endog2, &gamma, &kfilter.CM[0, 0], &inc)

        # Initialize W
        # Our Kalman gain is T P Z' F^{-1} but we need to initialize
        # W = T P Z', so we need to multiply by F
        # (not particularly efficient way to do this, but it's only
        # done for one time step so it's probably okay)
        blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
          &alpha, kfilter._kalman_gain, &kfilter.k_states,
                  kfilter._forecast_error_cov, &kfilter.k_endog,
          &beta, &kfilter.CW[0, 0], &kfilter.k_states)
    else:
        # Compute M = M + M @ W.T @ Z.T @ Fi_prev @ Z @ W @ M
        #           = M + (M @ W.T) @ Z.T @ (Fi_prev @ Z) @ (W @ M)

        # M @ W.T: (p x p) (p x m) -> (p x m)
        blas.dgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)

        # (Fi_prev @ Z) @ (W @ M): (p x m) (m x p) -> (p x p)
        blas.dgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CprevFiZ[0, 0], &kfilter.k_endog,
                  &kfilter.CMW[0, 0], &kfilter.k_endog,
          &beta, &kfilter.CtmpM[0, 0], &kfilter.k_endog)

        # (M @ W.T) @ Z.T: (p x m) (m x p) -> (p x p)
        blas.dgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  model._design, &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &kfilter.k_endog)

        # M + (M @ W.T @ Z.T) (Fi_prev @ Z @ W @ M) : (p x p) (p x p) -> (p x p)
        blas.dgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
          &alpha, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
                  &kfilter.CtmpM[0, 0], &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Compute W = (T - Kg @ Z) @ W
        # W -> tmpW
        blas.dcopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        # T -> tmp00
        blas.dcopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)

        # T - K @ Z: (m x p) (p x m) -> (m x m)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
            &gamma, kfilter._kalman_gain, &kfilter.k_states,
                    model._design, &model._k_endog,
            &alpha, kfilter._tmp00, &kfilter.k_states)

        # (T - T @ K @ Z[i]) @ tmpW -> W
        blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      &kfilter.CtmpW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CW[0, 0], &kfilter.k_states)

    # Save F^{-1} Z (this period's value used in next iteration)
    blas.dcopy(&model._k_endogstates, kfilter._tmp3, &inc, &kfilter.CprevFiZ[0, 0], &inc)

cdef int dupdating_conventional(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # #### Filtered state for time t
    # $a_{t|t} = a_t + P_t Z_t' F_t^{-1} v_t$  
    # $a_{t|t} = 1.0 * \\#_1 \\#_2 + 1.0 a_t$
    blas.dcopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.dgemv("N", &model._k_states, &model._k_endog,
          &alpha, kfilter._tmp1, &kfilter.k_states,
                  kfilter._tmp2, &inc,
          &alpha, kfilter._filtered_state, &inc)

    # #### Filtered state covariance for time t
    # Temporary computation used below
    if not kfilter.converged:
        # P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        blas.dgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._input_state_cov, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, &kfilter.CtmpW[0, 0], &kfilter.k_states)

    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.dcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times p) (p \times m) + (m \times m)$
        blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, &kfilter.CtmpW[0, 0], &kfilter.k_states,
                      kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # T_t P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        if model.identity_transition:
            blas.dcopy(&model._k_endogstates, &kfilter.CtmpW[0, 0], &inc, kfilter._kalman_gain, &inc)
        else:
            blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                &alpha, model._transition, &kfilter.k_states,
                        &kfilter.CtmpW[0, 0], &kfilter.k_states,
                &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int dprediction_conventional(dKalmanFilter kfilter, dStatespace model):

    # Constants
    cdef:
        int inc = 1
        int forecast_cov_t = kfilter.t
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.dcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    if model.identity_transition:
        blas.daxpy(&model._k_states, &alpha, kfilter._filtered_state, &inc, kfilter._predicted_state, &inc)
    else:
        blas.dgemv("N", &model._k_states, &model._k_states,
            &alpha, model._transition, &model._k_states,
                    kfilter._filtered_state, &inc,
            &alpha, kfilter._predicted_state, &inc)

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t|t} T_t' + Q_t^*$
    #
    # *Note*: this and does nothing at all to `predicted_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.dcopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
        # `tmp0` array used here, dimension $(m \times m)$  

        if kfilter.filter_method & FILTER_CHANDRASEKHAR:
            # Chandrasekhar_recursion
            dchandrasekhar_recursion(kfilter, model)

            # Chandrasekhar-based prediction step
            blas.dcopy(&model._k_states2, kfilter._input_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            # M @ W.T. (p x p) (p x m)
            blas.dgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                      &kfilter.CW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
            # P = P + W M W.T
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CW[0, 0], &kfilter.k_states,
                      &kfilter.CMW[0, 0], &kfilter.k_endog,
              &alpha, kfilter._predicted_state_cov, &kfilter.k_states)
        else:
            # Kalman filter prediction step
            # $\\#_0 = T_t P_{t|t} $

            if model.identity_transition:
                blas.daxpy(&model._k_states2, &alpha, kfilter._filtered_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            else:
                # $(m \times m) = (m \times m) (m \times m)$
                blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, model._transition, &model._k_states,
                            kfilter._filtered_state_cov, &kfilter.k_states,
                    &beta, kfilter._tmp0, &kfilter.k_states)
                # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
                # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
                blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, kfilter._tmp0, &kfilter.k_states,
                            model._transition, &model._k_states,
                    &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

    return 0


cdef np.float64_t dloglikelihood_conventional(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant):
    # Constants
    cdef:
        np.float64_t loglikelihood
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    loglikelihood = -0.5*(model._k_endog*dlog(2*M_PI) + determinant)

    if not kfilter.filter_method & FILTER_CONCENTRATED:
        loglikelihood = loglikelihood - 0.5*blas.ddot(&model._k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)

    return loglikelihood

cdef np.float64_t dscale_conventional(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        np.float64_t scale
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    scale = blas.ddot(&model._k_endog, kfilter._forecast_error, &inc, kfilter._tmp2, &inc)

    return scale

# ### Missing Observation Conventional Kalman filter
#
# See Durbin and Koopman (2012) Chapter 4.10
#
# Here k_endog is the same as usual, but the design matrix and observation
# covariance matrix are enforced to be zero matrices, and the loglikelihood
# is defined to be zero.

cdef int cforecast_missing_conventional(cKalmanFilter kfilter, cStatespace model):
    cdef int i, j
    cdef int inc = 1, design_t = 0
    cdef np.complex64_t alpha = 1

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # Just set to zeros, see below (this means if forecasts are required for
    # this part, they must be done in the wrappe)

    # #### Forecast error for time t  
    # It is undefined here, since obs is nan
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zero
    for i in range(kfilter.k_endog):
        kfilter._forecast[i] = 0
        kfilter._forecast_error[i] = 0

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv 0$
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zeros
    for i in range(kfilter.k_endog): # columns
        for j in range(kfilter.k_endog): # rows
            kfilter._forecast_error_cov[j + i*kfilter.k_endog] = 0

cdef int cupdating_missing_conventional(cKalmanFilter kfilter, cStatespace model):
    cdef int inc = 1

    # Simply copy over the input arrays ($a_t, P_t$) to the filtered arrays
    # ($a_{t|t}, P_{t|t}$)
    # Note: use kfilter dimensions since we just want to copy whole arrays;
    blas.ccopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.ccopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

cdef np.complex64_t cinverse_missing_conventional(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant)  except *:
    # Since the inverse of the forecast error covariance matrix is not
    # stored, we do not need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return -np.inf

cdef np.complex64_t cloglikelihood_missing_conventional(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant):
    return 0.0

cdef np.complex64_t cscale_missing_conventional(cKalmanFilter kfilter, cStatespace model):
    return 0.0

# ### Conventional Kalman filter
#
# The following are the above routines as defined in the conventional Kalman
# filter.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int cforecast_conventional(cKalmanFilter kfilter, cStatespace model):

    # Constants
    cdef:
        int inc = 1, ld, i, j
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # 
    # *Note*: $a_t$ is given from the initialization (for $t = 0$) or
    # from the previous iteration of the filter (for $t > 0$).

    # $\\# = d_t$
    blas.ccopy(&model._k_endog, model._obs_intercept, &inc, kfilter._forecast, &inc)
    # `forecast` $= 1.0 * Z_t a_t + 1.0 * \\#$  
    # $(p \times 1) = (p \times m) (m \times 1) + (p \times 1)$
    blas.cgemv("N", &model._k_endog, &model._k_states,
          &alpha, model._design, &model._k_endog,
                  kfilter._input_state, &inc,
          &alpha, kfilter._forecast, &inc)

    # #### Forecast error for time t  
    # `forecast_error` $\equiv v_t = y_t -$ `forecast`

    # $\\# = y_t$
    blas.ccopy(&model._k_endog, model._obs, &inc, kfilter._forecast_error, &inc)
    # $v_t = -1.0 * $ `forecast` $ + \\#$
    # $(p \times 1) = (p \times 1) + (p \times 1)$
    blas.caxpy(&model._k_endog, &gamma, kfilter._forecast, &inc, kfilter._forecast_error, &inc)

    # *Intermediate calculation* (used just below and then once more)  
    # `tmp1` array used here, dimension $(m \times p)$  
    # $\\#_1 = P_t Z_t'$  
    # $(m \times p) = (m \times m) (p \times m)'$
    blas.cgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
          &alpha, kfilter._input_state_cov, &kfilter.k_states,
                  model._design, &model._k_endog,
          &beta, kfilter._tmp1, &kfilter.k_states)

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv Z_t P_t Z_t' + H_t$
    # 
    # *Note*: this and does nothing at all to `forecast_error_cov` if
    # converged == True
    # TODO make sure when converged, the copies are made correctly
    if not kfilter.converged:
        # $\\# = H_t$
        # blas.ccopy(&kfilter.k_endog2, kfilter._obs_cov, &inc, kfilter._forecast_error_cov, &inc)
        for i in range(model._k_endog): # columns
            for j in range(model._k_endog): # rows
                kfilter._forecast_error_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]

        # $F_t = 1.0 * Z_t \\#_1 + 1.0 * \\#$
        blas.cgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_states,
              &alpha, model._design, &model._k_endog,
                     kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._forecast_error_cov, &kfilter.k_endog)

    return 0

cdef int cchandrasekhar_recursion(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int info
        int inc = 1
        int j, k
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # Chandrasekhar recursion
    # Initialization of the recursions
    if kfilter.t == 0:
        # Initialize M
        blas.ccopy(&model._k_endog2, kfilter._forecast_error_cov, &inc, &kfilter.CM[0, 0], &inc)
        lapack.cgetrf(&model._k_endog, &model._k_endog,
                        &kfilter.CM[0, 0], &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)
        lapack.cgetri(&model._k_endog, &kfilter.CM[0, 0], &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)
        blas.cscal(&model._k_endog2, &gamma, &kfilter.CM[0, 0], &inc)

        # Initialize W
        # Our Kalman gain is T P Z' F^{-1} but we need to initialize
        # W = T P Z', so we need to multiply by F
        # (not particularly efficient way to do this, but it's only
        # done for one time step so it's probably okay)
        blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
          &alpha, kfilter._kalman_gain, &kfilter.k_states,
                  kfilter._forecast_error_cov, &kfilter.k_endog,
          &beta, &kfilter.CW[0, 0], &kfilter.k_states)
    else:
        # Compute M = M + M @ W.T @ Z.T @ Fi_prev @ Z @ W @ M
        #           = M + (M @ W.T) @ Z.T @ (Fi_prev @ Z) @ (W @ M)

        # M @ W.T: (p x p) (p x m) -> (p x m)
        blas.cgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)

        # (Fi_prev @ Z) @ (W @ M): (p x m) (m x p) -> (p x p)
        blas.cgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CprevFiZ[0, 0], &kfilter.k_endog,
                  &kfilter.CMW[0, 0], &kfilter.k_endog,
          &beta, &kfilter.CtmpM[0, 0], &kfilter.k_endog)

        # (M @ W.T) @ Z.T: (p x m) (m x p) -> (p x p)
        blas.cgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  model._design, &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &kfilter.k_endog)

        # M + (M @ W.T @ Z.T) (Fi_prev @ Z @ W @ M) : (p x p) (p x p) -> (p x p)
        blas.cgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
          &alpha, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
                  &kfilter.CtmpM[0, 0], &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Compute W = (T - Kg @ Z) @ W
        # W -> tmpW
        blas.ccopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        # T -> tmp00
        blas.ccopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)

        # T - K @ Z: (m x p) (p x m) -> (m x m)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
            &gamma, kfilter._kalman_gain, &kfilter.k_states,
                    model._design, &model._k_endog,
            &alpha, kfilter._tmp00, &kfilter.k_states)

        # (T - T @ K @ Z[i]) @ tmpW -> W
        blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      &kfilter.CtmpW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CW[0, 0], &kfilter.k_states)

    # Save F^{-1} Z (this period's value used in next iteration)
    blas.ccopy(&model._k_endogstates, kfilter._tmp3, &inc, &kfilter.CprevFiZ[0, 0], &inc)

cdef int cupdating_conventional(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # #### Filtered state for time t
    # $a_{t|t} = a_t + P_t Z_t' F_t^{-1} v_t$  
    # $a_{t|t} = 1.0 * \\#_1 \\#_2 + 1.0 a_t$
    blas.ccopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.cgemv("N", &model._k_states, &model._k_endog,
          &alpha, kfilter._tmp1, &kfilter.k_states,
                  kfilter._tmp2, &inc,
          &alpha, kfilter._filtered_state, &inc)

    # #### Filtered state covariance for time t
    # Temporary computation used below
    if not kfilter.converged:
        # P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        blas.cgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._input_state_cov, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, &kfilter.CtmpW[0, 0], &kfilter.k_states)

    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.ccopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times p) (p \times m) + (m \times m)$
        blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, &kfilter.CtmpW[0, 0], &kfilter.k_states,
                      kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # T_t P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        if model.identity_transition:
            blas.ccopy(&model._k_endogstates, &kfilter.CtmpW[0, 0], &inc, kfilter._kalman_gain, &inc)
        else:
            blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                &alpha, model._transition, &kfilter.k_states,
                        &kfilter.CtmpW[0, 0], &kfilter.k_states,
                &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int cprediction_conventional(cKalmanFilter kfilter, cStatespace model):

    # Constants
    cdef:
        int inc = 1
        int forecast_cov_t = kfilter.t
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.ccopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    if model.identity_transition:
        blas.caxpy(&model._k_states, &alpha, kfilter._filtered_state, &inc, kfilter._predicted_state, &inc)
    else:
        blas.cgemv("N", &model._k_states, &model._k_states,
            &alpha, model._transition, &model._k_states,
                    kfilter._filtered_state, &inc,
            &alpha, kfilter._predicted_state, &inc)

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t|t} T_t' + Q_t^*$
    #
    # *Note*: this and does nothing at all to `predicted_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.ccopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
        # `tmp0` array used here, dimension $(m \times m)$  

        if kfilter.filter_method & FILTER_CHANDRASEKHAR:
            # Chandrasekhar_recursion
            cchandrasekhar_recursion(kfilter, model)

            # Chandrasekhar-based prediction step
            blas.ccopy(&model._k_states2, kfilter._input_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            # M @ W.T. (p x p) (p x m)
            blas.cgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                      &kfilter.CW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
            # P = P + W M W.T
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CW[0, 0], &kfilter.k_states,
                      &kfilter.CMW[0, 0], &kfilter.k_endog,
              &alpha, kfilter._predicted_state_cov, &kfilter.k_states)
        else:
            # Kalman filter prediction step
            # $\\#_0 = T_t P_{t|t} $

            if model.identity_transition:
                blas.caxpy(&model._k_states2, &alpha, kfilter._filtered_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            else:
                # $(m \times m) = (m \times m) (m \times m)$
                blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, model._transition, &model._k_states,
                            kfilter._filtered_state_cov, &kfilter.k_states,
                    &beta, kfilter._tmp0, &kfilter.k_states)
                # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
                # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
                blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, kfilter._tmp0, &kfilter.k_states,
                            model._transition, &model._k_states,
                    &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

    return 0


cdef np.complex64_t cloglikelihood_conventional(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant):
    # Constants
    cdef:
        np.complex64_t loglikelihood
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    loglikelihood = -0.5*(model._k_endog*zlog(2*M_PI) + determinant)

    if not kfilter.filter_method & FILTER_CONCENTRATED:
        blas.cgemv("N", &inc, &model._k_endog,
                       &alpha, kfilter._forecast_error, &inc,
                               kfilter._tmp2, &inc,
                       &beta, kfilter._tmp0, &inc)
        loglikelihood = loglikelihood - 0.5 * kfilter._tmp0[0]

    return loglikelihood

cdef np.complex64_t cscale_conventional(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        np.complex64_t scale
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    blas.cgemv("N", &inc, &model._k_endog,
                   &alpha, kfilter._forecast_error, &inc,
                           kfilter._tmp2, &inc,
                   &beta, kfilter._tmp0, &inc)
    scale = kfilter._tmp0[0]

    return scale

# ### Missing Observation Conventional Kalman filter
#
# See Durbin and Koopman (2012) Chapter 4.10
#
# Here k_endog is the same as usual, but the design matrix and observation
# covariance matrix are enforced to be zero matrices, and the loglikelihood
# is defined to be zero.

cdef int zforecast_missing_conventional(zKalmanFilter kfilter, zStatespace model):
    cdef int i, j
    cdef int inc = 1, design_t = 0
    cdef np.complex128_t alpha = 1

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # Just set to zeros, see below (this means if forecasts are required for
    # this part, they must be done in the wrappe)

    # #### Forecast error for time t  
    # It is undefined here, since obs is nan
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zero
    for i in range(kfilter.k_endog):
        kfilter._forecast[i] = 0
        kfilter._forecast_error[i] = 0

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv 0$
    # Note: use kfilter dimensions since we just want to set the whole array
    # to zeros
    for i in range(kfilter.k_endog): # columns
        for j in range(kfilter.k_endog): # rows
            kfilter._forecast_error_cov[j + i*kfilter.k_endog] = 0

cdef int zupdating_missing_conventional(zKalmanFilter kfilter, zStatespace model):
    cdef int inc = 1

    # Simply copy over the input arrays ($a_t, P_t$) to the filtered arrays
    # ($a_{t|t}, P_{t|t}$)
    # Note: use kfilter dimensions since we just want to copy whole arrays;
    blas.zcopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.zcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

cdef np.complex128_t zinverse_missing_conventional(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant)  except *:
    # Since the inverse of the forecast error covariance matrix is not
    # stored, we do not need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return -np.inf

cdef np.complex128_t zloglikelihood_missing_conventional(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant):
    return 0.0

cdef np.complex128_t zscale_missing_conventional(zKalmanFilter kfilter, zStatespace model):
    return 0.0

# ### Conventional Kalman filter
#
# The following are the above routines as defined in the conventional Kalman
# filter.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int zforecast_conventional(zKalmanFilter kfilter, zStatespace model):

    # Constants
    cdef:
        int inc = 1, ld, i, j
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # #### Forecast for time t  
    # `forecast` $= Z_t a_t + d_t$
    # 
    # *Note*: $a_t$ is given from the initialization (for $t = 0$) or
    # from the previous iteration of the filter (for $t > 0$).

    # $\\# = d_t$
    blas.zcopy(&model._k_endog, model._obs_intercept, &inc, kfilter._forecast, &inc)
    # `forecast` $= 1.0 * Z_t a_t + 1.0 * \\#$  
    # $(p \times 1) = (p \times m) (m \times 1) + (p \times 1)$
    blas.zgemv("N", &model._k_endog, &model._k_states,
          &alpha, model._design, &model._k_endog,
                  kfilter._input_state, &inc,
          &alpha, kfilter._forecast, &inc)

    # #### Forecast error for time t  
    # `forecast_error` $\equiv v_t = y_t -$ `forecast`

    # $\\# = y_t$
    blas.zcopy(&model._k_endog, model._obs, &inc, kfilter._forecast_error, &inc)
    # $v_t = -1.0 * $ `forecast` $ + \\#$
    # $(p \times 1) = (p \times 1) + (p \times 1)$
    blas.zaxpy(&model._k_endog, &gamma, kfilter._forecast, &inc, kfilter._forecast_error, &inc)

    # *Intermediate calculation* (used just below and then once more)  
    # `tmp1` array used here, dimension $(m \times p)$  
    # $\\#_1 = P_t Z_t'$  
    # $(m \times p) = (m \times m) (p \times m)'$
    blas.zgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
          &alpha, kfilter._input_state_cov, &kfilter.k_states,
                  model._design, &model._k_endog,
          &beta, kfilter._tmp1, &kfilter.k_states)

    # #### Forecast error covariance matrix for time t  
    # $F_t \equiv Z_t P_t Z_t' + H_t$
    # 
    # *Note*: this and does nothing at all to `forecast_error_cov` if
    # converged == True
    # TODO make sure when converged, the copies are made correctly
    if not kfilter.converged:
        # $\\# = H_t$
        # blas.zcopy(&kfilter.k_endog2, kfilter._obs_cov, &inc, kfilter._forecast_error_cov, &inc)
        for i in range(model._k_endog): # columns
            for j in range(model._k_endog): # rows
                kfilter._forecast_error_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog]

        # $F_t = 1.0 * Z_t \\#_1 + 1.0 * \\#$
        blas.zgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_states,
              &alpha, model._design, &model._k_endog,
                     kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._forecast_error_cov, &kfilter.k_endog)

    return 0

cdef int zchandrasekhar_recursion(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int info
        int inc = 1
        int j, k
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # Chandrasekhar recursion
    # Initialization of the recursions
    if kfilter.t == 0:
        # Initialize M
        blas.zcopy(&model._k_endog2, kfilter._forecast_error_cov, &inc, &kfilter.CM[0, 0], &inc)
        lapack.zgetrf(&model._k_endog, &model._k_endog,
                        &kfilter.CM[0, 0], &kfilter.k_endog,
                        kfilter._forecast_error_ipiv, &info)
        lapack.zgetri(&model._k_endog, &kfilter.CM[0, 0], &kfilter.k_endog,
               kfilter._forecast_error_ipiv, kfilter._forecast_error_work, &kfilter.ldwork, &info)
        blas.zscal(&model._k_endog2, &gamma, &kfilter.CM[0, 0], &inc)

        # Initialize W
        # Our Kalman gain is T P Z' F^{-1} but we need to initialize
        # W = T P Z', so we need to multiply by F
        # (not particularly efficient way to do this, but it's only
        # done for one time step so it's probably okay)
        blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
          &alpha, kfilter._kalman_gain, &kfilter.k_states,
                  kfilter._forecast_error_cov, &kfilter.k_endog,
          &beta, &kfilter.CW[0, 0], &kfilter.k_states)
    else:
        # Compute M = M + M @ W.T @ Z.T @ Fi_prev @ Z @ W @ M
        #           = M + (M @ W.T) @ Z.T @ (Fi_prev @ Z) @ (W @ M)

        # M @ W.T: (p x p) (p x m) -> (p x m)
        blas.zgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)

        # (Fi_prev @ Z) @ (W @ M): (p x m) (m x p) -> (p x p)
        blas.zgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CprevFiZ[0, 0], &kfilter.k_endog,
                  &kfilter.CMW[0, 0], &kfilter.k_endog,
          &beta, &kfilter.CtmpM[0, 0], &kfilter.k_endog)

        # (M @ W.T) @ Z.T: (p x m) (m x p) -> (p x p)
        blas.zgemm("N", "T", &model._k_endog, &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  model._design, &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &kfilter.k_endog)

        # M + (M @ W.T @ Z.T) (Fi_prev @ Z @ W @ M) : (p x p) (p x p) -> (p x p)
        blas.zgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
          &alpha, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
                  &kfilter.CtmpM[0, 0], &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Compute W = (T - Kg @ Z) @ W
        # W -> tmpW
        blas.zcopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        # T -> tmp00
        blas.zcopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)

        # T - K @ Z: (m x p) (p x m) -> (m x m)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
            &gamma, kfilter._kalman_gain, &kfilter.k_states,
                    model._design, &model._k_endog,
            &alpha, kfilter._tmp00, &kfilter.k_states)

        # (T - T @ K @ Z[i]) @ tmpW -> W
        blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      &kfilter.CtmpW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CW[0, 0], &kfilter.k_states)

    # Save F^{-1} Z (this period's value used in next iteration)
    blas.zcopy(&model._k_endogstates, kfilter._tmp3, &inc, &kfilter.CprevFiZ[0, 0], &inc)

cdef int zupdating_conventional(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # #### Filtered state for time t
    # $a_{t|t} = a_t + P_t Z_t' F_t^{-1} v_t$  
    # $a_{t|t} = 1.0 * \\#_1 \\#_2 + 1.0 a_t$
    blas.zcopy(&kfilter.k_states, kfilter._input_state, &inc, kfilter._filtered_state, &inc)
    blas.zgemv("N", &model._k_states, &model._k_endog,
          &alpha, kfilter._tmp1, &kfilter.k_states,
                  kfilter._tmp2, &inc,
          &alpha, kfilter._filtered_state, &inc)

    # #### Filtered state covariance for time t
    # Temporary computation used below
    if not kfilter.converged:
        # P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        blas.zgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._input_state_cov, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, &kfilter.CtmpW[0, 0], &kfilter.k_states)

    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.zcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times p) (p \times m) + (m \times m)$
        blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, &kfilter.CtmpW[0, 0], &kfilter.k_states,
                      kfilter._tmp1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # T_t P_t Z_t' F_t^{-1}
        # $(m \times p) = (m \times m) (m \times p)$
        if model.identity_transition:
            blas.zcopy(&model._k_endogstates, &kfilter.CtmpW[0, 0], &inc, kfilter._kalman_gain, &inc)
        else:
            blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                &alpha, model._transition, &kfilter.k_states,
                        &kfilter.CtmpW[0, 0], &kfilter.k_states,
                &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int zprediction_conventional(zKalmanFilter kfilter, zStatespace model):

    # Constants
    cdef:
        int inc = 1
        int forecast_cov_t = kfilter.t
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.zcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    if model.identity_transition:
        blas.zaxpy(&model._k_states, &alpha, kfilter._filtered_state, &inc, kfilter._predicted_state, &inc)
    else:
        blas.zgemv("N", &model._k_states, &model._k_states,
            &alpha, model._transition, &model._k_states,
                    kfilter._filtered_state, &inc,
            &alpha, kfilter._predicted_state, &inc)

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t|t} T_t' + Q_t^*$
    #
    # *Note*: this and does nothing at all to `predicted_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.zcopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
        # `tmp0` array used here, dimension $(m \times m)$  

        if kfilter.filter_method & FILTER_CHANDRASEKHAR:
            # Chandrasekhar_recursion
            zchandrasekhar_recursion(kfilter, model)

            # Chandrasekhar-based prediction step
            blas.zcopy(&model._k_states2, kfilter._input_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            # M @ W.T. (p x p) (p x m)
            blas.zgemm("N", "T", &model._k_endog, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                      &kfilter.CW[0, 0], &kfilter.k_states,
              &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
            # P = P + W M W.T
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, &kfilter.CW[0, 0], &kfilter.k_states,
                      &kfilter.CMW[0, 0], &kfilter.k_endog,
              &alpha, kfilter._predicted_state_cov, &kfilter.k_states)
        else:
            # Kalman filter prediction step
            # $\\#_0 = T_t P_{t|t} $

            if model.identity_transition:
                blas.zaxpy(&model._k_states2, &alpha, kfilter._filtered_state_cov, &inc, kfilter._predicted_state_cov, &inc)
            else:
                # $(m \times m) = (m \times m) (m \times m)$
                blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, model._transition, &model._k_states,
                            kfilter._filtered_state_cov, &kfilter.k_states,
                    &beta, kfilter._tmp0, &kfilter.k_states)
                # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
                # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
                blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                    &alpha, kfilter._tmp0, &kfilter.k_states,
                            model._transition, &model._k_states,
                    &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

    return 0


cdef np.complex128_t zloglikelihood_conventional(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant):
    # Constants
    cdef:
        np.complex128_t loglikelihood
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    loglikelihood = -0.5*(model._k_endog*zlog(2*M_PI) + determinant)

    if not kfilter.filter_method & FILTER_CONCENTRATED:
        blas.zgemv("N", &inc, &model._k_endog,
                       &alpha, kfilter._forecast_error, &inc,
                               kfilter._tmp2, &inc,
                       &beta, kfilter._tmp0, &inc)
        loglikelihood = loglikelihood - 0.5 * kfilter._tmp0[0]

    return loglikelihood

cdef np.complex128_t zscale_conventional(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        np.complex128_t scale
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    blas.zgemv("N", &inc, &model._k_endog,
                   &alpha, kfilter._forecast_error, &inc,
                           kfilter._tmp2, &inc,
                   &beta, kfilter._tmp0, &inc)
    scale = kfilter._tmp0[0]

    return scale
