#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Models

Author: Chad Fulton  
License: Simplified-BSD
"""

# Typical imports
cimport numpy as np
from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack

from statsmodels.tsa.statespace._kalman_filter cimport FILTER_CONCENTRATED


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
    # stored, we don't need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return 0.0

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
    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.scopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # `tmp0` array used here, dimension $(m \times m)$  
        # $\\#_0 = 1.0 * \\#_1 \\#_3$  
        # $(m \times m) = (m \times p) (p \times m)$
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._tmp0, &kfilter.k_states)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, kfilter._tmp0, &kfilter.k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # `tmp00` array used here, dimension $(m \times m)$  
        # $\\#_{00} = 1.0 * T_t P_t$  
        # $(m \times m) = (m \times m) (m \times m)$
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &beta, kfilter._tmp00, &kfilter.k_states)

        # K_t = 1.0 * \\#_{00} \\#_3'
        # $(m \times p) = (m \times m) (m \times p)$
        blas.sgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int sprediction_conventional(sKalmanFilter kfilter, sStatespace model):

    # Constants
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.scopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
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

        # $\\#_0 = T_t P_{t|t} $

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

    loglikelihood = -0.5*(model._k_endog*dlog(2*NPY_PI) + dlog(determinant))

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
    # stored, we don't need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return 0.0

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
    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.dcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # `tmp0` array used here, dimension $(m \times m)$  
        # $\\#_0 = 1.0 * \\#_1 \\#_3$  
        # $(m \times m) = (m \times p) (p \times m)$
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._tmp0, &kfilter.k_states)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, kfilter._tmp0, &kfilter.k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # `tmp00` array used here, dimension $(m \times m)$  
        # $\\#_{00} = 1.0 * T_t P_t$  
        # $(m \times m) = (m \times m) (m \times m)$
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &beta, kfilter._tmp00, &kfilter.k_states)

        # K_t = 1.0 * \\#_{00} \\#_3'
        # $(m \times p) = (m \times m) (m \times p)$
        blas.dgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int dprediction_conventional(dKalmanFilter kfilter, dStatespace model):

    # Constants
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.dcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
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

        # $\\#_0 = T_t P_{t|t} $

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

    loglikelihood = -0.5*(model._k_endog*dlog(2*NPY_PI) + dlog(determinant))

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
    # stored, we don't need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return 0.0

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
    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.ccopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # `tmp0` array used here, dimension $(m \times m)$  
        # $\\#_0 = 1.0 * \\#_1 \\#_3$  
        # $(m \times m) = (m \times p) (p \times m)$
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._tmp0, &kfilter.k_states)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, kfilter._tmp0, &kfilter.k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # `tmp00` array used here, dimension $(m \times m)$  
        # $\\#_{00} = 1.0 * T_t P_t$  
        # $(m \times m) = (m \times m) (m \times m)$
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &beta, kfilter._tmp00, &kfilter.k_states)

        # K_t = 1.0 * \\#_{00} \\#_3'
        # $(m \times p) = (m \times m) (m \times p)$
        blas.cgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int cprediction_conventional(cKalmanFilter kfilter, cStatespace model):

    # Constants
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.ccopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
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

        # $\\#_0 = T_t P_{t|t} $

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

    loglikelihood = -0.5*(model._k_endog*zlog(2*NPY_PI) + zlog(determinant))

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
    # stored, we don't need to fill it (e.g. with NPY_NAN values). Instead,
    # just do a noop here and return a zero determinant ($|0|$).
    return 0.0

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
    # $P_{t|t} = P_t - P_t Z_t' F_t^{-1} Z_t P_t$  
    # $P_{t|t} = P_t - \\#_1 \\#_3 P_t$  
    # 
    # *Note*: this and does nothing at all to `filtered_state_cov` if
    # converged == True
    if not kfilter.converged:
        blas.zcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc, kfilter._filtered_state_cov, &inc)

        # `tmp0` array used here, dimension $(m \times m)$  
        # $\\#_0 = 1.0 * \\#_1 \\#_3$  
        # $(m \times m) = (m \times p) (p \times m)$
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &alpha, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._tmp0, &kfilter.k_states)

        # $P_{t|t} = - 1.0 * \\# P_t + 1.0 * P_t$  
        # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, kfilter._tmp0, &kfilter.k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$
    #
    # *Note*: this and does nothing at all to `kalman_gain` if
    # converged == True
    # *Note*: Kim and Nelson (1999) have a different version of the Kalman
    # gain, defined as $P_t Z_t' F_t^{-1}$. That is not adopted here.
    if not kfilter.converged:
        # `tmp00` array used here, dimension $(m \times m)$  
        # $\\#_{00} = 1.0 * T_t P_t$  
        # $(m \times m) = (m \times m) (m \times m)$
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      kfilter._input_state_cov, &kfilter.k_states,
              &beta, kfilter._tmp00, &kfilter.k_states)

        # K_t = 1.0 * \\#_{00} \\#_3'
        # $(m \times p) = (m \times m) (m \times p)$
        blas.zgemm("N", "T", &model._k_states, &model._k_endog, &model._k_states,
              &alpha, kfilter._tmp00, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, kfilter._kalman_gain, &kfilter.k_states)

    return 0

cdef int zprediction_conventional(zKalmanFilter kfilter, zStatespace model):

    # Constants
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t|t} + c_t$
    blas.zcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
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

        # $\\#_0 = T_t P_{t|t} $

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

    loglikelihood = -0.5*(model._k_endog*zlog(2*NPY_PI) + zlog(determinant))

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
