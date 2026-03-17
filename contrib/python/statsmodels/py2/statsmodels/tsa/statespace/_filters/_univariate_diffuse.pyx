#cython: profile=False
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
import numpy as np
from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack

from statsmodels.tsa.statespace._kalman_filter cimport (
    FILTER_CONCENTRATED, MEMORY_NO_LIKELIHOOD, MEMORY_NO_STD_FORECAST)

from statsmodels.tsa.statespace._filters._univariate cimport (
    sforecast_error, sforecast_error_cov,
    stemp_arrays, spredicted_state, spredicted_state_cov)

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int sforecast_univariate_diffuse(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int i, j, k
        int inc = 1
        np.float32_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # Initialize the filtered states
    blas.scopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    blas.scopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                            kfilter._filtered_state_cov, &inc)
    blas.scopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                            kfilter._predicted_diffuse_state_cov, &inc)

    # Iterate over endogenous variables
    for i in range(model._k_endog):
        # forecast_t, v_t, 
        sforecast_error(kfilter, model, i)

        # F_{*,t}
        forecast_error_cov = sforecast_error_cov(kfilter, model, i)
        if forecast_error_cov < 0:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # Save temporary array data
        if not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov
            stemp_arrays(kfilter, model, i, forecast_error_cov_inv)

        # F_{\infty,t}
        forecast_error_diffuse_cov = sforecast_error_diffuse_cov(kfilter, model, i)
        if forecast_error_diffuse_cov < 0:
            kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = 0
            forecast_error_diffuse_cov = 0

        # F_{\infty, i, i, t} > 0
        if forecast_error_diffuse_cov > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv

            # K0 = M_inf[:, i:i+1] * F1
            blas.scopy(&kfilter.k_states, &kfilter._M_inf[i * kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.sscal(&kfilter.k_states, &F1, kfilter._tmpK0, &inc)
            # K1 = M[:, i:i+1] * F1 + M_inf[:, i:i+1] * F2
            # K1 = M[:, i:i+1] * F1 + K0 * F12
            blas.scopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK1, &inc)
            blas.sscal(&kfilter.k_states, &F1, kfilter._tmpK1, &inc)
            blas.saxpy(&kfilter.k_states, &F12, kfilter._tmpK0, &inc, kfilter._tmpK1, &inc)
            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.sger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
            # L1 = -np.dot(K1, Zi)
            kfilter.tmpL1[:] = 0
            blas.sger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK1, &inc, &model._design[i], &model._k_endog, kfilter._tmpL1, &kfilter.k_states)

            # a_t = a_t + K0[:, 0] * v[i]
            blas.saxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)

            # P_t = np.dot(P_t_inf, L1.T) + np.dot(P_t, L0.T)
            # `tmp0` array used here, dimension $(m \times m)$  
            blas.scopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                      kfilter._tmpL1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = np.dot(P_t_inf, L0.T)
            blas.scopy(&kfilter.k_states2, kfilter._predicted_diffuse_state_cov, &inc, kfilter._tmp0, &inc)
            blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)

            # Loglikelihood
            kfilter._loglikelihood[0] = (
                kfilter._loglikelihood[0] - 0.5*(
                    dlog(2 * NPY_PI * forecast_error_diffuse_cov)))
        elif forecast_error_cov > kfilter.tolerance_diffuse:
            kfilter.nobs_kendog_diffuse_nonsingular = kfilter.nobs_kendog_diffuse_nonsingular + 1
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.scopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.sscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.sger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1

            # a_t = a_t + K0[:, 0] * v[i]
            blas.saxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)
            # P_t = np.dot(P_t, L0)
            blas.scopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = P_t_inf
            # (noop)

            # Loglikelihood
            if not forecast_error_cov == 0:
                kfilter._loglikelihood[0] = (
                    kfilter._loglikelihood[0] - 0.5*(
                        dlog(2 * NPY_PI * forecast_error_cov)))

                if kfilter.filter_method & FILTER_CONCENTRATED:
                    kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
                else:
                    kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

        # Kalman gain
        blas.scopy(&kfilter.k_states, kfilter._tmpK0, &inc, &kfilter._kalman_gain[i * kfilter.k_states], &inc)

        # Prediction (done below)
        # a_t1[:] = np.dot(T, a_tt)
        # P_t1[:] = np.dot(np.dot(T, P_tt), T.T) + RQR
        # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)

    return 0


cdef np.float32_t sforecast_error_diffuse_cov(sKalmanFilter kfilter, sStatespace model, int i):
    cdef:
        int inc = 1
        np.float32_t alpha = 1
        np.float32_t beta = 0
        np.float32_t forecast_error_diffuse_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation*
    # `M_inf` array used here, dimension $(m \times 1)$  
    # $M_{i, \infty} = P_{t,i,\infty} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    blas.sgemv("N", &model._k_states, &model._k_states,
          &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    # $F_{t,i,\infty} \equiv Z_{t,i} P_{t,i,\infty} Z_{t,i}'$
    # blas.ssymv("U", &model._k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    forecast_error_diffuse_cov = (
        blas.sdot(&k_states, &model._design[i], &model._k_endog,
                                      &kfilter._M_inf[i * kfilter.k_states], &inc)
    )
    kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = forecast_error_diffuse_cov
    return forecast_error_diffuse_cov


cdef int sprediction_univariate_diffuse(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    spredicted_state(kfilter, model)
    spredicted_state_cov(kfilter, model)
    spredicted_diffuse_state_cov(kfilter, model)

    return 0


cdef void spredicted_diffuse_state_cov(sKalmanFilter kfilter, sStatespace model):
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    # Need special handling for the completely missing case, since the
    # conventional Kalman filter routines are used in this case and they don't
    # copy over the predicted diffuse state cov
    if model._nmissing == model.k_endog:
        blas.scopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                                kfilter._predicted_diffuse_state_cov, &inc)

    # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)


# Note: updating, inverse, loglikelihood are all performed in prior steps
cdef int supdating_univariate_diffuse(sKalmanFilter kfilter, sStatespace model):
    return 0

cdef np.float32_t sinverse_noop_univariate_diffuse(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    return 0

cdef np.float32_t sloglikelihood_univariate_diffuse(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant):
    return 0

from statsmodels.tsa.statespace._filters._univariate cimport (
    dforecast_error, dforecast_error_cov,
    dtemp_arrays, dpredicted_state, dpredicted_state_cov)

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int dforecast_univariate_diffuse(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int i, j, k
        int inc = 1
        np.float64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # Initialize the filtered states
    blas.dcopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    blas.dcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                            kfilter._filtered_state_cov, &inc)
    blas.dcopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                            kfilter._predicted_diffuse_state_cov, &inc)

    # Iterate over endogenous variables
    for i in range(model._k_endog):
        # forecast_t, v_t, 
        dforecast_error(kfilter, model, i)

        # F_{*,t}
        forecast_error_cov = dforecast_error_cov(kfilter, model, i)
        if forecast_error_cov < 0:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # Save temporary array data
        if not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov
            dtemp_arrays(kfilter, model, i, forecast_error_cov_inv)

        # F_{\infty,t}
        forecast_error_diffuse_cov = dforecast_error_diffuse_cov(kfilter, model, i)
        if forecast_error_diffuse_cov < 0:
            kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = 0
            forecast_error_diffuse_cov = 0

        # F_{\infty, i, i, t} > 0
        if forecast_error_diffuse_cov > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv

            # K0 = M_inf[:, i:i+1] * F1
            blas.dcopy(&kfilter.k_states, &kfilter._M_inf[i * kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.dscal(&kfilter.k_states, &F1, kfilter._tmpK0, &inc)
            # K1 = M[:, i:i+1] * F1 + M_inf[:, i:i+1] * F2
            # K1 = M[:, i:i+1] * F1 + K0 * F12
            blas.dcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK1, &inc)
            blas.dscal(&kfilter.k_states, &F1, kfilter._tmpK1, &inc)
            blas.daxpy(&kfilter.k_states, &F12, kfilter._tmpK0, &inc, kfilter._tmpK1, &inc)
            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.dger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
            # L1 = -np.dot(K1, Zi)
            kfilter.tmpL1[:] = 0
            blas.dger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK1, &inc, &model._design[i], &model._k_endog, kfilter._tmpL1, &kfilter.k_states)

            # a_t = a_t + K0[:, 0] * v[i]
            blas.daxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)

            # P_t = np.dot(P_t_inf, L1.T) + np.dot(P_t, L0.T)
            # `tmp0` array used here, dimension $(m \times m)$  
            blas.dcopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                      kfilter._tmpL1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = np.dot(P_t_inf, L0.T)
            blas.dcopy(&kfilter.k_states2, kfilter._predicted_diffuse_state_cov, &inc, kfilter._tmp0, &inc)
            blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)

            # Loglikelihood
            kfilter._loglikelihood[0] = (
                kfilter._loglikelihood[0] - 0.5*(
                    dlog(2 * NPY_PI * forecast_error_diffuse_cov)))
        elif forecast_error_cov > kfilter.tolerance_diffuse:
            kfilter.nobs_kendog_diffuse_nonsingular = kfilter.nobs_kendog_diffuse_nonsingular + 1
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.dcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.dscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.dger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1

            # a_t = a_t + K0[:, 0] * v[i]
            blas.daxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)
            # P_t = np.dot(P_t, L0)
            blas.dcopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = P_t_inf
            # (noop)

            # Loglikelihood
            if not forecast_error_cov == 0:
                kfilter._loglikelihood[0] = (
                    kfilter._loglikelihood[0] - 0.5*(
                        dlog(2 * NPY_PI * forecast_error_cov)))

                if kfilter.filter_method & FILTER_CONCENTRATED:
                    kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
                else:
                    kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

        # Kalman gain
        blas.dcopy(&kfilter.k_states, kfilter._tmpK0, &inc, &kfilter._kalman_gain[i * kfilter.k_states], &inc)

        # Prediction (done below)
        # a_t1[:] = np.dot(T, a_tt)
        # P_t1[:] = np.dot(np.dot(T, P_tt), T.T) + RQR
        # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)

    return 0


cdef np.float64_t dforecast_error_diffuse_cov(dKalmanFilter kfilter, dStatespace model, int i):
    cdef:
        int inc = 1
        np.float64_t alpha = 1
        np.float64_t beta = 0
        np.float64_t forecast_error_diffuse_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation*
    # `M_inf` array used here, dimension $(m \times 1)$  
    # $M_{i, \infty} = P_{t,i,\infty} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    blas.dgemv("N", &model._k_states, &model._k_states,
          &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    # $F_{t,i,\infty} \equiv Z_{t,i} P_{t,i,\infty} Z_{t,i}'$
    # blas.dsymv("U", &model._k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    forecast_error_diffuse_cov = (
        blas.ddot(&k_states, &model._design[i], &model._k_endog,
                                      &kfilter._M_inf[i * kfilter.k_states], &inc)
    )
    kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = forecast_error_diffuse_cov
    return forecast_error_diffuse_cov


cdef int dprediction_univariate_diffuse(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    dpredicted_state(kfilter, model)
    dpredicted_state_cov(kfilter, model)
    dpredicted_diffuse_state_cov(kfilter, model)

    return 0


cdef void dpredicted_diffuse_state_cov(dKalmanFilter kfilter, dStatespace model):
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    # Need special handling for the completely missing case, since the
    # conventional Kalman filter routines are used in this case and they don't
    # copy over the predicted diffuse state cov
    if model._nmissing == model.k_endog:
        blas.dcopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                                kfilter._predicted_diffuse_state_cov, &inc)

    # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)


# Note: updating, inverse, loglikelihood are all performed in prior steps
cdef int dupdating_univariate_diffuse(dKalmanFilter kfilter, dStatespace model):
    return 0

cdef np.float64_t dinverse_noop_univariate_diffuse(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    return 0

cdef np.float64_t dloglikelihood_univariate_diffuse(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant):
    return 0

from statsmodels.tsa.statespace._filters._univariate cimport (
    cforecast_error, cforecast_error_cov,
    ctemp_arrays, cpredicted_state, cpredicted_state_cov)

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int cforecast_univariate_diffuse(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int i, j, k
        int inc = 1
        np.complex64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # Initialize the filtered states
    blas.ccopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    blas.ccopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                            kfilter._filtered_state_cov, &inc)
    blas.ccopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                            kfilter._predicted_diffuse_state_cov, &inc)

    # Iterate over endogenous variables
    for i in range(model._k_endog):
        # forecast_t, v_t, 
        cforecast_error(kfilter, model, i)

        # F_{*,t}
        forecast_error_cov = cforecast_error_cov(kfilter, model, i)
        if forecast_error_cov.real < 0:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # Save temporary array data
        if not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov
            ctemp_arrays(kfilter, model, i, forecast_error_cov_inv)

        # F_{\infty,t}
        forecast_error_diffuse_cov = cforecast_error_diffuse_cov(kfilter, model, i)
        if forecast_error_diffuse_cov.real < 0:
            kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = 0
            forecast_error_diffuse_cov = 0

        # F_{\infty, i, i, t} > 0
        if forecast_error_diffuse_cov.real > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv

            # K0 = M_inf[:, i:i+1] * F1
            blas.ccopy(&kfilter.k_states, &kfilter._M_inf[i * kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.cscal(&kfilter.k_states, &F1, kfilter._tmpK0, &inc)
            # K1 = M[:, i:i+1] * F1 + M_inf[:, i:i+1] * F2
            # K1 = M[:, i:i+1] * F1 + K0 * F12
            blas.ccopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK1, &inc)
            blas.cscal(&kfilter.k_states, &F1, kfilter._tmpK1, &inc)
            blas.caxpy(&kfilter.k_states, &F12, kfilter._tmpK0, &inc, kfilter._tmpK1, &inc)
            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.cgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
            # L1 = -np.dot(K1, Zi)
            kfilter.tmpL1[:] = 0
            blas.cgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK1, &inc, &model._design[i], &model._k_endog, kfilter._tmpL1, &kfilter.k_states)

            # a_t = a_t + K0[:, 0] * v[i]
            blas.caxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)

            # P_t = np.dot(P_t_inf, L1.T) + np.dot(P_t, L0.T)
            # `tmp0` array used here, dimension $(m \times m)$  
            blas.ccopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                      kfilter._tmpL1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = np.dot(P_t_inf, L0.T)
            blas.ccopy(&kfilter.k_states2, kfilter._predicted_diffuse_state_cov, &inc, kfilter._tmp0, &inc)
            blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)

            # Loglikelihood
            kfilter._loglikelihood[0] = (
                kfilter._loglikelihood[0] - 0.5*(
                    zlog(2 * NPY_PI * forecast_error_diffuse_cov)))
        elif forecast_error_cov.real > kfilter.tolerance_diffuse:
            kfilter.nobs_kendog_diffuse_nonsingular = kfilter.nobs_kendog_diffuse_nonsingular + 1
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.ccopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.cscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.cgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1

            # a_t = a_t + K0[:, 0] * v[i]
            blas.caxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)
            # P_t = np.dot(P_t, L0)
            blas.ccopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = P_t_inf
            # (noop)

            # Loglikelihood
            if not forecast_error_cov == 0:
                kfilter._loglikelihood[0] = (
                    kfilter._loglikelihood[0] - 0.5*(
                        zlog(2 * NPY_PI * forecast_error_cov)))

                if kfilter.filter_method & FILTER_CONCENTRATED:
                    kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
                else:
                    kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

        # Kalman gain
        blas.ccopy(&kfilter.k_states, kfilter._tmpK0, &inc, &kfilter._kalman_gain[i * kfilter.k_states], &inc)

        # Prediction (done below)
        # a_t1[:] = np.dot(T, a_tt)
        # P_t1[:] = np.dot(np.dot(T, P_tt), T.T) + RQR
        # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)

    return 0


cdef np.complex64_t cforecast_error_diffuse_cov(cKalmanFilter kfilter, cStatespace model, int i):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1
        np.complex64_t beta = 0
        np.complex64_t forecast_error_diffuse_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation*
    # `M_inf` array used here, dimension $(m \times 1)$  
    # $M_{i, \infty} = P_{t,i,\infty} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    blas.cgemv("N", &model._k_states, &model._k_states,
          &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    # $F_{t,i,\infty} \equiv Z_{t,i} P_{t,i,\infty} Z_{t,i}'$
    # blas.cgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    blas.cgemv("N", &inc, &k_states,
                   &alpha, &kfilter._M_inf[i * kfilter.k_states], &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    forecast_error_diffuse_cov = kfilter._tmp0[0]
    kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = forecast_error_diffuse_cov
    return forecast_error_diffuse_cov


cdef int cprediction_univariate_diffuse(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    cpredicted_state(kfilter, model)
    cpredicted_state_cov(kfilter, model)
    cpredicted_diffuse_state_cov(kfilter, model)

    return 0


cdef void cpredicted_diffuse_state_cov(cKalmanFilter kfilter, cStatespace model):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    # Need special handling for the completely missing case, since the
    # conventional Kalman filter routines are used in this case and they don't
    # copy over the predicted diffuse state cov
    if model._nmissing == model.k_endog:
        blas.ccopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                                kfilter._predicted_diffuse_state_cov, &inc)

    # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)


# Note: updating, inverse, loglikelihood are all performed in prior steps
cdef int cupdating_univariate_diffuse(cKalmanFilter kfilter, cStatespace model):
    return 0

cdef np.complex64_t cinverse_noop_univariate_diffuse(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    return 0

cdef np.complex64_t cloglikelihood_univariate_diffuse(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant):
    return 0

from statsmodels.tsa.statespace._filters._univariate cimport (
    zforecast_error, zforecast_error_cov,
    ztemp_arrays, zpredicted_state, zpredicted_state_cov)

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int zforecast_univariate_diffuse(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int i, j, k
        int inc = 1
        np.complex128_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # Initialize the filtered states
    blas.zcopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    blas.zcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                            kfilter._filtered_state_cov, &inc)
    blas.zcopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                            kfilter._predicted_diffuse_state_cov, &inc)

    # Iterate over endogenous variables
    for i in range(model._k_endog):
        # forecast_t, v_t, 
        zforecast_error(kfilter, model, i)

        # F_{*,t}
        forecast_error_cov = zforecast_error_cov(kfilter, model, i)
        if forecast_error_cov.real < 0:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # Save temporary array data
        if not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov
            ztemp_arrays(kfilter, model, i, forecast_error_cov_inv)

        # F_{\infty,t}
        forecast_error_diffuse_cov = zforecast_error_diffuse_cov(kfilter, model, i)
        if forecast_error_diffuse_cov.real < 0:
            kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = 0
            forecast_error_diffuse_cov = 0

        # F_{\infty, i, i, t} > 0
        if forecast_error_diffuse_cov.real > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv

            # K0 = M_inf[:, i:i+1] * F1
            blas.zcopy(&kfilter.k_states, &kfilter._M_inf[i * kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.zscal(&kfilter.k_states, &F1, kfilter._tmpK0, &inc)
            # K1 = M[:, i:i+1] * F1 + M_inf[:, i:i+1] * F2
            # K1 = M[:, i:i+1] * F1 + K0 * F12
            blas.zcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK1, &inc)
            blas.zscal(&kfilter.k_states, &F1, kfilter._tmpK1, &inc)
            blas.zaxpy(&kfilter.k_states, &F12, kfilter._tmpK0, &inc, kfilter._tmpK1, &inc)
            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.zgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
            # L1 = -np.dot(K1, Zi)
            kfilter.tmpL1[:] = 0
            blas.zgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK1, &inc, &model._design[i], &model._k_endog, kfilter._tmpL1, &kfilter.k_states)

            # a_t = a_t + K0[:, 0] * v[i]
            blas.zaxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)

            # P_t = np.dot(P_t_inf, L1.T) + np.dot(P_t, L0.T)
            # `tmp0` array used here, dimension $(m \times m)$  
            blas.zcopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                      kfilter._tmpL1, &kfilter.k_states,
              &alpha, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = np.dot(P_t_inf, L0.T)
            blas.zcopy(&kfilter.k_states2, kfilter._predicted_diffuse_state_cov, &inc, kfilter._tmp0, &inc)
            blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)

            # Loglikelihood
            kfilter._loglikelihood[0] = (
                kfilter._loglikelihood[0] - 0.5*(
                    zlog(2 * NPY_PI * forecast_error_diffuse_cov)))
        elif forecast_error_cov.real > kfilter.tolerance_diffuse:
            kfilter.nobs_kendog_diffuse_nonsingular = kfilter.nobs_kendog_diffuse_nonsingular + 1
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.zcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.zscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.zgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1

            # a_t = a_t + K0[:, 0] * v[i]
            blas.zaxpy(&kfilter.k_states, &kfilter._forecast_error[i], kfilter._tmpK0, &inc, kfilter._filtered_state, &inc)
            # P_t = np.dot(P_t, L0)
            blas.zcopy(&kfilter.k_states2, kfilter._filtered_state_cov, &inc, kfilter._tmp0, &inc)
            blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
              &alpha, kfilter._tmp0, &kfilter.k_states,
                      kfilter._tmpL0, &kfilter.k_states,
              &beta, kfilter._filtered_state_cov, &kfilter.k_states)
            # P_t_inf = P_t_inf
            # (noop)

            # Loglikelihood
            if not forecast_error_cov == 0:
                kfilter._loglikelihood[0] = (
                    kfilter._loglikelihood[0] - 0.5*(
                        zlog(2 * NPY_PI * forecast_error_cov)))

                if kfilter.filter_method & FILTER_CONCENTRATED:
                    kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
                else:
                    kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

        # Kalman gain
        blas.zcopy(&kfilter.k_states, kfilter._tmpK0, &inc, &kfilter._kalman_gain[i * kfilter.k_states], &inc)

        # Prediction (done below)
        # a_t1[:] = np.dot(T, a_tt)
        # P_t1[:] = np.dot(np.dot(T, P_tt), T.T) + RQR
        # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)

    return 0


cdef np.complex128_t zforecast_error_diffuse_cov(zKalmanFilter kfilter, zStatespace model, int i):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1
        np.complex128_t beta = 0
        np.complex128_t forecast_error_diffuse_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation*
    # `M_inf` array used here, dimension $(m \times 1)$  
    # $M_{i, \infty} = P_{t,i,\infty} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    blas.zgemv("N", &model._k_states, &model._k_states,
          &alpha, kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    # $F_{t,i,\infty} \equiv Z_{t,i} P_{t,i,\infty} Z_{t,i}'$
    # blas.zgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M_inf[i * kfilter.k_states], &inc)

    blas.zgemv("N", &inc, &k_states,
                   &alpha, &kfilter._M_inf[i * kfilter.k_states], &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    forecast_error_diffuse_cov = kfilter._tmp0[0]
    kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog] = forecast_error_diffuse_cov
    return forecast_error_diffuse_cov


cdef int zprediction_univariate_diffuse(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    zpredicted_state(kfilter, model)
    zpredicted_state_cov(kfilter, model)
    zpredicted_diffuse_state_cov(kfilter, model)

    return 0


cdef void zpredicted_diffuse_state_cov(zKalmanFilter kfilter, zStatespace model):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    # Need special handling for the completely missing case, since the
    # conventional Kalman filter routines are used in this case and they don't
    # copy over the predicted diffuse state cov
    if model._nmissing == model.k_endog:
        blas.zcopy(&kfilter.k_states2, kfilter._input_diffuse_state_cov, &inc,
                                                kfilter._predicted_diffuse_state_cov, &inc)

    # P_t1_inf[:] = np.dot(np.dot(T, P_t_inf), T.T)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._predicted_diffuse_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_diffuse_state_cov, &kfilter.k_states)


# Note: updating, inverse, loglikelihood are all performed in prior steps
cdef int zupdating_univariate_diffuse(zKalmanFilter kfilter, zStatespace model):
    return 0

cdef np.complex128_t zinverse_noop_univariate_diffuse(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    return 0

cdef np.complex128_t zloglikelihood_univariate_diffuse(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant):
    return 0
