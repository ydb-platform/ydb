#cython: profile=False
#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
#cython: cpow=True
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
    MEMORY_NO_LIKELIHOOD, MEMORY_NO_FORECAST_COV, MEMORY_NO_STD_FORECAST,
    MEMORY_NO_SMOOTHING, FILTER_CONCENTRATED, FILTER_CHANDRASEKHAR)

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int sforecast_univariate(sKalmanFilter kfilter, sStatespace model):

    # Constants
    cdef:
        int i, j, k
        int inc = 1
        int forecast_cov_t = kfilter.t
        int check
        np.float32_t forecast_error_cov
        np.float32_t forecast_error_cov_inv
        np.float32_t forecast_error_cov_inv_prev = 0.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # Initialize the filtered states
    sinitialize_filtered(kfilter, model)

    # Make sure the loglikelihood is set to zero if necessary

    # Iterate over the observations at time t
    for i in range(model._k_endog):

        # #### Forecast for time t
        # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
        # Note: $Z_{t,i}$ is a row vector starting at [i,0,t] and ending at
        # [i,k_states,t]
        # Note: zdot and cdot are broken, so have to use gemv for those

        # #### Forecast error for time t
        # `forecast_error` $\equiv v_t = y_t -$ `forecast`
        sforecast_error(kfilter, model, i)

        # #### Forecast error covariance matrix for time t
        # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
        # TODO what about Kalman convergence?
        # Note: zdot and cdot are broken, so have to use gemv for those
        if not kfilter.converged:
            forecast_error_cov = sforecast_error_cov(kfilter, model, i)
        else:
            forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]

        # Handle numerical issues that can cause a very small negative
        # forecast_error_cov
        check = scheck1(kfilter, forecast_error_cov)
        if check:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # If we have a non-zero variance
        # (if we have a zero-variance then we are done with this iteration)
        check = scheck2(kfilter, i, forecast_error_cov)
        if check:
            forecast_error_cov_inv = sforecast_error_cov_inv(kfilter, model, i, forecast_error_cov)

            sstandardized_forecast_error(kfilter, model, i, forecast_error_cov_inv)

            # Save temporary array data
            stemp_arrays(kfilter, model, i, forecast_error_cov_inv)

            # #### Filtered state for time t
            # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
            # Make a new temporary array  
            # K_{t,i} = P_{t,i} Z_{t,i}' F_{t,i}^{-1}
            sfiltered_state(kfilter, model, i, forecast_error_cov_inv)

            # Chandrasekhar arrays
            if not kfilter.converged and (kfilter.filter_method & FILTER_CHANDRASEKHAR > 0):
                if kfilter.t > 0:
                    forecast_error_cov_inv_prev = 1.0 / kfilter.forecast_error_cov[i, i, forecast_cov_t - 1]
                schandrasekhar_recursion(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv, forecast_error_cov_inv_prev)

            # #### Filtered state covariance for time t
            # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
            if not kfilter.converged:
                sfiltered_state_cov(kfilter, model, i, forecast_error_cov_inv)

            # #### Loglikelihood
            sloglikelihood(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv)
        else:
            # Otherwise, we need to record that this observation is not associated
            # with a loglikelihood step (so that it can be excluded in the denominator
            # when computing the scale)
            kfilter.nobs_kendog_univariate_singular = kfilter.nobs_kendog_univariate_singular + 1

    ssymmetry(kfilter, model)

    return 0

cdef void sinitialize_filtered(sKalmanFilter kfilter, sStatespace model):
    cdef int inc = 1
    blas.scopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    if not kfilter.converged:
        blas.scopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                                kfilter._filtered_state_cov, &inc)

cdef int scheck1(sKalmanFilter kfilter, np.float32_t forecast_error_cov):
    if not kfilter.converged:
        return forecast_error_cov < 0
    else:
        return False

cdef int scheck2(sKalmanFilter kfilter, int i, np.float32_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_ipiv[i] = forecast_error_cov > kfilter.tolerance_diffuse
    return kfilter.forecast_error_ipiv[i]

cdef np.float32_t sforecast_error_cov_inv(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_fac[i, i] = 1.0 / forecast_error_cov

    return kfilter.forecast_error_fac[i, i]

cdef void sstandardized_forecast_error(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov_inv):
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            kfilter._standardized_forecast_error[i] = (
                kfilter._forecast_error[i] * forecast_error_cov_inv**0.5)

cdef void ssymmetry(sKalmanFilter kfilter, sStatespace model):
    cdef int j, k
    # Make final filtered_state_cov symmetric (is not currently symmetric
    # due to use of ?syr or ?her)
    if not kfilter.converged:
        for j in range(model._k_states):      # columns
            for k in range(model._k_states):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter._filtered_state_cov[j + k*kfilter.k_states] = kfilter._filtered_state_cov[k + j*kfilter.k_states]

cdef void sforecast_error(sKalmanFilter kfilter, sStatespace model, int i):
    cdef:
        int inc = 1
        np.float32_t alpha = 1
        np.float32_t beta = 0
        int k_states = model._k_states
    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
    kfilter._forecast[i] = (
        model._obs_intercept[i] +
        blas.sdot(&k_states, &model._design[i], &model._k_endog,
                                      kfilter._filtered_state, &inc)
    )

    # `forecast_error` $\equiv v_t = y_t -$ `forecast`
    kfilter._forecast_error[i] = model._obs[i] - kfilter._forecast[i]

cdef np.float32_t sforecast_error_cov(sKalmanFilter kfilter, sStatespace model, int i):
    cdef:
        int inc = 1
        np.float32_t alpha = 1
        np.float32_t beta = 0
        np.float32_t forecast_error_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation* (used just below and then once more)  
    # $M_{t,i} = P_{t,i} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    # blas.sgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M[i*kfilter.k_states], &inc)

    # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
    blas.ssymv("L", &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M[i*kfilter.k_states], &inc)

    forecast_error_cov = (
        model._obs_cov[i + i*model._k_endog] +
        blas.sdot(&k_states, &model._design[i], &model._k_endog,
                                      &kfilter._M[i*kfilter.k_states], &inc)
    )
    kfilter._forecast_error_cov[i + i*kfilter.k_endog] = forecast_error_cov
    return forecast_error_cov

cdef void stemp_arrays(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov_inv):
    cdef:
        int k_states = model._k_states
        tmp_1 = 0

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # $\\#_1 = P_{t,i} Z_{t,i}'$ - set above
    # $\\#_2 = v_{t,i} / F_{t,i}$
    kfilter._tmp2[i] = kfilter._forecast_error[i] * forecast_error_cov_inv
    # $\\#_3 = Z_{t,i} / F_{t,i}$
    # $\\#_4 = H_{t,i} / F_{t,i}$
    if not kfilter.converged:
        blas.scopy(&k_states, &model._design[i], &model._k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        blas.sscal(&k_states, &forecast_error_cov_inv, &kfilter._tmp3[i], &kfilter.k_endog)

        kfilter._tmp4[i + i*kfilter.k_endog] = model._obs_cov[i + i*model._k_endog] * forecast_error_cov_inv
    elif kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0:
        # If we're converged and we're not storing these arrays, then we
        # already have the converged values and there's nothing more to do
        pass
    else:
        # If we're converged and we are storing these arrays, then we
        # just need to copy them from the previous iteration
        blas.scopy(&k_states, &kfilter.tmp3[i, 0, kfilter.t - 1], &kfilter.k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        kfilter._tmp4[i + i*kfilter.k_endog] = kfilter.tmp4[i, i, kfilter.t - 1]

cdef void sfiltered_state(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov_inv):
    cdef int j
    # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
    for j in range(model._k_states):
        if not kfilter.converged:
            kfilter._kalman_gain[j + i*kfilter.k_states] = kfilter._M[j + i*kfilter.k_states] * forecast_error_cov_inv
        kfilter._filtered_state[j] = (
            kfilter._filtered_state[j] +
            kfilter._forecast_error[i] * kfilter._kalman_gain[j + i*kfilter.k_states]
        )

cdef void sfiltered_state_cov(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov_inv):
    cdef:
        int inc = 1, j, k
        np.float32_t scalar = -1.0 * forecast_error_cov_inv
        np.float32_t alpha = 1.0
        np.float32_t gamma = -1.0
        int k_states = model._k_states
        int k_states1 = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef
        if model._k_posdef > model._k_states:
            k_states1 = model._k_posdef + 1

    # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
    # blas.sger(&model._k_states, &model._k_states,
    #     &gamma, &kfilter._M[i*kfilter.k_states], &inc,
    #             &kfilter._kalman_gain[i*kfilter.k_states], &inc,
    #     kfilter._filtered_state_cov, &kfilter.k_states
    # )

    blas.ssyr("L", &model._k_states,
        &scalar, &kfilter._M[i*kfilter.k_states], &inc,
                 kfilter._filtered_state_cov, &kfilter.k_states
    )

cdef void schandrasekhar_recursion(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov, np.float32_t forecast_error_cov_inv, np.float32_t forecast_error_cov_inv_prev):
    # Constants
    cdef:
        int inc = 1
        int j, k
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # Initialization
    if kfilter.t == 0:
        if i == 0:
            kfilter.CM[:] = 0

        # W[:, i:i+1] = T @ (P @ Z[i].T)
        # W[:, i:i+1] = T @ K @ F[i, i]
        # Note: we scale by forecast error cov here b/c kalman_gain was
        # computed above as K = P @ Z[i].T @ (1 / F[i, i])
        blas.sgemv("N", &model._k_states, &model._k_states,
          &forecast_error_cov, model._transition, &model._k_states,
                  &kfilter._kalman_gain[i * kfilter.k_states], &inc,
          &beta, &kfilter.CW[0, i], &inc)
        # M[i, i] = Finv[i, i]
        kfilter.CM[i, i] = -forecast_error_cov_inv
    # Updating
    else:
        # M.T @ W.T. (p x p) (p x m)
        blas.sgemm("T", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
        # MW @ Z[i].T (p x m) (m x 1) -> (p x 1)
        blas.sgemv("N", &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &inc)

        # M = M + MWZ @ MWZ.T / F_prev[i, i]
        # Note: syr / syrk only fills in lower triangle here
        blas.ssyr("L", &model._k_endog,
            &forecast_error_cov_inv_prev, &kfilter.CMWZ[0, 0], &inc,
                     &kfilter.CM[0, 0], &kfilter.k_endog)

        # Fill in the upper triangle
        for j in range(model._k_endog):      # columns
            for k in range(model._k_endog):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter.CM[j, k] = kfilter.CM[k, j]

        # Compute W
        # W -> tmpW
        blas.scopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        if i == model.k_endog - 1:
            # W = (T - T @ K @ Z[i]) @ W

            # Compute T @ K: (m x m) (m x 1) -> (m x 1)
            # Note: we previously copied CW -> CtmpW, so overwriting CW is okay
            blas.sgemv("N", &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      &kfilter._kalman_gain[i*kfilter.k_states], &inc,
              &beta, &kfilter.CW[0, 0], &inc)
            # T -> tmp00
            blas.scopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)
            # T - (T @ K) @ Z[i]: (m x 1) (1 x m) -> (m x m)
            blas.sger(&model._k_states, &model._k_states,
                &gamma, &kfilter.CW[0, 0], &inc,
                       &model._design[i], &model._k_endog,
                kfilter._tmp00, &kfilter.k_states)

            # (T - T @ K @ Z[i]) @ tmpW -> W
            blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp00, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &beta, &kfilter.CW[0, 0], &kfilter.k_states)
        else:
            # W = (I - I @ K @ Z[i]) @ W
            # K @ Z[i] (m x 1) (1 x m) -> (m x m)
            kfilter.tmp0[:] = 0
            blas.sger(&model._k_states, &model._k_states,
                &alpha, &kfilter._kalman_gain[i*kfilter.k_states], &inc,
                        &model._design[i], &model._k_endog,
                kfilter._tmp0, &kfilter.k_states)

            # W = - K @ Z[i] @ W + W
            blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &gamma, kfilter._tmp0, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &alpha, &kfilter.CW[0, 0], &kfilter.k_states)


cdef void sloglikelihood(sKalmanFilter kfilter, sStatespace model, int i, np.float32_t forecast_error_cov, np.float32_t forecast_error_cov_inv):
    kfilter._loglikelihood[0] = (
        kfilter._loglikelihood[0] - 0.5*(
            dlog(2 * M_PI * forecast_error_cov)
        )
    )
    if kfilter.filter_method & FILTER_CONCENTRATED:
        kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
    else:
        kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

cdef int supdating_univariate(sKalmanFilter kfilter, sStatespace model):
    # the updating step was performed in the forecast_univariate step
    return 0

cdef int sprediction_univariate(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t,n} + c_t$

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    #
    # TODO check behavior during convergence
    if not model.companion_transition:
        spredicted_state(kfilter, model)
        if not kfilter.converged:
            if kfilter.filter_method & FILTER_CHANDRASEKHAR > 0:
                spredicted_state_cov_chandrasekhar(kfilter, model)
            else:
                spredicted_state_cov(kfilter, model)
    else:
        scompanion_predicted_state(kfilter, model)
        if not kfilter.converged:
            scompanion_predicted_state_cov(kfilter, model)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$  
    # Kalman gain calculation done in forecasting step.

    return 0

cdef void spredicted_state(sKalmanFilter kfilter, sStatespace model):
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.scopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.sgemv("N", &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

cdef void spredicted_state_cov(sKalmanFilter kfilter, sStatespace model):
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    blas.scopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
    # `tmp0` array used here, dimension $(m \times m)$  

    # $\\#_0 = T_t P_{t|t} $

    # $(m \times m) = (m \times m) (m \times m)$
    # blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
    #       &alpha, model._transition, &model._k_states,
    #               kfilter._filtered_state_cov, &kfilter.k_states,
    #       &beta, kfilter._tmp0, &kfilter.k_states)
    blas.ssymm("R", "L", &model._k_states, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

cdef void spredicted_state_cov_chandrasekhar(sKalmanFilter kfilter, sStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
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

cdef void scompanion_predicted_state(sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i
        int inc = 1
        np.float32_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.scopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.sgemv("N", &model._k_posdef, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

    for i in range(model._k_posdef, model._k_states):
        kfilter._predicted_state[i] = kfilter._predicted_state[i] + kfilter._filtered_state[i - model._k_posdef]

cdef void scompanion_predicted_state_cov(sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i, j, idx
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t tmp

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$

    # `tmp0` array used here, dimension $(p \times m)$  
    # $\\#_0 = \phi_t P_{t|t} $

    # $(p \times m) = (p \times m) (m \times m)$
    # TODO: symm?
    blas.sgemm("N", "N", &model._k_posdef, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
                
    # $P_{t+1} = 1.0 \\#_0 \phi_t' + 1.0 \\#$  
    # $(m \times m) = (p \times m) (m \times p) + (m \times m)$
    blas.sgemm("N", "T", &model._k_posdef, &model._k_posdef, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_state_cov, &kfilter.k_states)

    # Fill in the basic matrix blocks
    for i in range(kfilter.k_states):      # columns
        for j in range(kfilter.k_states):  # rows
            idx = j + i*kfilter.k_states

            # Add the Q matrix to the upper-left block
            if i < model._k_posdef and j < model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._predicted_state_cov[idx] + 
                    model._state_cov[j + i*model._k_posdef]
                )

            # Set the upper-right block to be the first m-p columns of
            # \phi _t P_{t|t}, and the lower-left block to the its transpose
            elif i >= model._k_posdef and j < model._k_posdef:
                tmp = kfilter._tmp0[j + (i-model._k_posdef)*kfilter.k_states]
                kfilter._predicted_state_cov[idx] = tmp
                kfilter._predicted_state_cov[i + j*model._k_states] = tmp

            # Set the lower-right block 
            elif i >= model._k_posdef and j >= model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._filtered_state_cov[(j - model._k_posdef) + (i - model._k_posdef)*kfilter.k_states]
                )

cdef np.float32_t sinverse_noop_univariate(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant) except *:
    return -np.inf

cdef np.float32_t sloglikelihood_univariate(sKalmanFilter kfilter, sStatespace model, np.float32_t determinant):
    return 0

cdef np.float32_t sscale_univariate(sKalmanFilter kfilter, sStatespace model):
    return 0

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int dforecast_univariate(dKalmanFilter kfilter, dStatespace model):

    # Constants
    cdef:
        int i, j, k
        int inc = 1
        int forecast_cov_t = kfilter.t
        int check
        np.float64_t forecast_error_cov
        np.float64_t forecast_error_cov_inv
        np.float64_t forecast_error_cov_inv_prev = 0.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # Initialize the filtered states
    dinitialize_filtered(kfilter, model)

    # Make sure the loglikelihood is set to zero if necessary

    # Iterate over the observations at time t
    for i in range(model._k_endog):

        # #### Forecast for time t
        # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
        # Note: $Z_{t,i}$ is a row vector starting at [i,0,t] and ending at
        # [i,k_states,t]
        # Note: zdot and cdot are broken, so have to use gemv for those

        # #### Forecast error for time t
        # `forecast_error` $\equiv v_t = y_t -$ `forecast`
        dforecast_error(kfilter, model, i)

        # #### Forecast error covariance matrix for time t
        # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
        # TODO what about Kalman convergence?
        # Note: zdot and cdot are broken, so have to use gemv for those
        if not kfilter.converged:
            forecast_error_cov = dforecast_error_cov(kfilter, model, i)
        else:
            forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]

        # Handle numerical issues that can cause a very small negative
        # forecast_error_cov
        check = dcheck1(kfilter, forecast_error_cov)
        if check:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # If we have a non-zero variance
        # (if we have a zero-variance then we are done with this iteration)
        check = dcheck2(kfilter, i, forecast_error_cov)
        if check:
            forecast_error_cov_inv = dforecast_error_cov_inv(kfilter, model, i, forecast_error_cov)

            dstandardized_forecast_error(kfilter, model, i, forecast_error_cov_inv)

            # Save temporary array data
            dtemp_arrays(kfilter, model, i, forecast_error_cov_inv)

            # #### Filtered state for time t
            # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
            # Make a new temporary array  
            # K_{t,i} = P_{t,i} Z_{t,i}' F_{t,i}^{-1}
            dfiltered_state(kfilter, model, i, forecast_error_cov_inv)

            # Chandrasekhar arrays
            if not kfilter.converged and (kfilter.filter_method & FILTER_CHANDRASEKHAR > 0):
                if kfilter.t > 0:
                    forecast_error_cov_inv_prev = 1.0 / kfilter.forecast_error_cov[i, i, forecast_cov_t - 1]
                dchandrasekhar_recursion(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv, forecast_error_cov_inv_prev)

            # #### Filtered state covariance for time t
            # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
            if not kfilter.converged:
                dfiltered_state_cov(kfilter, model, i, forecast_error_cov_inv)

            # #### Loglikelihood
            dloglikelihood(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv)
        else:
            # Otherwise, we need to record that this observation is not associated
            # with a loglikelihood step (so that it can be excluded in the denominator
            # when computing the scale)
            kfilter.nobs_kendog_univariate_singular = kfilter.nobs_kendog_univariate_singular + 1

    dsymmetry(kfilter, model)

    return 0

cdef void dinitialize_filtered(dKalmanFilter kfilter, dStatespace model):
    cdef int inc = 1
    blas.dcopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    if not kfilter.converged:
        blas.dcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                                kfilter._filtered_state_cov, &inc)

cdef int dcheck1(dKalmanFilter kfilter, np.float64_t forecast_error_cov):
    if not kfilter.converged:
        return forecast_error_cov < 0
    else:
        return False

cdef int dcheck2(dKalmanFilter kfilter, int i, np.float64_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_ipiv[i] = forecast_error_cov > kfilter.tolerance_diffuse
    return kfilter.forecast_error_ipiv[i]

cdef np.float64_t dforecast_error_cov_inv(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_fac[i, i] = 1.0 / forecast_error_cov

    return kfilter.forecast_error_fac[i, i]

cdef void dstandardized_forecast_error(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov_inv):
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            kfilter._standardized_forecast_error[i] = (
                kfilter._forecast_error[i] * forecast_error_cov_inv**0.5)

cdef void dsymmetry(dKalmanFilter kfilter, dStatespace model):
    cdef int j, k
    # Make final filtered_state_cov symmetric (is not currently symmetric
    # due to use of ?syr or ?her)
    if not kfilter.converged:
        for j in range(model._k_states):      # columns
            for k in range(model._k_states):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter._filtered_state_cov[j + k*kfilter.k_states] = kfilter._filtered_state_cov[k + j*kfilter.k_states]

cdef void dforecast_error(dKalmanFilter kfilter, dStatespace model, int i):
    cdef:
        int inc = 1
        np.float64_t alpha = 1
        np.float64_t beta = 0
        int k_states = model._k_states
    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
    kfilter._forecast[i] = (
        model._obs_intercept[i] +
        blas.ddot(&k_states, &model._design[i], &model._k_endog,
                                      kfilter._filtered_state, &inc)
    )

    # `forecast_error` $\equiv v_t = y_t -$ `forecast`
    kfilter._forecast_error[i] = model._obs[i] - kfilter._forecast[i]

cdef np.float64_t dforecast_error_cov(dKalmanFilter kfilter, dStatespace model, int i):
    cdef:
        int inc = 1
        np.float64_t alpha = 1
        np.float64_t beta = 0
        np.float64_t forecast_error_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation* (used just below and then once more)  
    # $M_{t,i} = P_{t,i} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    # blas.dgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M[i*kfilter.k_states], &inc)

    # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
    blas.dsymv("L", &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M[i*kfilter.k_states], &inc)

    forecast_error_cov = (
        model._obs_cov[i + i*model._k_endog] +
        blas.ddot(&k_states, &model._design[i], &model._k_endog,
                                      &kfilter._M[i*kfilter.k_states], &inc)
    )
    kfilter._forecast_error_cov[i + i*kfilter.k_endog] = forecast_error_cov
    return forecast_error_cov

cdef void dtemp_arrays(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov_inv):
    cdef:
        int k_states = model._k_states
        tmp_1 = 0

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # $\\#_1 = P_{t,i} Z_{t,i}'$ - set above
    # $\\#_2 = v_{t,i} / F_{t,i}$
    kfilter._tmp2[i] = kfilter._forecast_error[i] * forecast_error_cov_inv
    # $\\#_3 = Z_{t,i} / F_{t,i}$
    # $\\#_4 = H_{t,i} / F_{t,i}$
    if not kfilter.converged:
        blas.dcopy(&k_states, &model._design[i], &model._k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        blas.dscal(&k_states, &forecast_error_cov_inv, &kfilter._tmp3[i], &kfilter.k_endog)

        kfilter._tmp4[i + i*kfilter.k_endog] = model._obs_cov[i + i*model._k_endog] * forecast_error_cov_inv
    elif kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0:
        # If we're converged and we're not storing these arrays, then we
        # already have the converged values and there's nothing more to do
        pass
    else:
        # If we're converged and we are storing these arrays, then we
        # just need to copy them from the previous iteration
        blas.dcopy(&k_states, &kfilter.tmp3[i, 0, kfilter.t - 1], &kfilter.k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        kfilter._tmp4[i + i*kfilter.k_endog] = kfilter.tmp4[i, i, kfilter.t - 1]

cdef void dfiltered_state(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov_inv):
    cdef int j
    # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
    for j in range(model._k_states):
        if not kfilter.converged:
            kfilter._kalman_gain[j + i*kfilter.k_states] = kfilter._M[j + i*kfilter.k_states] * forecast_error_cov_inv
        kfilter._filtered_state[j] = (
            kfilter._filtered_state[j] +
            kfilter._forecast_error[i] * kfilter._kalman_gain[j + i*kfilter.k_states]
        )

cdef void dfiltered_state_cov(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov_inv):
    cdef:
        int inc = 1, j, k
        np.float64_t scalar = -1.0 * forecast_error_cov_inv
        np.float64_t alpha = 1.0
        np.float64_t gamma = -1.0
        int k_states = model._k_states
        int k_states1 = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef
        if model._k_posdef > model._k_states:
            k_states1 = model._k_posdef + 1

    # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
    # blas.dger(&model._k_states, &model._k_states,
    #     &gamma, &kfilter._M[i*kfilter.k_states], &inc,
    #             &kfilter._kalman_gain[i*kfilter.k_states], &inc,
    #     kfilter._filtered_state_cov, &kfilter.k_states
    # )

    blas.dsyr("L", &model._k_states,
        &scalar, &kfilter._M[i*kfilter.k_states], &inc,
                 kfilter._filtered_state_cov, &kfilter.k_states
    )

cdef void dchandrasekhar_recursion(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov, np.float64_t forecast_error_cov_inv, np.float64_t forecast_error_cov_inv_prev):
    # Constants
    cdef:
        int inc = 1
        int j, k
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # Initialization
    if kfilter.t == 0:
        if i == 0:
            kfilter.CM[:] = 0

        # W[:, i:i+1] = T @ (P @ Z[i].T)
        # W[:, i:i+1] = T @ K @ F[i, i]
        # Note: we scale by forecast error cov here b/c kalman_gain was
        # computed above as K = P @ Z[i].T @ (1 / F[i, i])
        blas.dgemv("N", &model._k_states, &model._k_states,
          &forecast_error_cov, model._transition, &model._k_states,
                  &kfilter._kalman_gain[i * kfilter.k_states], &inc,
          &beta, &kfilter.CW[0, i], &inc)
        # M[i, i] = Finv[i, i]
        kfilter.CM[i, i] = -forecast_error_cov_inv
    # Updating
    else:
        # M.T @ W.T. (p x p) (p x m)
        blas.dgemm("T", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
        # MW @ Z[i].T (p x m) (m x 1) -> (p x 1)
        blas.dgemv("N", &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &inc)

        # M = M + MWZ @ MWZ.T / F_prev[i, i]
        # Note: syr / syrk only fills in lower triangle here
        blas.dsyr("L", &model._k_endog,
            &forecast_error_cov_inv_prev, &kfilter.CMWZ[0, 0], &inc,
                     &kfilter.CM[0, 0], &kfilter.k_endog)

        # Fill in the upper triangle
        for j in range(model._k_endog):      # columns
            for k in range(model._k_endog):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter.CM[j, k] = kfilter.CM[k, j]

        # Compute W
        # W -> tmpW
        blas.dcopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        if i == model.k_endog - 1:
            # W = (T - T @ K @ Z[i]) @ W

            # Compute T @ K: (m x m) (m x 1) -> (m x 1)
            # Note: we previously copied CW -> CtmpW, so overwriting CW is okay
            blas.dgemv("N", &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      &kfilter._kalman_gain[i*kfilter.k_states], &inc,
              &beta, &kfilter.CW[0, 0], &inc)
            # T -> tmp00
            blas.dcopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)
            # T - (T @ K) @ Z[i]: (m x 1) (1 x m) -> (m x m)
            blas.dger(&model._k_states, &model._k_states,
                &gamma, &kfilter.CW[0, 0], &inc,
                       &model._design[i], &model._k_endog,
                kfilter._tmp00, &kfilter.k_states)

            # (T - T @ K @ Z[i]) @ tmpW -> W
            blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp00, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &beta, &kfilter.CW[0, 0], &kfilter.k_states)
        else:
            # W = (I - I @ K @ Z[i]) @ W
            # K @ Z[i] (m x 1) (1 x m) -> (m x m)
            kfilter.tmp0[:] = 0
            blas.dger(&model._k_states, &model._k_states,
                &alpha, &kfilter._kalman_gain[i*kfilter.k_states], &inc,
                        &model._design[i], &model._k_endog,
                kfilter._tmp0, &kfilter.k_states)

            # W = - K @ Z[i] @ W + W
            blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &gamma, kfilter._tmp0, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &alpha, &kfilter.CW[0, 0], &kfilter.k_states)


cdef void dloglikelihood(dKalmanFilter kfilter, dStatespace model, int i, np.float64_t forecast_error_cov, np.float64_t forecast_error_cov_inv):
    kfilter._loglikelihood[0] = (
        kfilter._loglikelihood[0] - 0.5*(
            dlog(2 * M_PI * forecast_error_cov)
        )
    )
    if kfilter.filter_method & FILTER_CONCENTRATED:
        kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
    else:
        kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

cdef int dupdating_univariate(dKalmanFilter kfilter, dStatespace model):
    # the updating step was performed in the forecast_univariate step
    return 0

cdef int dprediction_univariate(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t,n} + c_t$

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    #
    # TODO check behavior during convergence
    if not model.companion_transition:
        dpredicted_state(kfilter, model)
        if not kfilter.converged:
            if kfilter.filter_method & FILTER_CHANDRASEKHAR > 0:
                dpredicted_state_cov_chandrasekhar(kfilter, model)
            else:
                dpredicted_state_cov(kfilter, model)
    else:
        dcompanion_predicted_state(kfilter, model)
        if not kfilter.converged:
            dcompanion_predicted_state_cov(kfilter, model)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$  
    # Kalman gain calculation done in forecasting step.

    return 0

cdef void dpredicted_state(dKalmanFilter kfilter, dStatespace model):
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.dcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.dgemv("N", &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

cdef void dpredicted_state_cov(dKalmanFilter kfilter, dStatespace model):
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    blas.dcopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
    # `tmp0` array used here, dimension $(m \times m)$  

    # $\\#_0 = T_t P_{t|t} $

    # $(m \times m) = (m \times m) (m \times m)$
    # blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
    #       &alpha, model._transition, &model._k_states,
    #               kfilter._filtered_state_cov, &kfilter.k_states,
    #       &beta, kfilter._tmp0, &kfilter.k_states)
    blas.dsymm("R", "L", &model._k_states, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

cdef void dpredicted_state_cov_chandrasekhar(dKalmanFilter kfilter, dStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
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

cdef void dcompanion_predicted_state(dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i
        int inc = 1
        np.float64_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.dcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.dgemv("N", &model._k_posdef, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

    for i in range(model._k_posdef, model._k_states):
        kfilter._predicted_state[i] = kfilter._predicted_state[i] + kfilter._filtered_state[i - model._k_posdef]

cdef void dcompanion_predicted_state_cov(dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i, j, idx
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t tmp

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$

    # `tmp0` array used here, dimension $(p \times m)$  
    # $\\#_0 = \phi_t P_{t|t} $

    # $(p \times m) = (p \times m) (m \times m)$
    # TODO: symm?
    blas.dgemm("N", "N", &model._k_posdef, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
                
    # $P_{t+1} = 1.0 \\#_0 \phi_t' + 1.0 \\#$  
    # $(m \times m) = (p \times m) (m \times p) + (m \times m)$
    blas.dgemm("N", "T", &model._k_posdef, &model._k_posdef, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_state_cov, &kfilter.k_states)

    # Fill in the basic matrix blocks
    for i in range(kfilter.k_states):      # columns
        for j in range(kfilter.k_states):  # rows
            idx = j + i*kfilter.k_states

            # Add the Q matrix to the upper-left block
            if i < model._k_posdef and j < model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._predicted_state_cov[idx] + 
                    model._state_cov[j + i*model._k_posdef]
                )

            # Set the upper-right block to be the first m-p columns of
            # \phi _t P_{t|t}, and the lower-left block to the its transpose
            elif i >= model._k_posdef and j < model._k_posdef:
                tmp = kfilter._tmp0[j + (i-model._k_posdef)*kfilter.k_states]
                kfilter._predicted_state_cov[idx] = tmp
                kfilter._predicted_state_cov[i + j*model._k_states] = tmp

            # Set the lower-right block 
            elif i >= model._k_posdef and j >= model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._filtered_state_cov[(j - model._k_posdef) + (i - model._k_posdef)*kfilter.k_states]
                )

cdef np.float64_t dinverse_noop_univariate(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant) except *:
    return -np.inf

cdef np.float64_t dloglikelihood_univariate(dKalmanFilter kfilter, dStatespace model, np.float64_t determinant):
    return 0

cdef np.float64_t dscale_univariate(dKalmanFilter kfilter, dStatespace model):
    return 0

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int cforecast_univariate(cKalmanFilter kfilter, cStatespace model):

    # Constants
    cdef:
        int i, j, k
        int inc = 1
        int forecast_cov_t = kfilter.t
        int check
        np.complex64_t forecast_error_cov
        np.complex64_t forecast_error_cov_inv
        np.complex64_t forecast_error_cov_inv_prev = 0.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # Initialize the filtered states
    cinitialize_filtered(kfilter, model)

    # Make sure the loglikelihood is set to zero if necessary

    # Iterate over the observations at time t
    for i in range(model._k_endog):

        # #### Forecast for time t
        # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
        # Note: $Z_{t,i}$ is a row vector starting at [i,0,t] and ending at
        # [i,k_states,t]
        # Note: zdot and cdot are broken, so have to use gemv for those

        # #### Forecast error for time t
        # `forecast_error` $\equiv v_t = y_t -$ `forecast`
        cforecast_error(kfilter, model, i)

        # #### Forecast error covariance matrix for time t
        # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
        # TODO what about Kalman convergence?
        # Note: zdot and cdot are broken, so have to use gemv for those
        if not kfilter.converged:
            forecast_error_cov = cforecast_error_cov(kfilter, model, i)
        else:
            forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]

        # Handle numerical issues that can cause a very small negative
        # forecast_error_cov
        check = ccheck1(kfilter, forecast_error_cov)
        if check:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # If we have a non-zero variance
        # (if we have a zero-variance then we are done with this iteration)
        check = ccheck2(kfilter, i, forecast_error_cov)
        if check:
            forecast_error_cov_inv = cforecast_error_cov_inv(kfilter, model, i, forecast_error_cov)

            cstandardized_forecast_error(kfilter, model, i, forecast_error_cov_inv)

            # Save temporary array data
            ctemp_arrays(kfilter, model, i, forecast_error_cov_inv)

            # #### Filtered state for time t
            # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
            # Make a new temporary array  
            # K_{t,i} = P_{t,i} Z_{t,i}' F_{t,i}^{-1}
            cfiltered_state(kfilter, model, i, forecast_error_cov_inv)

            # Chandrasekhar arrays
            if not kfilter.converged and (kfilter.filter_method & FILTER_CHANDRASEKHAR > 0):
                if kfilter.t > 0:
                    forecast_error_cov_inv_prev = 1.0 / kfilter.forecast_error_cov[i, i, forecast_cov_t - 1]
                cchandrasekhar_recursion(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv, forecast_error_cov_inv_prev)

            # #### Filtered state covariance for time t
            # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
            if not kfilter.converged:
                cfiltered_state_cov(kfilter, model, i, forecast_error_cov_inv)

            # #### Loglikelihood
            cloglikelihood(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv)
        else:
            # Otherwise, we need to record that this observation is not associated
            # with a loglikelihood step (so that it can be excluded in the denominator
            # when computing the scale)
            kfilter.nobs_kendog_univariate_singular = kfilter.nobs_kendog_univariate_singular + 1

    csymmetry(kfilter, model)

    return 0

cdef void cinitialize_filtered(cKalmanFilter kfilter, cStatespace model):
    cdef int inc = 1
    blas.ccopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    if not kfilter.converged:
        blas.ccopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                                kfilter._filtered_state_cov, &inc)

cdef int ccheck1(cKalmanFilter kfilter, np.complex64_t forecast_error_cov):
    if not kfilter.converged:
        return forecast_error_cov.real < 0
    else:
        return False

cdef int ccheck2(cKalmanFilter kfilter, int i, np.complex64_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_ipiv[i] = forecast_error_cov.real > kfilter.tolerance_diffuse
    return kfilter.forecast_error_ipiv[i]

cdef np.complex64_t cforecast_error_cov_inv(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_fac[i, i] = 1.0 / forecast_error_cov

    return kfilter.forecast_error_fac[i, i]

cdef void cstandardized_forecast_error(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov_inv):
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            kfilter._standardized_forecast_error[i] = (
                kfilter._forecast_error[i] * forecast_error_cov_inv**0.5)

cdef void csymmetry(cKalmanFilter kfilter, cStatespace model):
    cdef int j, k
    # Make final filtered_state_cov symmetric (is not currently symmetric
    # due to use of ?syr or ?her)
    if not kfilter.converged:
        for j in range(model._k_states):      # columns
            for k in range(model._k_states):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter._filtered_state_cov[j + k*kfilter.k_states] = kfilter._filtered_state_cov[k + j*kfilter.k_states]

cdef void cforecast_error(cKalmanFilter kfilter, cStatespace model, int i):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1
        np.complex64_t beta = 0
        int k_states = model._k_states
    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
    blas.cgemv("N", &inc, &k_states,
                   &alpha, kfilter._filtered_state, &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    kfilter._forecast[i] = model._obs_intercept[i] + kfilter._tmp0[0]

    # `forecast_error` $\equiv v_t = y_t -$ `forecast`
    kfilter._forecast_error[i] = model._obs[i] - kfilter._forecast[i]

cdef np.complex64_t cforecast_error_cov(cKalmanFilter kfilter, cStatespace model, int i):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1
        np.complex64_t beta = 0
        np.complex64_t forecast_error_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation* (used just below and then once more)  
    # $M_{t,i} = P_{t,i} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    # blas.cgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M[i*kfilter.k_states], &inc)

    # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
    blas.csymm("R", "L", &inc, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M[i*kfilter.k_states], &inc)

    blas.cgemv("N", &inc, &k_states,
                   &alpha, &kfilter._M[i*kfilter.k_states], &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    forecast_error_cov = model._obs_cov[i + i*model._k_endog] + kfilter._tmp0[0]
    kfilter._forecast_error_cov[i + i*kfilter.k_endog] = forecast_error_cov
    return forecast_error_cov

cdef void ctemp_arrays(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov_inv):
    cdef:
        int k_states = model._k_states
        tmp_1 = 0

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # $\\#_1 = P_{t,i} Z_{t,i}'$ - set above
    # $\\#_2 = v_{t,i} / F_{t,i}$
    kfilter._tmp2[i] = kfilter._forecast_error[i] * forecast_error_cov_inv
    # $\\#_3 = Z_{t,i} / F_{t,i}$
    # $\\#_4 = H_{t,i} / F_{t,i}$
    if not kfilter.converged:
        blas.ccopy(&k_states, &model._design[i], &model._k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        blas.cscal(&k_states, &forecast_error_cov_inv, &kfilter._tmp3[i], &kfilter.k_endog)

        kfilter._tmp4[i + i*kfilter.k_endog] = model._obs_cov[i + i*model._k_endog] * forecast_error_cov_inv
    elif kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0:
        # If we're converged and we're not storing these arrays, then we
        # already have the converged values and there's nothing more to do
        pass
    else:
        # If we're converged and we are storing these arrays, then we
        # just need to copy them from the previous iteration
        blas.ccopy(&k_states, &kfilter.tmp3[i, 0, kfilter.t - 1], &kfilter.k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        kfilter._tmp4[i + i*kfilter.k_endog] = kfilter.tmp4[i, i, kfilter.t - 1]

cdef void cfiltered_state(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov_inv):
    cdef int j
    # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
    for j in range(model._k_states):
        if not kfilter.converged:
            kfilter._kalman_gain[j + i*kfilter.k_states] = kfilter._M[j + i*kfilter.k_states] * forecast_error_cov_inv
        kfilter._filtered_state[j] = (
            kfilter._filtered_state[j] +
            kfilter._forecast_error[i] * kfilter._kalman_gain[j + i*kfilter.k_states]
        )

cdef void cfiltered_state_cov(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov_inv):
    cdef:
        int inc = 1, j, k
        np.complex64_t scalar = -1.0 * forecast_error_cov_inv
        np.complex64_t alpha = 1.0
        np.complex64_t gamma = -1.0
        int k_states = model._k_states
        int k_states1 = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef
        if model._k_posdef > model._k_states:
            k_states1 = model._k_posdef + 1

    # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
    # blas.cgeru(&model._k_states, &model._k_states,
    #     &gamma, &kfilter._M[i*kfilter.k_states], &inc,
    #             &kfilter._kalman_gain[i*kfilter.k_states], &inc,
    #     kfilter._filtered_state_cov, &kfilter.k_states
    # )

    blas.csyrk("L", "N", &model._k_states, &inc,
        &scalar, &kfilter._M[i*kfilter.k_states], &kfilter.k_states,
        &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

cdef void cchandrasekhar_recursion(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov, np.complex64_t forecast_error_cov_inv, np.complex64_t forecast_error_cov_inv_prev):
    # Constants
    cdef:
        int inc = 1
        int j, k
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # Initialization
    if kfilter.t == 0:
        if i == 0:
            kfilter.CM[:] = 0

        # W[:, i:i+1] = T @ (P @ Z[i].T)
        # W[:, i:i+1] = T @ K @ F[i, i]
        # Note: we scale by forecast error cov here b/c kalman_gain was
        # computed above as K = P @ Z[i].T @ (1 / F[i, i])
        blas.cgemv("N", &model._k_states, &model._k_states,
          &forecast_error_cov, model._transition, &model._k_states,
                  &kfilter._kalman_gain[i * kfilter.k_states], &inc,
          &beta, &kfilter.CW[0, i], &inc)
        # M[i, i] = Finv[i, i]
        kfilter.CM[i, i] = -forecast_error_cov_inv
    # Updating
    else:
        # M.T @ W.T. (p x p) (p x m)
        blas.cgemm("T", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
        # MW @ Z[i].T (p x m) (m x 1) -> (p x 1)
        blas.cgemv("N", &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &inc)

        # M = M + MWZ @ MWZ.T / F_prev[i, i]
        # Note: syr / syrk only fills in lower triangle here
        blas.csyrk("L", "N", &model._k_endog, &inc,
            &forecast_error_cov_inv_prev, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
            &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Fill in the upper triangle
        for j in range(model._k_endog):      # columns
            for k in range(model._k_endog):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter.CM[j, k] = kfilter.CM[k, j]

        # Compute W
        # W -> tmpW
        blas.ccopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        if i == model.k_endog - 1:
            # W = (T - T @ K @ Z[i]) @ W

            # Compute T @ K: (m x m) (m x 1) -> (m x 1)
            # Note: we previously copied CW -> CtmpW, so overwriting CW is okay
            blas.cgemv("N", &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      &kfilter._kalman_gain[i*kfilter.k_states], &inc,
              &beta, &kfilter.CW[0, 0], &inc)
            # T -> tmp00
            blas.ccopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)
            # T - (T @ K) @ Z[i]: (m x 1) (1 x m) -> (m x m)
            blas.cgeru(&model._k_states, &model._k_states,
                &gamma, &kfilter.CW[0, 0], &inc,
                       &model._design[i], &model._k_endog,
                kfilter._tmp00, &kfilter.k_states)

            # (T - T @ K @ Z[i]) @ tmpW -> W
            blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp00, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &beta, &kfilter.CW[0, 0], &kfilter.k_states)
        else:
            # W = (I - I @ K @ Z[i]) @ W
            # K @ Z[i] (m x 1) (1 x m) -> (m x m)
            kfilter.tmp0[:] = 0
            blas.cgeru(&model._k_states, &model._k_states,
                &alpha, &kfilter._kalman_gain[i*kfilter.k_states], &inc,
                        &model._design[i], &model._k_endog,
                kfilter._tmp0, &kfilter.k_states)

            # W = - K @ Z[i] @ W + W
            blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &gamma, kfilter._tmp0, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &alpha, &kfilter.CW[0, 0], &kfilter.k_states)


cdef void cloglikelihood(cKalmanFilter kfilter, cStatespace model, int i, np.complex64_t forecast_error_cov, np.complex64_t forecast_error_cov_inv):
    kfilter._loglikelihood[0] = (
        kfilter._loglikelihood[0] - 0.5*(
            zlog(2 * M_PI * forecast_error_cov)
        )
    )
    if kfilter.filter_method & FILTER_CONCENTRATED:
        kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
    else:
        kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

cdef int cupdating_univariate(cKalmanFilter kfilter, cStatespace model):
    # the updating step was performed in the forecast_univariate step
    return 0

cdef int cprediction_univariate(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t,n} + c_t$

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    #
    # TODO check behavior during convergence
    if not model.companion_transition:
        cpredicted_state(kfilter, model)
        if not kfilter.converged:
            if kfilter.filter_method & FILTER_CHANDRASEKHAR > 0:
                cpredicted_state_cov_chandrasekhar(kfilter, model)
            else:
                cpredicted_state_cov(kfilter, model)
    else:
        ccompanion_predicted_state(kfilter, model)
        if not kfilter.converged:
            ccompanion_predicted_state_cov(kfilter, model)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$  
    # Kalman gain calculation done in forecasting step.

    return 0

cdef void cpredicted_state(cKalmanFilter kfilter, cStatespace model):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.ccopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.cgemv("N", &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

cdef void cpredicted_state_cov(cKalmanFilter kfilter, cStatespace model):
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    blas.ccopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
    # `tmp0` array used here, dimension $(m \times m)$  

    # $\\#_0 = T_t P_{t|t} $

    # $(m \times m) = (m \times m) (m \times m)$
    # blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
    #       &alpha, model._transition, &model._k_states,
    #               kfilter._filtered_state_cov, &kfilter.k_states,
    #       &beta, kfilter._tmp0, &kfilter.k_states)
    blas.csymm("R", "L", &model._k_states, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

cdef void cpredicted_state_cov_chandrasekhar(cKalmanFilter kfilter, cStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
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

cdef void ccompanion_predicted_state(cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex64_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.ccopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.cgemv("N", &model._k_posdef, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

    for i in range(model._k_posdef, model._k_states):
        kfilter._predicted_state[i] = kfilter._predicted_state[i] + kfilter._filtered_state[i - model._k_posdef]

cdef void ccompanion_predicted_state_cov(cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i, j, idx
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t tmp

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$

    # `tmp0` array used here, dimension $(p \times m)$  
    # $\\#_0 = \phi_t P_{t|t} $

    # $(p \times m) = (p \times m) (m \times m)$
    # TODO: symm?
    blas.cgemm("N", "N", &model._k_posdef, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
                
    # $P_{t+1} = 1.0 \\#_0 \phi_t' + 1.0 \\#$  
    # $(m \times m) = (p \times m) (m \times p) + (m \times m)$
    blas.cgemm("N", "T", &model._k_posdef, &model._k_posdef, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_state_cov, &kfilter.k_states)

    # Fill in the basic matrix blocks
    for i in range(kfilter.k_states):      # columns
        for j in range(kfilter.k_states):  # rows
            idx = j + i*kfilter.k_states

            # Add the Q matrix to the upper-left block
            if i < model._k_posdef and j < model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._predicted_state_cov[idx] + 
                    model._state_cov[j + i*model._k_posdef]
                )

            # Set the upper-right block to be the first m-p columns of
            # \phi _t P_{t|t}, and the lower-left block to the its transpose
            elif i >= model._k_posdef and j < model._k_posdef:
                tmp = kfilter._tmp0[j + (i-model._k_posdef)*kfilter.k_states]
                kfilter._predicted_state_cov[idx] = tmp
                kfilter._predicted_state_cov[i + j*model._k_states] = tmp

            # Set the lower-right block 
            elif i >= model._k_posdef and j >= model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._filtered_state_cov[(j - model._k_posdef) + (i - model._k_posdef)*kfilter.k_states]
                )

cdef np.complex64_t cinverse_noop_univariate(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant) except *:
    return -np.inf

cdef np.complex64_t cloglikelihood_univariate(cKalmanFilter kfilter, cStatespace model, np.complex64_t determinant):
    return 0

cdef np.complex64_t cscale_univariate(cKalmanFilter kfilter, cStatespace model):
    return 0

# ### Univariate Kalman filter
#
# The following are the routines as defined in the univariate Kalman filter.
#
# See Durbin and Koopman (2012) Chapter 6.4

cdef int zforecast_univariate(zKalmanFilter kfilter, zStatespace model):

    # Constants
    cdef:
        int i, j, k
        int inc = 1
        int forecast_cov_t = kfilter.t
        int check
        np.complex128_t forecast_error_cov
        np.complex128_t forecast_error_cov_inv
        np.complex128_t forecast_error_cov_inv_prev = 0.0

    if kfilter.conserve_memory & MEMORY_NO_FORECAST_COV > 0:
        forecast_cov_t = 1

    # Initialize the filtered states
    zinitialize_filtered(kfilter, model)

    # Make sure the loglikelihood is set to zero if necessary

    # Iterate over the observations at time t
    for i in range(model._k_endog):

        # #### Forecast for time t
        # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
        # Note: $Z_{t,i}$ is a row vector starting at [i,0,t] and ending at
        # [i,k_states,t]
        # Note: zdot and cdot are broken, so have to use gemv for those

        # #### Forecast error for time t
        # `forecast_error` $\equiv v_t = y_t -$ `forecast`
        zforecast_error(kfilter, model, i)

        # #### Forecast error covariance matrix for time t
        # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
        # TODO what about Kalman convergence?
        # Note: zdot and cdot are broken, so have to use gemv for those
        if not kfilter.converged:
            forecast_error_cov = zforecast_error_cov(kfilter, model, i)
        else:
            forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]

        # Handle numerical issues that can cause a very small negative
        # forecast_error_cov
        check = zcheck1(kfilter, forecast_error_cov)
        if check:
            kfilter._forecast_error_cov[i + i*kfilter.k_endog] = 0
            forecast_error_cov = 0

        # If we have a non-zero variance
        # (if we have a zero-variance then we are done with this iteration)
        check = zcheck2(kfilter, i, forecast_error_cov)
        if check:
            forecast_error_cov_inv = zforecast_error_cov_inv(kfilter, model, i, forecast_error_cov)

            zstandardized_forecast_error(kfilter, model, i, forecast_error_cov_inv)

            # Save temporary array data
            ztemp_arrays(kfilter, model, i, forecast_error_cov_inv)

            # #### Filtered state for time t
            # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
            # Make a new temporary array  
            # K_{t,i} = P_{t,i} Z_{t,i}' F_{t,i}^{-1}
            zfiltered_state(kfilter, model, i, forecast_error_cov_inv)

            # Chandrasekhar arrays
            if not kfilter.converged and (kfilter.filter_method & FILTER_CHANDRASEKHAR > 0):
                if kfilter.t > 0:
                    forecast_error_cov_inv_prev = 1.0 / kfilter.forecast_error_cov[i, i, forecast_cov_t - 1]
                zchandrasekhar_recursion(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv, forecast_error_cov_inv_prev)

            # #### Filtered state covariance for time t
            # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
            if not kfilter.converged:
                zfiltered_state_cov(kfilter, model, i, forecast_error_cov_inv)

            # #### Loglikelihood
            zloglikelihood(kfilter, model, i, forecast_error_cov, forecast_error_cov_inv)
        else:
            # Otherwise, we need to record that this observation is not associated
            # with a loglikelihood step (so that it can be excluded in the denominator
            # when computing the scale)
            kfilter.nobs_kendog_univariate_singular = kfilter.nobs_kendog_univariate_singular + 1

    zsymmetry(kfilter, model)

    return 0

cdef void zinitialize_filtered(zKalmanFilter kfilter, zStatespace model):
    cdef int inc = 1
    blas.zcopy(&kfilter.k_states, kfilter._input_state, &inc,
                                           kfilter._filtered_state, &inc)
    if not kfilter.converged:
        blas.zcopy(&kfilter.k_states2, kfilter._input_state_cov, &inc,
                                                kfilter._filtered_state_cov, &inc)

cdef int zcheck1(zKalmanFilter kfilter, np.complex128_t forecast_error_cov):
    if not kfilter.converged:
        return forecast_error_cov.real < 0
    else:
        return False

cdef int zcheck2(zKalmanFilter kfilter, int i, np.complex128_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_ipiv[i] = forecast_error_cov.real > kfilter.tolerance_diffuse
    return kfilter.forecast_error_ipiv[i]

cdef np.complex128_t zforecast_error_cov_inv(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov):
    if not kfilter.converged:
        kfilter.forecast_error_fac[i, i] = 1.0 / forecast_error_cov

    return kfilter.forecast_error_fac[i, i]

cdef void zstandardized_forecast_error(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov_inv):
    if not (kfilter.conserve_memory & MEMORY_NO_STD_FORECAST > 0):
            kfilter._standardized_forecast_error[i] = (
                kfilter._forecast_error[i] * forecast_error_cov_inv**0.5)

cdef void zsymmetry(zKalmanFilter kfilter, zStatespace model):
    cdef int j, k
    # Make final filtered_state_cov symmetric (is not currently symmetric
    # due to use of ?syr or ?her)
    if not kfilter.converged:
        for j in range(model._k_states):      # columns
            for k in range(model._k_states):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter._filtered_state_cov[j + k*kfilter.k_states] = kfilter._filtered_state_cov[k + j*kfilter.k_states]

cdef void zforecast_error(zKalmanFilter kfilter, zStatespace model, int i):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1
        np.complex128_t beta = 0
        int k_states = model._k_states
    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # `forecast` $= Z_{t,i} a_{t,i} + d_{t,i}$
    blas.zgemv("N", &inc, &k_states,
                   &alpha, kfilter._filtered_state, &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    kfilter._forecast[i] = model._obs_intercept[i] + kfilter._tmp0[0]

    # `forecast_error` $\equiv v_t = y_t -$ `forecast`
    kfilter._forecast_error[i] = model._obs[i] - kfilter._forecast[i]

cdef np.complex128_t zforecast_error_cov(zKalmanFilter kfilter, zStatespace model, int i):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1
        np.complex128_t beta = 0
        np.complex128_t forecast_error_cov
        int k_states = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # *Intermediate calculation* (used just below and then once more)  
    # $M_{t,i} = P_{t,i} Z_{t,i}'$  
    # $(m \times 1) = (m \times m) (1 \times m)'$
    # blas.zgemv("N", &model._k_states, &k_states,
    #       &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
    #               &model._design[i], &model._k_endog,
    #       &beta, &kfilter._M[i*kfilter.k_states], &inc)

    # $F_{t,i} \equiv Z_{t,i} P_{t,i} Z_{t,i}' + H_{t,i}$
    blas.zsymm("R", "L", &inc, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter._M[i*kfilter.k_states], &inc)

    blas.zgemv("N", &inc, &k_states,
                   &alpha, &kfilter._M[i*kfilter.k_states], &inc,
                           &model._design[i], &model._k_endog,
                   &beta, kfilter._tmp0, &inc)
    forecast_error_cov = model._obs_cov[i + i*model._k_endog] + kfilter._tmp0[0]
    kfilter._forecast_error_cov[i + i*kfilter.k_endog] = forecast_error_cov
    return forecast_error_cov

cdef void ztemp_arrays(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov_inv):
    cdef:
        int k_states = model._k_states
        tmp_1 = 0

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # $\\#_1 = P_{t,i} Z_{t,i}'$ - set above
    # $\\#_2 = v_{t,i} / F_{t,i}$
    kfilter._tmp2[i] = kfilter._forecast_error[i] * forecast_error_cov_inv
    # $\\#_3 = Z_{t,i} / F_{t,i}$
    # $\\#_4 = H_{t,i} / F_{t,i}$
    if not kfilter.converged:
        blas.zcopy(&k_states, &model._design[i], &model._k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        blas.zscal(&k_states, &forecast_error_cov_inv, &kfilter._tmp3[i], &kfilter.k_endog)

        kfilter._tmp4[i + i*kfilter.k_endog] = model._obs_cov[i + i*model._k_endog] * forecast_error_cov_inv
    elif kfilter.conserve_memory & MEMORY_NO_SMOOTHING > 0:
        # If we're converged and we're not storing these arrays, then we
        # already have the converged values and there's nothing more to do
        pass
    else:
        # If we're converged and we are storing these arrays, then we
        # just need to copy them from the previous iteration
        blas.zcopy(&k_states, &kfilter.tmp3[i, 0, kfilter.t - 1], &kfilter.k_endog,
                                       &kfilter._tmp3[i], &kfilter.k_endog)
        kfilter._tmp4[i + i*kfilter.k_endog] = kfilter.tmp4[i, i, kfilter.t - 1]

cdef void zfiltered_state(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov_inv):
    cdef int j
    # $a_{t,i+1} = a_{t,i} + P_{t,i} Z_{t,i}' F_{t,i}^{-1} v_{t,i}$  
    for j in range(model._k_states):
        if not kfilter.converged:
            kfilter._kalman_gain[j + i*kfilter.k_states] = kfilter._M[j + i*kfilter.k_states] * forecast_error_cov_inv
        kfilter._filtered_state[j] = (
            kfilter._filtered_state[j] +
            kfilter._forecast_error[i] * kfilter._kalman_gain[j + i*kfilter.k_states]
        )

cdef void zfiltered_state_cov(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov_inv):
    cdef:
        int inc = 1, j, k
        np.complex128_t scalar = -1.0 * forecast_error_cov_inv
        np.complex128_t alpha = 1.0
        np.complex128_t gamma = -1.0
        int k_states = model._k_states
        int k_states1 = model._k_states

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef
        if model._k_posdef > model._k_states:
            k_states1 = model._k_posdef + 1

    # $P_{t,i+1} = P_{t,i} - P_{t,i} Z_{t,i}' F_{t,i}^{-1} Z_{t,i} P_{t,i}'$
    # blas.zgeru(&model._k_states, &model._k_states,
    #     &gamma, &kfilter._M[i*kfilter.k_states], &inc,
    #             &kfilter._kalman_gain[i*kfilter.k_states], &inc,
    #     kfilter._filtered_state_cov, &kfilter.k_states
    # )

    blas.zsyrk("L", "N", &model._k_states, &inc,
        &scalar, &kfilter._M[i*kfilter.k_states], &kfilter.k_states,
        &alpha, kfilter._filtered_state_cov, &kfilter.k_states)

cdef void zchandrasekhar_recursion(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov, np.complex128_t forecast_error_cov_inv, np.complex128_t forecast_error_cov_inv_prev):
    # Constants
    cdef:
        int inc = 1
        int j, k
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # Initialization
    if kfilter.t == 0:
        if i == 0:
            kfilter.CM[:] = 0

        # W[:, i:i+1] = T @ (P @ Z[i].T)
        # W[:, i:i+1] = T @ K @ F[i, i]
        # Note: we scale by forecast error cov here b/c kalman_gain was
        # computed above as K = P @ Z[i].T @ (1 / F[i, i])
        blas.zgemv("N", &model._k_states, &model._k_states,
          &forecast_error_cov, model._transition, &model._k_states,
                  &kfilter._kalman_gain[i * kfilter.k_states], &inc,
          &beta, &kfilter.CW[0, i], &inc)
        # M[i, i] = Finv[i, i]
        kfilter.CM[i, i] = -forecast_error_cov_inv
    # Updating
    else:
        # M.T @ W.T. (p x p) (p x m)
        blas.zgemm("T", "T", &model._k_endog, &model._k_states, &model._k_endog,
          &alpha, &kfilter.CM[0, 0], &kfilter.k_endog,
                  &kfilter.CW[0, 0], &kfilter.k_states,
          &beta, &kfilter.CMW[0, 0], &kfilter.k_endog)
        # MW @ Z[i].T (p x m) (m x 1) -> (p x 1)
        blas.zgemv("N", &model._k_endog, &model._k_states,
          &alpha, &kfilter.CMW[0, 0], &kfilter.k_endog,
                  &model._design[i], &model._k_endog,
          &beta, &kfilter.CMWZ[0, 0], &inc)

        # M = M + MWZ @ MWZ.T / F_prev[i, i]
        # Note: syr / syrk only fills in lower triangle here
        blas.zsyrk("L", "N", &model._k_endog, &inc,
            &forecast_error_cov_inv_prev, &kfilter.CMWZ[0, 0], &kfilter.k_endog,
            &alpha, &kfilter.CM[0, 0], &kfilter.k_endog)

        # Fill in the upper triangle
        for j in range(model._k_endog):      # columns
            for k in range(model._k_endog):  # rows
                if k > j: # row > column => in lower triangle
                    kfilter.CM[j, k] = kfilter.CM[k, j]

        # Compute W
        # W -> tmpW
        blas.zcopy(&model._k_endogstates, &kfilter.CW[0, 0], &inc, &kfilter.CtmpW[0, 0], &inc)

        if i == model.k_endog - 1:
            # W = (T - T @ K @ Z[i]) @ W

            # Compute T @ K: (m x m) (m x 1) -> (m x 1)
            # Note: we previously copied CW -> CtmpW, so overwriting CW is okay
            blas.zgemv("N", &model._k_states, &model._k_states,
              &alpha, model._transition, &model._k_states,
                      &kfilter._kalman_gain[i*kfilter.k_states], &inc,
              &beta, &kfilter.CW[0, 0], &inc)
            # T -> tmp00
            blas.zcopy(&model._k_states2, model._transition, &inc, kfilter._tmp00, &inc)
            # T - (T @ K) @ Z[i]: (m x 1) (1 x m) -> (m x m)
            blas.zgeru(&model._k_states, &model._k_states,
                &gamma, &kfilter.CW[0, 0], &inc,
                       &model._design[i], &model._k_endog,
                kfilter._tmp00, &kfilter.k_states)

            # (T - T @ K @ Z[i]) @ tmpW -> W
            blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp00, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &beta, &kfilter.CW[0, 0], &kfilter.k_states)
        else:
            # W = (I - I @ K @ Z[i]) @ W
            # K @ Z[i] (m x 1) (1 x m) -> (m x m)
            kfilter.tmp0[:] = 0
            blas.zgeru(&model._k_states, &model._k_states,
                &alpha, &kfilter._kalman_gain[i*kfilter.k_states], &inc,
                        &model._design[i], &model._k_endog,
                kfilter._tmp0, &kfilter.k_states)

            # W = - K @ Z[i] @ W + W
            blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &gamma, kfilter._tmp0, &kfilter.k_states,
                          &kfilter.CtmpW[0, 0], &kfilter.k_states,
                  &alpha, &kfilter.CW[0, 0], &kfilter.k_states)


cdef void zloglikelihood(zKalmanFilter kfilter, zStatespace model, int i, np.complex128_t forecast_error_cov, np.complex128_t forecast_error_cov_inv):
    kfilter._loglikelihood[0] = (
        kfilter._loglikelihood[0] - 0.5*(
            zlog(2 * M_PI * forecast_error_cov)
        )
    )
    if kfilter.filter_method & FILTER_CONCENTRATED:
        kfilter._scale[0] = kfilter._scale[0] + kfilter._forecast_error[i]**2 * forecast_error_cov_inv
    else:
        kfilter._loglikelihood[0] = kfilter._loglikelihood[0] - 0.5 * (kfilter._forecast_error[i]**2 * forecast_error_cov_inv)

cdef int zupdating_univariate(zKalmanFilter kfilter, zStatespace model):
    # the updating step was performed in the forecast_univariate step
    return 0

cdef int zprediction_univariate(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int inc = 1
        int i, j
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # #### Predicted state for time t+1
    # $a_{t+1} = T_t a_{t,n} + c_t$

    # #### Predicted state covariance matrix for time t+1
    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    #
    # TODO check behavior during convergence
    if not model.companion_transition:
        zpredicted_state(kfilter, model)
        if not kfilter.converged:
            if kfilter.filter_method & FILTER_CHANDRASEKHAR > 0:
                zpredicted_state_cov_chandrasekhar(kfilter, model)
            else:
                zpredicted_state_cov(kfilter, model)
    else:
        zcompanion_predicted_state(kfilter, model)
        if not kfilter.converged:
            zcompanion_predicted_state_cov(kfilter, model)

    # #### Kalman gain for time t
    # $K_t = T_t P_t Z_t' F_t^{-1}$  
    # Kalman gain calculation done in forecasting step.

    return 0

cdef void zpredicted_state(zKalmanFilter kfilter, zStatespace model):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.zcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.zgemv("N", &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

cdef void zpredicted_state_cov(zKalmanFilter kfilter, zStatespace model):
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$
    blas.zcopy(&model._k_states2, model._selected_state_cov, &inc, kfilter._predicted_state_cov, &inc)
    # `tmp0` array used here, dimension $(m \times m)$  

    # $\\#_0 = T_t P_{t|t} $

    # $(m \times m) = (m \times m) (m \times m)$
    # blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
    #       &alpha, model._transition, &model._k_states,
    #               kfilter._filtered_state_cov, &kfilter.k_states,
    #       &beta, kfilter._tmp0, &kfilter.k_states)
    blas.zsymm("R", "L", &model._k_states, &model._k_states,
          &alpha, kfilter._filtered_state_cov, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
    # $P_{t+1} = 1.0 \\#_0 T_t' + 1.0 \\#$  
    # $(m \times m) = (m \times m) (m \times m) + (m \times m)$
    blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &alpha, kfilter._predicted_state_cov, &kfilter.k_states)

cdef void zpredicted_state_cov_chandrasekhar(zKalmanFilter kfilter, zStatespace model):
    # Constants
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
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

cdef void zcompanion_predicted_state(zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex128_t alpha = 1.0

    # $a_{t+1} = T_t a_{t,n} + c_t$
    blas.zcopy(&model._k_states, model._state_intercept, &inc, kfilter._predicted_state, &inc)
    blas.zgemv("N", &model._k_posdef, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state, &inc,
          &alpha, kfilter._predicted_state, &inc)

    for i in range(model._k_posdef, model._k_states):
        kfilter._predicted_state[i] = kfilter._predicted_state[i] + kfilter._filtered_state[i - model._k_posdef]

cdef void zcompanion_predicted_state_cov(zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i, j, idx
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t tmp

    # $P_{t+1} = T_t P_{t,n} T_t' + Q_t^*$

    # `tmp0` array used here, dimension $(p \times m)$  
    # $\\#_0 = \phi_t P_{t|t} $

    # $(p \times m) = (p \times m) (m \times m)$
    # TODO: symm?
    blas.zgemm("N", "N", &model._k_posdef, &model._k_states, &model._k_states,
          &alpha, model._transition, &model._k_states,
                  kfilter._filtered_state_cov, &kfilter.k_states,
          &beta, kfilter._tmp0, &kfilter.k_states)
                
    # $P_{t+1} = 1.0 \\#_0 \phi_t' + 1.0 \\#$  
    # $(m \times m) = (p \times m) (m \times p) + (m \times m)$
    blas.zgemm("N", "T", &model._k_posdef, &model._k_posdef, &model._k_states,
          &alpha, kfilter._tmp0, &kfilter.k_states,
                  model._transition, &model._k_states,
          &beta, kfilter._predicted_state_cov, &kfilter.k_states)

    # Fill in the basic matrix blocks
    for i in range(kfilter.k_states):      # columns
        for j in range(kfilter.k_states):  # rows
            idx = j + i*kfilter.k_states

            # Add the Q matrix to the upper-left block
            if i < model._k_posdef and j < model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._predicted_state_cov[idx] + 
                    model._state_cov[j + i*model._k_posdef]
                )

            # Set the upper-right block to be the first m-p columns of
            # \phi _t P_{t|t}, and the lower-left block to the its transpose
            elif i >= model._k_posdef and j < model._k_posdef:
                tmp = kfilter._tmp0[j + (i-model._k_posdef)*kfilter.k_states]
                kfilter._predicted_state_cov[idx] = tmp
                kfilter._predicted_state_cov[i + j*model._k_states] = tmp

            # Set the lower-right block 
            elif i >= model._k_posdef and j >= model._k_posdef:
                kfilter._predicted_state_cov[idx] = (
                    kfilter._filtered_state_cov[(j - model._k_posdef) + (i - model._k_posdef)*kfilter.k_states]
                )

cdef np.complex128_t zinverse_noop_univariate(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant) except *:
    return -np.inf

cdef np.complex128_t zloglikelihood_univariate(zKalmanFilter kfilter, zStatespace model, np.complex128_t determinant):
    return 0

cdef np.complex128_t zscale_univariate(zKalmanFilter kfilter, zStatespace model):
    return 0
