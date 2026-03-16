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

from statsmodels.tsa.statespace._kalman_smoother cimport (
    SMOOTHER_STATE, SMOOTHER_STATE_COV, SMOOTHER_DISTURBANCE,
    SMOOTHER_DISTURBANCE_COV
)

# ### Alternative conventional Kalman smoother
#
# The following are the above routines as defined in the conventional Kalman
# smoother.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int ssmoothed_estimators_measurement_alternative(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model) except *:
    cdef:
        int i
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    if model._nmissing == model.k_endog:
        # $L_t = T_t$  
        blas.scopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
        return 1

    # $C_t = (I - P_t Z_t' F_t^{-1} Z_t)$  
    # $C_t = (I - \\#_1 \\#_3)$  
    # $(m \times m) = (m \times m) + (m \times p) (p \times m)$
    # (this is required for any type of smoothing)
    blas.scopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, smoother._tmpL, &kfilter.k_states)
    for i in range(model._k_states):
        smoother.tmpL[i,i] = smoother.tmpL[i,i] + 1

    # Scaled smoothed estimator  
    # $\tilde r_{t} = Z_n' F_n^{-1} v_n + C_t' \hat r_n$  
    # $\tilde r_{t} = \\#_3' v_n + C_t' \hat r_n$  
    # $(m \times 1) = (m \times p) (p \times 1) + (m \times m) (m \times 1)$
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.sgemv("T", &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._tmp0, &inc)

        blas.scopy(&model._k_states, smoother._tmp0, &inc,
                                              smoother._input_scaled_smoothed_estimator, &inc)
        blas.sgemv("T", &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp3, &kfilter.k_endog,
                          &kfilter.forecast_error[0, smoother.t], &inc,
                  &alpha, smoother._input_scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\tilde N_{t} = Z_t' F_t^{-1} Z_t + C_t' \hat N_t C_t$  
    # $\tilde N_{t} = Z_t' \\#_3 + C_t' \hat N_t C_t$  
    # $(m \times m) = (m \times p) (p \times m) + (m \times m) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)
        blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_endog,
                  &alpha, model._design, &model._k_endog,
                          kfilter._tmp3, &kfilter.k_endog,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)

    # $L_t = T_t C_t$  
    # $(m \times m) = (m \times m) (m \times m)$
    blas.scopy(&model._k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._tmpL, &kfilter.k_states)

    # Smoothing error  
    # $u_t = \\#_2 - K_t' \tilde r_{t+1}$  
    # $(p \times 1) = (p \times 1) - (p \times m) (m \times 1)$ 
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE):
        blas.scopy(&kfilter.k_endog, kfilter._tmp2, &inc, smoother._smoothing_error, &inc)
        if smoother.t < model.nobs - 1:
            blas.sgemv("T", &model._k_states, &model._k_endog,
                      &gamma, kfilter._kalman_gain, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &alpha, smoother._smoothing_error, &inc)

cdef int ssmoothed_estimators_time_alternative(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    if smoother.t == 0:
        return 1

    # Scaled smoothed estimator  
    # $\hat r_{t-1} = T_t' \tilde r_t$  
    # $(m \times 1) = (m \times m) (m \times 1)
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.sgemv("T", &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\hat N_{t-1} = T_t' \tilde N_t T_t$  
    # $(m \times m) = (m \times p) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          model._transition, &kfilter.k_states,
                  &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

cdef int ssmoothed_state_alternative(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # $\hat \alpha_t = a_t|t + P_t|t \hat r_t$  
        # $(m \times 1) = (m \times 1) + (m \times m) (m \times 1)$  
        blas.scopy(&kfilter.k_states, &kfilter.filtered_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)

        blas.sgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # $V_t = P_t|t [I - \hat N_t P_t|t]$  
        # $(m \times m) = (m \times m) [(m \times m) - (m \times m) (m \times m)]$  
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.filtered_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int ssmoothed_disturbances_alternative(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0

    # At this point $\tilde r_t$ has been computed, and is stored in
    # scaled_smoothed_estimator[:, t] but $\varepsilon_t$ depends
    # on $\tilde r_{t+1}$, which is stored in scaled_smoothed_estimator[:, t+1]

    # Temporary arrays

    # $\\#_0 = R_t Q_t$  
    # $(m \times r) = (m \times r) (r \times r)$
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE | SMOOTHER_DISTURBANCE_COV):
        blas.sgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
                  &alpha, model._selection, &model._k_states,
                          model._state_cov, &model._k_posdef,
                  &beta, smoother._tmp0, &kfilter.k_states)

    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        # Smoothed measurement disturbance  
        # $\hat \varepsilon_t = H_t u_t$  
        # $(p \times 1) = (p \times p) (p \times 1)$  
        blas.sgemv("N", &model._k_endog, &model._k_endog,
                      &alpha, model._obs_cov, &model._k_endog,
                              smoother._smoothing_error, &inc,
                      &beta, smoother._smoothed_measurement_disturbance, &inc)

        # Smoothed state disturbance  
        # $\hat \eta_t = \\#_0' \tilde r_{t+1}$  
        # $(r \times 1) = (r \times m) (m \times 1)$  
        blas.sgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)
    
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        # $\\#_00 = K_t H_t$  
        # $(m \times p) = (m \times p) (p \times p)$  
        blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
                  &alpha, kfilter._kalman_gain, &kfilter.k_states,
                          model._obs_cov, &model._k_endog,
                  &beta, smoother._tmp00, &kfilter.k_states)

        # Smoothed measurement disturbance covariance matrix  
        # $Var(\varepsilon_t | Y_n) = H_t - H_t \\#_4 - \\#_00' \tilde N_t \\#_00$  
        # $(p \times p) = (p \times p) - (p \times p) (p \times p) - (p \times m) (m \times m) (m \times p)$  
        blas.sgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                  &gamma, model._obs_cov, &model._k_endog,
                          kfilter._tmp4, &kfilter.k_endog,
                  &beta, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        blas.sgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp00, &kfilter.k_states,
                  &beta, smoother._tmp000, &kfilter.k_states)

        blas.sgemm("T", "N", &model._k_endog, &model._k_endog, &model._k_states,
                  &gamma, smoother._tmp00, &kfilter.k_states,
                          smoother._tmp000, &kfilter.k_states,
                  &alpha, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        # blas.saxpy(&model._k_endog2, &alpha,
        #        model._obs_cov, &inc,
        #        smoother._smoothed_measurement_disturbance_cov, &inc)
        for i in range(kfilter.k_endog):
            for j in range(i+1):
                smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog] = model._obs_cov[i + j*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog]
                if not i == j:
                    smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog]
        
        # Smoothed state disturbance covariance matrix  
        # $Var(\eta_t | Y_n) = Q_t - \\#_0' \tilde N_t \\#_0$  
        # $(r \times r) = (r \times r) - (r \times m) (m \times m) (m \times r)$  
        blas.sgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

        blas.scopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.sgemm("T", "N", &model._k_posdef, &model._k_posdef, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

# ### Alternative conventional Kalman smoother
#
# The following are the above routines as defined in the conventional Kalman
# smoother.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int dsmoothed_estimators_measurement_alternative(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model) except *:
    cdef:
        int i
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    if model._nmissing == model.k_endog:
        # $L_t = T_t$  
        blas.dcopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
        return 1

    # $C_t = (I - P_t Z_t' F_t^{-1} Z_t)$  
    # $C_t = (I - \\#_1 \\#_3)$  
    # $(m \times m) = (m \times m) + (m \times p) (p \times m)$
    # (this is required for any type of smoothing)
    blas.dcopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, smoother._tmpL, &kfilter.k_states)
    for i in range(model._k_states):
        smoother.tmpL[i,i] = smoother.tmpL[i,i] + 1

    # Scaled smoothed estimator  
    # $\tilde r_{t} = Z_n' F_n^{-1} v_n + C_t' \hat r_n$  
    # $\tilde r_{t} = \\#_3' v_n + C_t' \hat r_n$  
    # $(m \times 1) = (m \times p) (p \times 1) + (m \times m) (m \times 1)$
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.dgemv("T", &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._tmp0, &inc)

        blas.dcopy(&model._k_states, smoother._tmp0, &inc,
                                              smoother._input_scaled_smoothed_estimator, &inc)
        blas.dgemv("T", &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp3, &kfilter.k_endog,
                          &kfilter.forecast_error[0, smoother.t], &inc,
                  &alpha, smoother._input_scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\tilde N_{t} = Z_t' F_t^{-1} Z_t + C_t' \hat N_t C_t$  
    # $\tilde N_{t} = Z_t' \\#_3 + C_t' \hat N_t C_t$  
    # $(m \times m) = (m \times p) (p \times m) + (m \times m) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)
        blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_endog,
                  &alpha, model._design, &model._k_endog,
                          kfilter._tmp3, &kfilter.k_endog,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)

    # $L_t = T_t C_t$  
    # $(m \times m) = (m \times m) (m \times m)$
    blas.dcopy(&model._k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._tmpL, &kfilter.k_states)

    # Smoothing error  
    # $u_t = \\#_2 - K_t' \tilde r_{t+1}$  
    # $(p \times 1) = (p \times 1) - (p \times m) (m \times 1)$ 
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE):
        blas.dcopy(&kfilter.k_endog, kfilter._tmp2, &inc, smoother._smoothing_error, &inc)
        if smoother.t < model.nobs - 1:
            blas.dgemv("T", &model._k_states, &model._k_endog,
                      &gamma, kfilter._kalman_gain, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &alpha, smoother._smoothing_error, &inc)

cdef int dsmoothed_estimators_time_alternative(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    if smoother.t == 0:
        return 1

    # Scaled smoothed estimator  
    # $\hat r_{t-1} = T_t' \tilde r_t$  
    # $(m \times 1) = (m \times m) (m \times 1)
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.dgemv("T", &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\hat N_{t-1} = T_t' \tilde N_t T_t$  
    # $(m \times m) = (m \times p) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          model._transition, &kfilter.k_states,
                  &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

cdef int dsmoothed_state_alternative(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # $\hat \alpha_t = a_t|t + P_t|t \hat r_t$  
        # $(m \times 1) = (m \times 1) + (m \times m) (m \times 1)$  
        blas.dcopy(&kfilter.k_states, &kfilter.filtered_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)

        blas.dgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # $V_t = P_t|t [I - \hat N_t P_t|t]$  
        # $(m \times m) = (m \times m) [(m \times m) - (m \times m) (m \times m)]$  
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.filtered_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int dsmoothed_disturbances_alternative(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0

    # At this point $\tilde r_t$ has been computed, and is stored in
    # scaled_smoothed_estimator[:, t] but $\varepsilon_t$ depends
    # on $\tilde r_{t+1}$, which is stored in scaled_smoothed_estimator[:, t+1]

    # Temporary arrays

    # $\\#_0 = R_t Q_t$  
    # $(m \times r) = (m \times r) (r \times r)$
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE | SMOOTHER_DISTURBANCE_COV):
        blas.dgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
                  &alpha, model._selection, &model._k_states,
                          model._state_cov, &model._k_posdef,
                  &beta, smoother._tmp0, &kfilter.k_states)

    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        # Smoothed measurement disturbance  
        # $\hat \varepsilon_t = H_t u_t$  
        # $(p \times 1) = (p \times p) (p \times 1)$  
        blas.dgemv("N", &model._k_endog, &model._k_endog,
                      &alpha, model._obs_cov, &model._k_endog,
                              smoother._smoothing_error, &inc,
                      &beta, smoother._smoothed_measurement_disturbance, &inc)

        # Smoothed state disturbance  
        # $\hat \eta_t = \\#_0' \tilde r_{t+1}$  
        # $(r \times 1) = (r \times m) (m \times 1)$  
        blas.dgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)
    
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        # $\\#_00 = K_t H_t$  
        # $(m \times p) = (m \times p) (p \times p)$  
        blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
                  &alpha, kfilter._kalman_gain, &kfilter.k_states,
                          model._obs_cov, &model._k_endog,
                  &beta, smoother._tmp00, &kfilter.k_states)

        # Smoothed measurement disturbance covariance matrix  
        # $Var(\varepsilon_t | Y_n) = H_t - H_t \\#_4 - \\#_00' \tilde N_t \\#_00$  
        # $(p \times p) = (p \times p) - (p \times p) (p \times p) - (p \times m) (m \times m) (m \times p)$  
        blas.dgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                  &gamma, model._obs_cov, &model._k_endog,
                          kfilter._tmp4, &kfilter.k_endog,
                  &beta, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        blas.dgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp00, &kfilter.k_states,
                  &beta, smoother._tmp000, &kfilter.k_states)

        blas.dgemm("T", "N", &model._k_endog, &model._k_endog, &model._k_states,
                  &gamma, smoother._tmp00, &kfilter.k_states,
                          smoother._tmp000, &kfilter.k_states,
                  &alpha, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        # blas.daxpy(&model._k_endog2, &alpha,
        #        model._obs_cov, &inc,
        #        smoother._smoothed_measurement_disturbance_cov, &inc)
        for i in range(kfilter.k_endog):
            for j in range(i+1):
                smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog] = model._obs_cov[i + j*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog]
                if not i == j:
                    smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog]
        
        # Smoothed state disturbance covariance matrix  
        # $Var(\eta_t | Y_n) = Q_t - \\#_0' \tilde N_t \\#_0$  
        # $(r \times r) = (r \times r) - (r \times m) (m \times m) (m \times r)$  
        blas.dgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

        blas.dcopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.dgemm("T", "N", &model._k_posdef, &model._k_posdef, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

# ### Alternative conventional Kalman smoother
#
# The following are the above routines as defined in the conventional Kalman
# smoother.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int csmoothed_estimators_measurement_alternative(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model) except *:
    cdef:
        int i
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    if model._nmissing == model.k_endog:
        # $L_t = T_t$  
        blas.ccopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
        return 1

    # $C_t = (I - P_t Z_t' F_t^{-1} Z_t)$  
    # $C_t = (I - \\#_1 \\#_3)$  
    # $(m \times m) = (m \times m) + (m \times p) (p \times m)$
    # (this is required for any type of smoothing)
    blas.ccopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, smoother._tmpL, &kfilter.k_states)
    for i in range(model._k_states):
        smoother.tmpL[i,i] = smoother.tmpL[i,i] + 1

    # Scaled smoothed estimator  
    # $\tilde r_{t} = Z_n' F_n^{-1} v_n + C_t' \hat r_n$  
    # $\tilde r_{t} = \\#_3' v_n + C_t' \hat r_n$  
    # $(m \times 1) = (m \times p) (p \times 1) + (m \times m) (m \times 1)$
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.cgemv("T", &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._tmp0, &inc)

        blas.ccopy(&model._k_states, smoother._tmp0, &inc,
                                              smoother._input_scaled_smoothed_estimator, &inc)
        blas.cgemv("T", &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp3, &kfilter.k_endog,
                          &kfilter.forecast_error[0, smoother.t], &inc,
                  &alpha, smoother._input_scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\tilde N_{t} = Z_t' F_t^{-1} Z_t + C_t' \hat N_t C_t$  
    # $\tilde N_{t} = Z_t' \\#_3 + C_t' \hat N_t C_t$  
    # $(m \times m) = (m \times p) (p \times m) + (m \times m) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)
        blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_endog,
                  &alpha, model._design, &model._k_endog,
                          kfilter._tmp3, &kfilter.k_endog,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)

    # $L_t = T_t C_t$  
    # $(m \times m) = (m \times m) (m \times m)$
    blas.ccopy(&model._k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._tmpL, &kfilter.k_states)

    # Smoothing error  
    # $u_t = \\#_2 - K_t' \tilde r_{t+1}$  
    # $(p \times 1) = (p \times 1) - (p \times m) (m \times 1)$ 
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE):
        blas.ccopy(&kfilter.k_endog, kfilter._tmp2, &inc, smoother._smoothing_error, &inc)
        if smoother.t < model.nobs - 1:
            blas.cgemv("T", &model._k_states, &model._k_endog,
                      &gamma, kfilter._kalman_gain, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &alpha, smoother._smoothing_error, &inc)

cdef int csmoothed_estimators_time_alternative(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    if smoother.t == 0:
        return 1

    # Scaled smoothed estimator  
    # $\hat r_{t-1} = T_t' \tilde r_t$  
    # $(m \times 1) = (m \times m) (m \times 1)
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.cgemv("T", &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\hat N_{t-1} = T_t' \tilde N_t T_t$  
    # $(m \times m) = (m \times p) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          model._transition, &kfilter.k_states,
                  &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

cdef int csmoothed_state_alternative(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # $\hat \alpha_t = a_t|t + P_t|t \hat r_t$  
        # $(m \times 1) = (m \times 1) + (m \times m) (m \times 1)$  
        blas.ccopy(&kfilter.k_states, &kfilter.filtered_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)

        blas.cgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # $V_t = P_t|t [I - \hat N_t P_t|t]$  
        # $(m \times m) = (m \times m) [(m \times m) - (m \times m) (m \times m)]$  
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.filtered_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int csmoothed_disturbances_alternative(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0

    # At this point $\tilde r_t$ has been computed, and is stored in
    # scaled_smoothed_estimator[:, t] but $\varepsilon_t$ depends
    # on $\tilde r_{t+1}$, which is stored in scaled_smoothed_estimator[:, t+1]

    # Temporary arrays

    # $\\#_0 = R_t Q_t$  
    # $(m \times r) = (m \times r) (r \times r)$
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE | SMOOTHER_DISTURBANCE_COV):
        blas.cgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
                  &alpha, model._selection, &model._k_states,
                          model._state_cov, &model._k_posdef,
                  &beta, smoother._tmp0, &kfilter.k_states)

    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        # Smoothed measurement disturbance  
        # $\hat \varepsilon_t = H_t u_t$  
        # $(p \times 1) = (p \times p) (p \times 1)$  
        blas.cgemv("N", &model._k_endog, &model._k_endog,
                      &alpha, model._obs_cov, &model._k_endog,
                              smoother._smoothing_error, &inc,
                      &beta, smoother._smoothed_measurement_disturbance, &inc)

        # Smoothed state disturbance  
        # $\hat \eta_t = \\#_0' \tilde r_{t+1}$  
        # $(r \times 1) = (r \times m) (m \times 1)$  
        blas.cgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)
    
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        # $\\#_00 = K_t H_t$  
        # $(m \times p) = (m \times p) (p \times p)$  
        blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
                  &alpha, kfilter._kalman_gain, &kfilter.k_states,
                          model._obs_cov, &model._k_endog,
                  &beta, smoother._tmp00, &kfilter.k_states)

        # Smoothed measurement disturbance covariance matrix  
        # $Var(\varepsilon_t | Y_n) = H_t - H_t \\#_4 - \\#_00' \tilde N_t \\#_00$  
        # $(p \times p) = (p \times p) - (p \times p) (p \times p) - (p \times m) (m \times m) (m \times p)$  
        blas.cgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                  &gamma, model._obs_cov, &model._k_endog,
                          kfilter._tmp4, &kfilter.k_endog,
                  &beta, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        blas.cgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp00, &kfilter.k_states,
                  &beta, smoother._tmp000, &kfilter.k_states)

        blas.cgemm("T", "N", &model._k_endog, &model._k_endog, &model._k_states,
                  &gamma, smoother._tmp00, &kfilter.k_states,
                          smoother._tmp000, &kfilter.k_states,
                  &alpha, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        # blas.caxpy(&model._k_endog2, &alpha,
        #        model._obs_cov, &inc,
        #        smoother._smoothed_measurement_disturbance_cov, &inc)
        for i in range(kfilter.k_endog):
            for j in range(i+1):
                smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog] = model._obs_cov[i + j*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog]
                if not i == j:
                    smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog]
        
        # Smoothed state disturbance covariance matrix  
        # $Var(\eta_t | Y_n) = Q_t - \\#_0' \tilde N_t \\#_0$  
        # $(r \times r) = (r \times r) - (r \times m) (m \times m) (m \times r)$  
        blas.cgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

        blas.ccopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.cgemm("T", "N", &model._k_posdef, &model._k_posdef, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

# ### Alternative conventional Kalman smoother
#
# The following are the above routines as defined in the conventional Kalman
# smoother.
#
# See Durbin and Koopman (2012) Chapter 4

cdef int zsmoothed_estimators_measurement_alternative(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model) except *:
    cdef:
        int i
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    if model._nmissing == model.k_endog:
        # $L_t = T_t$  
        blas.zcopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
        return 1

    # $C_t = (I - P_t Z_t' F_t^{-1} Z_t)$  
    # $C_t = (I - \\#_1 \\#_3)$  
    # $(m \times m) = (m \times m) + (m \times p) (p \times m)$
    # (this is required for any type of smoothing)
    blas.zcopy(&model._k_states2, model._transition, &inc, smoother._tmpL, &inc)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_endog,
              &gamma, kfilter._tmp1, &kfilter.k_states,
                      kfilter._tmp3, &kfilter.k_endog,
              &beta, smoother._tmpL, &kfilter.k_states)
    for i in range(model._k_states):
        smoother.tmpL[i,i] = smoother.tmpL[i,i] + 1

    # Scaled smoothed estimator  
    # $\tilde r_{t} = Z_n' F_n^{-1} v_n + C_t' \hat r_n$  
    # $\tilde r_{t} = \\#_3' v_n + C_t' \hat r_n$  
    # $(m \times 1) = (m \times p) (p \times 1) + (m \times m) (m \times 1)$
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.zgemv("T", &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._tmp0, &inc)

        blas.zcopy(&model._k_states, smoother._tmp0, &inc,
                                              smoother._input_scaled_smoothed_estimator, &inc)
        blas.zgemv("T", &model._k_endog, &model._k_states,
                  &alpha, kfilter._tmp3, &kfilter.k_endog,
                          &kfilter.forecast_error[0, smoother.t], &inc,
                  &alpha, smoother._input_scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\tilde N_{t} = Z_t' F_t^{-1} Z_t + C_t' \hat N_t C_t$  
    # $\tilde N_{t} = Z_t' \\#_3 + C_t' \hat N_t C_t$  
    # $(m \times m) = (m \times p) (p \times m) + (m \times m) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)
        blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_endog,
                  &alpha, model._design, &model._k_endog,
                          kfilter._tmp3, &kfilter.k_endog,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states)

    # $L_t = T_t C_t$  
    # $(m \times m) = (m \times m) (m \times m)$
    blas.zcopy(&model._k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, model._transition, &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._tmpL, &kfilter.k_states)

    # Smoothing error  
    # $u_t = \\#_2 - K_t' \tilde r_{t+1}$  
    # $(p \times 1) = (p \times 1) - (p \times m) (m \times 1)$ 
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE):
        blas.zcopy(&kfilter.k_endog, kfilter._tmp2, &inc, smoother._smoothing_error, &inc)
        if smoother.t < model.nobs - 1:
            blas.zgemv("T", &model._k_states, &model._k_endog,
                      &gamma, kfilter._kalman_gain, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &alpha, smoother._smoothing_error, &inc)

cdef int zsmoothed_estimators_time_alternative(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    if smoother.t == 0:
        return 1

    # Scaled smoothed estimator  
    # $\hat r_{t-1} = T_t' \tilde r_t$  
    # $(m \times 1) = (m \times m) (m \times 1)
    if smoother.smoother_output & (SMOOTHER_STATE | SMOOTHER_DISTURBANCE):
        blas.zgemv("T", &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &beta, smoother._scaled_smoothed_estimator, &inc)

    # Scaled smoothed estimator covariance matrix  
    # $\hat N_{t-1} = T_t' \tilde N_t T_t$  
    # $(m \times m) = (m \times p) (m \times m) (m \times m)$  
    if smoother.smoother_output & (SMOOTHER_STATE_COV | SMOOTHER_DISTURBANCE_COV):
        blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, model._transition, &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          model._transition, &kfilter.k_states,
                  &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

cdef int zsmoothed_state_alternative(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # $\hat \alpha_t = a_t|t + P_t|t \hat r_t$  
        # $(m \times 1) = (m \times 1) + (m \times m) (m \times 1)$  
        blas.zcopy(&kfilter.k_states, &kfilter.filtered_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)

        blas.zgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # $V_t = P_t|t [I - \hat N_t P_t|t]$  
        # $(m \times m) = (m \times m) [(m \times m) - (m \times m) (m \times m)]$  
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.filtered_state_cov[0, 0, smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.filtered_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int zsmoothed_disturbances_alternative(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef int i, j
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0

    # At this point $\tilde r_t$ has been computed, and is stored in
    # scaled_smoothed_estimator[:, t] but $\varepsilon_t$ depends
    # on $\tilde r_{t+1}$, which is stored in scaled_smoothed_estimator[:, t+1]

    # Temporary arrays

    # $\\#_0 = R_t Q_t$  
    # $(m \times r) = (m \times r) (r \times r)$
    if smoother.smoother_output & (SMOOTHER_DISTURBANCE | SMOOTHER_DISTURBANCE_COV):
        blas.zgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
                  &alpha, model._selection, &model._k_states,
                          model._state_cov, &model._k_posdef,
                  &beta, smoother._tmp0, &kfilter.k_states)

    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        # Smoothed measurement disturbance  
        # $\hat \varepsilon_t = H_t u_t$  
        # $(p \times 1) = (p \times p) (p \times 1)$  
        blas.zgemv("N", &model._k_endog, &model._k_endog,
                      &alpha, model._obs_cov, &model._k_endog,
                              smoother._smoothing_error, &inc,
                      &beta, smoother._smoothed_measurement_disturbance, &inc)

        # Smoothed state disturbance  
        # $\hat \eta_t = \\#_0' \tilde r_{t+1}$  
        # $(r \times 1) = (r \times m) (m \times 1)$  
        blas.zgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              &smoother.scaled_smoothed_estimator[0, smoother.t+1], &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)
    
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        # $\\#_00 = K_t H_t$  
        # $(m \times p) = (m \times p) (p \times p)$  
        blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_endog,
                  &alpha, kfilter._kalman_gain, &kfilter.k_states,
                          model._obs_cov, &model._k_endog,
                  &beta, smoother._tmp00, &kfilter.k_states)

        # Smoothed measurement disturbance covariance matrix  
        # $Var(\varepsilon_t | Y_n) = H_t - H_t \\#_4 - \\#_00' \tilde N_t \\#_00$  
        # $(p \times p) = (p \times p) - (p \times p) (p \times p) - (p \times m) (m \times m) (m \times p)$  
        blas.zgemm("N", "N", &model._k_endog, &model._k_endog, &model._k_endog,
                  &gamma, model._obs_cov, &model._k_endog,
                          kfilter._tmp4, &kfilter.k_endog,
                  &beta, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        blas.zgemm("N", "N", &model._k_states, &model._k_endog, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp00, &kfilter.k_states,
                  &beta, smoother._tmp000, &kfilter.k_states)

        blas.zgemm("T", "N", &model._k_endog, &model._k_endog, &model._k_states,
                  &gamma, smoother._tmp00, &kfilter.k_states,
                          smoother._tmp000, &kfilter.k_states,
                  &alpha, smoother._smoothed_measurement_disturbance_cov, &kfilter.k_endog)

        # blas.zaxpy(&model._k_endog2, &alpha,
        #        model._obs_cov, &inc,
        #        smoother._smoothed_measurement_disturbance_cov, &inc)
        for i in range(kfilter.k_endog):
            for j in range(i+1):
                smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog] = model._obs_cov[i + j*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[i + j*kfilter.k_endog]
                if not i == j:
                    smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog] = model._obs_cov[j + i*model._k_endog] + smoother._smoothed_measurement_disturbance_cov[j + i*kfilter.k_endog]
        
        # Smoothed state disturbance covariance matrix  
        # $Var(\eta_t | Y_n) = Q_t - \\#_0' \tilde N_t \\#_0$  
        # $(r \times r) = (r \times r) - (r \times m) (m \times m) (m \times r)$  
        blas.zgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t+1], &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

        blas.zcopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.zgemm("T", "N", &model._k_posdef, &model._k_posdef, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)
