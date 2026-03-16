#cython: profile=False
#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Models

Note: some of the calls below are not fully optimized (in the sense that some
things are re-computed rather than stored) but since these iterations will
almost always only be for a very few periods, gains from further optimization
are likely to be small.

Note: there is a typo in Durbin and Koopman (2012) in the equations for the
univariate smoothed measurement disturbances and smoothed measurement
disturbance covariances. In each equation (p157), the Kalman gain vector
K_{t,i} is used, but in fact these should be multiplied by the forecast error
covariance F_{t,i}. The original paper on the univariate approach, Koopman and
Durbin (2000) has the correct form. The typo arose because the original paper
defines the Kalman gain as K_{t,i} = P_{t,i} Z_{t,i}' but the book defines it
as K_{t,i} = P_{t,i} Z_{t,i}' F_{t,i}^{-1}, and the book does not correct the
disturbances formulas for this change.

Furthermore, in analogy to the disturbance smoother from chapter 4, the
formula for the univariate covariance ought to be subtracted from the
observation covariance.

So, what we ought to have is:

\hat \varepsilon_{t,i} = \sigma_{t,i}^2 F_{t,i}^{-1} (v_{t,i} - F_{t,i} K_{t,i}' r_{t,i})
Var(\hat \varepsilon_{t,i}) = \sigma_{t,i}^2 - \sigma_{t,i}^4 F_{t,i}^{-2} (v_{t,i} - F_{t,i} K_{t,i}' r_{t,i})

Author: Chad Fulton
License: Simplified-BSD
"""

# Typical imports
import numpy as np
cimport numpy as np
from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas

from statsmodels.tsa.statespace._kalman_smoother cimport (
    SMOOTHER_STATE, SMOOTHER_STATE_COV, SMOOTHER_STATE_AUTOCOV,
    SMOOTHER_DISTURBANCE, SMOOTHER_DISTURBANCE_COV
)

# ### Univariate diffuse Kalman smoother
#
# See Durbin and Koopman (2012) Chapter 5.3

cdef int ssmoothed_estimators_measurement_univariate_diffuse(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model) except *:
    cdef:
        int i, j, inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
        np.float32_t scalar
        int k_states = model._k_states
        np.float32_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12, F2

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

        smoother.scaled_smoothed_diffuse_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse1_estimator_cov[:, :, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse2_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothing error
    # (not used in the univariate approach)

    # Given r_{t,0}:
    # calculate r_{t-1,p}, ..., r_{t-1, 0} and N_{t-1,p}, ..., N_{t-1,0}

    # Clear temporary arrays used for cumulation of L0 and L1
    smoother.tmpL[:] = 0
    smoother.tmpL2[:] = 0
    for i in range(kfilter.k_states):
        smoother.tmpL[i, i] = 1
        smoother.tmpL2[i, i] = 1

    # Iterate
    for i in range(kfilter.k_endog-1,-1,-1):
        # Forecast error covariance and diffuse forecast error covariance
        # F_{*,t}
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        # F_{\infty,t}
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]

        # Intermediate computations (must be done first so we can set the
        # measurement disturbance variables correctly)
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv
            F2 = F12 * forecast_error_diffuse_cov_inv

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
        elif not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.scopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.sscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.sger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
        else:
            kfilter.tmpK0[:] = 0
            kfilter.tmpL0[:] = 0
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = 1

        # Cumulate L0 and L1
        blas.scopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          kfilter._tmpL0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            blas.scopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &beta, smoother._tmpL2, &kfilter.k_states)

        # Store values for measurement disturbance smoothing
        # np.dot(K0.T, rt)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            # Note: zdot and cdot are broken, so have to use gemv for those
            smoother._smoothed_measurement_disturbance[i] = (
                blas.sdot(&model._k_states, kfilter._tmpK0, &inc,
                                                     smoother._scaled_smoothed_estimator, &inc)
            )

        # Store values for measurement disturbance covariance smoothing
        # np.dot(np.dot(K0.T, Nt), K0)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            blas.sgemv("N", &model._k_states, &model._k_states,
                                     &alpha, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                             kfilter._tmpK0, &inc,
                                     &beta, smoother._tmp0, &inc)
            # Note: zdot and cdot are broken, so have to use gemv for those
            smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                blas.sdot(&model._k_states, kfilter._tmpK0, &inc,
                                                      smoother._tmp0, &inc)
            )

        # F_{\infty, i, i, t} > 0
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            # Update
            # rt_inf[:] = Zi.T[:, 0] * v[i] * F1 + np.dot(L0.T, rt_inf) + np.dot(L1.T, rt)
            blas.scopy(&model._k_states, smoother._scaled_smoothed_diffuse_estimator, &inc, smoother._tmp0, &inc)
            blas.scopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_diffuse_estimator, &inc)
            scalar = F1 * kfilter._forecast_error[i]
            blas.sgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_diffuse_estimator, &inc)

            blas.sgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL1, &kfilter.k_states,
                    smoother._scaled_smoothed_estimator, &inc,
                &alpha, smoother._scaled_smoothed_diffuse_estimator, &inc)

            # rt[:] = np.dot(L0.T, rt)
            blas.scopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.sgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &beta, smoother._scaled_smoothed_estimator, &inc)

            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            #  : np.dot(L0.T, Nt_inf2) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt_inf1.T) -> tmp0
            blas.sgemm("T", "T", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F2 to Nt_inf2, which is done below.

            # Nt_inf1[:] = ZiTZi * F1 + np.dot(np.dot(L0.T, Nt_inf1), L0) + np.dot(np.dot(L1.T, Nt), L0)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # #  : np.dot(L0.T, Nt) -> tmp0
            # blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, kfilter._tmpL0, &kfilter.k_states,
            #                   smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
            #           &beta, smoother._tmp0, &kfilter.k_states)
            # #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            # blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, smoother._tmp0, &kfilter.k_states,
            #                   kfilter._tmpL1, &kfilter.k_states,
            #           &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F1 to Nt_inf1, which is done below.

            # Now add in ZiTZi
            # ZiTZi
            smoother.tmp0[:] = 0
            blas.sger(&model._k_states, &model._k_states, &alpha, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._tmp0, &kfilter.k_states)

            # : Nt_inf2 += ZiTZi * F2
            blas.saxpy(&kfilter.k_states2, &F2, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc)
            # : Nt_inf1 += ZiTZi * F1
            blas.saxpy(&kfilter.k_states2, &F1, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc)

            # Nt[:] = np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

        # F_{\infty, i, i, t} = 0
        elif not forecast_error_cov == 0:
            # Update
            # Note: Koopman and Durbin (2003) subtract the latter term,
            # but they use a different definition of the smoothing
            # recursion in general than the book Durbin and Koopman; this
            # version is consistent with the definition of the smoothing
            # recursion in the book.
            # rt[:] = Zi.T[:, 0] * v[i] / F[i, i] + np.dot(L0.T, rt)
            blas.scopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.scopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator, &inc)
            scalar = forecast_error_cov_inv * kfilter._forecast_error[i]
            blas.sgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_estimator, &inc)
            # rt_inf[:] = rt_inf[:]

            # Nt[:] = ZiTZi / F[i, i] + np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            blas.sger(&model._k_states, &model._k_states, &forecast_error_cov_inv, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            # Nt_inf1[:] = np.dot(Nt_inf1, L0)
            blas.scopy(&model._k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc, smoother._tmp0, &inc)
            blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Nt_inf2[:] = Nt_inf2

    # Finalize the cumulated L0 and L1 by premultiplying by T
    # (and put them into kfilter.tmpL0 and kfilter.tmpL1)
    blas.scopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL0, &kfilter.k_states)
    blas.scopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL1, &kfilter.k_states)


cdef int ssmoothed_estimators_time_univariate_diffuse(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i, j, inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
        np.float32_t scalar
        int k_states = model._k_states

    if smoother.t == 0:
        return 1

    # TODO check that this is the right transition matrix to use in the case
    # of time-varying matrices
    # rt1[:] = np.dot(T1.T, rt)
    blas.sgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_estimator[0, smoother.t-1], &inc)
    # rt1_inf[:] = np.dot(T1.T, rt_inf)
    blas.sgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_diffuse_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_diffuse_estimator[0, smoother.t-1], &inc)

    # Nt1 = np.dot(np.dot(T1.T, Nt1), T1)
    blas.scopy(&kfilter.k_states2, smoother._scaled_smoothed_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf1 = np.dot(np.dot(T1.T, Nt1_inf1), T1)
    blas.scopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf2 = np.dot(np.dot(T1.T, Nt1_inf2), T1)
    blas.scopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.sgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)

cdef int ssmoothed_state_univariate_diffuse(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i, j, inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
        np.float32_t scalar
        int k_states = model._k_states

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # alpha_hat[:] = a_t + np.dot(P_t, rt) + np.dot(P_t_inf, rt_inf)
        blas.scopy(&kfilter.k_states, &kfilter.predicted_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)
        blas.sgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)
        blas.sgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # V[:] = P_t - np.dot(np.dot(P_t, Nt), P_t) - np.dot(np.dot(P_t, Nt_inf1), P_t_inf) - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        # : P_t [ I - N_t P_t]
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)
        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T
        # [or]
        # : - np.dot(np.dot(P_t, Nt_inf1.T), P_t_inf)
        blas.sgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.sgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int ssmoothed_disturbances_univariate_diffuse(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    # Note: this only differs from the conventional version in the
    # definition of the smoothed measurement disturbance and cov
    cdef int i, j
    cdef:
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
        np.float32_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, obs_cov

    # Temporary arrays

    # $\\#_0 = R_t Q_t$
    # $(m \times r) = (m \times r) (r \times r)$
    blas.sgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
              &alpha, model._selection, &model._k_states,
                      model._state_cov, &model._k_posdef,
              &beta, smoother._tmp0, &kfilter.k_states)

    for i in range(model._k_endog):
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]
        obs_cov = model._obs_cov[i + i*model._k_endog]

        # Smoothed measurement disturbance
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance[i] = -obs_cov * smoother._smoothed_measurement_disturbance[i]
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance[i] = obs_cov * (
                    kfilter._forecast_error[i] / forecast_error_cov - smoother._smoothed_measurement_disturbance[i])
            else:
                smoother._smoothed_measurement_disturbance[i] = 0

        # Smoothed measurement disturbance covariance matrix
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog]))
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * (1. / forecast_error_cov + smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog])))
            else:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = obs_cov

    # Smoothed state disturbance
    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        blas.sgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              smoother._input_scaled_smoothed_estimator, &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)

    # Smoothed state disturbance covariance matrix
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        blas.sgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        blas.scopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.sgemm("T", "N", &kfilter.k_posdef, &kfilter.k_posdef, &kfilter.k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

cdef int ssmoothed_state_autocov_univariate_diffuse(sKalmanSmoother smoother, sKalmanFilter kfilter, sStatespace model):
    cdef:
        int i
        int inc = 1
        np.float32_t alpha = 1.0
        np.float32_t beta = 0.0
        np.float32_t gamma = -1.0
    # The below code was a test to see if the diffuse autocov could be
    # relatively simply computed (i.e. which required certain terms to cancel
    # out), but it appears that it cannot (i.e. it requires the computation of
    # L2, which is nontrivial and not described by Durbin and Koopman).
    return 0

    # -P_t1 N0t -> tmp0
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # - P_t1_inf N2t - P_t1 N0t -> tmpL
    blas.scopy(&kfilter.k_states2, smoother._tmp0, &inc, smoother._tmpL, &inc)
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states)

    # I - P_t1_inf N1t - P_t1 N0t -> tmp0
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states)
    for i in range(kfilter.k_states):
        smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]

    # L0t P_t -> tmpL2
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmpL2, &kfilter.k_states)
    # L0t P_t + L1t P_t_inf -> tmpL2
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &alpha, smoother._tmpL2, &kfilter.k_states)

    # [I - P_t1_inf N1t - P_t1 N0t] (L1t P_t_inf + L0t P_t) -> smoothed_state_autocov
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL2, &kfilter.k_states,
                  &beta, smoother._smoothed_state_autocov, &kfilter.k_states)

    # L0t P_t_inf -> tmp0
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N2t - P_t1 N0t] L0t P_t_inf +-> smoothed_state_autocov
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

    # [- P_t1_inf N0t ] -> tmpL
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

    # L1t P_t -> tmp0
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N0t ] L1t P_t +-> smoothed_state_autocov
    blas.sgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

# ### Univariate diffuse Kalman smoother
#
# See Durbin and Koopman (2012) Chapter 5.3

cdef int dsmoothed_estimators_measurement_univariate_diffuse(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model) except *:
    cdef:
        int i, j, inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
        np.float64_t scalar
        int k_states = model._k_states
        np.float64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12, F2

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

        smoother.scaled_smoothed_diffuse_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse1_estimator_cov[:, :, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse2_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothing error
    # (not used in the univariate approach)

    # Given r_{t,0}:
    # calculate r_{t-1,p}, ..., r_{t-1, 0} and N_{t-1,p}, ..., N_{t-1,0}

    # Clear temporary arrays used for cumulation of L0 and L1
    smoother.tmpL[:] = 0
    smoother.tmpL2[:] = 0
    for i in range(kfilter.k_states):
        smoother.tmpL[i, i] = 1
        smoother.tmpL2[i, i] = 1

    # Iterate
    for i in range(kfilter.k_endog-1,-1,-1):
        # Forecast error covariance and diffuse forecast error covariance
        # F_{*,t}
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        # F_{\infty,t}
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]

        # Intermediate computations (must be done first so we can set the
        # measurement disturbance variables correctly)
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv
            F2 = F12 * forecast_error_diffuse_cov_inv

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
        elif not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.dcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.dscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.dger(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
        else:
            kfilter.tmpK0[:] = 0
            kfilter.tmpL0[:] = 0
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = 1

        # Cumulate L0 and L1
        blas.dcopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          kfilter._tmpL0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            blas.dcopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &beta, smoother._tmpL2, &kfilter.k_states)

        # Store values for measurement disturbance smoothing
        # np.dot(K0.T, rt)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            # Note: zdot and cdot are broken, so have to use gemv for those
            smoother._smoothed_measurement_disturbance[i] = (
                blas.ddot(&model._k_states, kfilter._tmpK0, &inc,
                                                     smoother._scaled_smoothed_estimator, &inc)
            )

        # Store values for measurement disturbance covariance smoothing
        # np.dot(np.dot(K0.T, Nt), K0)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            blas.dgemv("N", &model._k_states, &model._k_states,
                                     &alpha, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                             kfilter._tmpK0, &inc,
                                     &beta, smoother._tmp0, &inc)
            # Note: zdot and cdot are broken, so have to use gemv for those
            smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                blas.ddot(&model._k_states, kfilter._tmpK0, &inc,
                                                      smoother._tmp0, &inc)
            )

        # F_{\infty, i, i, t} > 0
        if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            # Update
            # rt_inf[:] = Zi.T[:, 0] * v[i] * F1 + np.dot(L0.T, rt_inf) + np.dot(L1.T, rt)
            blas.dcopy(&model._k_states, smoother._scaled_smoothed_diffuse_estimator, &inc, smoother._tmp0, &inc)
            blas.dcopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_diffuse_estimator, &inc)
            scalar = F1 * kfilter._forecast_error[i]
            blas.dgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_diffuse_estimator, &inc)

            blas.dgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL1, &kfilter.k_states,
                    smoother._scaled_smoothed_estimator, &inc,
                &alpha, smoother._scaled_smoothed_diffuse_estimator, &inc)

            # rt[:] = np.dot(L0.T, rt)
            blas.dcopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.dgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &beta, smoother._scaled_smoothed_estimator, &inc)

            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            #  : np.dot(L0.T, Nt_inf2) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt_inf1.T) -> tmp0
            blas.dgemm("T", "T", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F2 to Nt_inf2, which is done below.

            # Nt_inf1[:] = ZiTZi * F1 + np.dot(np.dot(L0.T, Nt_inf1), L0) + np.dot(np.dot(L1.T, Nt), L0)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # #  : np.dot(L0.T, Nt) -> tmp0
            # blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, kfilter._tmpL0, &kfilter.k_states,
            #                   smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
            #           &beta, smoother._tmp0, &kfilter.k_states)
            # #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            # blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, smoother._tmp0, &kfilter.k_states,
            #                   kfilter._tmpL1, &kfilter.k_states,
            #           &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F1 to Nt_inf1, which is done below.

            # Now add in ZiTZi
            # ZiTZi
            smoother.tmp0[:] = 0
            blas.dger(&model._k_states, &model._k_states, &alpha, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._tmp0, &kfilter.k_states)

            # : Nt_inf2 += ZiTZi * F2
            blas.daxpy(&kfilter.k_states2, &F2, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc)
            # : Nt_inf1 += ZiTZi * F1
            blas.daxpy(&kfilter.k_states2, &F1, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc)

            # Nt[:] = np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

        # F_{\infty, i, i, t} = 0
        elif not forecast_error_cov == 0:
            # Update
            # Note: Koopman and Durbin (2003) subtract the latter term,
            # but they use a different definition of the smoothing
            # recursion in general than the book Durbin and Koopman; this
            # version is consistent with the definition of the smoothing
            # recursion in the book.
            # rt[:] = Zi.T[:, 0] * v[i] / F[i, i] + np.dot(L0.T, rt)
            blas.dcopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.dcopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator, &inc)
            scalar = forecast_error_cov_inv * kfilter._forecast_error[i]
            blas.dgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_estimator, &inc)
            # rt_inf[:] = rt_inf[:]

            # Nt[:] = ZiTZi / F[i, i] + np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            blas.dger(&model._k_states, &model._k_states, &forecast_error_cov_inv, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            # Nt_inf1[:] = np.dot(Nt_inf1, L0)
            blas.dcopy(&model._k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc, smoother._tmp0, &inc)
            blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Nt_inf2[:] = Nt_inf2

    # Finalize the cumulated L0 and L1 by premultiplying by T
    # (and put them into kfilter.tmpL0 and kfilter.tmpL1)
    blas.dcopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL0, &kfilter.k_states)
    blas.dcopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL1, &kfilter.k_states)


cdef int dsmoothed_estimators_time_univariate_diffuse(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i, j, inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
        np.float64_t scalar
        int k_states = model._k_states

    if smoother.t == 0:
        return 1

    # TODO check that this is the right transition matrix to use in the case
    # of time-varying matrices
    # rt1[:] = np.dot(T1.T, rt)
    blas.dgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_estimator[0, smoother.t-1], &inc)
    # rt1_inf[:] = np.dot(T1.T, rt_inf)
    blas.dgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_diffuse_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_diffuse_estimator[0, smoother.t-1], &inc)

    # Nt1 = np.dot(np.dot(T1.T, Nt1), T1)
    blas.dcopy(&kfilter.k_states2, smoother._scaled_smoothed_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf1 = np.dot(np.dot(T1.T, Nt1_inf1), T1)
    blas.dcopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf2 = np.dot(np.dot(T1.T, Nt1_inf2), T1)
    blas.dcopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.dgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)

cdef int dsmoothed_state_univariate_diffuse(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i, j, inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
        np.float64_t scalar
        int k_states = model._k_states

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # alpha_hat[:] = a_t + np.dot(P_t, rt) + np.dot(P_t_inf, rt_inf)
        blas.dcopy(&kfilter.k_states, &kfilter.predicted_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)
        blas.dgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)
        blas.dgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # V[:] = P_t - np.dot(np.dot(P_t, Nt), P_t) - np.dot(np.dot(P_t, Nt_inf1), P_t_inf) - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        # : P_t [ I - N_t P_t]
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)
        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T
        # [or]
        # : - np.dot(np.dot(P_t, Nt_inf1.T), P_t_inf)
        blas.dgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.dgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int dsmoothed_disturbances_univariate_diffuse(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    # Note: this only differs from the conventional version in the
    # definition of the smoothed measurement disturbance and cov
    cdef int i, j
    cdef:
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
        np.float64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, obs_cov

    # Temporary arrays

    # $\\#_0 = R_t Q_t$
    # $(m \times r) = (m \times r) (r \times r)$
    blas.dgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
              &alpha, model._selection, &model._k_states,
                      model._state_cov, &model._k_posdef,
              &beta, smoother._tmp0, &kfilter.k_states)

    for i in range(model._k_endog):
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]
        obs_cov = model._obs_cov[i + i*model._k_endog]

        # Smoothed measurement disturbance
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance[i] = -obs_cov * smoother._smoothed_measurement_disturbance[i]
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance[i] = obs_cov * (
                    kfilter._forecast_error[i] / forecast_error_cov - smoother._smoothed_measurement_disturbance[i])
            else:
                smoother._smoothed_measurement_disturbance[i] = 0

        # Smoothed measurement disturbance covariance matrix
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            if dabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog]))
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * (1. / forecast_error_cov + smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog])))
            else:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = obs_cov

    # Smoothed state disturbance
    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        blas.dgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              smoother._input_scaled_smoothed_estimator, &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)

    # Smoothed state disturbance covariance matrix
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        blas.dgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        blas.dcopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.dgemm("T", "N", &kfilter.k_posdef, &kfilter.k_posdef, &kfilter.k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

cdef int dsmoothed_state_autocov_univariate_diffuse(dKalmanSmoother smoother, dKalmanFilter kfilter, dStatespace model):
    cdef:
        int i
        int inc = 1
        np.float64_t alpha = 1.0
        np.float64_t beta = 0.0
        np.float64_t gamma = -1.0
    # The below code was a test to see if the diffuse autocov could be
    # relatively simply computed (i.e. which required certain terms to cancel
    # out), but it appears that it cannot (i.e. it requires the computation of
    # L2, which is nontrivial and not described by Durbin and Koopman).
    return 0

    # -P_t1 N0t -> tmp0
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # - P_t1_inf N2t - P_t1 N0t -> tmpL
    blas.dcopy(&kfilter.k_states2, smoother._tmp0, &inc, smoother._tmpL, &inc)
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states)

    # I - P_t1_inf N1t - P_t1 N0t -> tmp0
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states)
    for i in range(kfilter.k_states):
        smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]

    # L0t P_t -> tmpL2
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmpL2, &kfilter.k_states)
    # L0t P_t + L1t P_t_inf -> tmpL2
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &alpha, smoother._tmpL2, &kfilter.k_states)

    # [I - P_t1_inf N1t - P_t1 N0t] (L1t P_t_inf + L0t P_t) -> smoothed_state_autocov
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL2, &kfilter.k_states,
                  &beta, smoother._smoothed_state_autocov, &kfilter.k_states)

    # L0t P_t_inf -> tmp0
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N2t - P_t1 N0t] L0t P_t_inf +-> smoothed_state_autocov
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

    # [- P_t1_inf N0t ] -> tmpL
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

    # L1t P_t -> tmp0
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N0t ] L1t P_t +-> smoothed_state_autocov
    blas.dgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

# ### Univariate diffuse Kalman smoother
#
# See Durbin and Koopman (2012) Chapter 5.3

cdef int csmoothed_estimators_measurement_univariate_diffuse(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model) except *:
    cdef:
        int i, j, inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
        np.complex64_t scalar
        int k_states = model._k_states
        np.complex64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12, F2

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

        smoother.scaled_smoothed_diffuse_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse1_estimator_cov[:, :, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse2_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothing error
    # (not used in the univariate approach)

    # Given r_{t,0}:
    # calculate r_{t-1,p}, ..., r_{t-1, 0} and N_{t-1,p}, ..., N_{t-1,0}

    # Clear temporary arrays used for cumulation of L0 and L1
    smoother.tmpL[:] = 0
    smoother.tmpL2[:] = 0
    for i in range(kfilter.k_states):
        smoother.tmpL[i, i] = 1
        smoother.tmpL2[i, i] = 1

    # Iterate
    for i in range(kfilter.k_endog-1,-1,-1):
        # Forecast error covariance and diffuse forecast error covariance
        # F_{*,t}
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        # F_{\infty,t}
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]

        # Intermediate computations (must be done first so we can set the
        # measurement disturbance variables correctly)
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv
            F2 = F12 * forecast_error_diffuse_cov_inv

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
        elif not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.ccopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.cscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.cgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
        else:
            kfilter.tmpK0[:] = 0
            kfilter.tmpL0[:] = 0
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = 1

        # Cumulate L0 and L1
        blas.ccopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          kfilter._tmpL0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            blas.ccopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &beta, smoother._tmpL2, &kfilter.k_states)

        # Store values for measurement disturbance smoothing
        # np.dot(K0.T, rt)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            # Note: zdot and cdot are broken, so have to use gemv for those
            blas.cgemv("N", &inc, &model._k_states,
                           &alpha, smoother._scaled_smoothed_estimator, &inc,
                                   kfilter._tmpK0, &inc,
                           &beta, &smoother._smoothed_measurement_disturbance[i], &inc)

        # Store values for measurement disturbance covariance smoothing
        # np.dot(np.dot(K0.T, Nt), K0)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            blas.cgemv("N", &model._k_states, &model._k_states,
                                     &alpha, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                             kfilter._tmpK0, &inc,
                                     &beta, smoother._tmp0, &inc)
            # Note: zdot and cdot are broken, so have to use gemv for those
            blas.cgemv("N", &inc, &model._k_states,
                           &alpha, smoother._tmp0, &inc,
                                   kfilter._tmpK0, &inc,
                           &beta, &smoother._smoothed_measurement_disturbance_cov[i*kfilter.k_endog + i], &inc)

        # F_{\infty, i, i, t} > 0
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            # Update
            # rt_inf[:] = Zi.T[:, 0] * v[i] * F1 + np.dot(L0.T, rt_inf) + np.dot(L1.T, rt)
            blas.ccopy(&model._k_states, smoother._scaled_smoothed_diffuse_estimator, &inc, smoother._tmp0, &inc)
            blas.ccopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_diffuse_estimator, &inc)
            scalar = F1 * kfilter._forecast_error[i]
            blas.cgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_diffuse_estimator, &inc)

            blas.cgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL1, &kfilter.k_states,
                    smoother._scaled_smoothed_estimator, &inc,
                &alpha, smoother._scaled_smoothed_diffuse_estimator, &inc)

            # rt[:] = np.dot(L0.T, rt)
            blas.ccopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.cgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &beta, smoother._scaled_smoothed_estimator, &inc)

            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            #  : np.dot(L0.T, Nt_inf2) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt_inf1.T) -> tmp0
            blas.cgemm("T", "T", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F2 to Nt_inf2, which is done below.

            # Nt_inf1[:] = ZiTZi * F1 + np.dot(np.dot(L0.T, Nt_inf1), L0) + np.dot(np.dot(L1.T, Nt), L0)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # #  : np.dot(L0.T, Nt) -> tmp0
            # blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, kfilter._tmpL0, &kfilter.k_states,
            #                   smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
            #           &beta, smoother._tmp0, &kfilter.k_states)
            # #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            # blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, smoother._tmp0, &kfilter.k_states,
            #                   kfilter._tmpL1, &kfilter.k_states,
            #           &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F1 to Nt_inf1, which is done below.

            # Now add in ZiTZi
            # ZiTZi
            smoother.tmp0[:] = 0
            blas.cgeru(&model._k_states, &model._k_states, &alpha, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._tmp0, &kfilter.k_states)

            # : Nt_inf2 += ZiTZi * F2
            blas.caxpy(&kfilter.k_states2, &F2, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc)
            # : Nt_inf1 += ZiTZi * F1
            blas.caxpy(&kfilter.k_states2, &F1, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc)

            # Nt[:] = np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

        # F_{\infty, i, i, t} = 0
        elif not forecast_error_cov == 0:
            # Update
            # Note: Koopman and Durbin (2003) subtract the latter term,
            # but they use a different definition of the smoothing
            # recursion in general than the book Durbin and Koopman; this
            # version is consistent with the definition of the smoothing
            # recursion in the book.
            # rt[:] = Zi.T[:, 0] * v[i] / F[i, i] + np.dot(L0.T, rt)
            blas.ccopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.ccopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator, &inc)
            scalar = forecast_error_cov_inv * kfilter._forecast_error[i]
            blas.cgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_estimator, &inc)
            # rt_inf[:] = rt_inf[:]

            # Nt[:] = ZiTZi / F[i, i] + np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            blas.cgeru(&model._k_states, &model._k_states, &forecast_error_cov_inv, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            # Nt_inf1[:] = np.dot(Nt_inf1, L0)
            blas.ccopy(&model._k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc, smoother._tmp0, &inc)
            blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Nt_inf2[:] = Nt_inf2

    # Finalize the cumulated L0 and L1 by premultiplying by T
    # (and put them into kfilter.tmpL0 and kfilter.tmpL1)
    blas.ccopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL0, &kfilter.k_states)
    blas.ccopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL1, &kfilter.k_states)


cdef int csmoothed_estimators_time_univariate_diffuse(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i, j, inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
        np.complex64_t scalar
        int k_states = model._k_states

    if smoother.t == 0:
        return 1

    # TODO check that this is the right transition matrix to use in the case
    # of time-varying matrices
    # rt1[:] = np.dot(T1.T, rt)
    blas.cgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_estimator[0, smoother.t-1], &inc)
    # rt1_inf[:] = np.dot(T1.T, rt_inf)
    blas.cgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_diffuse_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_diffuse_estimator[0, smoother.t-1], &inc)

    # Nt1 = np.dot(np.dot(T1.T, Nt1), T1)
    blas.ccopy(&kfilter.k_states2, smoother._scaled_smoothed_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf1 = np.dot(np.dot(T1.T, Nt1_inf1), T1)
    blas.ccopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf2 = np.dot(np.dot(T1.T, Nt1_inf2), T1)
    blas.ccopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.cgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)

cdef int csmoothed_state_univariate_diffuse(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i, j, inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
        np.complex64_t scalar
        int k_states = model._k_states

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # alpha_hat[:] = a_t + np.dot(P_t, rt) + np.dot(P_t_inf, rt_inf)
        blas.ccopy(&kfilter.k_states, &kfilter.predicted_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)
        blas.cgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)
        blas.cgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # V[:] = P_t - np.dot(np.dot(P_t, Nt), P_t) - np.dot(np.dot(P_t, Nt_inf1), P_t_inf) - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        # : P_t [ I - N_t P_t]
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)
        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T
        # [or]
        # : - np.dot(np.dot(P_t, Nt_inf1.T), P_t_inf)
        blas.cgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.cgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int csmoothed_disturbances_univariate_diffuse(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    # Note: this only differs from the conventional version in the
    # definition of the smoothed measurement disturbance and cov
    cdef int i, j
    cdef:
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
        np.complex64_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, obs_cov

    # Temporary arrays

    # $\\#_0 = R_t Q_t$
    # $(m \times r) = (m \times r) (r \times r)$
    blas.cgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
              &alpha, model._selection, &model._k_states,
                      model._state_cov, &model._k_posdef,
              &beta, smoother._tmp0, &kfilter.k_states)

    for i in range(model._k_endog):
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]
        obs_cov = model._obs_cov[i + i*model._k_endog]

        # Smoothed measurement disturbance
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance[i] = -obs_cov * smoother._smoothed_measurement_disturbance[i]
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance[i] = obs_cov * (
                    kfilter._forecast_error[i] / forecast_error_cov - smoother._smoothed_measurement_disturbance[i])
            else:
                smoother._smoothed_measurement_disturbance[i] = 0

        # Smoothed measurement disturbance covariance matrix
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog]))
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * (1. / forecast_error_cov + smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog])))
            else:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = obs_cov

    # Smoothed state disturbance
    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        blas.cgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              smoother._input_scaled_smoothed_estimator, &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)

    # Smoothed state disturbance covariance matrix
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        blas.cgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        blas.ccopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.cgemm("T", "N", &kfilter.k_posdef, &kfilter.k_posdef, &kfilter.k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

cdef int csmoothed_state_autocov_univariate_diffuse(cKalmanSmoother smoother, cKalmanFilter kfilter, cStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex64_t alpha = 1.0
        np.complex64_t beta = 0.0
        np.complex64_t gamma = -1.0
    # The below code was a test to see if the diffuse autocov could be
    # relatively simply computed (i.e. which required certain terms to cancel
    # out), but it appears that it cannot (i.e. it requires the computation of
    # L2, which is nontrivial and not described by Durbin and Koopman).
    return 0

    # -P_t1 N0t -> tmp0
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # - P_t1_inf N2t - P_t1 N0t -> tmpL
    blas.ccopy(&kfilter.k_states2, smoother._tmp0, &inc, smoother._tmpL, &inc)
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states)

    # I - P_t1_inf N1t - P_t1 N0t -> tmp0
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states)
    for i in range(kfilter.k_states):
        smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]

    # L0t P_t -> tmpL2
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmpL2, &kfilter.k_states)
    # L0t P_t + L1t P_t_inf -> tmpL2
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &alpha, smoother._tmpL2, &kfilter.k_states)

    # [I - P_t1_inf N1t - P_t1 N0t] (L1t P_t_inf + L0t P_t) -> smoothed_state_autocov
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL2, &kfilter.k_states,
                  &beta, smoother._smoothed_state_autocov, &kfilter.k_states)

    # L0t P_t_inf -> tmp0
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N2t - P_t1 N0t] L0t P_t_inf +-> smoothed_state_autocov
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

    # [- P_t1_inf N0t ] -> tmpL
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

    # L1t P_t -> tmp0
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N0t ] L1t P_t +-> smoothed_state_autocov
    blas.cgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

# ### Univariate diffuse Kalman smoother
#
# See Durbin and Koopman (2012) Chapter 5.3

cdef int zsmoothed_estimators_measurement_univariate_diffuse(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model) except *:
    cdef:
        int i, j, inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
        np.complex128_t scalar
        int k_states = model._k_states
        np.complex128_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, F1, F12, F2

    # Adjust for a VAR transition (i.e. design = [#, 0], where the zeros
    # correspond to all states except the first k_posdef states)
    if model.subset_design:
        k_states = model._k_posdef

    # Need to clear out the scaled_smoothed_estimator and
    # scaled_smoothed_estimator_cov in case we're re-running the filter
    if smoother.t == model.nobs - 1:
        smoother.scaled_smoothed_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_estimator_cov[:, :, model.nobs-1] = 0

        smoother.scaled_smoothed_diffuse_estimator[:, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse1_estimator_cov[:, :, model.nobs-1] = 0
        smoother.scaled_smoothed_diffuse2_estimator_cov[:, :, model.nobs-1] = 0

    # Smoothing error
    # (not used in the univariate approach)

    # Given r_{t,0}:
    # calculate r_{t-1,p}, ..., r_{t-1, 0} and N_{t-1,p}, ..., N_{t-1,0}

    # Clear temporary arrays used for cumulation of L0 and L1
    smoother.tmpL[:] = 0
    smoother.tmpL2[:] = 0
    for i in range(kfilter.k_states):
        smoother.tmpL[i, i] = 1
        smoother.tmpL2[i, i] = 1

    # Iterate
    for i in range(kfilter.k_endog-1,-1,-1):
        # Forecast error covariance and diffuse forecast error covariance
        # F_{*,t}
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        # F_{\infty,t}
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]

        # Intermediate computations (must be done first so we can set the
        # measurement disturbance variables correctly)
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            forecast_error_diffuse_cov_inv = 1.0 / forecast_error_diffuse_cov

            F1 = forecast_error_diffuse_cov_inv
            # Usually F2 = -forecast_error_cov * forecast_error_diffuse_cov_inv**2
            # but this version is more convenient for the *axpy call
            F12 = -forecast_error_cov * forecast_error_diffuse_cov_inv
            F2 = F12 * forecast_error_diffuse_cov_inv

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
        elif not forecast_error_cov == 0:
            forecast_error_cov_inv = 1.0 / forecast_error_cov

            # K0 = M[:, i:i+1] / F[i, i]
            blas.zcopy(&kfilter.k_states, &kfilter._M[i*kfilter.k_states], &inc, kfilter._tmpK0, &inc)
            blas.zscal(&kfilter.k_states, &forecast_error_cov_inv, kfilter._tmpK0, &inc)

            # L0 = np.eye(m) - np.dot(K0, Zi)
            kfilter.tmpL0[:] = 0
            blas.zgeru(&model._k_states, &model._k_states, &gamma, kfilter._tmpK0, &inc, &model._design[i], &model._k_endog, kfilter._tmpL0, &kfilter.k_states)
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = kfilter._tmpL0[j + j*kfilter.k_states] + 1
        else:
            kfilter.tmpK0[:] = 0
            kfilter.tmpL0[:] = 0
            for j in range(kfilter.k_states):
                kfilter._tmpL0[j + j*kfilter.k_states] = 1

        # Cumulate L0 and L1
        blas.zcopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          kfilter._tmpL0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            blas.zcopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &beta, smoother._tmpL2, &kfilter.k_states)

        # Store values for measurement disturbance smoothing
        # np.dot(K0.T, rt)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            # Note: zdot and cdot are broken, so have to use gemv for those
            blas.zgemv("N", &inc, &model._k_states,
                           &alpha, smoother._scaled_smoothed_estimator, &inc,
                                   kfilter._tmpK0, &inc,
                           &beta, &smoother._smoothed_measurement_disturbance[i], &inc)

        # Store values for measurement disturbance covariance smoothing
        # np.dot(np.dot(K0.T, Nt), K0)
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            blas.zgemv("N", &model._k_states, &model._k_states,
                                     &alpha, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                             kfilter._tmpK0, &inc,
                                     &beta, smoother._tmp0, &inc)
            # Note: zdot and cdot are broken, so have to use gemv for those
            blas.zgemv("N", &inc, &model._k_states,
                           &alpha, smoother._tmp0, &inc,
                                   kfilter._tmpK0, &inc,
                           &beta, &smoother._smoothed_measurement_disturbance_cov[i*kfilter.k_endog + i], &inc)

        # F_{\infty, i, i, t} > 0
        if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
            # Update
            # rt_inf[:] = Zi.T[:, 0] * v[i] * F1 + np.dot(L0.T, rt_inf) + np.dot(L1.T, rt)
            blas.zcopy(&model._k_states, smoother._scaled_smoothed_diffuse_estimator, &inc, smoother._tmp0, &inc)
            blas.zcopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_diffuse_estimator, &inc)
            scalar = F1 * kfilter._forecast_error[i]
            blas.zgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_diffuse_estimator, &inc)

            blas.zgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL1, &kfilter.k_states,
                    smoother._scaled_smoothed_estimator, &inc,
                &alpha, smoother._scaled_smoothed_diffuse_estimator, &inc)

            # rt[:] = np.dot(L0.T, rt)
            blas.zcopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.zgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &beta, smoother._scaled_smoothed_estimator, &inc)

            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            # Nt_inf2[:] = ZiTZi * F2 + np.dot(np.dot(L0.T, Nt_inf2), L0) + np.dot(np.dot(L0.T, Nt_inf1), L1) + np.dot(np.dot(L1.T, Nt_inf1.T), L0) + np.dot(np.dot(L1.T, Nt), L1)
            #  : np.dot(L0.T, Nt_inf2) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt_inf1.T) -> tmp0
            blas.zgemm("T", "T", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL1, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F2 to Nt_inf2, which is done below.

            # Nt_inf1[:] = ZiTZi * F1 + np.dot(np.dot(L0.T, Nt_inf1), L0) + np.dot(np.dot(L1.T, Nt), L0)
            #  : np.dot(L0.T, Nt_inf1) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            #  : np.dot(L1.T, Nt) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL1, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # #  : np.dot(L0.T, Nt) -> tmp0
            # blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, kfilter._tmpL0, &kfilter.k_states,
            #                   smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
            #           &beta, smoother._tmp0, &kfilter.k_states)
            # #  : np.dot(tmp0, L1) -> scaled_smoothed_diffuse2_estimator_cov
            # blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
            #           &alpha, smoother._tmp0, &kfilter.k_states,
            #                   kfilter._tmpL1, &kfilter.k_states,
            #           &alpha, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Note: still need to add ZiTZi * F1 to Nt_inf1, which is done below.

            # Now add in ZiTZi
            # ZiTZi
            smoother.tmp0[:] = 0
            blas.zgeru(&model._k_states, &model._k_states, &alpha, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._tmp0, &kfilter.k_states)

            # : Nt_inf2 += ZiTZi * F2
            blas.zaxpy(&kfilter.k_states2, &F2, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc)
            # : Nt_inf1 += ZiTZi * F1
            blas.zaxpy(&kfilter.k_states2, &F1, smoother._tmp0, &inc, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc)

            # Nt[:] = np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)

        # F_{\infty, i, i, t} = 0
        elif not forecast_error_cov == 0:
            # Update
            # Note: Koopman and Durbin (2003) subtract the latter term,
            # but they use a different definition of the smoothing
            # recursion in general than the book Durbin and Koopman; this
            # version is consistent with the definition of the smoothing
            # recursion in the book.
            # rt[:] = Zi.T[:, 0] * v[i] / F[i, i] + np.dot(L0.T, rt)
            blas.zcopy(&model._k_states, smoother._scaled_smoothed_estimator, &inc, smoother._tmp0, &inc)
            blas.zcopy(&model._k_states, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator, &inc)
            scalar = forecast_error_cov_inv * kfilter._forecast_error[i]
            blas.zgemv("T", &model._k_states, &model._k_states,
                &alpha, kfilter._tmpL0, &kfilter.k_states,
                    smoother._tmp0, &inc,
                &scalar, smoother._scaled_smoothed_estimator, &inc)
            # rt_inf[:] = rt_inf[:]

            # Nt[:] = ZiTZi / F[i, i] + np.dot(np.dot(L0.T, Nt), L0)
            #  : np.dot(L0.T, Nt) -> tmp0
            blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, kfilter._tmpL0, &kfilter.k_states,
                              smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &beta, smoother._tmp0, &kfilter.k_states)
            #  : np.dot(tmp0, L0) -> scaled_smoothed_diffuse2_estimator_cov
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            blas.zgeru(&model._k_states, &model._k_states, &forecast_error_cov_inv, &model._design[i], &model._k_endog, &model._design[i], &model._k_endog, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states)
            # Nt_inf1[:] = np.dot(Nt_inf1, L0)
            blas.zcopy(&model._k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc, smoother._tmp0, &inc)
            blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              kfilter._tmpL0, &kfilter.k_states,
                      &beta, smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states)
            # Nt_inf2[:] = Nt_inf2

    # Finalize the cumulated L0 and L1 by premultiplying by T
    # (and put them into kfilter.tmpL0 and kfilter.tmpL1)
    blas.zcopy(&kfilter.k_states2, smoother._tmpL, &inc, smoother._tmp0, &inc)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL0, &kfilter.k_states)
    blas.zcopy(&kfilter.k_states2, smoother._tmpL2, &inc, smoother._tmp0, &inc)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                      &alpha, model._transition, &kfilter.k_states,
                              smoother._tmp0, &kfilter.k_states,
                      &beta, kfilter._tmpL1, &kfilter.k_states)


cdef int zsmoothed_estimators_time_univariate_diffuse(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i, j, inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
        np.complex128_t scalar
        int k_states = model._k_states

    if smoother.t == 0:
        return 1

    # TODO check that this is the right transition matrix to use in the case
    # of time-varying matrices
    # rt1[:] = np.dot(T1.T, rt)
    blas.zgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_estimator[0, smoother.t-1], &inc)
    # rt1_inf[:] = np.dot(T1.T, rt_inf)
    blas.zgemv("T", &model._k_states, &model._k_states,
                             &alpha, model._transition, &model._k_states,
                                     smoother._scaled_smoothed_diffuse_estimator, &inc,
                             &beta, &smoother.scaled_smoothed_diffuse_estimator[0, smoother.t-1], &inc)

    # Nt1 = np.dot(np.dot(T1.T, Nt1), T1)
    blas.zcopy(&kfilter.k_states2, smoother._scaled_smoothed_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf1 = np.dot(np.dot(T1.T, Nt1_inf1), T1)
    blas.zcopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse1_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse1_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)
    # Nt1_inf2 = np.dot(np.dot(T1.T, Nt1_inf2), T1)
    blas.zcopy(&kfilter.k_states2, smoother._scaled_smoothed_diffuse2_estimator_cov, &inc,
                                             &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &inc)
    blas.zgemm("T", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, model._transition, &model._k_states,
                                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                                  &beta, smoother._tmp0, &kfilter.k_states)
    blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                                  &alpha, smoother._tmp0, &kfilter.k_states,
                                          model._transition, &model._k_states,
                                  &beta, &smoother.scaled_smoothed_diffuse2_estimator_cov[0, 0, smoother.t-1], &kfilter.k_states)

cdef int zsmoothed_state_univariate_diffuse(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i, j, inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
        np.complex128_t scalar
        int k_states = model._k_states

    # Smoothed state
    if smoother.smoother_output & SMOOTHER_STATE:
        # alpha_hat[:] = a_t + np.dot(P_t, rt) + np.dot(P_t_inf, rt_inf)
        blas.zcopy(&kfilter.k_states, &kfilter.predicted_state[0,smoother.t], &inc, smoother._smoothed_state, &inc)
        blas.zgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)
        blas.zgemv("N", &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse_estimator, &inc,
                  &alpha, smoother._smoothed_state, &inc)

    # Smoothed state covariance
    if smoother.smoother_output & SMOOTHER_STATE_COV:
        # V[:] = P_t - np.dot(np.dot(P_t, Nt), P_t) - np.dot(np.dot(P_t, Nt_inf1), P_t_inf) - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        # : P_t [ I - N_t P_t]
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &gamma, smoother._scaled_smoothed_estimator_cov, &kfilter.k_states,
                      &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
              &beta, smoother._tmp0, &kfilter.k_states)
        for i in range(kfilter.k_states):
            smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
              &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                      smoother._tmp0, &kfilter.k_states,
              &beta, smoother._smoothed_state_cov, &kfilter.k_states)
        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # : - np.dot(np.dot(P_t_inf, Nt_inf1), P_t).T
        # [or]
        # : - np.dot(np.dot(P_t, Nt_inf1.T), P_t_inf)
        blas.zgemm("N", "T", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

        # - np.dot(np.dot(P_t_inf, Nt_inf2), P_t_inf)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &alpha, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                          smoother._scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)
        blas.zgemm("N", "N", &model._k_states, &model._k_states, &model._k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0,0,smoother.t], &kfilter.k_states,
                  &alpha, smoother._smoothed_state_cov, &kfilter.k_states)

cdef int zsmoothed_disturbances_univariate_diffuse(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    # Note: this only differs from the conventional version in the
    # definition of the smoothed measurement disturbance and cov
    cdef int i, j
    cdef:
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
        np.complex128_t forecast_error_cov, forecast_error_cov_inv, forecast_error_diffuse_cov, forecast_error_diffuse_cov_inv, obs_cov

    # Temporary arrays

    # $\\#_0 = R_t Q_t$
    # $(m \times r) = (m \times r) (r \times r)$
    blas.zgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_posdef,
              &alpha, model._selection, &model._k_states,
                      model._state_cov, &model._k_posdef,
              &beta, smoother._tmp0, &kfilter.k_states)

    for i in range(model._k_endog):
        forecast_error_cov = kfilter._forecast_error_cov[i + i*kfilter.k_endog]
        forecast_error_diffuse_cov = kfilter._forecast_error_diffuse_cov[i + i*kfilter.k_endog]
        obs_cov = model._obs_cov[i + i*model._k_endog]

        # Smoothed measurement disturbance
        if smoother.smoother_output & SMOOTHER_DISTURBANCE:
            if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance[i] = -obs_cov * smoother._smoothed_measurement_disturbance[i]
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance[i] = obs_cov * (
                    kfilter._forecast_error[i] / forecast_error_cov - smoother._smoothed_measurement_disturbance[i])
            else:
                smoother._smoothed_measurement_disturbance[i] = 0

        # Smoothed measurement disturbance covariance matrix
        if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
            if zabs(forecast_error_diffuse_cov) > kfilter.tolerance_diffuse:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog]))
            elif not forecast_error_cov == 0:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = (
                    obs_cov * (1 - obs_cov * (1. / forecast_error_cov + smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog])))
            else:
                smoother._smoothed_measurement_disturbance_cov[i + i*kfilter.k_endog] = obs_cov

    # Smoothed state disturbance
    if smoother.smoother_output & SMOOTHER_DISTURBANCE:
        blas.zgemv("T", &model._k_states, &model._k_posdef,
                      &alpha, smoother._tmp0, &kfilter.k_states,
                              smoother._input_scaled_smoothed_estimator, &inc,
                      &beta, smoother._smoothed_state_disturbance, &inc)

    # Smoothed state disturbance covariance matrix
    if smoother.smoother_output & SMOOTHER_DISTURBANCE_COV:
        blas.zgemm("N", "N", &model._k_states, &model._k_posdef, &model._k_states,
                  &alpha, smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)
        blas.zcopy(&model._k_posdef2, model._state_cov, &inc, smoother._smoothed_state_disturbance_cov, &inc)
        blas.zgemm("T", "N", &kfilter.k_posdef, &kfilter.k_posdef, &kfilter.k_states,
                  &gamma, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_disturbance_cov, &kfilter.k_posdef)

cdef int zsmoothed_state_autocov_univariate_diffuse(zKalmanSmoother smoother, zKalmanFilter kfilter, zStatespace model):
    cdef:
        int i
        int inc = 1
        np.complex128_t alpha = 1.0
        np.complex128_t beta = 0.0
        np.complex128_t gamma = -1.0
    # The below code was a test to see if the diffuse autocov could be
    # relatively simply computed (i.e. which required certain terms to cancel
    # out), but it appears that it cannot (i.e. it requires the computation of
    # L2, which is nontrivial and not described by Durbin and Koopman).
    return 0

    # -P_t1 N0t -> tmp0
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # - P_t1_inf N2t - P_t1 N0t -> tmpL
    blas.zcopy(&kfilter.k_states2, smoother._tmp0, &inc, smoother._tmpL, &inc)
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse2_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states)

    # I - P_t1_inf N1t - P_t1 N0t -> tmp0
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_diffuse1_estimator_cov, &kfilter.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states)
    for i in range(kfilter.k_states):
        smoother.tmp0[i,i] = 1 + smoother.tmp0[i,i]

    # L0t P_t -> tmpL2
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmpL2, &kfilter.k_states)
    # L0t P_t + L1t P_t_inf -> tmpL2
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &alpha, smoother._tmpL2, &kfilter.k_states)

    # [I - P_t1_inf N1t - P_t1 N0t] (L1t P_t_inf + L0t P_t) -> smoothed_state_autocov
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmp0, &kfilter.k_states,
                          smoother._tmpL2, &kfilter.k_states,
                  &beta, smoother._smoothed_state_autocov, &kfilter.k_states)

    # L0t P_t_inf -> tmp0
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL0, &kfilter.k_states,
                          &kfilter.predicted_diffuse_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N2t - P_t1 N0t] L0t P_t_inf +-> smoothed_state_autocov
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)

    # [- P_t1_inf N0t ] -> tmpL
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &gamma, &kfilter.predicted_diffuse_state_cov[0,0,smoother.t+1], &kfilter.k_states,
                          smoother._input_scaled_smoothed_estimator_cov, &kfilter.k_states,
                  &beta, smoother._tmpL, &kfilter.k_states)

    # L1t P_t -> tmp0
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, kfilter._tmpL1, &kfilter.k_states,
                          &kfilter.predicted_state_cov[0, 0, smoother.t], &kfilter.k_states,
                  &beta, smoother._tmp0, &kfilter.k_states)

    # [- P_t1_inf N0t ] L1t P_t +-> smoothed_state_autocov
    blas.zgemm("N", "N", &model.k_states, &model.k_states, &model.k_states,
                  &alpha, smoother._tmpL, &kfilter.k_states,
                          smoother._tmp0, &kfilter.k_states,
                  &alpha, smoother._smoothed_state_autocov, &kfilter.k_states)
