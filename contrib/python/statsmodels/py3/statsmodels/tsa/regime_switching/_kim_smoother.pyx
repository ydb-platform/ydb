#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
Kim smoother

Author: Chad Fulton  
License: Simplified-BSD
"""

# Typical imports
import numpy as np
import warnings
cimport numpy as np
cimport cython
from statsmodels.src.math cimport dlog, zlog, dexp, zexp

cdef int FORTRAN = 1


cpdef skim_smoother_log(int nobs, int k_regimes, int order,
                             np.float32_t [:, :, :] regime_transition,
                             np.float32_t [:, :] predicted_joint_probabilities,
                             np.float32_t [:, :] filtered_joint_probabilities,
                             np.float32_t [:, :] smoothed_joint_probabilities):
    cdef int t, i, j, k, ix, regime_transition_t = 0, time_varying_regime_transition
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
        np.float32_t [:] tmp_joint_probabilities, tmp_probabilities_fraction

    time_varying_regime_transition = regime_transition.shape[2] > 1
    tmp_joint_probabilities = np.zeros(k_regimes_order_p2, dtype=np.float32)
    tmp_probabilities_fraction = np.zeros(k_regimes_order_p1, dtype=np.float32)

    # S_T, S_{T-1}, ..., S_{T-r} | T
    smoothed_joint_probabilities[:, nobs-1] = filtered_joint_probabilities[:, nobs-1]

    with nogil:
        for t in range(nobs - 2, -1, -1):
            if time_varying_regime_transition:
                regime_transition_t = t + 1

            skim_smoother_log_iteration(t, k_regimes, order,
                                             tmp_joint_probabilities,
                                             tmp_probabilities_fraction,
                                             regime_transition[:, :, regime_transition_t],
                                             predicted_joint_probabilities[:, t+1],
                                             filtered_joint_probabilities[:, t],
                                             smoothed_joint_probabilities[:, t+1],
                                             smoothed_joint_probabilities[:, t])


cdef void skim_smoother_log_iteration(int tt, int k_regimes, int order,
                             np.float32_t [:] tmp_joint_probabilities,
                             np.float32_t [:] tmp_probabilities_fraction,
                             np.float32_t [:, :] regime_transition,
                             np.float32_t [:] predicted_joint_probabilities,
                             np.float32_t [:] filtered_joint_probabilities,
                             np.float32_t [:] prev_smoothed_joint_probabilities,
                             np.float32_t [:] next_smoothed_joint_probabilities) noexcept nogil:
    cdef int t, i, j, k, ix
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
    cdef np.float64_t tmp_max_real
    cdef np.float32_t tmp_max

    # Pr[S_{t+1}, S_t, ..., S_{t-r+1} | t] = Pr[S_{t+1} | S_t] * Pr[S_t, ..., S_{t-r+1} | t]
    ix = 0
    for i in range(k_regimes):
        for j in range(k_regimes):
            for k in range(k_regimes_order):
                tmp_joint_probabilities[ix] = (
                    filtered_joint_probabilities[j * k_regimes_order + k] +
                    regime_transition[i, j])
                ix += 1

    # S_{t+1}, S_t, ..., S_{t-r+2} | T / S_{t+1}, S_t, ..., S_{t-r+2} | t
    for i in range(k_regimes_order_p1):
        # TODO: do I need to worry about some value for predicted_joint_probabilities?
        tmp_probabilities_fraction[i] = (
            prev_smoothed_joint_probabilities[i] -
            predicted_joint_probabilities[i])

    # S_{t+1}, S_t, ..., S_{t-r+1} | T
    ix = 0
    for i in range(k_regimes_order_p1):
        for j in range(k_regimes):
            tmp_joint_probabilities[ix] = (
                tmp_probabilities_fraction[i] +
                tmp_joint_probabilities[ix])
            ix = ix + 1

    for i in range(k_regimes_order_p1):
        tmp_max_real = tmp_joint_probabilities[i]
        tmp_max = tmp_joint_probabilities[i]

        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            if tmp_joint_probabilities[ix] > tmp_max_real:
                tmp_max_real = tmp_joint_probabilities[ix]
                tmp_max = tmp_joint_probabilities[ix]

        next_smoothed_joint_probabilities[i] = 0
        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            next_smoothed_joint_probabilities[i] = (
                next_smoothed_joint_probabilities[i] +
                dexp(tmp_joint_probabilities[ix] - tmp_max))

        next_smoothed_joint_probabilities[i] = (tmp_max +
              dlog(next_smoothed_joint_probabilities[i]))


cpdef dkim_smoother_log(int nobs, int k_regimes, int order,
                             np.float64_t [:, :, :] regime_transition,
                             np.float64_t [:, :] predicted_joint_probabilities,
                             np.float64_t [:, :] filtered_joint_probabilities,
                             np.float64_t [:, :] smoothed_joint_probabilities):
    cdef int t, i, j, k, ix, regime_transition_t = 0, time_varying_regime_transition
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
        np.float64_t [:] tmp_joint_probabilities, tmp_probabilities_fraction

    time_varying_regime_transition = regime_transition.shape[2] > 1
    tmp_joint_probabilities = np.zeros(k_regimes_order_p2, dtype=float)
    tmp_probabilities_fraction = np.zeros(k_regimes_order_p1, dtype=float)

    # S_T, S_{T-1}, ..., S_{T-r} | T
    smoothed_joint_probabilities[:, nobs-1] = filtered_joint_probabilities[:, nobs-1]

    with nogil:
        for t in range(nobs - 2, -1, -1):
            if time_varying_regime_transition:
                regime_transition_t = t + 1

            dkim_smoother_log_iteration(t, k_regimes, order,
                                             tmp_joint_probabilities,
                                             tmp_probabilities_fraction,
                                             regime_transition[:, :, regime_transition_t],
                                             predicted_joint_probabilities[:, t+1],
                                             filtered_joint_probabilities[:, t],
                                             smoothed_joint_probabilities[:, t+1],
                                             smoothed_joint_probabilities[:, t])


cdef void dkim_smoother_log_iteration(int tt, int k_regimes, int order,
                             np.float64_t [:] tmp_joint_probabilities,
                             np.float64_t [:] tmp_probabilities_fraction,
                             np.float64_t [:, :] regime_transition,
                             np.float64_t [:] predicted_joint_probabilities,
                             np.float64_t [:] filtered_joint_probabilities,
                             np.float64_t [:] prev_smoothed_joint_probabilities,
                             np.float64_t [:] next_smoothed_joint_probabilities) noexcept nogil:
    cdef int t, i, j, k, ix
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
    cdef np.float64_t tmp_max_real
    cdef np.float64_t tmp_max

    # Pr[S_{t+1}, S_t, ..., S_{t-r+1} | t] = Pr[S_{t+1} | S_t] * Pr[S_t, ..., S_{t-r+1} | t]
    ix = 0
    for i in range(k_regimes):
        for j in range(k_regimes):
            for k in range(k_regimes_order):
                tmp_joint_probabilities[ix] = (
                    filtered_joint_probabilities[j * k_regimes_order + k] +
                    regime_transition[i, j])
                ix += 1

    # S_{t+1}, S_t, ..., S_{t-r+2} | T / S_{t+1}, S_t, ..., S_{t-r+2} | t
    for i in range(k_regimes_order_p1):
        # TODO: do I need to worry about some value for predicted_joint_probabilities?
        tmp_probabilities_fraction[i] = (
            prev_smoothed_joint_probabilities[i] -
            predicted_joint_probabilities[i])

    # S_{t+1}, S_t, ..., S_{t-r+1} | T
    ix = 0
    for i in range(k_regimes_order_p1):
        for j in range(k_regimes):
            tmp_joint_probabilities[ix] = (
                tmp_probabilities_fraction[i] +
                tmp_joint_probabilities[ix])
            ix = ix + 1

    for i in range(k_regimes_order_p1):
        tmp_max_real = tmp_joint_probabilities[i]
        tmp_max = tmp_joint_probabilities[i]

        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            if tmp_joint_probabilities[ix] > tmp_max_real:
                tmp_max_real = tmp_joint_probabilities[ix]
                tmp_max = tmp_joint_probabilities[ix]

        next_smoothed_joint_probabilities[i] = 0
        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            next_smoothed_joint_probabilities[i] = (
                next_smoothed_joint_probabilities[i] +
                dexp(tmp_joint_probabilities[ix] - tmp_max))

        next_smoothed_joint_probabilities[i] = (tmp_max +
              dlog(next_smoothed_joint_probabilities[i]))


cpdef ckim_smoother_log(int nobs, int k_regimes, int order,
                             np.complex64_t [:, :, :] regime_transition,
                             np.complex64_t [:, :] predicted_joint_probabilities,
                             np.complex64_t [:, :] filtered_joint_probabilities,
                             np.complex64_t [:, :] smoothed_joint_probabilities):
    cdef int t, i, j, k, ix, regime_transition_t = 0, time_varying_regime_transition
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
        np.complex64_t [:] tmp_joint_probabilities, tmp_probabilities_fraction

    time_varying_regime_transition = regime_transition.shape[2] > 1
    tmp_joint_probabilities = np.zeros(k_regimes_order_p2, dtype=np.complex64)
    tmp_probabilities_fraction = np.zeros(k_regimes_order_p1, dtype=np.complex64)

    # S_T, S_{T-1}, ..., S_{T-r} | T
    smoothed_joint_probabilities[:, nobs-1] = filtered_joint_probabilities[:, nobs-1]

    with nogil:
        for t in range(nobs - 2, -1, -1):
            if time_varying_regime_transition:
                regime_transition_t = t + 1

            ckim_smoother_log_iteration(t, k_regimes, order,
                                             tmp_joint_probabilities,
                                             tmp_probabilities_fraction,
                                             regime_transition[:, :, regime_transition_t],
                                             predicted_joint_probabilities[:, t+1],
                                             filtered_joint_probabilities[:, t],
                                             smoothed_joint_probabilities[:, t+1],
                                             smoothed_joint_probabilities[:, t])


cdef void ckim_smoother_log_iteration(int tt, int k_regimes, int order,
                             np.complex64_t [:] tmp_joint_probabilities,
                             np.complex64_t [:] tmp_probabilities_fraction,
                             np.complex64_t [:, :] regime_transition,
                             np.complex64_t [:] predicted_joint_probabilities,
                             np.complex64_t [:] filtered_joint_probabilities,
                             np.complex64_t [:] prev_smoothed_joint_probabilities,
                             np.complex64_t [:] next_smoothed_joint_probabilities) noexcept nogil:
    cdef int t, i, j, k, ix
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
    cdef np.float64_t tmp_max_real
    cdef np.complex64_t tmp_max

    # Pr[S_{t+1}, S_t, ..., S_{t-r+1} | t] = Pr[S_{t+1} | S_t] * Pr[S_t, ..., S_{t-r+1} | t]
    ix = 0
    for i in range(k_regimes):
        for j in range(k_regimes):
            for k in range(k_regimes_order):
                tmp_joint_probabilities[ix] = (
                    filtered_joint_probabilities[j * k_regimes_order + k] +
                    regime_transition[i, j])
                ix += 1

    # S_{t+1}, S_t, ..., S_{t-r+2} | T / S_{t+1}, S_t, ..., S_{t-r+2} | t
    for i in range(k_regimes_order_p1):
        # TODO: do I need to worry about some value for predicted_joint_probabilities?
        tmp_probabilities_fraction[i] = (
            prev_smoothed_joint_probabilities[i] -
            predicted_joint_probabilities[i])

    # S_{t+1}, S_t, ..., S_{t-r+1} | T
    ix = 0
    for i in range(k_regimes_order_p1):
        for j in range(k_regimes):
            tmp_joint_probabilities[ix] = (
                tmp_probabilities_fraction[i] +
                tmp_joint_probabilities[ix])
            ix = ix + 1

    for i in range(k_regimes_order_p1):
        tmp_max_real = tmp_joint_probabilities[i].real
        tmp_max = tmp_joint_probabilities[i]

        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            if tmp_joint_probabilities[ix].real > tmp_max_real:
                tmp_max_real = tmp_joint_probabilities[ix].real
                tmp_max = tmp_joint_probabilities[ix]

        next_smoothed_joint_probabilities[i] = 0
        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            next_smoothed_joint_probabilities[i] = (
                next_smoothed_joint_probabilities[i] +
                zexp(tmp_joint_probabilities[ix] - tmp_max))

        next_smoothed_joint_probabilities[i] = (tmp_max +
              zlog(next_smoothed_joint_probabilities[i]))


cpdef zkim_smoother_log(int nobs, int k_regimes, int order,
                             np.complex128_t [:, :, :] regime_transition,
                             np.complex128_t [:, :] predicted_joint_probabilities,
                             np.complex128_t [:, :] filtered_joint_probabilities,
                             np.complex128_t [:, :] smoothed_joint_probabilities):
    cdef int t, i, j, k, ix, regime_transition_t = 0, time_varying_regime_transition
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
        np.complex128_t [:] tmp_joint_probabilities, tmp_probabilities_fraction

    time_varying_regime_transition = regime_transition.shape[2] > 1
    tmp_joint_probabilities = np.zeros(k_regimes_order_p2, dtype=complex)
    tmp_probabilities_fraction = np.zeros(k_regimes_order_p1, dtype=complex)

    # S_T, S_{T-1}, ..., S_{T-r} | T
    smoothed_joint_probabilities[:, nobs-1] = filtered_joint_probabilities[:, nobs-1]

    with nogil:
        for t in range(nobs - 2, -1, -1):
            if time_varying_regime_transition:
                regime_transition_t = t + 1

            zkim_smoother_log_iteration(t, k_regimes, order,
                                             tmp_joint_probabilities,
                                             tmp_probabilities_fraction,
                                             regime_transition[:, :, regime_transition_t],
                                             predicted_joint_probabilities[:, t+1],
                                             filtered_joint_probabilities[:, t],
                                             smoothed_joint_probabilities[:, t+1],
                                             smoothed_joint_probabilities[:, t])


cdef void zkim_smoother_log_iteration(int tt, int k_regimes, int order,
                             np.complex128_t [:] tmp_joint_probabilities,
                             np.complex128_t [:] tmp_probabilities_fraction,
                             np.complex128_t [:, :] regime_transition,
                             np.complex128_t [:] predicted_joint_probabilities,
                             np.complex128_t [:] filtered_joint_probabilities,
                             np.complex128_t [:] prev_smoothed_joint_probabilities,
                             np.complex128_t [:] next_smoothed_joint_probabilities) noexcept nogil:
    cdef int t, i, j, k, ix
    cdef:
        int k_regimes_order_m1 = k_regimes**(order - 1)
        int k_regimes_order = k_regimes**order
        int k_regimes_order_p1 = k_regimes**(order + 1)
        int k_regimes_order_p2 = k_regimes**(order + 2)
    cdef np.float64_t tmp_max_real
    cdef np.complex128_t tmp_max

    # Pr[S_{t+1}, S_t, ..., S_{t-r+1} | t] = Pr[S_{t+1} | S_t] * Pr[S_t, ..., S_{t-r+1} | t]
    ix = 0
    for i in range(k_regimes):
        for j in range(k_regimes):
            for k in range(k_regimes_order):
                tmp_joint_probabilities[ix] = (
                    filtered_joint_probabilities[j * k_regimes_order + k] +
                    regime_transition[i, j])
                ix += 1

    # S_{t+1}, S_t, ..., S_{t-r+2} | T / S_{t+1}, S_t, ..., S_{t-r+2} | t
    for i in range(k_regimes_order_p1):
        # TODO: do I need to worry about some value for predicted_joint_probabilities?
        tmp_probabilities_fraction[i] = (
            prev_smoothed_joint_probabilities[i] -
            predicted_joint_probabilities[i])

    # S_{t+1}, S_t, ..., S_{t-r+1} | T
    ix = 0
    for i in range(k_regimes_order_p1):
        for j in range(k_regimes):
            tmp_joint_probabilities[ix] = (
                tmp_probabilities_fraction[i] +
                tmp_joint_probabilities[ix])
            ix = ix + 1

    for i in range(k_regimes_order_p1):
        tmp_max_real = tmp_joint_probabilities[i].real
        tmp_max = tmp_joint_probabilities[i]

        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            if tmp_joint_probabilities[ix].real > tmp_max_real:
                tmp_max_real = tmp_joint_probabilities[ix].real
                tmp_max = tmp_joint_probabilities[ix]

        next_smoothed_joint_probabilities[i] = 0
        for j in range(k_regimes):
            ix = j * k_regimes_order_p1 + i
            next_smoothed_joint_probabilities[i] = (
                next_smoothed_joint_probabilities[i] +
                zexp(tmp_joint_probabilities[ix] - tmp_max))

        next_smoothed_joint_probabilities[i] = (tmp_max +
              zlog(next_smoothed_joint_probabilities[i]))
