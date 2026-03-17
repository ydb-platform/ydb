#cython: profile=False
#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
"Cholesky Factor Algorithm" (CFA) simulation smoothing

TODO:

- Allow fewer simulations than nobs / unlimited simulations in time-varying
  case

References
----------
.. [*] McCausland, William J., Shirley Miller, and Denis Pelletier.
       "Simulation smoothing for state-space models: A computational
       efficiency analysis."
       Computational Statistics & Data Analysis 55, no. 1 (2011): 199-212.
.. [*] Chan, Joshua CC, and Ivan Jeliazkov.
       "Efficient simulation and integrated likelihood estimation in
       state space models."
       International Journal of Mathematical Modelling and Numerical
       Optimisation 1, no. 1-2 (2009): 101-120.

Author: Chad Fulton  
License: BSD-3
"""

# Typical imports
import numpy as np
import warnings
cimport numpy as np
cimport cython

np.import_array()

cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack
cimport statsmodels.tsa.statespace._tools as tools

cdef int FORTRAN = 1

cdef class sCFASimulationSmoother(object):
    """
    Notes
    -----
    Currently not implemented (but could be):

    - Diffuse initialization
    - Support for collapsed observation vector

    Cannot be implemented:

    - Degenerate initial state distribution
    - Degenerate observation shock vector
    - Degenerate state shock vector

    """

    def __init__(self, sStatespace model):
        cdef int inc = 1
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.model = model
        self.k_states = model.k_states
        self.k_states2 = self.k_states**2
        self.order = self.model.nobs * self.k_states
        self.lower_bandwidth = 2 * self.k_states - 1

        # Validate that our model is acceptable
        if self.model.nobs == 1:
            # TODO: this is probably easily fixable
            raise NotImplementedError('Cannot use CFA simulation smoothing'
                                      ' with a single observation.')

        # Posterior mean vector
        # Note: we'll define this as two-dimensional, although we'll want to
        # access it though it is a "one-dimensional" stacked vector at one
        # point. But that's not a problem given the column-major ordering here,
        # we can just pretend it's a one-dimensional array of length
        # k_states * nobs
        dim2[0] = self.k_states; dim2[1] = self.model.nobs;
        self.prior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self.posterior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

        # Sparse storage for Cholesky factor of posterior covariance matrix
        dim2[0] = self.lower_bandwidth + 1; dim2[1] = self.order;
        self.posterior_cov_inv_chol = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self.K = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

        # Intermediate computation arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.initial_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.model.k_endog;
        self.obs_cov_fac = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.selected_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

        # Temporary arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.QiT = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self.TQiT = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self.TQiTpQ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self.ZHiZ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.k_states;
        self.HiZ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim1[0] = self.model.k_endog;
        self.ymd = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT32, FORTRAN)

        # Pointers
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    def __reduce__(self):
        args = (self.model,)
        state = {'order': self.order,
                 'lower_bandwidth': self.lower_bandwidth,
                 'prior_mean': np.array(self.prior_mean, copy=True, order='F'),
                 'posterior_mean': np.array(self.posterior_mean, copy=True, order='F'),
                 'posterior_cov_inv_chol': np.array(self.posterior_cov_inv_chol, copy=True, order='F'),
                 'K': np.array(self.K, copy=True, order='F'),
                 'initial_state_cov_inv': np.array(self.initial_state_cov_inv, copy=True, order='F'),
                 'obs_cov_fac': np.array(self.obs_cov_fac, copy=True, order='F'),
                 'selected_state_cov_inv': np.array(self.selected_state_cov_inv, copy=True, order='F'),
                 'QiT': np.array(self.QiT, copy=True, order='F'),
                 'TQiT': np.array(self.TQiT, copy=True, order='F'),
                 'TQiTpQ': np.array(self.TQiTpQ, copy=True, order='F'),
                 'ZHiZ': np.array(self.ZHiZ, copy=True, order='F'),
                 'HiZ': np.array(self.HiZ, copy=True, order='F'),
                 'ymd': np.array(self.ymd, copy=True, order='F'),
                 }
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.order = state['order']
        self.lower_bandwidth = state['lower_bandwidth']
        self.prior_mean = state['prior_mean']
        self.posterior_mean = state['posterior_mean']
        self.posterior_cov_inv_chol = state['posterior_cov_inv_chol']
        self.K = state['K']
        self.initial_state_cov_inv = state['initial_state_cov_inv']
        self.obs_cov_fac = state['obs_cov_fac']
        self.selected_state_cov_inv = state['selected_state_cov_inv']
        self.QiT = state['QiT']
        self.TQiT = state['TQiT']
        self.TQiTpQ = state['TQiTpQ']
        self.ZHiZ = state['ZHiZ']
        self.HiZ = state['HiZ']
        self.ymd = state['ymd']
        self._reinitialize_pointers()

    cdef void _reinitialize_pointers(self) except *:
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    cpdef int update_sparse_posterior_moments(self) except *:
        # Update the computation of posterior moments for the entire state
        # vector, with the  covariance matrix represented in what Scipy calls
        # "lower diagonal ordered form" - see e.g. Scipy's documentation page
        # for linalg.cholesky_banded for details of the storage format
        cdef:
            int t, i, j, rows, ld, info
            int reset_missing = 0
            int inc = 1
            np.float32_t alpha = 1.0
            np.float32_t beta = 0.0
            np.float32_t gamma = -1.0
            int time_varying_obs_intercept = self.model.obs_intercept.shape[1] > 1
            int time_varying_design = self.model.design.shape[2] > 1
            int time_varying_obs_cov = self.model.obs_cov.shape[2] > 1
            int time_varying_state_intercept = self.model.state_intercept.shape[1] > 1
            int time_varying_transition = self.model.transition.shape[2] > 1
            int time_varying_selection = self.model.selection.shape[2] > 1
            int time_varying_state_cov = self.model.state_cov.shape[2] > 1
            int time_varying_selected_state_cov = time_varying_selection or time_varying_state_cov

        self.model.seek(0, False, False)

        # Invert initial state covariance matrix
        # (we actually need the inverse here)
        if self.model.initialized_diffuse:
            self.initial_state_cov_inv[:] = 0.
        else:
            blas.scopy(&self.k_states2, self.model._initial_state_cov, &inc, self._initial_state_cov_inv, &inc)
            lapack.spotrf("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)
            if info > 0:
                raise np.linalg.LinAlgError('Non-positive-definite initial state covariance matrix.')
            elif info < 0:
                raise np.linalg.LinAlgError('Invalid value in initial state covariance matrix.')
            lapack.spotri("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)

        # Iterate over periods
        j = 0  # this will be a column counter
        for t in range(self.model.nobs):
            # Update the model representation to the current time point
            self.model.seek(t, False, False)

            # Check if we need to reset arrays based on Z for missing data
            reset_missing = 0
            if t > 0:
                for i in range(self.model.k_endog):
                    reset_missing = reset_missing + (not self.model.missing[i,t] == self.model.missing[i, t - 1])

            # Invert selected_state_cov
            # (again, we actually need the inverse itself)
            if t == 0 or time_varying_selected_state_cov:
                blas.scopy(&self.k_states2, self.model._selected_state_cov, &inc, self._selected_state_cov_inv, &inc)
                lapack.spotrf("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)
                if info > 0:
                    raise np.linalg.LinAlgError('Non-positive-definite selected state covariance matrix encountered at period %d' % t)
                elif info < 0:
                    raise np.linalg.LinAlgError('Invalid value in selected state covariance matrix encountered at period %d' % t)
                lapack.spotri("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)

            # T(RQRi)T + (RQRi)
            if (t == 0 or time_varying_transition or
                    time_varying_selected_state_cov):
                # QiT = (RQRi) @ T
                blas.ssymm("L", "L", &self.k_states, &self.k_states,
                                    &alpha, self._selected_state_cov_inv, &self.k_states,
                                        self.model._transition, &self.k_states,
                                    &beta, self._QiT, &self.k_states)

                # TQiT = T.T @ QiT
                blas.sgemm("T", "N", &self.k_states, &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                        self._QiT, &self.k_states,
                                    &beta, self._TQiT, &self.k_states)

                # TQiTpQ = TQiT + RQRi
                # Note: there will not be correct entries in the upper triangle, because
                # selected_state_cov was only set in the lower triangle. However, that's
                # okay because we only reference the lower triangle, below
                blas.scopy(&self.k_states2, self._selected_state_cov_inv, &inc, self._TQiTpQ, &inc)
                blas.saxpy(&self.k_states2, &alpha, self._TQiT, &inc, self._TQiTpQ, &inc)

            # Z (Hi) Z
            if t == 0 or reset_missing or time_varying_design or time_varying_obs_cov:

                # Cholesky of obs_cov
                if t == 0 or reset_missing or time_varying_obs_cov:
                    blas.scopy(&self.model._k_endog2, self.model._obs_cov, &inc, self._obs_cov_fac, &inc)
                    lapack.spotrf("L", &self.model._k_endog, self._obs_cov_fac, &self.model._k_endog, &info)
                    if info > 0:
                        raise np.linalg.LinAlgError('Non-positive-definite observation covariance matrix encountered at period %d' % t)
                    elif info < 0:
                        raise np.linalg.LinAlgError('Invalid value in observation covariance matrix encountered at period %d' % t)

                # HiZ = Hi Z
                blas.scopy(&self.model._k_endogstates, self.model._design, &inc, self._HiZ, &inc)
                lapack.spotrs("L", &self.model._k_endog, &self.k_states,
                                       self._obs_cov_fac, &self.model._k_endog,
                                       self._HiZ, &self.model._k_endog, &info)

                # ZHiZ = Z.T @ HiZ
                blas.sgemm("T", "N", &self.k_states, &self.k_states, &self.model._k_endog,
                                    &alpha, self.model._design, &self.model._k_endog,
                                        self._HiZ, &self.model._k_endog,
                                    &beta, self._ZHiZ, &self.k_states)

            # Fill in K and posterior_cov_inv_chol (which is right now just
            # going to contain posterior_cov_inv, and then we will take the
            # sparse Cholesky after the loop), prior mean
            # Iterate over columns in this (*, k_states) "block" of K, P
            for i in range(self.k_states):
                rows = self.k_states - i

                if t < self.model.nobs - 1:
                    if t == 0:
                        # K[:q - i, col] = (TQiT + P0i)[i:, i]
                        blas.scopy(&rows, &self.TQiT[i, i], &inc, &self.K[0, j], &inc)
                        blas.saxpy(&rows, &alpha, &self.initial_state_cov_inv[i, i], &inc,
                                                            &self.K[0, j], &inc)
                    else:
                        # K[:q - i, col] = TQiTpQ[i:, i]
                        blas.scopy(&rows, &self.TQiTpQ[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.scopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.saxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)
                    # K[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.scopy(&self.k_states, &self.QiT[0, i], &inc, &self.K[rows, j], &inc)
                    blas.sscal(&self.k_states, &gamma, &self.K[rows, j], &inc)
                    # P[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.scopy(&self.k_states, &self.K[rows, j], &inc,
                                                               &self.posterior_cov_inv_chol[rows, j], &inc)

                # Last period is different
                elif t == self.model.nobs - 1:
                    # K[:q - i, col] = RQRi[i:, i]
                    blas.scopy(&rows, &self.selected_state_cov_inv[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.scopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.saxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)

                # Advance the column counter
                j = j + 1

            # Prior mean
            if t == 0:
                # prior_mean[:, 0] = initial_state
                blas.scopy(&self.k_states, self.model._initial_state, &inc, &self.prior_mean[0, 0], &inc)
            else:
                # prior_mean[:, t] = c[:, t] + T @ prior_mean[:, t-1]
                blas.scopy(&self.k_states, self.model._state_intercept, &inc, &self.prior_mean[0, t], &inc)
                blas.sgemv("N", &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                            &self.prior_mean[0, t - 1], &inc,
                                    &alpha, &self.prior_mean[0, t], &inc)

            # (Hi Z)' (y - d)
            if self.model.nmissing[t] == self.model.k_endog:
                self.posterior_mean[:, t] = 0
            else:
                blas.scopy(&self.model._k_endog, self.model._obs, &inc, &self.ymd[0], &inc)
                blas.saxpy(&self.model._k_endog, &gamma, self.model._obs_intercept, &inc, &self.ymd[0], &inc)
                blas.sgemv("T", &self.model._k_endog, &self.k_states,
                                         &alpha, self._HiZ, &self.model._k_endog,
                                                &self.ymd[0], &inc,
                                         &beta, &self.posterior_mean[0, t], &inc)

        # Orders of sparse matrices
        order = self.model.nobs * self.k_states
        ld = self.lower_bandwidth + 1

        # Compute the sparse Cholesky factor of posterior cov
        lapack.spbtrf("L", &self.order, &self.lower_bandwidth,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &info)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite joint posterior covariance matrix encountered.')
        elif info < 0:
            raise np.linalg.LinAlgError('Invalid value in joint posterior covariance matrix encountered.')

        # Compute the posterior mean
        # posterior_mean = P^{-1} (K prior_mean + ZHimd)
        blas.ssbmv("L", &self.order, &self.lower_bandwidth,
                            &alpha, self._K, &ld,
                                    &self.prior_mean[0, 0], &inc,
                            &alpha, &self.posterior_mean[0, 0], &inc)
        lapack.spbtrs("L", &self.order, &self.lower_bandwidth, &inc,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &self.posterior_mean[0, 0], &self.order, &info)

    cpdef simulate(self, variates=None):
        cdef int inc = 1
        cdef int ld = self.lower_bandwidth + 1
        cdef np.float32_t alpha = 1.0
        cdef np.float32_t [:] u

        # Sample u from N(0, I)
        if variates is None:
            u = np.random.normal(size=self.order).astype(np.float32)
        else:
            u = variates

            tools.validate_vector_shape('variates', &u.shape[0],
                                        self.order, None)

        # Solve L' x = u to get x \sim N(0, P^{-1})
        # (L = posterior_cov_inv_chol is lower triangular)
        blas.stbsv("L", "T", "N", &self.order, &self.lower_bandwidth,
                            &self.posterior_cov_inv_chol[0, 0], &ld,
                            &u[0], &inc)

        # Add in the posterior mean
        blas.saxpy(&self.order, &alpha, &self.posterior_mean[0, 0], &inc, &u[0], &inc)

        return np.array(u).reshape(self.model.nobs, self.k_states).T

cdef class dCFASimulationSmoother(object):
    """
    Notes
    -----
    Currently not implemented (but could be):

    - Diffuse initialization
    - Support for collapsed observation vector

    Cannot be implemented:

    - Degenerate initial state distribution
    - Degenerate observation shock vector
    - Degenerate state shock vector

    """

    def __init__(self, dStatespace model):
        cdef int inc = 1
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.model = model
        self.k_states = model.k_states
        self.k_states2 = self.k_states**2
        self.order = self.model.nobs * self.k_states
        self.lower_bandwidth = 2 * self.k_states - 1

        # Validate that our model is acceptable
        if self.model.nobs == 1:
            # TODO: this is probably easily fixable
            raise NotImplementedError('Cannot use CFA simulation smoothing'
                                      ' with a single observation.')

        # Posterior mean vector
        # Note: we'll define this as two-dimensional, although we'll want to
        # access it though it is a "one-dimensional" stacked vector at one
        # point. But that's not a problem given the column-major ordering here,
        # we can just pretend it's a one-dimensional array of length
        # k_states * nobs
        dim2[0] = self.k_states; dim2[1] = self.model.nobs;
        self.prior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self.posterior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

        # Sparse storage for Cholesky factor of posterior covariance matrix
        dim2[0] = self.lower_bandwidth + 1; dim2[1] = self.order;
        self.posterior_cov_inv_chol = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self.K = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

        # Intermediate computation arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.initial_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.model.k_endog;
        self.obs_cov_fac = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.selected_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

        # Temporary arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.QiT = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self.TQiT = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self.TQiTpQ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self.ZHiZ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.k_states;
        self.HiZ = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim1[0] = self.model.k_endog;
        self.ymd = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)

        # Pointers
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    def __reduce__(self):
        args = (self.model,)
        state = {'order': self.order,
                 'lower_bandwidth': self.lower_bandwidth,
                 'prior_mean': np.array(self.prior_mean, copy=True, order='F'),
                 'posterior_mean': np.array(self.posterior_mean, copy=True, order='F'),
                 'posterior_cov_inv_chol': np.array(self.posterior_cov_inv_chol, copy=True, order='F'),
                 'K': np.array(self.K, copy=True, order='F'),
                 'initial_state_cov_inv': np.array(self.initial_state_cov_inv, copy=True, order='F'),
                 'obs_cov_fac': np.array(self.obs_cov_fac, copy=True, order='F'),
                 'selected_state_cov_inv': np.array(self.selected_state_cov_inv, copy=True, order='F'),
                 'QiT': np.array(self.QiT, copy=True, order='F'),
                 'TQiT': np.array(self.TQiT, copy=True, order='F'),
                 'TQiTpQ': np.array(self.TQiTpQ, copy=True, order='F'),
                 'ZHiZ': np.array(self.ZHiZ, copy=True, order='F'),
                 'HiZ': np.array(self.HiZ, copy=True, order='F'),
                 'ymd': np.array(self.ymd, copy=True, order='F'),
                 }
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.order = state['order']
        self.lower_bandwidth = state['lower_bandwidth']
        self.prior_mean = state['prior_mean']
        self.posterior_mean = state['posterior_mean']
        self.posterior_cov_inv_chol = state['posterior_cov_inv_chol']
        self.K = state['K']
        self.initial_state_cov_inv = state['initial_state_cov_inv']
        self.obs_cov_fac = state['obs_cov_fac']
        self.selected_state_cov_inv = state['selected_state_cov_inv']
        self.QiT = state['QiT']
        self.TQiT = state['TQiT']
        self.TQiTpQ = state['TQiTpQ']
        self.ZHiZ = state['ZHiZ']
        self.HiZ = state['HiZ']
        self.ymd = state['ymd']
        self._reinitialize_pointers()

    cdef void _reinitialize_pointers(self) except *:
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    cpdef int update_sparse_posterior_moments(self) except *:
        # Update the computation of posterior moments for the entire state
        # vector, with the  covariance matrix represented in what Scipy calls
        # "lower diagonal ordered form" - see e.g. Scipy's documentation page
        # for linalg.cholesky_banded for details of the storage format
        cdef:
            int t, i, j, rows, ld, info
            int reset_missing = 0
            int inc = 1
            np.float64_t alpha = 1.0
            np.float64_t beta = 0.0
            np.float64_t gamma = -1.0
            int time_varying_obs_intercept = self.model.obs_intercept.shape[1] > 1
            int time_varying_design = self.model.design.shape[2] > 1
            int time_varying_obs_cov = self.model.obs_cov.shape[2] > 1
            int time_varying_state_intercept = self.model.state_intercept.shape[1] > 1
            int time_varying_transition = self.model.transition.shape[2] > 1
            int time_varying_selection = self.model.selection.shape[2] > 1
            int time_varying_state_cov = self.model.state_cov.shape[2] > 1
            int time_varying_selected_state_cov = time_varying_selection or time_varying_state_cov

        self.model.seek(0, False, False)

        # Invert initial state covariance matrix
        # (we actually need the inverse here)
        if self.model.initialized_diffuse:
            self.initial_state_cov_inv[:] = 0.
        else:
            blas.dcopy(&self.k_states2, self.model._initial_state_cov, &inc, self._initial_state_cov_inv, &inc)
            lapack.dpotrf("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)
            if info > 0:
                raise np.linalg.LinAlgError('Non-positive-definite initial state covariance matrix.')
            elif info < 0:
                raise np.linalg.LinAlgError('Invalid value in initial state covariance matrix.')
            lapack.dpotri("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)

        # Iterate over periods
        j = 0  # this will be a column counter
        for t in range(self.model.nobs):
            # Update the model representation to the current time point
            self.model.seek(t, False, False)

            # Check if we need to reset arrays based on Z for missing data
            reset_missing = 0
            if t > 0:
                for i in range(self.model.k_endog):
                    reset_missing = reset_missing + (not self.model.missing[i,t] == self.model.missing[i, t - 1])

            # Invert selected_state_cov
            # (again, we actually need the inverse itself)
            if t == 0 or time_varying_selected_state_cov:
                blas.dcopy(&self.k_states2, self.model._selected_state_cov, &inc, self._selected_state_cov_inv, &inc)
                lapack.dpotrf("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)
                if info > 0:
                    raise np.linalg.LinAlgError('Non-positive-definite selected state covariance matrix encountered at period %d' % t)
                elif info < 0:
                    raise np.linalg.LinAlgError('Invalid value in selected state covariance matrix encountered at period %d' % t)
                lapack.dpotri("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)

            # T(RQRi)T + (RQRi)
            if (t == 0 or time_varying_transition or
                    time_varying_selected_state_cov):
                # QiT = (RQRi) @ T
                blas.dsymm("L", "L", &self.k_states, &self.k_states,
                                    &alpha, self._selected_state_cov_inv, &self.k_states,
                                        self.model._transition, &self.k_states,
                                    &beta, self._QiT, &self.k_states)

                # TQiT = T.T @ QiT
                blas.dgemm("T", "N", &self.k_states, &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                        self._QiT, &self.k_states,
                                    &beta, self._TQiT, &self.k_states)

                # TQiTpQ = TQiT + RQRi
                # Note: there will not be correct entries in the upper triangle, because
                # selected_state_cov was only set in the lower triangle. However, that's
                # okay because we only reference the lower triangle, below
                blas.dcopy(&self.k_states2, self._selected_state_cov_inv, &inc, self._TQiTpQ, &inc)
                blas.daxpy(&self.k_states2, &alpha, self._TQiT, &inc, self._TQiTpQ, &inc)

            # Z (Hi) Z
            if t == 0 or reset_missing or time_varying_design or time_varying_obs_cov:

                # Cholesky of obs_cov
                if t == 0 or reset_missing or time_varying_obs_cov:
                    blas.dcopy(&self.model._k_endog2, self.model._obs_cov, &inc, self._obs_cov_fac, &inc)
                    lapack.dpotrf("L", &self.model._k_endog, self._obs_cov_fac, &self.model._k_endog, &info)
                    if info > 0:
                        raise np.linalg.LinAlgError('Non-positive-definite observation covariance matrix encountered at period %d' % t)
                    elif info < 0:
                        raise np.linalg.LinAlgError('Invalid value in observation covariance matrix encountered at period %d' % t)

                # HiZ = Hi Z
                blas.dcopy(&self.model._k_endogstates, self.model._design, &inc, self._HiZ, &inc)
                lapack.dpotrs("L", &self.model._k_endog, &self.k_states,
                                       self._obs_cov_fac, &self.model._k_endog,
                                       self._HiZ, &self.model._k_endog, &info)

                # ZHiZ = Z.T @ HiZ
                blas.dgemm("T", "N", &self.k_states, &self.k_states, &self.model._k_endog,
                                    &alpha, self.model._design, &self.model._k_endog,
                                        self._HiZ, &self.model._k_endog,
                                    &beta, self._ZHiZ, &self.k_states)

            # Fill in K and posterior_cov_inv_chol (which is right now just
            # going to contain posterior_cov_inv, and then we will take the
            # sparse Cholesky after the loop), prior mean
            # Iterate over columns in this (*, k_states) "block" of K, P
            for i in range(self.k_states):
                rows = self.k_states - i

                if t < self.model.nobs - 1:
                    if t == 0:
                        # K[:q - i, col] = (TQiT + P0i)[i:, i]
                        blas.dcopy(&rows, &self.TQiT[i, i], &inc, &self.K[0, j], &inc)
                        blas.daxpy(&rows, &alpha, &self.initial_state_cov_inv[i, i], &inc,
                                                            &self.K[0, j], &inc)
                    else:
                        # K[:q - i, col] = TQiTpQ[i:, i]
                        blas.dcopy(&rows, &self.TQiTpQ[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.dcopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.daxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)
                    # K[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.dcopy(&self.k_states, &self.QiT[0, i], &inc, &self.K[rows, j], &inc)
                    blas.dscal(&self.k_states, &gamma, &self.K[rows, j], &inc)
                    # P[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.dcopy(&self.k_states, &self.K[rows, j], &inc,
                                                               &self.posterior_cov_inv_chol[rows, j], &inc)

                # Last period is different
                elif t == self.model.nobs - 1:
                    # K[:q - i, col] = RQRi[i:, i]
                    blas.dcopy(&rows, &self.selected_state_cov_inv[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.dcopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.daxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)

                # Advance the column counter
                j = j + 1

            # Prior mean
            if t == 0:
                # prior_mean[:, 0] = initial_state
                blas.dcopy(&self.k_states, self.model._initial_state, &inc, &self.prior_mean[0, 0], &inc)
            else:
                # prior_mean[:, t] = c[:, t] + T @ prior_mean[:, t-1]
                blas.dcopy(&self.k_states, self.model._state_intercept, &inc, &self.prior_mean[0, t], &inc)
                blas.dgemv("N", &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                            &self.prior_mean[0, t - 1], &inc,
                                    &alpha, &self.prior_mean[0, t], &inc)

            # (Hi Z)' (y - d)
            if self.model.nmissing[t] == self.model.k_endog:
                self.posterior_mean[:, t] = 0
            else:
                blas.dcopy(&self.model._k_endog, self.model._obs, &inc, &self.ymd[0], &inc)
                blas.daxpy(&self.model._k_endog, &gamma, self.model._obs_intercept, &inc, &self.ymd[0], &inc)
                blas.dgemv("T", &self.model._k_endog, &self.k_states,
                                         &alpha, self._HiZ, &self.model._k_endog,
                                                &self.ymd[0], &inc,
                                         &beta, &self.posterior_mean[0, t], &inc)

        # Orders of sparse matrices
        order = self.model.nobs * self.k_states
        ld = self.lower_bandwidth + 1

        # Compute the sparse Cholesky factor of posterior cov
        lapack.dpbtrf("L", &self.order, &self.lower_bandwidth,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &info)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite joint posterior covariance matrix encountered.')
        elif info < 0:
            raise np.linalg.LinAlgError('Invalid value in joint posterior covariance matrix encountered.')

        # Compute the posterior mean
        # posterior_mean = P^{-1} (K prior_mean + ZHimd)
        blas.dsbmv("L", &self.order, &self.lower_bandwidth,
                            &alpha, self._K, &ld,
                                    &self.prior_mean[0, 0], &inc,
                            &alpha, &self.posterior_mean[0, 0], &inc)
        lapack.dpbtrs("L", &self.order, &self.lower_bandwidth, &inc,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &self.posterior_mean[0, 0], &self.order, &info)

    cpdef simulate(self, variates=None):
        cdef int inc = 1
        cdef int ld = self.lower_bandwidth + 1
        cdef np.float64_t alpha = 1.0
        cdef np.float64_t [:] u

        # Sample u from N(0, I)
        if variates is None:
            u = np.random.normal(size=self.order).astype(float)
        else:
            u = variates

            tools.validate_vector_shape('variates', &u.shape[0],
                                        self.order, None)

        # Solve L' x = u to get x \sim N(0, P^{-1})
        # (L = posterior_cov_inv_chol is lower triangular)
        blas.dtbsv("L", "T", "N", &self.order, &self.lower_bandwidth,
                            &self.posterior_cov_inv_chol[0, 0], &ld,
                            &u[0], &inc)

        # Add in the posterior mean
        blas.daxpy(&self.order, &alpha, &self.posterior_mean[0, 0], &inc, &u[0], &inc)

        return np.array(u).reshape(self.model.nobs, self.k_states).T

cdef class cCFASimulationSmoother(object):
    """
    Notes
    -----
    Currently not implemented (but could be):

    - Diffuse initialization
    - Support for collapsed observation vector

    Cannot be implemented:

    - Degenerate initial state distribution
    - Degenerate observation shock vector
    - Degenerate state shock vector

    """

    def __init__(self, cStatespace model):
        cdef int inc = 1
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.model = model
        self.k_states = model.k_states
        self.k_states2 = self.k_states**2
        self.order = self.model.nobs * self.k_states
        self.lower_bandwidth = 2 * self.k_states - 1

        # Validate that our model is acceptable
        if self.model.nobs == 1:
            # TODO: this is probably easily fixable
            raise NotImplementedError('Cannot use CFA simulation smoothing'
                                      ' with a single observation.')

        # Posterior mean vector
        # Note: we'll define this as two-dimensional, although we'll want to
        # access it though it is a "one-dimensional" stacked vector at one
        # point. But that's not a problem given the column-major ordering here,
        # we can just pretend it's a one-dimensional array of length
        # k_states * nobs
        dim2[0] = self.k_states; dim2[1] = self.model.nobs;
        self.prior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self.posterior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

        # Sparse storage for Cholesky factor of posterior covariance matrix
        dim2[0] = self.lower_bandwidth + 1; dim2[1] = self.order;
        self.posterior_cov_inv_chol = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self.K = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

        # Intermediate computation arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.initial_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.model.k_endog;
        self.obs_cov_fac = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.selected_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

        # Temporary arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.QiT = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self.TQiT = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self.TQiTpQ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self.ZHiZ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.k_states;
        self.HiZ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim1[0] = self.model.k_endog;
        self.ymd = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX64, FORTRAN)

        # Pointers
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    def __reduce__(self):
        args = (self.model,)
        state = {'order': self.order,
                 'lower_bandwidth': self.lower_bandwidth,
                 'prior_mean': np.array(self.prior_mean, copy=True, order='F'),
                 'posterior_mean': np.array(self.posterior_mean, copy=True, order='F'),
                 'posterior_cov_inv_chol': np.array(self.posterior_cov_inv_chol, copy=True, order='F'),
                 'K': np.array(self.K, copy=True, order='F'),
                 'initial_state_cov_inv': np.array(self.initial_state_cov_inv, copy=True, order='F'),
                 'obs_cov_fac': np.array(self.obs_cov_fac, copy=True, order='F'),
                 'selected_state_cov_inv': np.array(self.selected_state_cov_inv, copy=True, order='F'),
                 'QiT': np.array(self.QiT, copy=True, order='F'),
                 'TQiT': np.array(self.TQiT, copy=True, order='F'),
                 'TQiTpQ': np.array(self.TQiTpQ, copy=True, order='F'),
                 'ZHiZ': np.array(self.ZHiZ, copy=True, order='F'),
                 'HiZ': np.array(self.HiZ, copy=True, order='F'),
                 'ymd': np.array(self.ymd, copy=True, order='F'),
                 }
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.order = state['order']
        self.lower_bandwidth = state['lower_bandwidth']
        self.prior_mean = state['prior_mean']
        self.posterior_mean = state['posterior_mean']
        self.posterior_cov_inv_chol = state['posterior_cov_inv_chol']
        self.K = state['K']
        self.initial_state_cov_inv = state['initial_state_cov_inv']
        self.obs_cov_fac = state['obs_cov_fac']
        self.selected_state_cov_inv = state['selected_state_cov_inv']
        self.QiT = state['QiT']
        self.TQiT = state['TQiT']
        self.TQiTpQ = state['TQiTpQ']
        self.ZHiZ = state['ZHiZ']
        self.HiZ = state['HiZ']
        self.ymd = state['ymd']
        self._reinitialize_pointers()

    cdef void _reinitialize_pointers(self) except *:
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    cpdef int update_sparse_posterior_moments(self) except *:
        # Update the computation of posterior moments for the entire state
        # vector, with the  covariance matrix represented in what Scipy calls
        # "lower diagonal ordered form" - see e.g. Scipy's documentation page
        # for linalg.cholesky_banded for details of the storage format
        cdef:
            int t, i, j, rows, ld, info
            int reset_missing = 0
            int inc = 1
            np.complex64_t alpha = 1.0
            np.complex64_t beta = 0.0
            np.complex64_t gamma = -1.0
            int time_varying_obs_intercept = self.model.obs_intercept.shape[1] > 1
            int time_varying_design = self.model.design.shape[2] > 1
            int time_varying_obs_cov = self.model.obs_cov.shape[2] > 1
            int time_varying_state_intercept = self.model.state_intercept.shape[1] > 1
            int time_varying_transition = self.model.transition.shape[2] > 1
            int time_varying_selection = self.model.selection.shape[2] > 1
            int time_varying_state_cov = self.model.state_cov.shape[2] > 1
            int time_varying_selected_state_cov = time_varying_selection or time_varying_state_cov

        self.model.seek(0, False, False)

        # Invert initial state covariance matrix
        # (we actually need the inverse here)
        if self.model.initialized_diffuse:
            self.initial_state_cov_inv[:] = 0.
        else:
            blas.ccopy(&self.k_states2, self.model._initial_state_cov, &inc, self._initial_state_cov_inv, &inc)
            lapack.cpotrf("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)
            if info > 0:
                raise np.linalg.LinAlgError('Non-positive-definite initial state covariance matrix.')
            elif info < 0:
                raise np.linalg.LinAlgError('Invalid value in initial state covariance matrix.')
            lapack.cpotri("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)

        # Iterate over periods
        j = 0  # this will be a column counter
        for t in range(self.model.nobs):
            # Update the model representation to the current time point
            self.model.seek(t, False, False)

            # Check if we need to reset arrays based on Z for missing data
            reset_missing = 0
            if t > 0:
                for i in range(self.model.k_endog):
                    reset_missing = reset_missing + (not self.model.missing[i,t] == self.model.missing[i, t - 1])

            # Invert selected_state_cov
            # (again, we actually need the inverse itself)
            if t == 0 or time_varying_selected_state_cov:
                blas.ccopy(&self.k_states2, self.model._selected_state_cov, &inc, self._selected_state_cov_inv, &inc)
                lapack.cpotrf("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)
                if info > 0:
                    raise np.linalg.LinAlgError('Non-positive-definite selected state covariance matrix encountered at period %d' % t)
                elif info < 0:
                    raise np.linalg.LinAlgError('Invalid value in selected state covariance matrix encountered at period %d' % t)
                lapack.cpotri("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)

            # T(RQRi)T + (RQRi)
            if (t == 0 or time_varying_transition or
                    time_varying_selected_state_cov):
                # QiT = (RQRi) @ T
                blas.csymm("L", "L", &self.k_states, &self.k_states,
                                    &alpha, self._selected_state_cov_inv, &self.k_states,
                                        self.model._transition, &self.k_states,
                                    &beta, self._QiT, &self.k_states)

                # TQiT = T.T @ QiT
                blas.cgemm("T", "N", &self.k_states, &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                        self._QiT, &self.k_states,
                                    &beta, self._TQiT, &self.k_states)

                # TQiTpQ = TQiT + RQRi
                # Note: there will not be correct entries in the upper triangle, because
                # selected_state_cov was only set in the lower triangle. However, that's
                # okay because we only reference the lower triangle, below
                blas.ccopy(&self.k_states2, self._selected_state_cov_inv, &inc, self._TQiTpQ, &inc)
                blas.caxpy(&self.k_states2, &alpha, self._TQiT, &inc, self._TQiTpQ, &inc)

            # Z (Hi) Z
            if t == 0 or reset_missing or time_varying_design or time_varying_obs_cov:

                # Cholesky of obs_cov
                if t == 0 or reset_missing or time_varying_obs_cov:
                    blas.ccopy(&self.model._k_endog2, self.model._obs_cov, &inc, self._obs_cov_fac, &inc)
                    lapack.cpotrf("L", &self.model._k_endog, self._obs_cov_fac, &self.model._k_endog, &info)
                    if info > 0:
                        raise np.linalg.LinAlgError('Non-positive-definite observation covariance matrix encountered at period %d' % t)
                    elif info < 0:
                        raise np.linalg.LinAlgError('Invalid value in observation covariance matrix encountered at period %d' % t)

                # HiZ = Hi Z
                blas.ccopy(&self.model._k_endogstates, self.model._design, &inc, self._HiZ, &inc)
                lapack.cpotrs("L", &self.model._k_endog, &self.k_states,
                                       self._obs_cov_fac, &self.model._k_endog,
                                       self._HiZ, &self.model._k_endog, &info)

                # ZHiZ = Z.T @ HiZ
                blas.cgemm("T", "N", &self.k_states, &self.k_states, &self.model._k_endog,
                                    &alpha, self.model._design, &self.model._k_endog,
                                        self._HiZ, &self.model._k_endog,
                                    &beta, self._ZHiZ, &self.k_states)

            # Fill in K and posterior_cov_inv_chol (which is right now just
            # going to contain posterior_cov_inv, and then we will take the
            # sparse Cholesky after the loop), prior mean
            # Iterate over columns in this (*, k_states) "block" of K, P
            for i in range(self.k_states):
                rows = self.k_states - i

                if t < self.model.nobs - 1:
                    if t == 0:
                        # K[:q - i, col] = (TQiT + P0i)[i:, i]
                        blas.ccopy(&rows, &self.TQiT[i, i], &inc, &self.K[0, j], &inc)
                        blas.caxpy(&rows, &alpha, &self.initial_state_cov_inv[i, i], &inc,
                                                            &self.K[0, j], &inc)
                    else:
                        # K[:q - i, col] = TQiTpQ[i:, i]
                        blas.ccopy(&rows, &self.TQiTpQ[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.ccopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.caxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)
                    # K[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.ccopy(&self.k_states, &self.QiT[0, i], &inc, &self.K[rows, j], &inc)
                    blas.cscal(&self.k_states, &gamma, &self.K[rows, j], &inc)
                    # P[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.ccopy(&self.k_states, &self.K[rows, j], &inc,
                                                               &self.posterior_cov_inv_chol[rows, j], &inc)

                # Last period is different
                elif t == self.model.nobs - 1:
                    # K[:q - i, col] = RQRi[i:, i]
                    blas.ccopy(&rows, &self.selected_state_cov_inv[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.ccopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.caxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)

                # Advance the column counter
                j = j + 1

            # Prior mean
            if t == 0:
                # prior_mean[:, 0] = initial_state
                blas.ccopy(&self.k_states, self.model._initial_state, &inc, &self.prior_mean[0, 0], &inc)
            else:
                # prior_mean[:, t] = c[:, t] + T @ prior_mean[:, t-1]
                blas.ccopy(&self.k_states, self.model._state_intercept, &inc, &self.prior_mean[0, t], &inc)
                blas.cgemv("N", &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                            &self.prior_mean[0, t - 1], &inc,
                                    &alpha, &self.prior_mean[0, t], &inc)

            # (Hi Z)' (y - d)
            if self.model.nmissing[t] == self.model.k_endog:
                self.posterior_mean[:, t] = 0
            else:
                blas.ccopy(&self.model._k_endog, self.model._obs, &inc, &self.ymd[0], &inc)
                blas.caxpy(&self.model._k_endog, &gamma, self.model._obs_intercept, &inc, &self.ymd[0], &inc)
                blas.cgemv("T", &self.model._k_endog, &self.k_states,
                                         &alpha, self._HiZ, &self.model._k_endog,
                                                &self.ymd[0], &inc,
                                         &beta, &self.posterior_mean[0, t], &inc)

        # Orders of sparse matrices
        order = self.model.nobs * self.k_states
        ld = self.lower_bandwidth + 1

        # Compute the sparse Cholesky factor of posterior cov
        lapack.cpbtrf("L", &self.order, &self.lower_bandwidth,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &info)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite joint posterior covariance matrix encountered.')
        elif info < 0:
            raise np.linalg.LinAlgError('Invalid value in joint posterior covariance matrix encountered.')

        # Compute the posterior mean
        # posterior_mean = P^{-1} (K prior_mean + ZHimd)
        blas.chbmv("L", &self.order, &self.lower_bandwidth,
                            &alpha, self._K, &ld,
                                    &self.prior_mean[0, 0], &inc,
                            &alpha, &self.posterior_mean[0, 0], &inc)
        lapack.cpbtrs("L", &self.order, &self.lower_bandwidth, &inc,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &self.posterior_mean[0, 0], &self.order, &info)

    cpdef simulate(self, variates=None):
        cdef int inc = 1
        cdef int ld = self.lower_bandwidth + 1
        cdef np.complex64_t alpha = 1.0
        cdef np.complex64_t [:] u

        # Sample u from N(0, I)
        if variates is None:
            u = np.random.normal(size=self.order).astype(np.complex64)
        else:
            u = variates

            tools.validate_vector_shape('variates', &u.shape[0],
                                        self.order, None)

        # Solve L' x = u to get x \sim N(0, P^{-1})
        # (L = posterior_cov_inv_chol is lower triangular)
        blas.ctbsv("L", "T", "N", &self.order, &self.lower_bandwidth,
                            &self.posterior_cov_inv_chol[0, 0], &ld,
                            &u[0], &inc)

        # Add in the posterior mean
        blas.caxpy(&self.order, &alpha, &self.posterior_mean[0, 0], &inc, &u[0], &inc)

        return np.array(u).reshape(self.model.nobs, self.k_states).T

cdef class zCFASimulationSmoother(object):
    """
    Notes
    -----
    Currently not implemented (but could be):

    - Diffuse initialization
    - Support for collapsed observation vector

    Cannot be implemented:

    - Degenerate initial state distribution
    - Degenerate observation shock vector
    - Degenerate state shock vector

    """

    def __init__(self, zStatespace model):
        cdef int inc = 1
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.model = model
        self.k_states = model.k_states
        self.k_states2 = self.k_states**2
        self.order = self.model.nobs * self.k_states
        self.lower_bandwidth = 2 * self.k_states - 1

        # Validate that our model is acceptable
        if self.model.nobs == 1:
            # TODO: this is probably easily fixable
            raise NotImplementedError('Cannot use CFA simulation smoothing'
                                      ' with a single observation.')

        # Posterior mean vector
        # Note: we'll define this as two-dimensional, although we'll want to
        # access it though it is a "one-dimensional" stacked vector at one
        # point. But that's not a problem given the column-major ordering here,
        # we can just pretend it's a one-dimensional array of length
        # k_states * nobs
        dim2[0] = self.k_states; dim2[1] = self.model.nobs;
        self.prior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self.posterior_mean = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

        # Sparse storage for Cholesky factor of posterior covariance matrix
        dim2[0] = self.lower_bandwidth + 1; dim2[1] = self.order;
        self.posterior_cov_inv_chol = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self.K = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

        # Intermediate computation arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.initial_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.model.k_endog;
        self.obs_cov_fac = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.selected_state_cov_inv = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

        # Temporary arrays
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self.QiT = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self.TQiT = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self.TQiTpQ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self.ZHiZ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.model.k_endog; dim2[1] = self.k_states;
        self.HiZ = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim1[0] = self.model.k_endog;
        self.ymd = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX128, FORTRAN)

        # Pointers
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    def __reduce__(self):
        args = (self.model,)
        state = {'order': self.order,
                 'lower_bandwidth': self.lower_bandwidth,
                 'prior_mean': np.array(self.prior_mean, copy=True, order='F'),
                 'posterior_mean': np.array(self.posterior_mean, copy=True, order='F'),
                 'posterior_cov_inv_chol': np.array(self.posterior_cov_inv_chol, copy=True, order='F'),
                 'K': np.array(self.K, copy=True, order='F'),
                 'initial_state_cov_inv': np.array(self.initial_state_cov_inv, copy=True, order='F'),
                 'obs_cov_fac': np.array(self.obs_cov_fac, copy=True, order='F'),
                 'selected_state_cov_inv': np.array(self.selected_state_cov_inv, copy=True, order='F'),
                 'QiT': np.array(self.QiT, copy=True, order='F'),
                 'TQiT': np.array(self.TQiT, copy=True, order='F'),
                 'TQiTpQ': np.array(self.TQiTpQ, copy=True, order='F'),
                 'ZHiZ': np.array(self.ZHiZ, copy=True, order='F'),
                 'HiZ': np.array(self.HiZ, copy=True, order='F'),
                 'ymd': np.array(self.ymd, copy=True, order='F'),
                 }
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.order = state['order']
        self.lower_bandwidth = state['lower_bandwidth']
        self.prior_mean = state['prior_mean']
        self.posterior_mean = state['posterior_mean']
        self.posterior_cov_inv_chol = state['posterior_cov_inv_chol']
        self.K = state['K']
        self.initial_state_cov_inv = state['initial_state_cov_inv']
        self.obs_cov_fac = state['obs_cov_fac']
        self.selected_state_cov_inv = state['selected_state_cov_inv']
        self.QiT = state['QiT']
        self.TQiT = state['TQiT']
        self.TQiTpQ = state['TQiTpQ']
        self.ZHiZ = state['ZHiZ']
        self.HiZ = state['HiZ']
        self.ymd = state['ymd']
        self._reinitialize_pointers()

    cdef void _reinitialize_pointers(self) except *:
        self._K = &self.K[0, 0]
        self._initial_state_cov_inv = &self.initial_state_cov_inv[0, 0]
        self._obs_cov_fac = &self.obs_cov_fac[0, 0]
        self._selected_state_cov_inv = &self.selected_state_cov_inv[0, 0]

        self._QiT = &self.QiT[0, 0]
        self._TQiT = &self.TQiT[0, 0]
        self._TQiTpQ = &self.TQiTpQ[0, 0]
        self._ZHiZ = &self.ZHiZ[0, 0]
        self._HiZ = &self.HiZ[0, 0]
        self._ymd = &self.ymd[0]

    cpdef int update_sparse_posterior_moments(self) except *:
        # Update the computation of posterior moments for the entire state
        # vector, with the  covariance matrix represented in what Scipy calls
        # "lower diagonal ordered form" - see e.g. Scipy's documentation page
        # for linalg.cholesky_banded for details of the storage format
        cdef:
            int t, i, j, rows, ld, info
            int reset_missing = 0
            int inc = 1
            np.complex128_t alpha = 1.0
            np.complex128_t beta = 0.0
            np.complex128_t gamma = -1.0
            int time_varying_obs_intercept = self.model.obs_intercept.shape[1] > 1
            int time_varying_design = self.model.design.shape[2] > 1
            int time_varying_obs_cov = self.model.obs_cov.shape[2] > 1
            int time_varying_state_intercept = self.model.state_intercept.shape[1] > 1
            int time_varying_transition = self.model.transition.shape[2] > 1
            int time_varying_selection = self.model.selection.shape[2] > 1
            int time_varying_state_cov = self.model.state_cov.shape[2] > 1
            int time_varying_selected_state_cov = time_varying_selection or time_varying_state_cov

        self.model.seek(0, False, False)

        # Invert initial state covariance matrix
        # (we actually need the inverse here)
        if self.model.initialized_diffuse:
            self.initial_state_cov_inv[:] = 0.
        else:
            blas.zcopy(&self.k_states2, self.model._initial_state_cov, &inc, self._initial_state_cov_inv, &inc)
            lapack.zpotrf("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)
            if info > 0:
                raise np.linalg.LinAlgError('Non-positive-definite initial state covariance matrix.')
            elif info < 0:
                raise np.linalg.LinAlgError('Invalid value in initial state covariance matrix.')
            lapack.zpotri("L", &self.k_states, self._initial_state_cov_inv, &self.k_states, &info)

        # Iterate over periods
        j = 0  # this will be a column counter
        for t in range(self.model.nobs):
            # Update the model representation to the current time point
            self.model.seek(t, False, False)

            # Check if we need to reset arrays based on Z for missing data
            reset_missing = 0
            if t > 0:
                for i in range(self.model.k_endog):
                    reset_missing = reset_missing + (not self.model.missing[i,t] == self.model.missing[i, t - 1])

            # Invert selected_state_cov
            # (again, we actually need the inverse itself)
            if t == 0 or time_varying_selected_state_cov:
                blas.zcopy(&self.k_states2, self.model._selected_state_cov, &inc, self._selected_state_cov_inv, &inc)
                lapack.zpotrf("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)
                if info > 0:
                    raise np.linalg.LinAlgError('Non-positive-definite selected state covariance matrix encountered at period %d' % t)
                elif info < 0:
                    raise np.linalg.LinAlgError('Invalid value in selected state covariance matrix encountered at period %d' % t)
                lapack.zpotri("L", &self.k_states, self._selected_state_cov_inv, &self.k_states, &info)

            # T(RQRi)T + (RQRi)
            if (t == 0 or time_varying_transition or
                    time_varying_selected_state_cov):
                # QiT = (RQRi) @ T
                blas.zsymm("L", "L", &self.k_states, &self.k_states,
                                    &alpha, self._selected_state_cov_inv, &self.k_states,
                                        self.model._transition, &self.k_states,
                                    &beta, self._QiT, &self.k_states)

                # TQiT = T.T @ QiT
                blas.zgemm("T", "N", &self.k_states, &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                        self._QiT, &self.k_states,
                                    &beta, self._TQiT, &self.k_states)

                # TQiTpQ = TQiT + RQRi
                # Note: there will not be correct entries in the upper triangle, because
                # selected_state_cov was only set in the lower triangle. However, that's
                # okay because we only reference the lower triangle, below
                blas.zcopy(&self.k_states2, self._selected_state_cov_inv, &inc, self._TQiTpQ, &inc)
                blas.zaxpy(&self.k_states2, &alpha, self._TQiT, &inc, self._TQiTpQ, &inc)

            # Z (Hi) Z
            if t == 0 or reset_missing or time_varying_design or time_varying_obs_cov:

                # Cholesky of obs_cov
                if t == 0 or reset_missing or time_varying_obs_cov:
                    blas.zcopy(&self.model._k_endog2, self.model._obs_cov, &inc, self._obs_cov_fac, &inc)
                    lapack.zpotrf("L", &self.model._k_endog, self._obs_cov_fac, &self.model._k_endog, &info)
                    if info > 0:
                        raise np.linalg.LinAlgError('Non-positive-definite observation covariance matrix encountered at period %d' % t)
                    elif info < 0:
                        raise np.linalg.LinAlgError('Invalid value in observation covariance matrix encountered at period %d' % t)

                # HiZ = Hi Z
                blas.zcopy(&self.model._k_endogstates, self.model._design, &inc, self._HiZ, &inc)
                lapack.zpotrs("L", &self.model._k_endog, &self.k_states,
                                       self._obs_cov_fac, &self.model._k_endog,
                                       self._HiZ, &self.model._k_endog, &info)

                # ZHiZ = Z.T @ HiZ
                blas.zgemm("T", "N", &self.k_states, &self.k_states, &self.model._k_endog,
                                    &alpha, self.model._design, &self.model._k_endog,
                                        self._HiZ, &self.model._k_endog,
                                    &beta, self._ZHiZ, &self.k_states)

            # Fill in K and posterior_cov_inv_chol (which is right now just
            # going to contain posterior_cov_inv, and then we will take the
            # sparse Cholesky after the loop), prior mean
            # Iterate over columns in this (*, k_states) "block" of K, P
            for i in range(self.k_states):
                rows = self.k_states - i

                if t < self.model.nobs - 1:
                    if t == 0:
                        # K[:q - i, col] = (TQiT + P0i)[i:, i]
                        blas.zcopy(&rows, &self.TQiT[i, i], &inc, &self.K[0, j], &inc)
                        blas.zaxpy(&rows, &alpha, &self.initial_state_cov_inv[i, i], &inc,
                                                            &self.K[0, j], &inc)
                    else:
                        # K[:q - i, col] = TQiTpQ[i:, i]
                        blas.zcopy(&rows, &self.TQiTpQ[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.zcopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.zaxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)
                    # K[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.zcopy(&self.k_states, &self.QiT[0, i], &inc, &self.K[rows, j], &inc)
                    blas.zscal(&self.k_states, &gamma, &self.K[rows, j], &inc)
                    # P[q - i:q + q - i, col] = -QiT[:q, i]
                    blas.zcopy(&self.k_states, &self.K[rows, j], &inc,
                                                               &self.posterior_cov_inv_chol[rows, j], &inc)

                # Last period is different
                elif t == self.model.nobs - 1:
                    # K[:q - i, col] = RQRi[i:, i]
                    blas.zcopy(&rows, &self.selected_state_cov_inv[i, i], &inc, &self.K[0, j], &inc)

                    # P[:q - i, col] = K[:q - i, col] + ZHiZ[i:, i]
                    blas.zcopy(&rows, &self.K[0, j], &inc, &self.posterior_cov_inv_chol[0, j], &inc)
                    blas.zaxpy(&rows, &alpha, &self.ZHiZ[i, i], &inc,
                                                        &self.posterior_cov_inv_chol[0, j], &inc)

                # Advance the column counter
                j = j + 1

            # Prior mean
            if t == 0:
                # prior_mean[:, 0] = initial_state
                blas.zcopy(&self.k_states, self.model._initial_state, &inc, &self.prior_mean[0, 0], &inc)
            else:
                # prior_mean[:, t] = c[:, t] + T @ prior_mean[:, t-1]
                blas.zcopy(&self.k_states, self.model._state_intercept, &inc, &self.prior_mean[0, t], &inc)
                blas.zgemv("N", &self.k_states, &self.k_states,
                                    &alpha, self.model._transition, &self.k_states,
                                            &self.prior_mean[0, t - 1], &inc,
                                    &alpha, &self.prior_mean[0, t], &inc)

            # (Hi Z)' (y - d)
            if self.model.nmissing[t] == self.model.k_endog:
                self.posterior_mean[:, t] = 0
            else:
                blas.zcopy(&self.model._k_endog, self.model._obs, &inc, &self.ymd[0], &inc)
                blas.zaxpy(&self.model._k_endog, &gamma, self.model._obs_intercept, &inc, &self.ymd[0], &inc)
                blas.zgemv("T", &self.model._k_endog, &self.k_states,
                                         &alpha, self._HiZ, &self.model._k_endog,
                                                &self.ymd[0], &inc,
                                         &beta, &self.posterior_mean[0, t], &inc)

        # Orders of sparse matrices
        order = self.model.nobs * self.k_states
        ld = self.lower_bandwidth + 1

        # Compute the sparse Cholesky factor of posterior cov
        lapack.zpbtrf("L", &self.order, &self.lower_bandwidth,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &info)
        if info > 0:
            raise np.linalg.LinAlgError('Non-positive-definite joint posterior covariance matrix encountered.')
        elif info < 0:
            raise np.linalg.LinAlgError('Invalid value in joint posterior covariance matrix encountered.')

        # Compute the posterior mean
        # posterior_mean = P^{-1} (K prior_mean + ZHimd)
        blas.zhbmv("L", &self.order, &self.lower_bandwidth,
                            &alpha, self._K, &ld,
                                    &self.prior_mean[0, 0], &inc,
                            &alpha, &self.posterior_mean[0, 0], &inc)
        lapack.zpbtrs("L", &self.order, &self.lower_bandwidth, &inc,
                               &self.posterior_cov_inv_chol[0, 0], &ld,
                               &self.posterior_mean[0, 0], &self.order, &info)

    cpdef simulate(self, variates=None):
        cdef int inc = 1
        cdef int ld = self.lower_bandwidth + 1
        cdef np.complex128_t alpha = 1.0
        cdef np.complex128_t [:] u

        # Sample u from N(0, I)
        if variates is None:
            u = np.random.normal(size=self.order).astype(complex)
        else:
            u = variates

            tools.validate_vector_shape('variates', &u.shape[0],
                                        self.order, None)

        # Solve L' x = u to get x \sim N(0, P^{-1})
        # (L = posterior_cov_inv_chol is lower triangular)
        blas.ztbsv("L", "T", "N", &self.order, &self.lower_bandwidth,
                            &self.posterior_cov_inv_chol[0, 0], &ld,
                            &u[0], &inc)

        # Add in the posterior mean
        blas.zaxpy(&self.order, &alpha, &self.posterior_mean[0, 0], &inc, &u[0], &inc)

        return np.array(u).reshape(self.model.nobs, self.k_states).T
