#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Models - Initialization

Author: Chad Fulton  
License: Simplified-BSD
"""

# Typical imports
import numpy as np
import warnings
cimport numpy as np
cimport cython

np.import_array()

from statsmodels.src.math cimport *
cimport scipy.linalg.cython_blas as blas
cimport scipy.linalg.cython_lapack as lapack
cimport statsmodels.tsa.statespace._tools as tools
from statsmodels.tsa.statespace._representation cimport sStatespace
from statsmodels.tsa.statespace._representation cimport dStatespace
from statsmodels.tsa.statespace._representation cimport cStatespace
from statsmodels.tsa.statespace._representation cimport zStatespace

cdef int FORTRAN = 1

## State Space Initialization
cdef class sInitialization(object):

    def __init__(self, int k_states, np.float32_t [:] constant,
                 np.float32_t [::1, :] stationary_cov,
                 np.float64_t approximate_diffuse_variance=1e6):
        cdef:
            int k
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.k_states = k_states
        self.constant = constant
        self.stationary_cov = stationary_cov
        self.approximate_diffuse_variance = approximate_diffuse_variance

        # Validate
        tools.validate_vector_shape('known constant', &self.constant.shape[0], self.k_states, None)
        tools.validate_matrix_shape('known covariance', &self.stationary_cov.shape[0], self.k_states, self.k_states, None)

        # Internal temporary matrices
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_transition = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_selected_state_cov = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)

    def __reduce__(self):
        init = (self.k_states, np.array(self.constant), np.array(self.stationary_cov),
                self.approximate_diffuse_variance)
        state = {'_tmp_transition': np.array(self._tmp_transition, copy=True, order='F'),
                 '_tmp_selected_state_cov': np.array(self._tmp_selected_state_cov, copy=True, order='F')}
        return (self.__class__, init, state)

    def __setstate__(self, state):
        self._tmp_transition = state['_tmp_transition']
        self._tmp_selected_state_cov = state['_tmp_selected_state_cov']

    cpdef int initialize(self, initialization_type, int offset,
                         sStatespace model,
                         np.float32_t [:] initial_state_mean,
                         np.float32_t [::1, :] initial_diffuse_state_cov,
                         np.float32_t [::1, :] initial_stationary_state_cov,
                         int complex_step=False) except 1:

        if offset + self.k_states > model.k_states:
            raise ValueError('Invalid offset.')
        tools.validate_vector_shape('initial state mean', &initial_state_mean.shape[0], model.k_states, None)
        tools.validate_matrix_shape('initial diffuse state cov', &initial_diffuse_state_cov.shape[0], model.k_states, model.k_states, None)
        tools.validate_matrix_shape('initial stationary state cov', &initial_stationary_state_cov.shape[0], model.k_states, model.k_states, None)

        if initialization_type == 'known':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_known_stationary_cov(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'diffuse':
            self.initialize_diffuse(offset, initial_diffuse_state_cov)
            self.clear_constant(offset, initial_state_mean)
            self.clear_cov(offset, initial_stationary_state_cov)
        elif initialization_type == 'approximate_diffuse':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_approximate_diffuse(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'stationary':
            self.initialize_stationary_constant(offset, model, initial_state_mean, complex_step)
            self.initialize_stationary_stationary_cov(offset, model, initial_stationary_state_cov, complex_step)
            self.clear_cov(offset, initial_diffuse_state_cov)
        else:
            raise ValueError('Invalid initialization type')

        return 0

    cdef int clear_constant(self, int offset, np.float32_t [:] initial_state_mean) except 1:
        initial_state_mean[offset:offset + self.k_states] = 0
        return 0

    cdef int clear_cov(self, int offset, np.float32_t [::1, :] cov) except 1:
        cov[offset:offset + self.k_states, offset:offset + self.k_states] = 0
        return 0

    cdef int initialize_known_constant(self, int offset,
                                        np.float32_t [:] initial_state_mean) except 1:
        cdef int inc = 1
        blas.scopy(&self.k_states, &self.constant[0], &inc,
                                     &initial_state_mean[offset], &inc)

        return 0

    cdef int initialize_known_stationary_cov(self, int offset,
                                              np.float32_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i, inc = 1
        # Copy columns
        for i in range(self.k_states):
            blas.scopy(&self.k_states, &self.stationary_cov[0, i], &inc,
                                         &initial_stationary_state_cov[offset, offset + i], &inc)

        return 0

    cdef int initialize_diffuse(self, int offset,
                                 np.float32_t [::1, :] initial_diffuse_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_diffuse_state_cov[i, i] = 1

        return 0

    cdef int initialize_approximate_diffuse(self, int offset,
                                             np.float32_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_stationary_state_cov[i, i] = self.approximate_diffuse_variance

        return 0

    cdef int initialize_stationary_constant(self, int offset, sStatespace model,
                                    np.float32_t [:] initial_state_mean,
                                    int complex_step=False) except 1:

        cdef:
            np.npy_intp dim2[2]
            int i, info, inc = 1
            int k_states2 = self.k_states**2
            np.float64_t asum, tol = 1e-9
            cdef np.float32_t scalar
            cdef int [::1,:] ipiv

        # Clear the unconditional mean (for this block)
        initial_state_mean[offset:offset + self.k_states] = 0

        # Check if the state intercept is all zeros; if it is, then the
        # unconditional mean is also all zeros
        asum = blas.sasum(&model.k_states, &model.state_intercept[0, 0], &inc)

        # If the state intercept is non-zero, compute the mean
        if asum > tol:
            dim2[0] = self.k_states
            dim2[1] = self.k_states
            ipiv = np.PyArray_ZEROS(2, dim2, np.NPY_INT32, FORTRAN)

            # Create T - I
            # (copy colummns)
            for i in range(self.k_states):
                blas.scopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                    &self._tmp_transition[0,i], &inc)
                self._tmp_transition[i, i] = self._tmp_transition[i, i] - 1
            # Multiply by -1 to get I - T
            scalar = -1.0
            blas.sscal(&k_states2, &scalar, &self._tmp_transition[0, 0], &inc)

            # c
            blas.scopy(&self.k_states, &model.state_intercept[offset,0], &inc,
                                                &initial_state_mean[offset], &inc)

            # Solve (I - T) x = c
            lapack.sgetrf(&self.k_states, &self.k_states, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &info)
            lapack.sgetrs('N', &self.k_states, &inc, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &initial_state_mean[offset], &self.k_states, &info)

        return 0

    cdef int initialize_stationary_stationary_cov(self, int offset, sStatespace model,
                                        np.float32_t [::1, :] initial_stationary_state_cov,
                                        int complex_step=False) except 1:
        cdef:
            int i, inc = 1
            int k_states2 = self.k_states**2

        # Create selected state covariance matrix
        tools._sselect_cov(self.k_states, model.k_posdef, model.k_states,
                             &model.tmp[0,0],
                             &model.selection[offset,0,0],
                             &model.state_cov[0,0,0],
                             &self._tmp_selected_state_cov[0,0])

        # Create a copy of the transition matrix
        # (copy colummns)
        for i in range(self.k_states):
            blas.scopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                &self._tmp_transition[0,i], &inc)

        # Solve the discrete Lyapunov equation to the get initial state
        # covariance matrix
        tools._ssolve_discrete_lyapunov(
            &self._tmp_transition[0,0], &self._tmp_selected_state_cov[0,0], self.k_states, complex_step)

        # Copy into initial_stationary_state_cov
        # (copy colummns)
        for i in range(self.k_states):
            blas.scopy(&self.k_states, &self._tmp_selected_state_cov[0,i], &inc,
                                                &initial_stationary_state_cov[offset,offset + i], &inc)

        return 0


## State Space Initialization
cdef class dInitialization(object):

    def __init__(self, int k_states, np.float64_t [:] constant,
                 np.float64_t [::1, :] stationary_cov,
                 np.float64_t approximate_diffuse_variance=1e6):
        cdef:
            int k
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.k_states = k_states
        self.constant = constant
        self.stationary_cov = stationary_cov
        self.approximate_diffuse_variance = approximate_diffuse_variance

        # Validate
        tools.validate_vector_shape('known constant', &self.constant.shape[0], self.k_states, None)
        tools.validate_matrix_shape('known covariance', &self.stationary_cov.shape[0], self.k_states, self.k_states, None)

        # Internal temporary matrices
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_transition = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_selected_state_cov = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)

    def __reduce__(self):
        init = (self.k_states, np.array(self.constant), np.array(self.stationary_cov),
                self.approximate_diffuse_variance)
        state = {'_tmp_transition': np.array(self._tmp_transition, copy=True, order='F'),
                 '_tmp_selected_state_cov': np.array(self._tmp_selected_state_cov, copy=True, order='F')}
        return (self.__class__, init, state)

    def __setstate__(self, state):
        self._tmp_transition = state['_tmp_transition']
        self._tmp_selected_state_cov = state['_tmp_selected_state_cov']

    cpdef int initialize(self, initialization_type, int offset,
                         dStatespace model,
                         np.float64_t [:] initial_state_mean,
                         np.float64_t [::1, :] initial_diffuse_state_cov,
                         np.float64_t [::1, :] initial_stationary_state_cov,
                         int complex_step=False) except 1:

        if offset + self.k_states > model.k_states:
            raise ValueError('Invalid offset.')
        tools.validate_vector_shape('initial state mean', &initial_state_mean.shape[0], model.k_states, None)
        tools.validate_matrix_shape('initial diffuse state cov', &initial_diffuse_state_cov.shape[0], model.k_states, model.k_states, None)
        tools.validate_matrix_shape('initial stationary state cov', &initial_stationary_state_cov.shape[0], model.k_states, model.k_states, None)

        if initialization_type == 'known':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_known_stationary_cov(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'diffuse':
            self.initialize_diffuse(offset, initial_diffuse_state_cov)
            self.clear_constant(offset, initial_state_mean)
            self.clear_cov(offset, initial_stationary_state_cov)
        elif initialization_type == 'approximate_diffuse':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_approximate_diffuse(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'stationary':
            self.initialize_stationary_constant(offset, model, initial_state_mean, complex_step)
            self.initialize_stationary_stationary_cov(offset, model, initial_stationary_state_cov, complex_step)
            self.clear_cov(offset, initial_diffuse_state_cov)
        else:
            raise ValueError('Invalid initialization type')

        return 0

    cdef int clear_constant(self, int offset, np.float64_t [:] initial_state_mean) except 1:
        initial_state_mean[offset:offset + self.k_states] = 0
        return 0

    cdef int clear_cov(self, int offset, np.float64_t [::1, :] cov) except 1:
        cov[offset:offset + self.k_states, offset:offset + self.k_states] = 0
        return 0

    cdef int initialize_known_constant(self, int offset,
                                        np.float64_t [:] initial_state_mean) except 1:
        cdef int inc = 1
        blas.dcopy(&self.k_states, &self.constant[0], &inc,
                                     &initial_state_mean[offset], &inc)

        return 0

    cdef int initialize_known_stationary_cov(self, int offset,
                                              np.float64_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i, inc = 1
        # Copy columns
        for i in range(self.k_states):
            blas.dcopy(&self.k_states, &self.stationary_cov[0, i], &inc,
                                         &initial_stationary_state_cov[offset, offset + i], &inc)

        return 0

    cdef int initialize_diffuse(self, int offset,
                                 np.float64_t [::1, :] initial_diffuse_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_diffuse_state_cov[i, i] = 1

        return 0

    cdef int initialize_approximate_diffuse(self, int offset,
                                             np.float64_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_stationary_state_cov[i, i] = self.approximate_diffuse_variance

        return 0

    cdef int initialize_stationary_constant(self, int offset, dStatespace model,
                                    np.float64_t [:] initial_state_mean,
                                    int complex_step=False) except 1:

        cdef:
            np.npy_intp dim2[2]
            int i, info, inc = 1
            int k_states2 = self.k_states**2
            np.float64_t asum, tol = 1e-9
            cdef np.float64_t scalar
            cdef int [::1,:] ipiv

        # Clear the unconditional mean (for this block)
        initial_state_mean[offset:offset + self.k_states] = 0

        # Check if the state intercept is all zeros; if it is, then the
        # unconditional mean is also all zeros
        asum = blas.dasum(&model.k_states, &model.state_intercept[0, 0], &inc)

        # If the state intercept is non-zero, compute the mean
        if asum > tol:
            dim2[0] = self.k_states
            dim2[1] = self.k_states
            ipiv = np.PyArray_ZEROS(2, dim2, np.NPY_INT32, FORTRAN)

            # Create T - I
            # (copy colummns)
            for i in range(self.k_states):
                blas.dcopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                    &self._tmp_transition[0,i], &inc)
                self._tmp_transition[i, i] = self._tmp_transition[i, i] - 1
            # Multiply by -1 to get I - T
            scalar = -1.0
            blas.dscal(&k_states2, &scalar, &self._tmp_transition[0, 0], &inc)

            # c
            blas.dcopy(&self.k_states, &model.state_intercept[offset,0], &inc,
                                                &initial_state_mean[offset], &inc)

            # Solve (I - T) x = c
            lapack.dgetrf(&self.k_states, &self.k_states, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &info)
            lapack.dgetrs('N', &self.k_states, &inc, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &initial_state_mean[offset], &self.k_states, &info)

        return 0

    cdef int initialize_stationary_stationary_cov(self, int offset, dStatespace model,
                                        np.float64_t [::1, :] initial_stationary_state_cov,
                                        int complex_step=False) except 1:
        cdef:
            int i, inc = 1
            int k_states2 = self.k_states**2

        # Create selected state covariance matrix
        tools._dselect_cov(self.k_states, model.k_posdef, model.k_states,
                             &model.tmp[0,0],
                             &model.selection[offset,0,0],
                             &model.state_cov[0,0,0],
                             &self._tmp_selected_state_cov[0,0])

        # Create a copy of the transition matrix
        # (copy colummns)
        for i in range(self.k_states):
            blas.dcopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                &self._tmp_transition[0,i], &inc)

        # Solve the discrete Lyapunov equation to the get initial state
        # covariance matrix
        tools._dsolve_discrete_lyapunov(
            &self._tmp_transition[0,0], &self._tmp_selected_state_cov[0,0], self.k_states, complex_step)

        # Copy into initial_stationary_state_cov
        # (copy colummns)
        for i in range(self.k_states):
            blas.dcopy(&self.k_states, &self._tmp_selected_state_cov[0,i], &inc,
                                                &initial_stationary_state_cov[offset,offset + i], &inc)

        return 0


## State Space Initialization
cdef class cInitialization(object):

    def __init__(self, int k_states, np.complex64_t [:] constant,
                 np.complex64_t [::1, :] stationary_cov,
                 np.float64_t approximate_diffuse_variance=1e6):
        cdef:
            int k
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.k_states = k_states
        self.constant = constant
        self.stationary_cov = stationary_cov
        self.approximate_diffuse_variance = approximate_diffuse_variance

        # Validate
        tools.validate_vector_shape('known constant', &self.constant.shape[0], self.k_states, None)
        tools.validate_matrix_shape('known covariance', &self.stationary_cov.shape[0], self.k_states, self.k_states, None)

        # Internal temporary matrices
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_transition = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_selected_state_cov = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)

    def __reduce__(self):
        init = (self.k_states, np.array(self.constant), np.array(self.stationary_cov),
                self.approximate_diffuse_variance)
        state = {'_tmp_transition': np.array(self._tmp_transition, copy=True, order='F'),
                 '_tmp_selected_state_cov': np.array(self._tmp_selected_state_cov, copy=True, order='F')}
        return (self.__class__, init, state)

    def __setstate__(self, state):
        self._tmp_transition = state['_tmp_transition']
        self._tmp_selected_state_cov = state['_tmp_selected_state_cov']

    cpdef int initialize(self, initialization_type, int offset,
                         cStatespace model,
                         np.complex64_t [:] initial_state_mean,
                         np.complex64_t [::1, :] initial_diffuse_state_cov,
                         np.complex64_t [::1, :] initial_stationary_state_cov,
                         int complex_step=False) except 1:

        if offset + self.k_states > model.k_states:
            raise ValueError('Invalid offset.')
        tools.validate_vector_shape('initial state mean', &initial_state_mean.shape[0], model.k_states, None)
        tools.validate_matrix_shape('initial diffuse state cov', &initial_diffuse_state_cov.shape[0], model.k_states, model.k_states, None)
        tools.validate_matrix_shape('initial stationary state cov', &initial_stationary_state_cov.shape[0], model.k_states, model.k_states, None)

        if initialization_type == 'known':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_known_stationary_cov(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'diffuse':
            self.initialize_diffuse(offset, initial_diffuse_state_cov)
            self.clear_constant(offset, initial_state_mean)
            self.clear_cov(offset, initial_stationary_state_cov)
        elif initialization_type == 'approximate_diffuse':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_approximate_diffuse(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'stationary':
            self.initialize_stationary_constant(offset, model, initial_state_mean, complex_step)
            self.initialize_stationary_stationary_cov(offset, model, initial_stationary_state_cov, complex_step)
            self.clear_cov(offset, initial_diffuse_state_cov)
        else:
            raise ValueError('Invalid initialization type')

        return 0

    cdef int clear_constant(self, int offset, np.complex64_t [:] initial_state_mean) except 1:
        initial_state_mean[offset:offset + self.k_states] = 0
        return 0

    cdef int clear_cov(self, int offset, np.complex64_t [::1, :] cov) except 1:
        cov[offset:offset + self.k_states, offset:offset + self.k_states] = 0
        return 0

    cdef int initialize_known_constant(self, int offset,
                                        np.complex64_t [:] initial_state_mean) except 1:
        cdef int inc = 1
        blas.ccopy(&self.k_states, &self.constant[0], &inc,
                                     &initial_state_mean[offset], &inc)

        return 0

    cdef int initialize_known_stationary_cov(self, int offset,
                                              np.complex64_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i, inc = 1
        # Copy columns
        for i in range(self.k_states):
            blas.ccopy(&self.k_states, &self.stationary_cov[0, i], &inc,
                                         &initial_stationary_state_cov[offset, offset + i], &inc)

        return 0

    cdef int initialize_diffuse(self, int offset,
                                 np.complex64_t [::1, :] initial_diffuse_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_diffuse_state_cov[i, i] = 1

        return 0

    cdef int initialize_approximate_diffuse(self, int offset,
                                             np.complex64_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_stationary_state_cov[i, i] = self.approximate_diffuse_variance

        return 0

    cdef int initialize_stationary_constant(self, int offset, cStatespace model,
                                    np.complex64_t [:] initial_state_mean,
                                    int complex_step=False) except 1:

        cdef:
            np.npy_intp dim2[2]
            int i, info, inc = 1
            int k_states2 = self.k_states**2
            np.float64_t asum, tol = 1e-9
            cdef np.complex64_t scalar
            cdef int [::1,:] ipiv

        # Clear the unconditional mean (for this block)
        initial_state_mean[offset:offset + self.k_states] = 0

        # Check if the state intercept is all zeros; if it is, then the
        # unconditional mean is also all zeros
        asum = blas.scasum(&model.k_states, &model.state_intercept[0, 0], &inc)

        # If the state intercept is non-zero, compute the mean
        if asum > tol:
            dim2[0] = self.k_states
            dim2[1] = self.k_states
            ipiv = np.PyArray_ZEROS(2, dim2, np.NPY_INT32, FORTRAN)

            # Create T - I
            # (copy colummns)
            for i in range(self.k_states):
                blas.ccopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                    &self._tmp_transition[0,i], &inc)
                self._tmp_transition[i, i] = self._tmp_transition[i, i] - 1
            # Multiply by -1 to get I - T
            scalar = -1.0
            blas.cscal(&k_states2, &scalar, &self._tmp_transition[0, 0], &inc)

            # c
            blas.ccopy(&self.k_states, &model.state_intercept[offset,0], &inc,
                                                &initial_state_mean[offset], &inc)

            # Solve (I - T) x = c
            lapack.cgetrf(&self.k_states, &self.k_states, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &info)
            lapack.cgetrs('N', &self.k_states, &inc, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &initial_state_mean[offset], &self.k_states, &info)

        return 0

    cdef int initialize_stationary_stationary_cov(self, int offset, cStatespace model,
                                        np.complex64_t [::1, :] initial_stationary_state_cov,
                                        int complex_step=False) except 1:
        cdef:
            int i, inc = 1
            int k_states2 = self.k_states**2

        # Create selected state covariance matrix
        tools._cselect_cov(self.k_states, model.k_posdef, model.k_states,
                             &model.tmp[0,0],
                             &model.selection[offset,0,0],
                             &model.state_cov[0,0,0],
                             &self._tmp_selected_state_cov[0,0])

        # Create a copy of the transition matrix
        # (copy colummns)
        for i in range(self.k_states):
            blas.ccopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                &self._tmp_transition[0,i], &inc)

        # Solve the discrete Lyapunov equation to the get initial state
        # covariance matrix
        tools._csolve_discrete_lyapunov(
            &self._tmp_transition[0,0], &self._tmp_selected_state_cov[0,0], self.k_states, complex_step)

        # Copy into initial_stationary_state_cov
        # (copy colummns)
        for i in range(self.k_states):
            blas.ccopy(&self.k_states, &self._tmp_selected_state_cov[0,i], &inc,
                                                &initial_stationary_state_cov[offset,offset + i], &inc)

        return 0


## State Space Initialization
cdef class zInitialization(object):

    def __init__(self, int k_states, np.complex128_t [:] constant,
                 np.complex128_t [::1, :] stationary_cov,
                 np.float64_t approximate_diffuse_variance=1e6):
        cdef:
            int k
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]

        self.k_states = k_states
        self.constant = constant
        self.stationary_cov = stationary_cov
        self.approximate_diffuse_variance = approximate_diffuse_variance

        # Validate
        tools.validate_vector_shape('known constant', &self.constant.shape[0], self.k_states, None)
        tools.validate_matrix_shape('known covariance', &self.stationary_cov.shape[0], self.k_states, self.k_states, None)

        # Internal temporary matrices
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_transition = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.k_states; dim2[1] = self.k_states;
        self._tmp_selected_state_cov = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)

    def __reduce__(self):
        init = (self.k_states, np.array(self.constant), np.array(self.stationary_cov),
                self.approximate_diffuse_variance)
        state = {'_tmp_transition': np.array(self._tmp_transition, copy=True, order='F'),
                 '_tmp_selected_state_cov': np.array(self._tmp_selected_state_cov, copy=True, order='F')}
        return (self.__class__, init, state)

    def __setstate__(self, state):
        self._tmp_transition = state['_tmp_transition']
        self._tmp_selected_state_cov = state['_tmp_selected_state_cov']

    cpdef int initialize(self, initialization_type, int offset,
                         zStatespace model,
                         np.complex128_t [:] initial_state_mean,
                         np.complex128_t [::1, :] initial_diffuse_state_cov,
                         np.complex128_t [::1, :] initial_stationary_state_cov,
                         int complex_step=False) except 1:

        if offset + self.k_states > model.k_states:
            raise ValueError('Invalid offset.')
        tools.validate_vector_shape('initial state mean', &initial_state_mean.shape[0], model.k_states, None)
        tools.validate_matrix_shape('initial diffuse state cov', &initial_diffuse_state_cov.shape[0], model.k_states, model.k_states, None)
        tools.validate_matrix_shape('initial stationary state cov', &initial_stationary_state_cov.shape[0], model.k_states, model.k_states, None)

        if initialization_type == 'known':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_known_stationary_cov(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'diffuse':
            self.initialize_diffuse(offset, initial_diffuse_state_cov)
            self.clear_constant(offset, initial_state_mean)
            self.clear_cov(offset, initial_stationary_state_cov)
        elif initialization_type == 'approximate_diffuse':
            self.initialize_known_constant(offset, initial_state_mean)
            self.initialize_approximate_diffuse(offset, initial_stationary_state_cov)
            self.clear_cov(offset, initial_diffuse_state_cov)
        elif initialization_type == 'stationary':
            self.initialize_stationary_constant(offset, model, initial_state_mean, complex_step)
            self.initialize_stationary_stationary_cov(offset, model, initial_stationary_state_cov, complex_step)
            self.clear_cov(offset, initial_diffuse_state_cov)
        else:
            raise ValueError('Invalid initialization type')

        return 0

    cdef int clear_constant(self, int offset, np.complex128_t [:] initial_state_mean) except 1:
        initial_state_mean[offset:offset + self.k_states] = 0
        return 0

    cdef int clear_cov(self, int offset, np.complex128_t [::1, :] cov) except 1:
        cov[offset:offset + self.k_states, offset:offset + self.k_states] = 0
        return 0

    cdef int initialize_known_constant(self, int offset,
                                        np.complex128_t [:] initial_state_mean) except 1:
        cdef int inc = 1
        blas.zcopy(&self.k_states, &self.constant[0], &inc,
                                     &initial_state_mean[offset], &inc)

        return 0

    cdef int initialize_known_stationary_cov(self, int offset,
                                              np.complex128_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i, inc = 1
        # Copy columns
        for i in range(self.k_states):
            blas.zcopy(&self.k_states, &self.stationary_cov[0, i], &inc,
                                         &initial_stationary_state_cov[offset, offset + i], &inc)

        return 0

    cdef int initialize_diffuse(self, int offset,
                                 np.complex128_t [::1, :] initial_diffuse_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_diffuse_state_cov[i, i] = 1

        return 0

    cdef int initialize_approximate_diffuse(self, int offset,
                                             np.complex128_t [::1, :] initial_stationary_state_cov) except 1:
        cdef int i
        for i in range(offset, offset + self.k_states):
            initial_stationary_state_cov[i, i] = self.approximate_diffuse_variance

        return 0

    cdef int initialize_stationary_constant(self, int offset, zStatespace model,
                                    np.complex128_t [:] initial_state_mean,
                                    int complex_step=False) except 1:

        cdef:
            np.npy_intp dim2[2]
            int i, info, inc = 1
            int k_states2 = self.k_states**2
            np.float64_t asum, tol = 1e-9
            cdef np.complex128_t scalar
            cdef int [::1,:] ipiv

        # Clear the unconditional mean (for this block)
        initial_state_mean[offset:offset + self.k_states] = 0

        # Check if the state intercept is all zeros; if it is, then the
        # unconditional mean is also all zeros
        asum = blas.dzasum(&model.k_states, &model.state_intercept[0, 0], &inc)

        # If the state intercept is non-zero, compute the mean
        if asum > tol:
            dim2[0] = self.k_states
            dim2[1] = self.k_states
            ipiv = np.PyArray_ZEROS(2, dim2, np.NPY_INT32, FORTRAN)

            # Create T - I
            # (copy colummns)
            for i in range(self.k_states):
                blas.zcopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                    &self._tmp_transition[0,i], &inc)
                self._tmp_transition[i, i] = self._tmp_transition[i, i] - 1
            # Multiply by -1 to get I - T
            scalar = -1.0
            blas.zscal(&k_states2, &scalar, &self._tmp_transition[0, 0], &inc)

            # c
            blas.zcopy(&self.k_states, &model.state_intercept[offset,0], &inc,
                                                &initial_state_mean[offset], &inc)

            # Solve (I - T) x = c
            lapack.zgetrf(&self.k_states, &self.k_states, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &info)
            lapack.zgetrs('N', &self.k_states, &inc, &self._tmp_transition[0, 0], &self.k_states,
                                   &ipiv[0, 0], &initial_state_mean[offset], &self.k_states, &info)

        return 0

    cdef int initialize_stationary_stationary_cov(self, int offset, zStatespace model,
                                        np.complex128_t [::1, :] initial_stationary_state_cov,
                                        int complex_step=False) except 1:
        cdef:
            int i, inc = 1
            int k_states2 = self.k_states**2

        # Create selected state covariance matrix
        tools._zselect_cov(self.k_states, model.k_posdef, model.k_states,
                             &model.tmp[0,0],
                             &model.selection[offset,0,0],
                             &model.state_cov[0,0,0],
                             &self._tmp_selected_state_cov[0,0])

        # Create a copy of the transition matrix
        # (copy colummns)
        for i in range(self.k_states):
            blas.zcopy(&self.k_states, &model.transition[offset,offset + i,0], &inc,
                                                &self._tmp_transition[0,i], &inc)

        # Solve the discrete Lyapunov equation to the get initial state
        # covariance matrix
        tools._zsolve_discrete_lyapunov(
            &self._tmp_transition[0,0], &self._tmp_selected_state_cov[0,0], self.k_states, complex_step)

        # Copy into initial_stationary_state_cov
        # (copy colummns)
        for i in range(self.k_states):
            blas.zcopy(&self.k_states, &self._tmp_selected_state_cov[0,i], &inc,
                                                &initial_stationary_state_cov[offset,offset + i], &inc)

        return 0

