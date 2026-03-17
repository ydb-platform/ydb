#cython: profile=False
#cython: boundscheck=False
#cython: wraparound=False
#cython: cdivision=False
"""
State Space Models

Author: Chad Fulton  
License: Simplified-BSD
"""

# ## Constants

# ### Smoothers
cdef int SMOOTHER_STATE = 0x01           # Durbin and Koopman (2012), Chapter 4.4.2
cdef int SMOOTHER_STATE_COV = 0x02       # Durbin and Koopman (2012), Chapter 4.4.3
cdef int SMOOTHER_DISTURBANCE = 0x04     # Durbin and Koopman (2012), Chapter 4.5
cdef int SMOOTHER_DISTURBANCE_COV = 0x08 # Durbin and Koopman (2012), Chapter 4.5
cdef int SMOOTHER_STATE_AUTOCOV = 0x10       # Durbin and Koopman (2012), Chapter 4.7
cdef int SMOOTHER_ALL = (
    SMOOTHER_STATE | SMOOTHER_STATE_COV | SMOOTHER_STATE_AUTOCOV |
    SMOOTHER_DISTURBANCE | SMOOTHER_DISTURBANCE_COV
)

cdef int SMOOTH_CONVENTIONAL = 0x01
cdef int SMOOTH_CLASSICAL = 0x02
cdef int SMOOTH_ALTERNATIVE = 0x04
cdef int SMOOTH_UNIVARIATE = 0x08

from statsmodels.tsa.statespace._kalman_filter cimport (
    FILTER_CONVENTIONAL, FILTER_UNIVARIATE, FILTER_COLLAPSED,
    MEMORY_NO_PREDICTED, MEMORY_NO_GAIN, MEMORY_NO_SMOOTHING
)

# Typical imports
import numpy as np
import warnings
cimport numpy as np
cimport cython

np.import_array()

cimport scipy.linalg.cython_blas as blas

cdef int FORTRAN = 1

from statsmodels.tsa.statespace._smoothers._conventional cimport (
    ssmoothed_estimators_missing_conventional,
    ssmoothed_disturbances_missing_conventional,
    ssmoothed_estimators_measurement_conventional,
    ssmoothed_estimators_time_conventional,
    ssmoothed_state_conventional,
    ssmoothed_state_autocov_conventional,
    ssmoothed_disturbances_conventional
)
from statsmodels.tsa.statespace._smoothers._univariate cimport (
    ssmoothed_estimators_measurement_univariate,
    ssmoothed_estimators_time_univariate,
    ssmoothed_disturbances_univariate
)
from statsmodels.tsa.statespace._smoothers._univariate_diffuse cimport (
    ssmoothed_estimators_measurement_univariate_diffuse,
    ssmoothed_estimators_time_univariate_diffuse,
    ssmoothed_state_univariate_diffuse,
    ssmoothed_disturbances_univariate_diffuse,
    ssmoothed_state_autocov_univariate_diffuse
)
from statsmodels.tsa.statespace._smoothers._classical cimport (
    ssmoothed_estimators_measurement_classical,
    ssmoothed_estimators_time_classical,
    ssmoothed_state_classical
)
from statsmodels.tsa.statespace._smoothers._alternative cimport (
    ssmoothed_estimators_measurement_alternative,
    ssmoothed_estimators_time_alternative,
    ssmoothed_state_alternative,
    ssmoothed_disturbances_alternative
)

# ## Kalman filter
cdef class sKalmanSmoother(object):
    """
    sKalmanSmoother(model, kfilter, smoother_output=SMOOTHING_ALL)

    A representation of the Kalman smoother recursions; it performs a single
    backwards pass through the data (after the forwards pass via the Kalman
    filter has already been completed). In all cases, it calculates:

    - `scaled_smoothed_estimator`
    - `smoothing_error`

    it can optionally peform three types of smoothing:

    - State smoothing provides `smoothed_state` and `smoothed_state_cov`
    - Disturbance smoothing provides `smoothed_measurement_disturbance` and
      `smoothed_state_disturbance`
    - Simulation smoothing provides `sampled_measurement_disturbance` and
      `sampled_state_disturbance` (note that this requires Disturbance
      smoothing as well).

    Note: this output arrays in this class are always defined in-memory
    according to the original dimensions in the sStatespace object.

    Note: if the `filter_method` of the underlying sKalmanFilter
    changes, the smoother *must* be reset using the object callable (__call__)
    or the `reset` method. This is because when the filter method is changed,
    the filter output arrays are reset.
    """

    # ### Statespace model
    # cdef readonly sStatespace model
    # ### Kalman filter
    # cdef readonly sKalmanFilter kfilter

    # ### Smoother parameters
    # Holds the time-iteration state of the filter  
    # *Note*: must be changed using the `seek` method
    # cdef readonly int t
    # cdef readonly int smoother_output
    # Keep track of the filter method against which the arrays were created
    # so that we can re-allocate memory if the filter method changes.
    # cdef readonly int filter_method

    # ### Kalman smoother properties

    # `scaled_smoothed_estimator` $\equiv r_t$ is the **scaled smoothed estimator** of $\eta_t$ $(m \times T)$  
    # cdef readonly np.float32_t [::1,:] scaled_smoothed_estimator

    # `scaled_smoothed_estimator_cov` $\equiv N_t$ is the **scaled smoothed estimator covariance matrix** $(m \times m \times T)$  
    # cdef readonly np.float32_t [::1,:,:] scaled_smoothed_estimator_cov

    # `smoothing_error` $\equiv u_t = F_{t}^{-1} v_t - K_t' r_t$ is the **smoothing error** $(p \times T)$
    # cdef readonly np.float32_t [::1,:] smoothing_error

    # `smoothed_state` $\equiv \hat \alpha_t = E(\alpha_t | Y_n)$ is the **smoothed estimator** of the state $(m \times T)$
    # cdef readonly np.float32_t [::1,:] smoothed_state

    # `smoothed_state_cov` $\equiv V_t = Var(\alpha_t | Y_n)$ is the **smoothed state covariance matrix** $(m \times m \times T)$
    # cdef readonly np.float32_t [::1,:,:] smoothed_state_cov

    # `smoothed_measurement_disturbance` $\equiv \hat \varepsilon_t = E(\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance** $(p \times T)$
    # cdef readonly np.float32_t [::1,:] smoothed_measurement_disturbance

    # `smoothed_state_disturbance` $\equiv \hat \eta_t = E(\eta_t | Y_n)$ is the **smoothed state disturbance** $(r \times T)$
    # cdef readonly np.float32_t [::1,:] smoothed_state_disturbance

    # `smoothed_measurement_disturbance_cov` $\equiv Var (\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance covariance matrix** $(p \times p \times T)$
    # cdef readonly np.float32_t [::1,:,:] smoothed_measurement_disturbance_cov

    # `smoothed_state_disturbance` $\equiv Var (\eta_t | Y_n)$ is the **smoothed state disturbance covariance matrix** $(r \times r \times T)$
    # cdef readonly np.float32_t [::1,:,:] smoothed_state_disturbance_cov

    # ### Temporary arrays
    # These matrices are used to temporarily hold selected observation vectors,
    # design matrices, and observation covariance matrices in the case of
    # missing data.  
    # The following are contiguous memory segments which are then used to
    # store the data in the above matrices.
    # cdef readonly np.float32_t [:] selected_design
    # cdef readonly np.float32_t [:] selected_obs_cov
    # These hold the memory allocations of the unnamed temporary arrays
    # cdef readonly np.float32_t [::1,:] tmpL, tmpL2, tmp0, tmp00, tmp000

    # ### Pointers to current-iteration arrays

    # Statespace
    # cdef np.float32_t * _design
    # cdef np.float32_t * _obs_cov
    # cdef np.float32_t * _transition
    # cdef np.float32_t * _selection
    # cdef np.float32_t * _state_cov

    # Kalman filter
    # cdef np.float32_t * _predicted_state
    # cdef np.float32_t * _predicted_state_cov
    # cdef np.float32_t * _kalman_gain

    # cdef np.float32_t * _tmp1
    # cdef np.float32_t * _tmp2
    # cdef np.float32_t * _tmp3
    # cdef np.float32_t * _tmp4

    # Kalman smoother
    # cdef np.float32_t * _input_scaled_smoothed_estimator
    # cdef np.float32_t * _input_scaled_smoothed_estimator_cov

    # cdef np.float32_t * _scaled_smoothed_estimator
    # cdef np.float32_t * _scaled_smoothed_estimator_cov
    # cdef np.float32_t * _smoothing_error
    # cdef np.float32_t * _smoothed_state
    # cdef np.float32_t * _smoothed_state_cov
    # cdef np.float32_t * _smoothed_measurement_disturbance
    # cdef np.float32_t * _smoothed_state_disturbance
    # cdef np.float32_t * _smoothed_measurement_disturbance_cov
    # cdef np.float32_t * _smoothed_state_disturbance_cov

    # cdef np.float32_t * _tmpL
    # cdef np.float32_t * _tmpL2
    # cdef np.float32_t * _tmp0
    # cdef np.float32_t * _tmp00
    # cdef np.float32_t * _tmp000

    # ### Pointers to current-iteration Kalman smoothing functions
    # cdef int (*smooth_estimators)(
    #     sKalmanSmoother, sKalmanFilter, sStatespace
    # )
    # cdef int (*smooth_state)(
    #     sKalmanSmoother, sKalmanFilter, sStatespace
    # )
    # cdef int (*smooth_disturbances)(
    #     sKalmanSmoother, sKalmanFilter, sStatespace
    # )

    # ### Define some constants
    # cdef readonly int k_endog, k_states, k_posdef, k_endog2, k_states2, k_posdef2, k_endogstates, k_statesposdef

    def __init__(self,
                 sStatespace model,
                 sKalmanFilter kfilter,
                 int smoother_output=SMOOTHER_ALL,
                 int smooth_method=0):

        # Save the model
        self.model = model
        self.kfilter = kfilter

        # Save the parameters
        self.filter_method = kfilter.filter_method

        # Make sure the appropriate output has been stored in the filter
        if self.kfilter.conserve_memory & MEMORY_NO_PREDICTED:
            raise ValueError('Cannot perform smoothing without all predicted states')

        if self.kfilter.conserve_memory & MEMORY_NO_GAIN:
            raise ValueError('Cannot perform smoothing without all Kalman gains')

        if self.kfilter.conserve_memory & MEMORY_NO_SMOOTHING:
            raise ValueError('Cannot perform smoothing without all smoothing variables')

        # Set smoothing output and initialize output arrays
        self.set_smoother_output(smoother_output)
        self.set_smooth_method(smooth_method)

    cdef allocate_arrays(self):
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]
            np.npy_intp dim3[3]
        # #### Allocate arrays for calculations
        # Note: these are defined in memory according to the kfilter dimensions
        #       In the case of FILTERED_COLLAPSED, the smoothed measurement
        #       output describes only the component of transformed observations
        #       that is related to the states.

        # Arrays for Kalman smoother output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothing_error = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs;
        self.smoothed_state = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs;
        self.smoothed_state_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothed_measurement_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim2[0] = self.kfilter.k_posdef; dim2[1] = self.model.nobs;
        self.smoothed_state_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim3[0] = self.kfilter.k_endog; dim3[1] = self.kfilter.k_endog; dim3[2] = self.model.nobs;
        self.smoothed_measurement_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)
        dim3[0] = self.kfilter.k_posdef; dim3[1] = self.kfilter.k_posdef; dim3[2] = self.model.nobs;
        self.smoothed_state_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)

        # Innovations transition matrix (L_t = T_t - K_t Z_t)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.innovations_transition = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)

        # Smoothed state autocovariance arrays
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.smoothed_state_autocov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)

        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_states;
        self.tmp_autocov = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmp_autocov = &self.tmp_autocov[0, 0]

        # Diffuse output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_diffuse_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_diffuse1_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)
        self.scaled_smoothed_diffuse2_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT32, FORTRAN)

        # #### Arrays for temporary calculations
        # *Note*: in math notation below, a $\\#$ will represent a generic
        # temporary array, and a $\\#_i$ will represent a named temporary array.

        # # $L_t$ $(m \times m)$, also holds $(m \times r)$ sometimes
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmpL = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmpL = &self.tmpL[0, 0]
        self.tmpL2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmpL2 = &self.tmpL2[0, 0]

        # # Holds arrays of dimension $(m \times m)$ and $(m \times r)$
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmp0 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmp0 = &self.tmp0[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp00 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmp00 = &self.tmp00[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp000 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT32, FORTRAN)
        self._tmp000 = &self.tmp000[0, 0]

        # Arrays for missing data
        # dim1[0] = self.kfilter.k_endog * self.kfilter.k_states;
        # self.selected_design = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT32, FORTRAN)
        # dim1[0] = self.kfilter.k_endog2;
        # self.selected_obs_cov = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT32, FORTRAN)

    def __reduce__(self):
        state = {
            't': self.t,
            '_smooth_method': self._smooth_method,
            'scaled_smoothed_estimator': np.array(self.scaled_smoothed_estimator, copy=True, order='F'),
            'scaled_smoothed_estimator_cov': np.array(self.scaled_smoothed_estimator_cov, copy=True, order='F'),
            'smoothing_error': np.array(self.smoothing_error, copy=True, order='F'),
            'smoothed_state': np.array(self.smoothed_state, copy=True, order='F'),
            'smoothed_state_cov': np.array(self.smoothed_state_cov, copy=True, order='F'),
            'smoothed_measurement_disturbance': np.array(self.smoothed_measurement_disturbance, copy=True, order='F'),
            'smoothed_state_disturbance': np.array(self.smoothed_state_disturbance, copy=True, order='F'),
            'smoothed_measurement_disturbance_cov': np.array(self.smoothed_measurement_disturbance_cov, copy=True, order='F'),
            'smoothed_state_disturbance_cov': np.array(self.smoothed_state_disturbance_cov, copy=True, order='F'),
            'smoothed_state_autocov': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'innovations_transition': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'tmp_autocov': np.array(self.tmp_autocov, copy=True, order='F'),
            'scaled_smoothed_diffuse_estimator': np.array(self.scaled_smoothed_diffuse_estimator, copy=True, order='F'),
            'scaled_smoothed_diffuse1_estimator_cov': np.array(self.scaled_smoothed_diffuse1_estimator_cov, copy=True, order='F'),
            'scaled_smoothed_diffuse2_estimator_cov': np.array(self.scaled_smoothed_diffuse2_estimator_cov, copy=True, order='F'),
            'tmpL': np.array(self.tmpL, copy=True, order='F'),
            'tmpL2': np.array(self.tmpL2, copy=True, order='F'),
            'tmp0': np.array(self.tmp0, copy=True, order='F'),
            'tmp00': np.array(self.tmp00, copy=True, order='F'),
            'tmp000': np.array(self.tmp000, copy=True, order='F')
        }
        args = (self.model, self.kfilter, self.smoother_output, self.smooth_method)
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.t = state['t']
        self._smooth_method = state['_smooth_method']
        self.scaled_smoothed_estimator = state['scaled_smoothed_estimator']
        self.scaled_smoothed_estimator_cov = state['scaled_smoothed_estimator_cov']
        self.smoothing_error = state['smoothing_error']
        self.smoothed_state = state['smoothed_state']
        self.smoothed_state_cov = state['smoothed_state_cov']
        self.smoothed_measurement_disturbance = state['smoothed_measurement_disturbance']
        self.smoothed_state_disturbance = state['smoothed_state_disturbance']
        self.smoothed_measurement_disturbance_cov = state['smoothed_measurement_disturbance_cov']
        self.smoothed_state_disturbance_cov = state['smoothed_state_disturbance_cov']
        self.smoothed_state_autocov = state['smoothed_state_autocov']
        self.innovations_transition = state['innovations_transition']
        self.tmp_autocov = state['tmp_autocov']
        self.scaled_smoothed_diffuse_estimator = state['scaled_smoothed_diffuse_estimator']
        self.scaled_smoothed_diffuse1_estimator_cov = state['scaled_smoothed_diffuse1_estimator_cov']
        self.scaled_smoothed_diffuse2_estimator_cov = state['scaled_smoothed_diffuse2_estimator_cov']
        self.tmpL = state['tmpL']
        self.tmpL2 = state['tmpL2']
        self.tmp0 = state['tmp0']
        self.tmp00 = state['tmp00']
        self.tmp000 = state['tmp000']
        self.initialize_smoother_object_pointers()
        self._initialize_temp_pointers()

    cdef void _initialize_temp_pointers(self) except *:
        self._tmpL = &self.tmpL[0, 0]
        self._tmpL2 = &self.tmpL2[0, 0]
        self._tmp0 = &self.tmp0[0, 0]
        self._tmp00 = &self.tmp00[0, 0]
        self._tmp000 = &self.tmp000[0, 0]
        self._tmp_autocov = &self.tmp_autocov[0, 0]

    cdef int check_filter_method_changed(self):
        return not self.kfilter.filter_method == self.filter_method

    cdef int reset_filter_method(self, int force_reset=True):
        cdef int changed = self.check_filter_method_changed()

        if changed or force_reset:
            # Save the new method
            self.filter_method = self.kfilter.filter_method
            # Reset matrices
            self.allocate_arrays()
            # Reset the smooth method (in case it was based on the filter
            # method)
            self.set_smooth_method(self.smooth_method)

        return changed

    cpdef set_smoother_output(self, int smoother_output, int force_reset=True):
        if not smoother_output == self.smoother_output or force_reset:
            # Change the smoother output flag
            self.smoother_output = smoother_output

            # Reset matrices
            self.reset(True)

    cpdef set_smooth_method(self, int smooth_method):
        cdef int _smooth_method
        self.smooth_method = smooth_method

        # If no smooth method provided, use default for the filter type
        if self.smooth_method == 0:
            self.reset_filter_method(False)
            if self.kfilter.filter_method & FILTER_UNIVARIATE:
                _smooth_method = SMOOTH_UNIVARIATE
            else:
                _smooth_method = SMOOTH_CONVENTIONAL
        else:
            _smooth_method = self.smooth_method

        # Make sure we do not have an invalid smooth method for our filter
        # method
        if((_smooth_method & SMOOTH_UNIVARIATE) and not (self.filter_method & FILTER_UNIVARIATE) or 
                (self.filter_method & FILTER_UNIVARIATE) and not (_smooth_method & SMOOTH_UNIVARIATE)):
            raise ValueError('Invalid smoothing method: can only use'
                             ' univariate smoothing when univariate filtering'
                             ' has been used previously.')


        self._smooth_method = _smooth_method

    cpdef reset(self, int force_reset=False):
        """
        reset(self)

        Reset the smoother.
        """
        # Reset the filter method (if necessary)
        self.reset_filter_method(force_reset)

        # Set the time
        self.t = self.model.nobs-1

    cpdef seek(self, unsigned int t):
        """
        seek(self, t)

        Change the time-state of the smoother

        Notes
        -----
        Between seek calls, the `filter_method` parameter of the associated
        Kalman filter object is not allowed to change. If the `filter_method`
        has changed, either recall the smoother using the object callable or
        explicitly reset the smoother using the `reset` method.
        """
        # Make sure the seek location is valid
        if not t == 0 and t >= <unsigned int>self.model.nobs:
            raise IndexError("Observation index out of range")

        # Make sure we have not changed filter methods in-between seeking
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother seek() calls."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")
        self.t = t

    def __iter__(self):
        return self

    def __call__(self, int smoother_output=-1):
        """
        Iterate the smoother across the entire set of observations.
        """
        cdef int i

        # Reset the smoother
        self.reset()
        
        # Perform backwards smoothing iterations
        for i in range(self.model.nobs-1,-1,-1):
            next(self)

    def __next__(self):
        """
        Perform an iteration of the Kalman smoother
        """
        cdef int t, inc = 1
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse

        # Get time subscript, and stop the iterator if at the end
        if not self.t >= 0:
            raise StopIteration

        # Make sure we have not changed filter methods in-between iterations
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother iterations."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")

        # Initialize pointers to current-iteration objects
        self.initialize_statespace_object_pointers()
        self.initialize_filter_object_pointers()
        self.initialize_smoother_object_pointers()

        # Initialize pointers to appropriate Kalman smoothing functions
        self.initialize_function_pointers()

        # Clear measurement disturbance variables` if we switched from
        # multivariate to univariate (because we might have off-diagonal
        # elements stored from a previous multivariate run that wouldn't have
        # been cleared yet)
        if self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t - 1] == 1:
            if self._smooth_method & SMOOTH_CLASSICAL:
                raise NotImplementedError(
                    'Cannot use classical smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                raise NotImplementedError(
                    'Cannot use alternative smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            self.smoothing_error[..., :self.t] = 0
            self.smoothed_measurement_disturbance_cov[..., :self.t] = 0
            self.innovations_transition[..., :self.t] = 0

        # Conventional timing of the measurement step of the scaled smoothed
        # estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state and covariance matrix  
        # $\hat \alpha_t, V_t$
        if self.smoother_output & (SMOOTHER_STATE | SMOOTHER_STATE_COV):
            self.smooth_state(self, self.kfilter, self.model)

        # Modified Byrson-Frazier timing of the measurement step of the scaled
        # smoothed estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if not diffuse and self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state autocovariance matrix
        if diffuse:
            blas.scopy(&self.kfilter.k_states2, self.kfilter._tmpL0, &inc, self._innovations_transition, &inc)
        else:
            blas.scopy(&self.kfilter.k_states2, self._tmpL, &inc, self._innovations_transition, &inc)
        if self.smoother_output & SMOOTHER_STATE_AUTOCOV:
            if diffuse:
                ssmoothed_state_autocov_univariate_diffuse(self, self.kfilter, self.model)
            else:
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t+1]
                ssmoothed_state_autocov_conventional(self, self.kfilter, self.model)
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t]

        # Smoothed disturbances  
        # $\hat \eta_t, \hat \varepsilon_t, Var(\eta_t | Y_n), Var(\varepsilon_t | Y_n)$
        if self.smoother_output & SMOOTHER_DISTURBANCE:
            self.smooth_disturbances(self, self.kfilter, self.model)

        # Time step of the scaled smoothed estimator and covariance matrix
        self.smooth_estimators_time(self, self.kfilter, self.model)

        # When switching from the non-univariate filtering methods to the
        # univariate filtering method, we need to run one or more
        # univariate time smoothing steps to get the appropriate values (e.g.
        # for scaled_smoothed_estimator). Moreover, the alternative and
        # classical methods have different timing.
        # This happens in two cases:
        # 1. Exact diffuse filtering and smoothing only uses the univariate
        #    method, so if non-univariate filtering methods are used for
        #    the remaining filtering and smoothing then we need to run this.
        # 2. If the multivariate filtering method failed in a particular time
        #    step (due to a singular forecast error covariance matrix) and
        #    the univariate method was used as a fallback, then we need to run
        #    this.
        if ((self.kfilter.nobs_diffuse > 0 and self.t == self.kfilter.nobs_diffuse) or
                (self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t-1] == 1)):
            t = self.t
            if self._smooth_method & SMOOTH_CONVENTIONAL:
                ssmoothed_estimators_time_univariate(self, self.kfilter, self.model)
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                self.t = self.t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                ssmoothed_estimators_time_univariate(self, self.kfilter, self.model)
                self.t = t
            elif self._smooth_method & SMOOTH_CLASSICAL:
                self.t = t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                ssmoothed_estimators_measurement_classical(self, self.kfilter, self.model)
                self.t = t
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                ssmoothed_estimators_time_univariate(self, self.kfilter, self.model)

        # Note: the above diffuse handling for alternative and classical
        # handling modifies the pointers, and should be the last part of this
        # function

        # Advance the smoother
        self.t -= 1

    cdef void initialize_statespace_object_pointers(self) except *:
        cdef:
            int transform_diagonalize = 0
            int transform_generalized_collapse = 0
            int collapse_occurred = 0

        # Determine which transformations (would) need to be made
        transform_generalized_collapse = self.kfilter.filter_method & FILTER_COLLAPSED
        if not transform_generalized_collapse:
            transform_diagonalize = self.kfilter.univariate_filter[self.t]

        # Initialize object-level pointers to statespace arrays
        # Note: does not matter what transformations were required for the
        #       filter; we do not need to perform them for the smoother
        # TODO  actually we do, to get _design, _obs_cov, etc. However we do not
        #       need it to recalculate the selected_obs and loglikelihood, so
        #       need to decouple those parts from the generalized collapse
        self.model.seek(self.t, transform_diagonalize, transform_generalized_collapse)

        # Initialize object-level pointers to statespace arrays
        # self._design = self.model._design
        # self._obs_cov = self.model._obs_cov
        # self._transition = self.model._transition
        # self._selection = self.model._selection
        # self._state_cov = self.model._state_cov

        # A collapse would not actually occur in a given iteration, even with
        # the FILTER_COLLAPSED flag, in the case that there was enough missing
        # data that k_endog - nmissing <= k_states
        # collapse_occurred = (
        #     transform_generalized_collapse and
        #     self.model.k_endog - self.model._nmissing > self.model.k_states
        # )

        # If a collapse should have occurred, the dimensions need to be
        # adjusted (because we did not tell the model about the collapse in the
        # seek() call above)
        # if collapse_occurred:
        #     self.model.set_dimensions(self.model.k_states,
        #                               self.model.k_states,
        #                               self.model.k_posdef)

    cdef void initialize_filter_object_pointers(self):
        # cdef:
        #     int t = self.t
        #     int inc = 1

        # # Initialize object-level pointers to output arrays
        # self._predicted_state = &self.kfilter.predicted_state[0, t]
        # self._predicted_state_cov = &self.kfilter.predicted_state_cov[0, 0, t]
        # self._kalman_gain = &self.kfilter.kalman_gain[0, 0, t]

        # # Initialize object-level pointers to named temporary arrays
        # self._tmp1 = &self.kfilter.tmp1[0, 0, t]
        # self._tmp2 = &self.kfilter.tmp2[0, t]
        # self._tmp3 = &self.kfilter.tmp3[0, 0, t]
        # self._tmp4 = &self.kfilter.tmp4[0, 0, t]

        self.kfilter.seek(self.t, False)
        self.kfilter.initialize_filter_object_pointers()

    cdef void initialize_smoother_object_pointers(self) except *:
        cdef:
            int t = self.t
            int inc = 1
            int diffuse = self.t < self.kfilter.nobs_diffuse

        # Initialize object-level pointers to output arrays
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t+1]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
        else:  # if self._smooth_method & SMOOTH_ALTERNATIVE
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t-1]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t-1]

        self._smoothing_error = &self.smoothing_error[0, t]
        self._smoothed_state = &self.smoothed_state[0, t]
        self._smoothed_state_cov = &self.smoothed_state_cov[0, 0, t]
        self._smoothed_measurement_disturbance = &self.smoothed_measurement_disturbance[0, t]
        self._smoothed_state_disturbance = &self.smoothed_state_disturbance[0, t]
        self._smoothed_measurement_disturbance_cov = &self.smoothed_measurement_disturbance_cov[0, 0, t]
        self._smoothed_state_disturbance_cov = &self.smoothed_state_disturbance_cov[0, 0, t]

        self._innovations_transition = &self.innovations_transition[0, 0, t]
        self._smoothed_state_autocov = &self.smoothed_state_autocov[0, 0, t]

        # Diffuse
        if diffuse:
            self._input_scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t+1]
            self._input_scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t+1]
            self._input_scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t]
            self._scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t]
            self._scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t]


    cdef void initialize_function_pointers(self) except *:
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse
        # Diffuse smoother
        if diffuse:
            self.smooth_estimators_measurement = ssmoothed_estimators_measurement_univariate_diffuse
            self.smooth_estimators_time = ssmoothed_estimators_time_univariate_diffuse
            self.smooth_state = ssmoothed_state_univariate_diffuse
            self.smooth_disturbances = ssmoothed_disturbances_univariate_diffuse
        # Univariate (modified Bryson-Frazier) smoother
        elif (self._smooth_method & SMOOTH_UNIVARIATE) or self.kfilter.univariate_filter[self.t]:
            self.smooth_estimators_measurement = ssmoothed_estimators_measurement_univariate
            self.smooth_estimators_time = ssmoothed_estimators_time_univariate
            self.smooth_state = ssmoothed_state_conventional
            self.smooth_disturbances = ssmoothed_disturbances_univariate
        # Multivariate modified Bryson-Frazier smoother
        elif self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement = ssmoothed_estimators_measurement_alternative
            self.smooth_estimators_time = ssmoothed_estimators_time_alternative
            self.smooth_state = ssmoothed_state_alternative
            self.smooth_disturbances = ssmoothed_disturbances_alternative
        # Multivariate classical (Anderson and Moore) smoother
        elif self._smooth_method & SMOOTH_CLASSICAL:
            self.smooth_estimators_measurement = ssmoothed_estimators_measurement_classical
            self.smooth_estimators_time = ssmoothed_estimators_time_classical
            self.smooth_state = ssmoothed_state_classical
            self.smooth_disturbances = ssmoothed_disturbances_conventional
        # Multivariate conventional (Durbin and Koopman) smoother
        elif self._smooth_method & SMOOTH_CONVENTIONAL:
            self.smooth_estimators_measurement = ssmoothed_estimators_measurement_conventional
            self.smooth_estimators_time = ssmoothed_estimators_time_conventional
            self.smooth_state = ssmoothed_state_conventional
            self.smooth_disturbances = ssmoothed_disturbances_conventional
        else:
            raise NotImplementedError("Smoother method not available.")

        # Handle completely missing data
        # (All methods except the conventional method can use the same routines in this case)
        # This is essentially just an application of the smoothed_estimators_time_* step.
        if not diffuse and self._smooth_method & SMOOTH_CONVENTIONAL and self.model._nmissing == self.model.k_endog:
            # Change the smoothing functions to take into account a missing observation
            self.smooth_estimators_measurement = ssmoothed_estimators_missing_conventional
            # (no need to change the state smoothing recursion)
            # self.smooth_state = ssmoothed_state_missing_conventional
            self.smooth_disturbances = ssmoothed_disturbances_missing_conventional

from statsmodels.tsa.statespace._smoothers._conventional cimport (
    dsmoothed_estimators_missing_conventional,
    dsmoothed_disturbances_missing_conventional,
    dsmoothed_estimators_measurement_conventional,
    dsmoothed_estimators_time_conventional,
    dsmoothed_state_conventional,
    dsmoothed_state_autocov_conventional,
    dsmoothed_disturbances_conventional
)
from statsmodels.tsa.statespace._smoothers._univariate cimport (
    dsmoothed_estimators_measurement_univariate,
    dsmoothed_estimators_time_univariate,
    dsmoothed_disturbances_univariate
)
from statsmodels.tsa.statespace._smoothers._univariate_diffuse cimport (
    dsmoothed_estimators_measurement_univariate_diffuse,
    dsmoothed_estimators_time_univariate_diffuse,
    dsmoothed_state_univariate_diffuse,
    dsmoothed_disturbances_univariate_diffuse,
    dsmoothed_state_autocov_univariate_diffuse
)
from statsmodels.tsa.statespace._smoothers._classical cimport (
    dsmoothed_estimators_measurement_classical,
    dsmoothed_estimators_time_classical,
    dsmoothed_state_classical
)
from statsmodels.tsa.statespace._smoothers._alternative cimport (
    dsmoothed_estimators_measurement_alternative,
    dsmoothed_estimators_time_alternative,
    dsmoothed_state_alternative,
    dsmoothed_disturbances_alternative
)

# ## Kalman filter
cdef class dKalmanSmoother(object):
    """
    dKalmanSmoother(model, kfilter, smoother_output=SMOOTHING_ALL)

    A representation of the Kalman smoother recursions; it performs a single
    backwards pass through the data (after the forwards pass via the Kalman
    filter has already been completed). In all cases, it calculates:

    - `scaled_smoothed_estimator`
    - `smoothing_error`

    it can optionally peform three types of smoothing:

    - State smoothing provides `smoothed_state` and `smoothed_state_cov`
    - Disturbance smoothing provides `smoothed_measurement_disturbance` and
      `smoothed_state_disturbance`
    - Simulation smoothing provides `sampled_measurement_disturbance` and
      `sampled_state_disturbance` (note that this requires Disturbance
      smoothing as well).

    Note: this output arrays in this class are always defined in-memory
    according to the original dimensions in the dStatespace object.

    Note: if the `filter_method` of the underlying dKalmanFilter
    changes, the smoother *must* be reset using the object callable (__call__)
    or the `reset` method. This is because when the filter method is changed,
    the filter output arrays are reset.
    """

    # ### Statespace model
    # cdef readonly dStatespace model
    # ### Kalman filter
    # cdef readonly dKalmanFilter kfilter

    # ### Smoother parameters
    # Holds the time-iteration state of the filter  
    # *Note*: must be changed using the `seek` method
    # cdef readonly int t
    # cdef readonly int smoother_output
    # Keep track of the filter method against which the arrays were created
    # so that we can re-allocate memory if the filter method changes.
    # cdef readonly int filter_method

    # ### Kalman smoother properties

    # `scaled_smoothed_estimator` $\equiv r_t$ is the **scaled smoothed estimator** of $\eta_t$ $(m \times T)$  
    # cdef readonly np.float64_t [::1,:] scaled_smoothed_estimator

    # `scaled_smoothed_estimator_cov` $\equiv N_t$ is the **scaled smoothed estimator covariance matrix** $(m \times m \times T)$  
    # cdef readonly np.float64_t [::1,:,:] scaled_smoothed_estimator_cov

    # `smoothing_error` $\equiv u_t = F_{t}^{-1} v_t - K_t' r_t$ is the **smoothing error** $(p \times T)$
    # cdef readonly np.float64_t [::1,:] smoothing_error

    # `smoothed_state` $\equiv \hat \alpha_t = E(\alpha_t | Y_n)$ is the **smoothed estimator** of the state $(m \times T)$
    # cdef readonly np.float64_t [::1,:] smoothed_state

    # `smoothed_state_cov` $\equiv V_t = Var(\alpha_t | Y_n)$ is the **smoothed state covariance matrix** $(m \times m \times T)$
    # cdef readonly np.float64_t [::1,:,:] smoothed_state_cov

    # `smoothed_measurement_disturbance` $\equiv \hat \varepsilon_t = E(\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance** $(p \times T)$
    # cdef readonly np.float64_t [::1,:] smoothed_measurement_disturbance

    # `smoothed_state_disturbance` $\equiv \hat \eta_t = E(\eta_t | Y_n)$ is the **smoothed state disturbance** $(r \times T)$
    # cdef readonly np.float64_t [::1,:] smoothed_state_disturbance

    # `smoothed_measurement_disturbance_cov` $\equiv Var (\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance covariance matrix** $(p \times p \times T)$
    # cdef readonly np.float64_t [::1,:,:] smoothed_measurement_disturbance_cov

    # `smoothed_state_disturbance` $\equiv Var (\eta_t | Y_n)$ is the **smoothed state disturbance covariance matrix** $(r \times r \times T)$
    # cdef readonly np.float64_t [::1,:,:] smoothed_state_disturbance_cov

    # ### Temporary arrays
    # These matrices are used to temporarily hold selected observation vectors,
    # design matrices, and observation covariance matrices in the case of
    # missing data.  
    # The following are contiguous memory segments which are then used to
    # store the data in the above matrices.
    # cdef readonly np.float64_t [:] selected_design
    # cdef readonly np.float64_t [:] selected_obs_cov
    # These hold the memory allocations of the unnamed temporary arrays
    # cdef readonly np.float64_t [::1,:] tmpL, tmpL2, tmp0, tmp00, tmp000

    # ### Pointers to current-iteration arrays

    # Statespace
    # cdef np.float64_t * _design
    # cdef np.float64_t * _obs_cov
    # cdef np.float64_t * _transition
    # cdef np.float64_t * _selection
    # cdef np.float64_t * _state_cov

    # Kalman filter
    # cdef np.float64_t * _predicted_state
    # cdef np.float64_t * _predicted_state_cov
    # cdef np.float64_t * _kalman_gain

    # cdef np.float64_t * _tmp1
    # cdef np.float64_t * _tmp2
    # cdef np.float64_t * _tmp3
    # cdef np.float64_t * _tmp4

    # Kalman smoother
    # cdef np.float64_t * _input_scaled_smoothed_estimator
    # cdef np.float64_t * _input_scaled_smoothed_estimator_cov

    # cdef np.float64_t * _scaled_smoothed_estimator
    # cdef np.float64_t * _scaled_smoothed_estimator_cov
    # cdef np.float64_t * _smoothing_error
    # cdef np.float64_t * _smoothed_state
    # cdef np.float64_t * _smoothed_state_cov
    # cdef np.float64_t * _smoothed_measurement_disturbance
    # cdef np.float64_t * _smoothed_state_disturbance
    # cdef np.float64_t * _smoothed_measurement_disturbance_cov
    # cdef np.float64_t * _smoothed_state_disturbance_cov

    # cdef np.float64_t * _tmpL
    # cdef np.float64_t * _tmpL2
    # cdef np.float64_t * _tmp0
    # cdef np.float64_t * _tmp00
    # cdef np.float64_t * _tmp000

    # ### Pointers to current-iteration Kalman smoothing functions
    # cdef int (*smooth_estimators)(
    #     dKalmanSmoother, dKalmanFilter, dStatespace
    # )
    # cdef int (*smooth_state)(
    #     dKalmanSmoother, dKalmanFilter, dStatespace
    # )
    # cdef int (*smooth_disturbances)(
    #     dKalmanSmoother, dKalmanFilter, dStatespace
    # )

    # ### Define some constants
    # cdef readonly int k_endog, k_states, k_posdef, k_endog2, k_states2, k_posdef2, k_endogstates, k_statesposdef

    def __init__(self,
                 dStatespace model,
                 dKalmanFilter kfilter,
                 int smoother_output=SMOOTHER_ALL,
                 int smooth_method=0):

        # Save the model
        self.model = model
        self.kfilter = kfilter

        # Save the parameters
        self.filter_method = kfilter.filter_method

        # Make sure the appropriate output has been stored in the filter
        if self.kfilter.conserve_memory & MEMORY_NO_PREDICTED:
            raise ValueError('Cannot perform smoothing without all predicted states')

        if self.kfilter.conserve_memory & MEMORY_NO_GAIN:
            raise ValueError('Cannot perform smoothing without all Kalman gains')

        if self.kfilter.conserve_memory & MEMORY_NO_SMOOTHING:
            raise ValueError('Cannot perform smoothing without all smoothing variables')

        # Set smoothing output and initialize output arrays
        self.set_smoother_output(smoother_output)
        self.set_smooth_method(smooth_method)

    cdef allocate_arrays(self):
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]
            np.npy_intp dim3[3]
        # #### Allocate arrays for calculations
        # Note: these are defined in memory according to the kfilter dimensions
        #       In the case of FILTERED_COLLAPSED, the smoothed measurement
        #       output describes only the component of transformed observations
        #       that is related to the states.

        # Arrays for Kalman smoother output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothing_error = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs;
        self.smoothed_state = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs;
        self.smoothed_state_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothed_measurement_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim2[0] = self.kfilter.k_posdef; dim2[1] = self.model.nobs;
        self.smoothed_state_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim3[0] = self.kfilter.k_endog; dim3[1] = self.kfilter.k_endog; dim3[2] = self.model.nobs;
        self.smoothed_measurement_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)
        dim3[0] = self.kfilter.k_posdef; dim3[1] = self.kfilter.k_posdef; dim3[2] = self.model.nobs;
        self.smoothed_state_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)

        # Innovations transition matrix (L_t = T_t - K_t Z_t)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.innovations_transition = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)

        # Smoothed state autocovariance arrays
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.smoothed_state_autocov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)

        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_states;
        self.tmp_autocov = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmp_autocov = &self.tmp_autocov[0, 0]

        # Diffuse output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_diffuse_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_diffuse1_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)
        self.scaled_smoothed_diffuse2_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_FLOAT64, FORTRAN)

        # #### Arrays for temporary calculations
        # *Note*: in math notation below, a $\\#$ will represent a generic
        # temporary array, and a $\\#_i$ will represent a named temporary array.

        # # $L_t$ $(m \times m)$, also holds $(m \times r)$ sometimes
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmpL = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmpL = &self.tmpL[0, 0]
        self.tmpL2 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmpL2 = &self.tmpL2[0, 0]

        # # Holds arrays of dimension $(m \times m)$ and $(m \times r)$
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmp0 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmp0 = &self.tmp0[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp00 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmp00 = &self.tmp00[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp000 = np.PyArray_ZEROS(2, dim2, np.NPY_FLOAT64, FORTRAN)
        self._tmp000 = &self.tmp000[0, 0]

        # Arrays for missing data
        # dim1[0] = self.kfilter.k_endog * self.kfilter.k_states;
        # self.selected_design = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)
        # dim1[0] = self.kfilter.k_endog2;
        # self.selected_obs_cov = np.PyArray_ZEROS(1, dim1, np.NPY_FLOAT64, FORTRAN)

    def __reduce__(self):
        state = {
            't': self.t,
            '_smooth_method': self._smooth_method,
            'scaled_smoothed_estimator': np.array(self.scaled_smoothed_estimator, copy=True, order='F'),
            'scaled_smoothed_estimator_cov': np.array(self.scaled_smoothed_estimator_cov, copy=True, order='F'),
            'smoothing_error': np.array(self.smoothing_error, copy=True, order='F'),
            'smoothed_state': np.array(self.smoothed_state, copy=True, order='F'),
            'smoothed_state_cov': np.array(self.smoothed_state_cov, copy=True, order='F'),
            'smoothed_measurement_disturbance': np.array(self.smoothed_measurement_disturbance, copy=True, order='F'),
            'smoothed_state_disturbance': np.array(self.smoothed_state_disturbance, copy=True, order='F'),
            'smoothed_measurement_disturbance_cov': np.array(self.smoothed_measurement_disturbance_cov, copy=True, order='F'),
            'smoothed_state_disturbance_cov': np.array(self.smoothed_state_disturbance_cov, copy=True, order='F'),
            'smoothed_state_autocov': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'innovations_transition': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'tmp_autocov': np.array(self.tmp_autocov, copy=True, order='F'),
            'scaled_smoothed_diffuse_estimator': np.array(self.scaled_smoothed_diffuse_estimator, copy=True, order='F'),
            'scaled_smoothed_diffuse1_estimator_cov': np.array(self.scaled_smoothed_diffuse1_estimator_cov, copy=True, order='F'),
            'scaled_smoothed_diffuse2_estimator_cov': np.array(self.scaled_smoothed_diffuse2_estimator_cov, copy=True, order='F'),
            'tmpL': np.array(self.tmpL, copy=True, order='F'),
            'tmpL2': np.array(self.tmpL2, copy=True, order='F'),
            'tmp0': np.array(self.tmp0, copy=True, order='F'),
            'tmp00': np.array(self.tmp00, copy=True, order='F'),
            'tmp000': np.array(self.tmp000, copy=True, order='F')
        }
        args = (self.model, self.kfilter, self.smoother_output, self.smooth_method)
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.t = state['t']
        self._smooth_method = state['_smooth_method']
        self.scaled_smoothed_estimator = state['scaled_smoothed_estimator']
        self.scaled_smoothed_estimator_cov = state['scaled_smoothed_estimator_cov']
        self.smoothing_error = state['smoothing_error']
        self.smoothed_state = state['smoothed_state']
        self.smoothed_state_cov = state['smoothed_state_cov']
        self.smoothed_measurement_disturbance = state['smoothed_measurement_disturbance']
        self.smoothed_state_disturbance = state['smoothed_state_disturbance']
        self.smoothed_measurement_disturbance_cov = state['smoothed_measurement_disturbance_cov']
        self.smoothed_state_disturbance_cov = state['smoothed_state_disturbance_cov']
        self.smoothed_state_autocov = state['smoothed_state_autocov']
        self.innovations_transition = state['innovations_transition']
        self.tmp_autocov = state['tmp_autocov']
        self.scaled_smoothed_diffuse_estimator = state['scaled_smoothed_diffuse_estimator']
        self.scaled_smoothed_diffuse1_estimator_cov = state['scaled_smoothed_diffuse1_estimator_cov']
        self.scaled_smoothed_diffuse2_estimator_cov = state['scaled_smoothed_diffuse2_estimator_cov']
        self.tmpL = state['tmpL']
        self.tmpL2 = state['tmpL2']
        self.tmp0 = state['tmp0']
        self.tmp00 = state['tmp00']
        self.tmp000 = state['tmp000']
        self.initialize_smoother_object_pointers()
        self._initialize_temp_pointers()

    cdef void _initialize_temp_pointers(self) except *:
        self._tmpL = &self.tmpL[0, 0]
        self._tmpL2 = &self.tmpL2[0, 0]
        self._tmp0 = &self.tmp0[0, 0]
        self._tmp00 = &self.tmp00[0, 0]
        self._tmp000 = &self.tmp000[0, 0]
        self._tmp_autocov = &self.tmp_autocov[0, 0]

    cdef int check_filter_method_changed(self):
        return not self.kfilter.filter_method == self.filter_method

    cdef int reset_filter_method(self, int force_reset=True):
        cdef int changed = self.check_filter_method_changed()

        if changed or force_reset:
            # Save the new method
            self.filter_method = self.kfilter.filter_method
            # Reset matrices
            self.allocate_arrays()
            # Reset the smooth method (in case it was based on the filter
            # method)
            self.set_smooth_method(self.smooth_method)

        return changed

    cpdef set_smoother_output(self, int smoother_output, int force_reset=True):
        if not smoother_output == self.smoother_output or force_reset:
            # Change the smoother output flag
            self.smoother_output = smoother_output

            # Reset matrices
            self.reset(True)

    cpdef set_smooth_method(self, int smooth_method):
        cdef int _smooth_method
        self.smooth_method = smooth_method

        # If no smooth method provided, use default for the filter type
        if self.smooth_method == 0:
            self.reset_filter_method(False)
            if self.kfilter.filter_method & FILTER_UNIVARIATE:
                _smooth_method = SMOOTH_UNIVARIATE
            else:
                _smooth_method = SMOOTH_CONVENTIONAL
        else:
            _smooth_method = self.smooth_method

        # Make sure we do not have an invalid smooth method for our filter
        # method
        if((_smooth_method & SMOOTH_UNIVARIATE) and not (self.filter_method & FILTER_UNIVARIATE) or 
                (self.filter_method & FILTER_UNIVARIATE) and not (_smooth_method & SMOOTH_UNIVARIATE)):
            raise ValueError('Invalid smoothing method: can only use'
                             ' univariate smoothing when univariate filtering'
                             ' has been used previously.')


        self._smooth_method = _smooth_method

    cpdef reset(self, int force_reset=False):
        """
        reset(self)

        Reset the smoother.
        """
        # Reset the filter method (if necessary)
        self.reset_filter_method(force_reset)

        # Set the time
        self.t = self.model.nobs-1

    cpdef seek(self, unsigned int t):
        """
        seek(self, t)

        Change the time-state of the smoother

        Notes
        -----
        Between seek calls, the `filter_method` parameter of the associated
        Kalman filter object is not allowed to change. If the `filter_method`
        has changed, either recall the smoother using the object callable or
        explicitly reset the smoother using the `reset` method.
        """
        # Make sure the seek location is valid
        if not t == 0 and t >= <unsigned int>self.model.nobs:
            raise IndexError("Observation index out of range")

        # Make sure we have not changed filter methods in-between seeking
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother seek() calls."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")
        self.t = t

    def __iter__(self):
        return self

    def __call__(self, int smoother_output=-1):
        """
        Iterate the smoother across the entire set of observations.
        """
        cdef int i

        # Reset the smoother
        self.reset()
        
        # Perform backwards smoothing iterations
        for i in range(self.model.nobs-1,-1,-1):
            next(self)

    def __next__(self):
        """
        Perform an iteration of the Kalman smoother
        """
        cdef int t, inc = 1
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse

        # Get time subscript, and stop the iterator if at the end
        if not self.t >= 0:
            raise StopIteration

        # Make sure we have not changed filter methods in-between iterations
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother iterations."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")

        # Initialize pointers to current-iteration objects
        self.initialize_statespace_object_pointers()
        self.initialize_filter_object_pointers()
        self.initialize_smoother_object_pointers()

        # Initialize pointers to appropriate Kalman smoothing functions
        self.initialize_function_pointers()

        # Clear measurement disturbance variables` if we switched from
        # multivariate to univariate (because we might have off-diagonal
        # elements stored from a previous multivariate run that wouldn't have
        # been cleared yet)
        if self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t - 1] == 1:
            if self._smooth_method & SMOOTH_CLASSICAL:
                raise NotImplementedError(
                    'Cannot use classical smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                raise NotImplementedError(
                    'Cannot use alternative smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            self.smoothing_error[..., :self.t] = 0
            self.smoothed_measurement_disturbance_cov[..., :self.t] = 0
            self.innovations_transition[..., :self.t] = 0

        # Conventional timing of the measurement step of the scaled smoothed
        # estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state and covariance matrix  
        # $\hat \alpha_t, V_t$
        if self.smoother_output & (SMOOTHER_STATE | SMOOTHER_STATE_COV):
            self.smooth_state(self, self.kfilter, self.model)

        # Modified Byrson-Frazier timing of the measurement step of the scaled
        # smoothed estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if not diffuse and self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state autocovariance matrix
        if diffuse:
            blas.dcopy(&self.kfilter.k_states2, self.kfilter._tmpL0, &inc, self._innovations_transition, &inc)
        else:
            blas.dcopy(&self.kfilter.k_states2, self._tmpL, &inc, self._innovations_transition, &inc)
        if self.smoother_output & SMOOTHER_STATE_AUTOCOV:
            if diffuse:
                dsmoothed_state_autocov_univariate_diffuse(self, self.kfilter, self.model)
            else:
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t+1]
                dsmoothed_state_autocov_conventional(self, self.kfilter, self.model)
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t]

        # Smoothed disturbances  
        # $\hat \eta_t, \hat \varepsilon_t, Var(\eta_t | Y_n), Var(\varepsilon_t | Y_n)$
        if self.smoother_output & SMOOTHER_DISTURBANCE:
            self.smooth_disturbances(self, self.kfilter, self.model)

        # Time step of the scaled smoothed estimator and covariance matrix
        self.smooth_estimators_time(self, self.kfilter, self.model)

        # When switching from the non-univariate filtering methods to the
        # univariate filtering method, we need to run one or more
        # univariate time smoothing steps to get the appropriate values (e.g.
        # for scaled_smoothed_estimator). Moreover, the alternative and
        # classical methods have different timing.
        # This happens in two cases:
        # 1. Exact diffuse filtering and smoothing only uses the univariate
        #    method, so if non-univariate filtering methods are used for
        #    the remaining filtering and smoothing then we need to run this.
        # 2. If the multivariate filtering method failed in a particular time
        #    step (due to a singular forecast error covariance matrix) and
        #    the univariate method was used as a fallback, then we need to run
        #    this.
        if ((self.kfilter.nobs_diffuse > 0 and self.t == self.kfilter.nobs_diffuse) or
                (self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t-1] == 1)):
            t = self.t
            if self._smooth_method & SMOOTH_CONVENTIONAL:
                dsmoothed_estimators_time_univariate(self, self.kfilter, self.model)
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                self.t = self.t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                dsmoothed_estimators_time_univariate(self, self.kfilter, self.model)
                self.t = t
            elif self._smooth_method & SMOOTH_CLASSICAL:
                self.t = t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                dsmoothed_estimators_measurement_classical(self, self.kfilter, self.model)
                self.t = t
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                dsmoothed_estimators_time_univariate(self, self.kfilter, self.model)

        # Note: the above diffuse handling for alternative and classical
        # handling modifies the pointers, and should be the last part of this
        # function

        # Advance the smoother
        self.t -= 1

    cdef void initialize_statespace_object_pointers(self) except *:
        cdef:
            int transform_diagonalize = 0
            int transform_generalized_collapse = 0
            int collapse_occurred = 0

        # Determine which transformations (would) need to be made
        transform_generalized_collapse = self.kfilter.filter_method & FILTER_COLLAPSED
        if not transform_generalized_collapse:
            transform_diagonalize = self.kfilter.univariate_filter[self.t]

        # Initialize object-level pointers to statespace arrays
        # Note: does not matter what transformations were required for the
        #       filter; we do not need to perform them for the smoother
        # TODO  actually we do, to get _design, _obs_cov, etc. However we do not
        #       need it to recalculate the selected_obs and loglikelihood, so
        #       need to decouple those parts from the generalized collapse
        self.model.seek(self.t, transform_diagonalize, transform_generalized_collapse)

        # Initialize object-level pointers to statespace arrays
        # self._design = self.model._design
        # self._obs_cov = self.model._obs_cov
        # self._transition = self.model._transition
        # self._selection = self.model._selection
        # self._state_cov = self.model._state_cov

        # A collapse would not actually occur in a given iteration, even with
        # the FILTER_COLLAPSED flag, in the case that there was enough missing
        # data that k_endog - nmissing <= k_states
        # collapse_occurred = (
        #     transform_generalized_collapse and
        #     self.model.k_endog - self.model._nmissing > self.model.k_states
        # )

        # If a collapse should have occurred, the dimensions need to be
        # adjusted (because we did not tell the model about the collapse in the
        # seek() call above)
        # if collapse_occurred:
        #     self.model.set_dimensions(self.model.k_states,
        #                               self.model.k_states,
        #                               self.model.k_posdef)

    cdef void initialize_filter_object_pointers(self):
        # cdef:
        #     int t = self.t
        #     int inc = 1

        # # Initialize object-level pointers to output arrays
        # self._predicted_state = &self.kfilter.predicted_state[0, t]
        # self._predicted_state_cov = &self.kfilter.predicted_state_cov[0, 0, t]
        # self._kalman_gain = &self.kfilter.kalman_gain[0, 0, t]

        # # Initialize object-level pointers to named temporary arrays
        # self._tmp1 = &self.kfilter.tmp1[0, 0, t]
        # self._tmp2 = &self.kfilter.tmp2[0, t]
        # self._tmp3 = &self.kfilter.tmp3[0, 0, t]
        # self._tmp4 = &self.kfilter.tmp4[0, 0, t]

        self.kfilter.seek(self.t, False)
        self.kfilter.initialize_filter_object_pointers()

    cdef void initialize_smoother_object_pointers(self) except *:
        cdef:
            int t = self.t
            int inc = 1
            int diffuse = self.t < self.kfilter.nobs_diffuse

        # Initialize object-level pointers to output arrays
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t+1]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
        else:  # if self._smooth_method & SMOOTH_ALTERNATIVE
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t-1]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t-1]

        self._smoothing_error = &self.smoothing_error[0, t]
        self._smoothed_state = &self.smoothed_state[0, t]
        self._smoothed_state_cov = &self.smoothed_state_cov[0, 0, t]
        self._smoothed_measurement_disturbance = &self.smoothed_measurement_disturbance[0, t]
        self._smoothed_state_disturbance = &self.smoothed_state_disturbance[0, t]
        self._smoothed_measurement_disturbance_cov = &self.smoothed_measurement_disturbance_cov[0, 0, t]
        self._smoothed_state_disturbance_cov = &self.smoothed_state_disturbance_cov[0, 0, t]

        self._innovations_transition = &self.innovations_transition[0, 0, t]
        self._smoothed_state_autocov = &self.smoothed_state_autocov[0, 0, t]

        # Diffuse
        if diffuse:
            self._input_scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t+1]
            self._input_scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t+1]
            self._input_scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t]
            self._scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t]
            self._scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t]


    cdef void initialize_function_pointers(self) except *:
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse
        # Diffuse smoother
        if diffuse:
            self.smooth_estimators_measurement = dsmoothed_estimators_measurement_univariate_diffuse
            self.smooth_estimators_time = dsmoothed_estimators_time_univariate_diffuse
            self.smooth_state = dsmoothed_state_univariate_diffuse
            self.smooth_disturbances = dsmoothed_disturbances_univariate_diffuse
        # Univariate (modified Bryson-Frazier) smoother
        elif (self._smooth_method & SMOOTH_UNIVARIATE) or self.kfilter.univariate_filter[self.t]:
            self.smooth_estimators_measurement = dsmoothed_estimators_measurement_univariate
            self.smooth_estimators_time = dsmoothed_estimators_time_univariate
            self.smooth_state = dsmoothed_state_conventional
            self.smooth_disturbances = dsmoothed_disturbances_univariate
        # Multivariate modified Bryson-Frazier smoother
        elif self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement = dsmoothed_estimators_measurement_alternative
            self.smooth_estimators_time = dsmoothed_estimators_time_alternative
            self.smooth_state = dsmoothed_state_alternative
            self.smooth_disturbances = dsmoothed_disturbances_alternative
        # Multivariate classical (Anderson and Moore) smoother
        elif self._smooth_method & SMOOTH_CLASSICAL:
            self.smooth_estimators_measurement = dsmoothed_estimators_measurement_classical
            self.smooth_estimators_time = dsmoothed_estimators_time_classical
            self.smooth_state = dsmoothed_state_classical
            self.smooth_disturbances = dsmoothed_disturbances_conventional
        # Multivariate conventional (Durbin and Koopman) smoother
        elif self._smooth_method & SMOOTH_CONVENTIONAL:
            self.smooth_estimators_measurement = dsmoothed_estimators_measurement_conventional
            self.smooth_estimators_time = dsmoothed_estimators_time_conventional
            self.smooth_state = dsmoothed_state_conventional
            self.smooth_disturbances = dsmoothed_disturbances_conventional
        else:
            raise NotImplementedError("Smoother method not available.")

        # Handle completely missing data
        # (All methods except the conventional method can use the same routines in this case)
        # This is essentially just an application of the smoothed_estimators_time_* step.
        if not diffuse and self._smooth_method & SMOOTH_CONVENTIONAL and self.model._nmissing == self.model.k_endog:
            # Change the smoothing functions to take into account a missing observation
            self.smooth_estimators_measurement = dsmoothed_estimators_missing_conventional
            # (no need to change the state smoothing recursion)
            # self.smooth_state = dsmoothed_state_missing_conventional
            self.smooth_disturbances = dsmoothed_disturbances_missing_conventional

from statsmodels.tsa.statespace._smoothers._conventional cimport (
    csmoothed_estimators_missing_conventional,
    csmoothed_disturbances_missing_conventional,
    csmoothed_estimators_measurement_conventional,
    csmoothed_estimators_time_conventional,
    csmoothed_state_conventional,
    csmoothed_state_autocov_conventional,
    csmoothed_disturbances_conventional
)
from statsmodels.tsa.statespace._smoothers._univariate cimport (
    csmoothed_estimators_measurement_univariate,
    csmoothed_estimators_time_univariate,
    csmoothed_disturbances_univariate
)
from statsmodels.tsa.statespace._smoothers._univariate_diffuse cimport (
    csmoothed_estimators_measurement_univariate_diffuse,
    csmoothed_estimators_time_univariate_diffuse,
    csmoothed_state_univariate_diffuse,
    csmoothed_disturbances_univariate_diffuse,
    csmoothed_state_autocov_univariate_diffuse
)
from statsmodels.tsa.statespace._smoothers._classical cimport (
    csmoothed_estimators_measurement_classical,
    csmoothed_estimators_time_classical,
    csmoothed_state_classical
)
from statsmodels.tsa.statespace._smoothers._alternative cimport (
    csmoothed_estimators_measurement_alternative,
    csmoothed_estimators_time_alternative,
    csmoothed_state_alternative,
    csmoothed_disturbances_alternative
)

# ## Kalman filter
cdef class cKalmanSmoother(object):
    """
    cKalmanSmoother(model, kfilter, smoother_output=SMOOTHING_ALL)

    A representation of the Kalman smoother recursions; it performs a single
    backwards pass through the data (after the forwards pass via the Kalman
    filter has already been completed). In all cases, it calculates:

    - `scaled_smoothed_estimator`
    - `smoothing_error`

    it can optionally peform three types of smoothing:

    - State smoothing provides `smoothed_state` and `smoothed_state_cov`
    - Disturbance smoothing provides `smoothed_measurement_disturbance` and
      `smoothed_state_disturbance`
    - Simulation smoothing provides `sampled_measurement_disturbance` and
      `sampled_state_disturbance` (note that this requires Disturbance
      smoothing as well).

    Note: this output arrays in this class are always defined in-memory
    according to the original dimensions in the cStatespace object.

    Note: if the `filter_method` of the underlying cKalmanFilter
    changes, the smoother *must* be reset using the object callable (__call__)
    or the `reset` method. This is because when the filter method is changed,
    the filter output arrays are reset.
    """

    # ### Statespace model
    # cdef readonly cStatespace model
    # ### Kalman filter
    # cdef readonly cKalmanFilter kfilter

    # ### Smoother parameters
    # Holds the time-iteration state of the filter  
    # *Note*: must be changed using the `seek` method
    # cdef readonly int t
    # cdef readonly int smoother_output
    # Keep track of the filter method against which the arrays were created
    # so that we can re-allocate memory if the filter method changes.
    # cdef readonly int filter_method

    # ### Kalman smoother properties

    # `scaled_smoothed_estimator` $\equiv r_t$ is the **scaled smoothed estimator** of $\eta_t$ $(m \times T)$  
    # cdef readonly np.complex64_t [::1,:] scaled_smoothed_estimator

    # `scaled_smoothed_estimator_cov` $\equiv N_t$ is the **scaled smoothed estimator covariance matrix** $(m \times m \times T)$  
    # cdef readonly np.complex64_t [::1,:,:] scaled_smoothed_estimator_cov

    # `smoothing_error` $\equiv u_t = F_{t}^{-1} v_t - K_t' r_t$ is the **smoothing error** $(p \times T)$
    # cdef readonly np.complex64_t [::1,:] smoothing_error

    # `smoothed_state` $\equiv \hat \alpha_t = E(\alpha_t | Y_n)$ is the **smoothed estimator** of the state $(m \times T)$
    # cdef readonly np.complex64_t [::1,:] smoothed_state

    # `smoothed_state_cov` $\equiv V_t = Var(\alpha_t | Y_n)$ is the **smoothed state covariance matrix** $(m \times m \times T)$
    # cdef readonly np.complex64_t [::1,:,:] smoothed_state_cov

    # `smoothed_measurement_disturbance` $\equiv \hat \varepsilon_t = E(\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance** $(p \times T)$
    # cdef readonly np.complex64_t [::1,:] smoothed_measurement_disturbance

    # `smoothed_state_disturbance` $\equiv \hat \eta_t = E(\eta_t | Y_n)$ is the **smoothed state disturbance** $(r \times T)$
    # cdef readonly np.complex64_t [::1,:] smoothed_state_disturbance

    # `smoothed_measurement_disturbance_cov` $\equiv Var (\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance covariance matrix** $(p \times p \times T)$
    # cdef readonly np.complex64_t [::1,:,:] smoothed_measurement_disturbance_cov

    # `smoothed_state_disturbance` $\equiv Var (\eta_t | Y_n)$ is the **smoothed state disturbance covariance matrix** $(r \times r \times T)$
    # cdef readonly np.complex64_t [::1,:,:] smoothed_state_disturbance_cov

    # ### Temporary arrays
    # These matrices are used to temporarily hold selected observation vectors,
    # design matrices, and observation covariance matrices in the case of
    # missing data.  
    # The following are contiguous memory segments which are then used to
    # store the data in the above matrices.
    # cdef readonly np.complex64_t [:] selected_design
    # cdef readonly np.complex64_t [:] selected_obs_cov
    # These hold the memory allocations of the unnamed temporary arrays
    # cdef readonly np.complex64_t [::1,:] tmpL, tmpL2, tmp0, tmp00, tmp000

    # ### Pointers to current-iteration arrays

    # Statespace
    # cdef np.complex64_t * _design
    # cdef np.complex64_t * _obs_cov
    # cdef np.complex64_t * _transition
    # cdef np.complex64_t * _selection
    # cdef np.complex64_t * _state_cov

    # Kalman filter
    # cdef np.complex64_t * _predicted_state
    # cdef np.complex64_t * _predicted_state_cov
    # cdef np.complex64_t * _kalman_gain

    # cdef np.complex64_t * _tmp1
    # cdef np.complex64_t * _tmp2
    # cdef np.complex64_t * _tmp3
    # cdef np.complex64_t * _tmp4

    # Kalman smoother
    # cdef np.complex64_t * _input_scaled_smoothed_estimator
    # cdef np.complex64_t * _input_scaled_smoothed_estimator_cov

    # cdef np.complex64_t * _scaled_smoothed_estimator
    # cdef np.complex64_t * _scaled_smoothed_estimator_cov
    # cdef np.complex64_t * _smoothing_error
    # cdef np.complex64_t * _smoothed_state
    # cdef np.complex64_t * _smoothed_state_cov
    # cdef np.complex64_t * _smoothed_measurement_disturbance
    # cdef np.complex64_t * _smoothed_state_disturbance
    # cdef np.complex64_t * _smoothed_measurement_disturbance_cov
    # cdef np.complex64_t * _smoothed_state_disturbance_cov

    # cdef np.complex64_t * _tmpL
    # cdef np.complex64_t * _tmpL2
    # cdef np.complex64_t * _tmp0
    # cdef np.complex64_t * _tmp00
    # cdef np.complex64_t * _tmp000

    # ### Pointers to current-iteration Kalman smoothing functions
    # cdef int (*smooth_estimators)(
    #     cKalmanSmoother, cKalmanFilter, cStatespace
    # )
    # cdef int (*smooth_state)(
    #     cKalmanSmoother, cKalmanFilter, cStatespace
    # )
    # cdef int (*smooth_disturbances)(
    #     cKalmanSmoother, cKalmanFilter, cStatespace
    # )

    # ### Define some constants
    # cdef readonly int k_endog, k_states, k_posdef, k_endog2, k_states2, k_posdef2, k_endogstates, k_statesposdef

    def __init__(self,
                 cStatespace model,
                 cKalmanFilter kfilter,
                 int smoother_output=SMOOTHER_ALL,
                 int smooth_method=0):

        # Save the model
        self.model = model
        self.kfilter = kfilter

        # Save the parameters
        self.filter_method = kfilter.filter_method

        # Make sure the appropriate output has been stored in the filter
        if self.kfilter.conserve_memory & MEMORY_NO_PREDICTED:
            raise ValueError('Cannot perform smoothing without all predicted states')

        if self.kfilter.conserve_memory & MEMORY_NO_GAIN:
            raise ValueError('Cannot perform smoothing without all Kalman gains')

        if self.kfilter.conserve_memory & MEMORY_NO_SMOOTHING:
            raise ValueError('Cannot perform smoothing without all smoothing variables')

        # Set smoothing output and initialize output arrays
        self.set_smoother_output(smoother_output)
        self.set_smooth_method(smooth_method)

    cdef allocate_arrays(self):
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]
            np.npy_intp dim3[3]
        # #### Allocate arrays for calculations
        # Note: these are defined in memory according to the kfilter dimensions
        #       In the case of FILTERED_COLLAPSED, the smoothed measurement
        #       output describes only the component of transformed observations
        #       that is related to the states.

        # Arrays for Kalman smoother output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothing_error = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs;
        self.smoothed_state = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs;
        self.smoothed_state_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothed_measurement_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim2[0] = self.kfilter.k_posdef; dim2[1] = self.model.nobs;
        self.smoothed_state_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim3[0] = self.kfilter.k_endog; dim3[1] = self.kfilter.k_endog; dim3[2] = self.model.nobs;
        self.smoothed_measurement_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)
        dim3[0] = self.kfilter.k_posdef; dim3[1] = self.kfilter.k_posdef; dim3[2] = self.model.nobs;
        self.smoothed_state_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)

        # Innovations transition matrix (L_t = T_t - K_t Z_t)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.innovations_transition = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)

        # Smoothed state autocovariance arrays
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.smoothed_state_autocov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)

        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_states;
        self.tmp_autocov = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmp_autocov = &self.tmp_autocov[0, 0]

        # Diffuse output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_diffuse_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_diffuse1_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)
        self.scaled_smoothed_diffuse2_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX64, FORTRAN)

        # #### Arrays for temporary calculations
        # *Note*: in math notation below, a $\\#$ will represent a generic
        # temporary array, and a $\\#_i$ will represent a named temporary array.

        # # $L_t$ $(m \times m)$, also holds $(m \times r)$ sometimes
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmpL = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmpL = &self.tmpL[0, 0]
        self.tmpL2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmpL2 = &self.tmpL2[0, 0]

        # # Holds arrays of dimension $(m \times m)$ and $(m \times r)$
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmp0 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmp0 = &self.tmp0[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp00 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmp00 = &self.tmp00[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp000 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX64, FORTRAN)
        self._tmp000 = &self.tmp000[0, 0]

        # Arrays for missing data
        # dim1[0] = self.kfilter.k_endog * self.kfilter.k_states;
        # self.selected_design = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX64, FORTRAN)
        # dim1[0] = self.kfilter.k_endog2;
        # self.selected_obs_cov = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX64, FORTRAN)

    def __reduce__(self):
        state = {
            't': self.t,
            '_smooth_method': self._smooth_method,
            'scaled_smoothed_estimator': np.array(self.scaled_smoothed_estimator, copy=True, order='F'),
            'scaled_smoothed_estimator_cov': np.array(self.scaled_smoothed_estimator_cov, copy=True, order='F'),
            'smoothing_error': np.array(self.smoothing_error, copy=True, order='F'),
            'smoothed_state': np.array(self.smoothed_state, copy=True, order='F'),
            'smoothed_state_cov': np.array(self.smoothed_state_cov, copy=True, order='F'),
            'smoothed_measurement_disturbance': np.array(self.smoothed_measurement_disturbance, copy=True, order='F'),
            'smoothed_state_disturbance': np.array(self.smoothed_state_disturbance, copy=True, order='F'),
            'smoothed_measurement_disturbance_cov': np.array(self.smoothed_measurement_disturbance_cov, copy=True, order='F'),
            'smoothed_state_disturbance_cov': np.array(self.smoothed_state_disturbance_cov, copy=True, order='F'),
            'smoothed_state_autocov': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'innovations_transition': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'tmp_autocov': np.array(self.tmp_autocov, copy=True, order='F'),
            'scaled_smoothed_diffuse_estimator': np.array(self.scaled_smoothed_diffuse_estimator, copy=True, order='F'),
            'scaled_smoothed_diffuse1_estimator_cov': np.array(self.scaled_smoothed_diffuse1_estimator_cov, copy=True, order='F'),
            'scaled_smoothed_diffuse2_estimator_cov': np.array(self.scaled_smoothed_diffuse2_estimator_cov, copy=True, order='F'),
            'tmpL': np.array(self.tmpL, copy=True, order='F'),
            'tmpL2': np.array(self.tmpL2, copy=True, order='F'),
            'tmp0': np.array(self.tmp0, copy=True, order='F'),
            'tmp00': np.array(self.tmp00, copy=True, order='F'),
            'tmp000': np.array(self.tmp000, copy=True, order='F')
        }
        args = (self.model, self.kfilter, self.smoother_output, self.smooth_method)
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.t = state['t']
        self._smooth_method = state['_smooth_method']
        self.scaled_smoothed_estimator = state['scaled_smoothed_estimator']
        self.scaled_smoothed_estimator_cov = state['scaled_smoothed_estimator_cov']
        self.smoothing_error = state['smoothing_error']
        self.smoothed_state = state['smoothed_state']
        self.smoothed_state_cov = state['smoothed_state_cov']
        self.smoothed_measurement_disturbance = state['smoothed_measurement_disturbance']
        self.smoothed_state_disturbance = state['smoothed_state_disturbance']
        self.smoothed_measurement_disturbance_cov = state['smoothed_measurement_disturbance_cov']
        self.smoothed_state_disturbance_cov = state['smoothed_state_disturbance_cov']
        self.smoothed_state_autocov = state['smoothed_state_autocov']
        self.innovations_transition = state['innovations_transition']
        self.tmp_autocov = state['tmp_autocov']
        self.scaled_smoothed_diffuse_estimator = state['scaled_smoothed_diffuse_estimator']
        self.scaled_smoothed_diffuse1_estimator_cov = state['scaled_smoothed_diffuse1_estimator_cov']
        self.scaled_smoothed_diffuse2_estimator_cov = state['scaled_smoothed_diffuse2_estimator_cov']
        self.tmpL = state['tmpL']
        self.tmpL2 = state['tmpL2']
        self.tmp0 = state['tmp0']
        self.tmp00 = state['tmp00']
        self.tmp000 = state['tmp000']
        self.initialize_smoother_object_pointers()
        self._initialize_temp_pointers()

    cdef void _initialize_temp_pointers(self) except *:
        self._tmpL = &self.tmpL[0, 0]
        self._tmpL2 = &self.tmpL2[0, 0]
        self._tmp0 = &self.tmp0[0, 0]
        self._tmp00 = &self.tmp00[0, 0]
        self._tmp000 = &self.tmp000[0, 0]
        self._tmp_autocov = &self.tmp_autocov[0, 0]

    cdef int check_filter_method_changed(self):
        return not self.kfilter.filter_method == self.filter_method

    cdef int reset_filter_method(self, int force_reset=True):
        cdef int changed = self.check_filter_method_changed()

        if changed or force_reset:
            # Save the new method
            self.filter_method = self.kfilter.filter_method
            # Reset matrices
            self.allocate_arrays()
            # Reset the smooth method (in case it was based on the filter
            # method)
            self.set_smooth_method(self.smooth_method)

        return changed

    cpdef set_smoother_output(self, int smoother_output, int force_reset=True):
        if not smoother_output == self.smoother_output or force_reset:
            # Change the smoother output flag
            self.smoother_output = smoother_output

            # Reset matrices
            self.reset(True)

    cpdef set_smooth_method(self, int smooth_method):
        cdef int _smooth_method
        self.smooth_method = smooth_method

        # If no smooth method provided, use default for the filter type
        if self.smooth_method == 0:
            self.reset_filter_method(False)
            if self.kfilter.filter_method & FILTER_UNIVARIATE:
                _smooth_method = SMOOTH_UNIVARIATE
            else:
                _smooth_method = SMOOTH_CONVENTIONAL
        else:
            _smooth_method = self.smooth_method

        # Make sure we do not have an invalid smooth method for our filter
        # method
        if((_smooth_method & SMOOTH_UNIVARIATE) and not (self.filter_method & FILTER_UNIVARIATE) or 
                (self.filter_method & FILTER_UNIVARIATE) and not (_smooth_method & SMOOTH_UNIVARIATE)):
            raise ValueError('Invalid smoothing method: can only use'
                             ' univariate smoothing when univariate filtering'
                             ' has been used previously.')


        self._smooth_method = _smooth_method

    cpdef reset(self, int force_reset=False):
        """
        reset(self)

        Reset the smoother.
        """
        # Reset the filter method (if necessary)
        self.reset_filter_method(force_reset)

        # Set the time
        self.t = self.model.nobs-1

    cpdef seek(self, unsigned int t):
        """
        seek(self, t)

        Change the time-state of the smoother

        Notes
        -----
        Between seek calls, the `filter_method` parameter of the associated
        Kalman filter object is not allowed to change. If the `filter_method`
        has changed, either recall the smoother using the object callable or
        explicitly reset the smoother using the `reset` method.
        """
        # Make sure the seek location is valid
        if not t == 0 and t >= <unsigned int>self.model.nobs:
            raise IndexError("Observation index out of range")

        # Make sure we have not changed filter methods in-between seeking
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother seek() calls."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")
        self.t = t

    def __iter__(self):
        return self

    def __call__(self, int smoother_output=-1):
        """
        Iterate the smoother across the entire set of observations.
        """
        cdef int i

        # Reset the smoother
        self.reset()
        
        # Perform backwards smoothing iterations
        for i in range(self.model.nobs-1,-1,-1):
            next(self)

    def __next__(self):
        """
        Perform an iteration of the Kalman smoother
        """
        cdef int t, inc = 1
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse

        # Get time subscript, and stop the iterator if at the end
        if not self.t >= 0:
            raise StopIteration

        # Make sure we have not changed filter methods in-between iterations
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother iterations."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")

        # Initialize pointers to current-iteration objects
        self.initialize_statespace_object_pointers()
        self.initialize_filter_object_pointers()
        self.initialize_smoother_object_pointers()

        # Initialize pointers to appropriate Kalman smoothing functions
        self.initialize_function_pointers()

        # Clear measurement disturbance variables` if we switched from
        # multivariate to univariate (because we might have off-diagonal
        # elements stored from a previous multivariate run that wouldn't have
        # been cleared yet)
        if self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t - 1] == 1:
            if self._smooth_method & SMOOTH_CLASSICAL:
                raise NotImplementedError(
                    'Cannot use classical smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                raise NotImplementedError(
                    'Cannot use alternative smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            self.smoothing_error[..., :self.t] = 0
            self.smoothed_measurement_disturbance_cov[..., :self.t] = 0
            self.innovations_transition[..., :self.t] = 0

        # Conventional timing of the measurement step of the scaled smoothed
        # estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state and covariance matrix  
        # $\hat \alpha_t, V_t$
        if self.smoother_output & (SMOOTHER_STATE | SMOOTHER_STATE_COV):
            self.smooth_state(self, self.kfilter, self.model)

        # Modified Byrson-Frazier timing of the measurement step of the scaled
        # smoothed estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if not diffuse and self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state autocovariance matrix
        if diffuse:
            blas.ccopy(&self.kfilter.k_states2, self.kfilter._tmpL0, &inc, self._innovations_transition, &inc)
        else:
            blas.ccopy(&self.kfilter.k_states2, self._tmpL, &inc, self._innovations_transition, &inc)
        if self.smoother_output & SMOOTHER_STATE_AUTOCOV:
            if diffuse:
                csmoothed_state_autocov_univariate_diffuse(self, self.kfilter, self.model)
            else:
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t+1]
                csmoothed_state_autocov_conventional(self, self.kfilter, self.model)
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t]

        # Smoothed disturbances  
        # $\hat \eta_t, \hat \varepsilon_t, Var(\eta_t | Y_n), Var(\varepsilon_t | Y_n)$
        if self.smoother_output & SMOOTHER_DISTURBANCE:
            self.smooth_disturbances(self, self.kfilter, self.model)

        # Time step of the scaled smoothed estimator and covariance matrix
        self.smooth_estimators_time(self, self.kfilter, self.model)

        # When switching from the non-univariate filtering methods to the
        # univariate filtering method, we need to run one or more
        # univariate time smoothing steps to get the appropriate values (e.g.
        # for scaled_smoothed_estimator). Moreover, the alternative and
        # classical methods have different timing.
        # This happens in two cases:
        # 1. Exact diffuse filtering and smoothing only uses the univariate
        #    method, so if non-univariate filtering methods are used for
        #    the remaining filtering and smoothing then we need to run this.
        # 2. If the multivariate filtering method failed in a particular time
        #    step (due to a singular forecast error covariance matrix) and
        #    the univariate method was used as a fallback, then we need to run
        #    this.
        if ((self.kfilter.nobs_diffuse > 0 and self.t == self.kfilter.nobs_diffuse) or
                (self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t-1] == 1)):
            t = self.t
            if self._smooth_method & SMOOTH_CONVENTIONAL:
                csmoothed_estimators_time_univariate(self, self.kfilter, self.model)
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                self.t = self.t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                csmoothed_estimators_time_univariate(self, self.kfilter, self.model)
                self.t = t
            elif self._smooth_method & SMOOTH_CLASSICAL:
                self.t = t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                csmoothed_estimators_measurement_classical(self, self.kfilter, self.model)
                self.t = t
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                csmoothed_estimators_time_univariate(self, self.kfilter, self.model)

        # Note: the above diffuse handling for alternative and classical
        # handling modifies the pointers, and should be the last part of this
        # function

        # Advance the smoother
        self.t -= 1

    cdef void initialize_statespace_object_pointers(self) except *:
        cdef:
            int transform_diagonalize = 0
            int transform_generalized_collapse = 0
            int collapse_occurred = 0

        # Determine which transformations (would) need to be made
        transform_generalized_collapse = self.kfilter.filter_method & FILTER_COLLAPSED
        if not transform_generalized_collapse:
            transform_diagonalize = self.kfilter.univariate_filter[self.t]

        # Initialize object-level pointers to statespace arrays
        # Note: does not matter what transformations were required for the
        #       filter; we do not need to perform them for the smoother
        # TODO  actually we do, to get _design, _obs_cov, etc. However we do not
        #       need it to recalculate the selected_obs and loglikelihood, so
        #       need to decouple those parts from the generalized collapse
        self.model.seek(self.t, transform_diagonalize, transform_generalized_collapse)

        # Initialize object-level pointers to statespace arrays
        # self._design = self.model._design
        # self._obs_cov = self.model._obs_cov
        # self._transition = self.model._transition
        # self._selection = self.model._selection
        # self._state_cov = self.model._state_cov

        # A collapse would not actually occur in a given iteration, even with
        # the FILTER_COLLAPSED flag, in the case that there was enough missing
        # data that k_endog - nmissing <= k_states
        # collapse_occurred = (
        #     transform_generalized_collapse and
        #     self.model.k_endog - self.model._nmissing > self.model.k_states
        # )

        # If a collapse should have occurred, the dimensions need to be
        # adjusted (because we did not tell the model about the collapse in the
        # seek() call above)
        # if collapse_occurred:
        #     self.model.set_dimensions(self.model.k_states,
        #                               self.model.k_states,
        #                               self.model.k_posdef)

    cdef void initialize_filter_object_pointers(self):
        # cdef:
        #     int t = self.t
        #     int inc = 1

        # # Initialize object-level pointers to output arrays
        # self._predicted_state = &self.kfilter.predicted_state[0, t]
        # self._predicted_state_cov = &self.kfilter.predicted_state_cov[0, 0, t]
        # self._kalman_gain = &self.kfilter.kalman_gain[0, 0, t]

        # # Initialize object-level pointers to named temporary arrays
        # self._tmp1 = &self.kfilter.tmp1[0, 0, t]
        # self._tmp2 = &self.kfilter.tmp2[0, t]
        # self._tmp3 = &self.kfilter.tmp3[0, 0, t]
        # self._tmp4 = &self.kfilter.tmp4[0, 0, t]

        self.kfilter.seek(self.t, False)
        self.kfilter.initialize_filter_object_pointers()

    cdef void initialize_smoother_object_pointers(self) except *:
        cdef:
            int t = self.t
            int inc = 1
            int diffuse = self.t < self.kfilter.nobs_diffuse

        # Initialize object-level pointers to output arrays
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t+1]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
        else:  # if self._smooth_method & SMOOTH_ALTERNATIVE
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t-1]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t-1]

        self._smoothing_error = &self.smoothing_error[0, t]
        self._smoothed_state = &self.smoothed_state[0, t]
        self._smoothed_state_cov = &self.smoothed_state_cov[0, 0, t]
        self._smoothed_measurement_disturbance = &self.smoothed_measurement_disturbance[0, t]
        self._smoothed_state_disturbance = &self.smoothed_state_disturbance[0, t]
        self._smoothed_measurement_disturbance_cov = &self.smoothed_measurement_disturbance_cov[0, 0, t]
        self._smoothed_state_disturbance_cov = &self.smoothed_state_disturbance_cov[0, 0, t]

        self._innovations_transition = &self.innovations_transition[0, 0, t]
        self._smoothed_state_autocov = &self.smoothed_state_autocov[0, 0, t]

        # Diffuse
        if diffuse:
            self._input_scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t+1]
            self._input_scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t+1]
            self._input_scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t]
            self._scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t]
            self._scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t]


    cdef void initialize_function_pointers(self) except *:
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse
        # Diffuse smoother
        if diffuse:
            self.smooth_estimators_measurement = csmoothed_estimators_measurement_univariate_diffuse
            self.smooth_estimators_time = csmoothed_estimators_time_univariate_diffuse
            self.smooth_state = csmoothed_state_univariate_diffuse
            self.smooth_disturbances = csmoothed_disturbances_univariate_diffuse
        # Univariate (modified Bryson-Frazier) smoother
        elif (self._smooth_method & SMOOTH_UNIVARIATE) or self.kfilter.univariate_filter[self.t]:
            self.smooth_estimators_measurement = csmoothed_estimators_measurement_univariate
            self.smooth_estimators_time = csmoothed_estimators_time_univariate
            self.smooth_state = csmoothed_state_conventional
            self.smooth_disturbances = csmoothed_disturbances_univariate
        # Multivariate modified Bryson-Frazier smoother
        elif self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement = csmoothed_estimators_measurement_alternative
            self.smooth_estimators_time = csmoothed_estimators_time_alternative
            self.smooth_state = csmoothed_state_alternative
            self.smooth_disturbances = csmoothed_disturbances_alternative
        # Multivariate classical (Anderson and Moore) smoother
        elif self._smooth_method & SMOOTH_CLASSICAL:
            self.smooth_estimators_measurement = csmoothed_estimators_measurement_classical
            self.smooth_estimators_time = csmoothed_estimators_time_classical
            self.smooth_state = csmoothed_state_classical
            self.smooth_disturbances = csmoothed_disturbances_conventional
        # Multivariate conventional (Durbin and Koopman) smoother
        elif self._smooth_method & SMOOTH_CONVENTIONAL:
            self.smooth_estimators_measurement = csmoothed_estimators_measurement_conventional
            self.smooth_estimators_time = csmoothed_estimators_time_conventional
            self.smooth_state = csmoothed_state_conventional
            self.smooth_disturbances = csmoothed_disturbances_conventional
        else:
            raise NotImplementedError("Smoother method not available.")

        # Handle completely missing data
        # (All methods except the conventional method can use the same routines in this case)
        # This is essentially just an application of the smoothed_estimators_time_* step.
        if not diffuse and self._smooth_method & SMOOTH_CONVENTIONAL and self.model._nmissing == self.model.k_endog:
            # Change the smoothing functions to take into account a missing observation
            self.smooth_estimators_measurement = csmoothed_estimators_missing_conventional
            # (no need to change the state smoothing recursion)
            # self.smooth_state = csmoothed_state_missing_conventional
            self.smooth_disturbances = csmoothed_disturbances_missing_conventional

from statsmodels.tsa.statespace._smoothers._conventional cimport (
    zsmoothed_estimators_missing_conventional,
    zsmoothed_disturbances_missing_conventional,
    zsmoothed_estimators_measurement_conventional,
    zsmoothed_estimators_time_conventional,
    zsmoothed_state_conventional,
    zsmoothed_state_autocov_conventional,
    zsmoothed_disturbances_conventional
)
from statsmodels.tsa.statespace._smoothers._univariate cimport (
    zsmoothed_estimators_measurement_univariate,
    zsmoothed_estimators_time_univariate,
    zsmoothed_disturbances_univariate
)
from statsmodels.tsa.statespace._smoothers._univariate_diffuse cimport (
    zsmoothed_estimators_measurement_univariate_diffuse,
    zsmoothed_estimators_time_univariate_diffuse,
    zsmoothed_state_univariate_diffuse,
    zsmoothed_disturbances_univariate_diffuse,
    zsmoothed_state_autocov_univariate_diffuse
)
from statsmodels.tsa.statespace._smoothers._classical cimport (
    zsmoothed_estimators_measurement_classical,
    zsmoothed_estimators_time_classical,
    zsmoothed_state_classical
)
from statsmodels.tsa.statespace._smoothers._alternative cimport (
    zsmoothed_estimators_measurement_alternative,
    zsmoothed_estimators_time_alternative,
    zsmoothed_state_alternative,
    zsmoothed_disturbances_alternative
)

# ## Kalman filter
cdef class zKalmanSmoother(object):
    """
    zKalmanSmoother(model, kfilter, smoother_output=SMOOTHING_ALL)

    A representation of the Kalman smoother recursions; it performs a single
    backwards pass through the data (after the forwards pass via the Kalman
    filter has already been completed). In all cases, it calculates:

    - `scaled_smoothed_estimator`
    - `smoothing_error`

    it can optionally peform three types of smoothing:

    - State smoothing provides `smoothed_state` and `smoothed_state_cov`
    - Disturbance smoothing provides `smoothed_measurement_disturbance` and
      `smoothed_state_disturbance`
    - Simulation smoothing provides `sampled_measurement_disturbance` and
      `sampled_state_disturbance` (note that this requires Disturbance
      smoothing as well).

    Note: this output arrays in this class are always defined in-memory
    according to the original dimensions in the zStatespace object.

    Note: if the `filter_method` of the underlying zKalmanFilter
    changes, the smoother *must* be reset using the object callable (__call__)
    or the `reset` method. This is because when the filter method is changed,
    the filter output arrays are reset.
    """

    # ### Statespace model
    # cdef readonly zStatespace model
    # ### Kalman filter
    # cdef readonly zKalmanFilter kfilter

    # ### Smoother parameters
    # Holds the time-iteration state of the filter  
    # *Note*: must be changed using the `seek` method
    # cdef readonly int t
    # cdef readonly int smoother_output
    # Keep track of the filter method against which the arrays were created
    # so that we can re-allocate memory if the filter method changes.
    # cdef readonly int filter_method

    # ### Kalman smoother properties

    # `scaled_smoothed_estimator` $\equiv r_t$ is the **scaled smoothed estimator** of $\eta_t$ $(m \times T)$  
    # cdef readonly np.complex128_t [::1,:] scaled_smoothed_estimator

    # `scaled_smoothed_estimator_cov` $\equiv N_t$ is the **scaled smoothed estimator covariance matrix** $(m \times m \times T)$  
    # cdef readonly np.complex128_t [::1,:,:] scaled_smoothed_estimator_cov

    # `smoothing_error` $\equiv u_t = F_{t}^{-1} v_t - K_t' r_t$ is the **smoothing error** $(p \times T)$
    # cdef readonly np.complex128_t [::1,:] smoothing_error

    # `smoothed_state` $\equiv \hat \alpha_t = E(\alpha_t | Y_n)$ is the **smoothed estimator** of the state $(m \times T)$
    # cdef readonly np.complex128_t [::1,:] smoothed_state

    # `smoothed_state_cov` $\equiv V_t = Var(\alpha_t | Y_n)$ is the **smoothed state covariance matrix** $(m \times m \times T)$
    # cdef readonly np.complex128_t [::1,:,:] smoothed_state_cov

    # `smoothed_measurement_disturbance` $\equiv \hat \varepsilon_t = E(\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance** $(p \times T)$
    # cdef readonly np.complex128_t [::1,:] smoothed_measurement_disturbance

    # `smoothed_state_disturbance` $\equiv \hat \eta_t = E(\eta_t | Y_n)$ is the **smoothed state disturbance** $(r \times T)$
    # cdef readonly np.complex128_t [::1,:] smoothed_state_disturbance

    # `smoothed_measurement_disturbance_cov` $\equiv Var (\varepsilon_t | Y_n)$ is the **smoothed measurement disturbance covariance matrix** $(p \times p \times T)$
    # cdef readonly np.complex128_t [::1,:,:] smoothed_measurement_disturbance_cov

    # `smoothed_state_disturbance` $\equiv Var (\eta_t | Y_n)$ is the **smoothed state disturbance covariance matrix** $(r \times r \times T)$
    # cdef readonly np.complex128_t [::1,:,:] smoothed_state_disturbance_cov

    # ### Temporary arrays
    # These matrices are used to temporarily hold selected observation vectors,
    # design matrices, and observation covariance matrices in the case of
    # missing data.  
    # The following are contiguous memory segments which are then used to
    # store the data in the above matrices.
    # cdef readonly np.complex128_t [:] selected_design
    # cdef readonly np.complex128_t [:] selected_obs_cov
    # These hold the memory allocations of the unnamed temporary arrays
    # cdef readonly np.complex128_t [::1,:] tmpL, tmpL2, tmp0, tmp00, tmp000

    # ### Pointers to current-iteration arrays

    # Statespace
    # cdef np.complex128_t * _design
    # cdef np.complex128_t * _obs_cov
    # cdef np.complex128_t * _transition
    # cdef np.complex128_t * _selection
    # cdef np.complex128_t * _state_cov

    # Kalman filter
    # cdef np.complex128_t * _predicted_state
    # cdef np.complex128_t * _predicted_state_cov
    # cdef np.complex128_t * _kalman_gain

    # cdef np.complex128_t * _tmp1
    # cdef np.complex128_t * _tmp2
    # cdef np.complex128_t * _tmp3
    # cdef np.complex128_t * _tmp4

    # Kalman smoother
    # cdef np.complex128_t * _input_scaled_smoothed_estimator
    # cdef np.complex128_t * _input_scaled_smoothed_estimator_cov

    # cdef np.complex128_t * _scaled_smoothed_estimator
    # cdef np.complex128_t * _scaled_smoothed_estimator_cov
    # cdef np.complex128_t * _smoothing_error
    # cdef np.complex128_t * _smoothed_state
    # cdef np.complex128_t * _smoothed_state_cov
    # cdef np.complex128_t * _smoothed_measurement_disturbance
    # cdef np.complex128_t * _smoothed_state_disturbance
    # cdef np.complex128_t * _smoothed_measurement_disturbance_cov
    # cdef np.complex128_t * _smoothed_state_disturbance_cov

    # cdef np.complex128_t * _tmpL
    # cdef np.complex128_t * _tmpL2
    # cdef np.complex128_t * _tmp0
    # cdef np.complex128_t * _tmp00
    # cdef np.complex128_t * _tmp000

    # ### Pointers to current-iteration Kalman smoothing functions
    # cdef int (*smooth_estimators)(
    #     zKalmanSmoother, zKalmanFilter, zStatespace
    # )
    # cdef int (*smooth_state)(
    #     zKalmanSmoother, zKalmanFilter, zStatespace
    # )
    # cdef int (*smooth_disturbances)(
    #     zKalmanSmoother, zKalmanFilter, zStatespace
    # )

    # ### Define some constants
    # cdef readonly int k_endog, k_states, k_posdef, k_endog2, k_states2, k_posdef2, k_endogstates, k_statesposdef

    def __init__(self,
                 zStatespace model,
                 zKalmanFilter kfilter,
                 int smoother_output=SMOOTHER_ALL,
                 int smooth_method=0):

        # Save the model
        self.model = model
        self.kfilter = kfilter

        # Save the parameters
        self.filter_method = kfilter.filter_method

        # Make sure the appropriate output has been stored in the filter
        if self.kfilter.conserve_memory & MEMORY_NO_PREDICTED:
            raise ValueError('Cannot perform smoothing without all predicted states')

        if self.kfilter.conserve_memory & MEMORY_NO_GAIN:
            raise ValueError('Cannot perform smoothing without all Kalman gains')

        if self.kfilter.conserve_memory & MEMORY_NO_SMOOTHING:
            raise ValueError('Cannot perform smoothing without all smoothing variables')

        # Set smoothing output and initialize output arrays
        self.set_smoother_output(smoother_output)
        self.set_smooth_method(smooth_method)

    cdef allocate_arrays(self):
        cdef:
            np.npy_intp dim1[1]
            np.npy_intp dim2[2]
            np.npy_intp dim3[3]
        # #### Allocate arrays for calculations
        # Note: these are defined in memory according to the kfilter dimensions
        #       In the case of FILTERED_COLLAPSED, the smoothed measurement
        #       output describes only the component of transformed observations
        #       that is related to the states.

        # Arrays for Kalman smoother output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothing_error = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs;
        self.smoothed_state = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs;
        self.smoothed_state_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.kfilter.k_endog; dim2[1] = self.model.nobs;
        self.smoothed_measurement_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim2[0] = self.kfilter.k_posdef; dim2[1] = self.model.nobs;
        self.smoothed_state_disturbance = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim3[0] = self.kfilter.k_endog; dim3[1] = self.kfilter.k_endog; dim3[2] = self.model.nobs;
        self.smoothed_measurement_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)
        dim3[0] = self.kfilter.k_posdef; dim3[1] = self.kfilter.k_posdef; dim3[2] = self.model.nobs;
        self.smoothed_state_disturbance_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)

        # Innovations transition matrix (L_t = T_t - K_t Z_t)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.innovations_transition = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)

        # Smoothed state autocovariance arrays
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs
        self.smoothed_state_autocov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)

        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_states;
        self.tmp_autocov = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmp_autocov = &self.tmp_autocov[0, 0]

        # Diffuse output
        dim2[0] = self.kfilter.k_states; dim2[1] = self.model.nobs+1;
        self.scaled_smoothed_diffuse_estimator = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        dim3[0] = self.kfilter.k_states; dim3[1] = self.kfilter.k_states; dim3[2] = self.model.nobs+1;
        self.scaled_smoothed_diffuse1_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)
        self.scaled_smoothed_diffuse2_estimator_cov = np.PyArray_ZEROS(3, dim3, np.NPY_COMPLEX128, FORTRAN)

        # #### Arrays for temporary calculations
        # *Note*: in math notation below, a $\\#$ will represent a generic
        # temporary array, and a $\\#_i$ will represent a named temporary array.

        # # $L_t$ $(m \times m)$, also holds $(m \times r)$ sometimes
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmpL = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmpL = &self.tmpL[0, 0]
        self.tmpL2 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmpL2 = &self.tmpL2[0, 0]

        # # Holds arrays of dimension $(m \times m)$ and $(m \times r)$
        dim2[0] = self.kfilter.k_states; dim2[1] = max(self.kfilter.k_states, self.kfilter.k_posdef);
        self.tmp0 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmp0 = &self.tmp0[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp00 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmp00 = &self.tmp00[0, 0]

        # # Holds arrays of dimension $(m \times p)$
        dim2[0] = self.kfilter.k_states; dim2[1] = self.kfilter.k_endog;
        self.tmp000 = np.PyArray_ZEROS(2, dim2, np.NPY_COMPLEX128, FORTRAN)
        self._tmp000 = &self.tmp000[0, 0]

        # Arrays for missing data
        # dim1[0] = self.kfilter.k_endog * self.kfilter.k_states;
        # self.selected_design = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX128, FORTRAN)
        # dim1[0] = self.kfilter.k_endog2;
        # self.selected_obs_cov = np.PyArray_ZEROS(1, dim1, np.NPY_COMPLEX128, FORTRAN)

    def __reduce__(self):
        state = {
            't': self.t,
            '_smooth_method': self._smooth_method,
            'scaled_smoothed_estimator': np.array(self.scaled_smoothed_estimator, copy=True, order='F'),
            'scaled_smoothed_estimator_cov': np.array(self.scaled_smoothed_estimator_cov, copy=True, order='F'),
            'smoothing_error': np.array(self.smoothing_error, copy=True, order='F'),
            'smoothed_state': np.array(self.smoothed_state, copy=True, order='F'),
            'smoothed_state_cov': np.array(self.smoothed_state_cov, copy=True, order='F'),
            'smoothed_measurement_disturbance': np.array(self.smoothed_measurement_disturbance, copy=True, order='F'),
            'smoothed_state_disturbance': np.array(self.smoothed_state_disturbance, copy=True, order='F'),
            'smoothed_measurement_disturbance_cov': np.array(self.smoothed_measurement_disturbance_cov, copy=True, order='F'),
            'smoothed_state_disturbance_cov': np.array(self.smoothed_state_disturbance_cov, copy=True, order='F'),
            'smoothed_state_autocov': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'innovations_transition': np.array(self.smoothed_state_autocov, copy=True, order='F'),
            'tmp_autocov': np.array(self.tmp_autocov, copy=True, order='F'),
            'scaled_smoothed_diffuse_estimator': np.array(self.scaled_smoothed_diffuse_estimator, copy=True, order='F'),
            'scaled_smoothed_diffuse1_estimator_cov': np.array(self.scaled_smoothed_diffuse1_estimator_cov, copy=True, order='F'),
            'scaled_smoothed_diffuse2_estimator_cov': np.array(self.scaled_smoothed_diffuse2_estimator_cov, copy=True, order='F'),
            'tmpL': np.array(self.tmpL, copy=True, order='F'),
            'tmpL2': np.array(self.tmpL2, copy=True, order='F'),
            'tmp0': np.array(self.tmp0, copy=True, order='F'),
            'tmp00': np.array(self.tmp00, copy=True, order='F'),
            'tmp000': np.array(self.tmp000, copy=True, order='F')
        }
        args = (self.model, self.kfilter, self.smoother_output, self.smooth_method)
        return (self.__class__, args, state)

    def __setstate__(self, state):
        self.t = state['t']
        self._smooth_method = state['_smooth_method']
        self.scaled_smoothed_estimator = state['scaled_smoothed_estimator']
        self.scaled_smoothed_estimator_cov = state['scaled_smoothed_estimator_cov']
        self.smoothing_error = state['smoothing_error']
        self.smoothed_state = state['smoothed_state']
        self.smoothed_state_cov = state['smoothed_state_cov']
        self.smoothed_measurement_disturbance = state['smoothed_measurement_disturbance']
        self.smoothed_state_disturbance = state['smoothed_state_disturbance']
        self.smoothed_measurement_disturbance_cov = state['smoothed_measurement_disturbance_cov']
        self.smoothed_state_disturbance_cov = state['smoothed_state_disturbance_cov']
        self.smoothed_state_autocov = state['smoothed_state_autocov']
        self.innovations_transition = state['innovations_transition']
        self.tmp_autocov = state['tmp_autocov']
        self.scaled_smoothed_diffuse_estimator = state['scaled_smoothed_diffuse_estimator']
        self.scaled_smoothed_diffuse1_estimator_cov = state['scaled_smoothed_diffuse1_estimator_cov']
        self.scaled_smoothed_diffuse2_estimator_cov = state['scaled_smoothed_diffuse2_estimator_cov']
        self.tmpL = state['tmpL']
        self.tmpL2 = state['tmpL2']
        self.tmp0 = state['tmp0']
        self.tmp00 = state['tmp00']
        self.tmp000 = state['tmp000']
        self.initialize_smoother_object_pointers()
        self._initialize_temp_pointers()

    cdef void _initialize_temp_pointers(self) except *:
        self._tmpL = &self.tmpL[0, 0]
        self._tmpL2 = &self.tmpL2[0, 0]
        self._tmp0 = &self.tmp0[0, 0]
        self._tmp00 = &self.tmp00[0, 0]
        self._tmp000 = &self.tmp000[0, 0]
        self._tmp_autocov = &self.tmp_autocov[0, 0]

    cdef int check_filter_method_changed(self):
        return not self.kfilter.filter_method == self.filter_method

    cdef int reset_filter_method(self, int force_reset=True):
        cdef int changed = self.check_filter_method_changed()

        if changed or force_reset:
            # Save the new method
            self.filter_method = self.kfilter.filter_method
            # Reset matrices
            self.allocate_arrays()
            # Reset the smooth method (in case it was based on the filter
            # method)
            self.set_smooth_method(self.smooth_method)

        return changed

    cpdef set_smoother_output(self, int smoother_output, int force_reset=True):
        if not smoother_output == self.smoother_output or force_reset:
            # Change the smoother output flag
            self.smoother_output = smoother_output

            # Reset matrices
            self.reset(True)

    cpdef set_smooth_method(self, int smooth_method):
        cdef int _smooth_method
        self.smooth_method = smooth_method

        # If no smooth method provided, use default for the filter type
        if self.smooth_method == 0:
            self.reset_filter_method(False)
            if self.kfilter.filter_method & FILTER_UNIVARIATE:
                _smooth_method = SMOOTH_UNIVARIATE
            else:
                _smooth_method = SMOOTH_CONVENTIONAL
        else:
            _smooth_method = self.smooth_method

        # Make sure we do not have an invalid smooth method for our filter
        # method
        if((_smooth_method & SMOOTH_UNIVARIATE) and not (self.filter_method & FILTER_UNIVARIATE) or 
                (self.filter_method & FILTER_UNIVARIATE) and not (_smooth_method & SMOOTH_UNIVARIATE)):
            raise ValueError('Invalid smoothing method: can only use'
                             ' univariate smoothing when univariate filtering'
                             ' has been used previously.')


        self._smooth_method = _smooth_method

    cpdef reset(self, int force_reset=False):
        """
        reset(self)

        Reset the smoother.
        """
        # Reset the filter method (if necessary)
        self.reset_filter_method(force_reset)

        # Set the time
        self.t = self.model.nobs-1

    cpdef seek(self, unsigned int t):
        """
        seek(self, t)

        Change the time-state of the smoother

        Notes
        -----
        Between seek calls, the `filter_method` parameter of the associated
        Kalman filter object is not allowed to change. If the `filter_method`
        has changed, either recall the smoother using the object callable or
        explicitly reset the smoother using the `reset` method.
        """
        # Make sure the seek location is valid
        if not t == 0 and t >= <unsigned int>self.model.nobs:
            raise IndexError("Observation index out of range")

        # Make sure we have not changed filter methods in-between seeking
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother seek() calls."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")
        self.t = t

    def __iter__(self):
        return self

    def __call__(self, int smoother_output=-1):
        """
        Iterate the smoother across the entire set of observations.
        """
        cdef int i

        # Reset the smoother
        self.reset()
        
        # Perform backwards smoothing iterations
        for i in range(self.model.nobs-1,-1,-1):
            next(self)

    def __next__(self):
        """
        Perform an iteration of the Kalman smoother
        """
        cdef int t, inc = 1
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse

        # Get time subscript, and stop the iterator if at the end
        if not self.t >= 0:
            raise StopIteration

        # Make sure we have not changed filter methods in-between iterations
        if self.check_filter_method_changed():
            raise RuntimeError("Filter method in associated Kalman filter was"
                               " changed in between smoother iterations."
                               " If the filter method is changed, the smoother"
                               " must be called from the beginning. Use the"
                               " object callable (`__call__`) or the `reset`"
                               " method.")

        # Initialize pointers to current-iteration objects
        self.initialize_statespace_object_pointers()
        self.initialize_filter_object_pointers()
        self.initialize_smoother_object_pointers()

        # Initialize pointers to appropriate Kalman smoothing functions
        self.initialize_function_pointers()

        # Clear measurement disturbance variables` if we switched from
        # multivariate to univariate (because we might have off-diagonal
        # elements stored from a previous multivariate run that wouldn't have
        # been cleared yet)
        if self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t - 1] == 1:
            if self._smooth_method & SMOOTH_CLASSICAL:
                raise NotImplementedError(
                    'Cannot use classical smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                raise NotImplementedError(
                    'Cannot use alternative smoothing when the multivariate'
                    ' filter has fallen back to univariate filtering.')
            self.smoothing_error[..., :self.t] = 0
            self.smoothed_measurement_disturbance_cov[..., :self.t] = 0
            self.innovations_transition[..., :self.t] = 0

        # Conventional timing of the measurement step of the scaled smoothed
        # estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state and covariance matrix  
        # $\hat \alpha_t, V_t$
        if self.smoother_output & (SMOOTHER_STATE | SMOOTHER_STATE_COV):
            self.smooth_state(self, self.kfilter, self.model)

        # Modified Byrson-Frazier timing of the measurement step of the scaled
        # smoothed estimator and covariance matrix, smoothing error  
        # $L_t, r_{t-1}, N_{t-1}, u_t$
        if not diffuse and self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement(self, self.kfilter, self.model)

        # Smoothed state autocovariance matrix
        if diffuse:
            blas.zcopy(&self.kfilter.k_states2, self.kfilter._tmpL0, &inc, self._innovations_transition, &inc)
        else:
            blas.zcopy(&self.kfilter.k_states2, self._tmpL, &inc, self._innovations_transition, &inc)
        if self.smoother_output & SMOOTHER_STATE_AUTOCOV:
            if diffuse:
                zsmoothed_state_autocov_univariate_diffuse(self, self.kfilter, self.model)
            else:
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t+1]
                zsmoothed_state_autocov_conventional(self, self.kfilter, self.model)
                if self.smooth_method & SMOOTH_ALTERNATIVE:
                    self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, self.t]

        # Smoothed disturbances  
        # $\hat \eta_t, \hat \varepsilon_t, Var(\eta_t | Y_n), Var(\varepsilon_t | Y_n)$
        if self.smoother_output & SMOOTHER_DISTURBANCE:
            self.smooth_disturbances(self, self.kfilter, self.model)

        # Time step of the scaled smoothed estimator and covariance matrix
        self.smooth_estimators_time(self, self.kfilter, self.model)

        # When switching from the non-univariate filtering methods to the
        # univariate filtering method, we need to run one or more
        # univariate time smoothing steps to get the appropriate values (e.g.
        # for scaled_smoothed_estimator). Moreover, the alternative and
        # classical methods have different timing.
        # This happens in two cases:
        # 1. Exact diffuse filtering and smoothing only uses the univariate
        #    method, so if non-univariate filtering methods are used for
        #    the remaining filtering and smoothing then we need to run this.
        # 2. If the multivariate filtering method failed in a particular time
        #    step (due to a singular forecast error covariance matrix) and
        #    the univariate method was used as a fallback, then we need to run
        #    this.
        if ((self.kfilter.nobs_diffuse > 0 and self.t == self.kfilter.nobs_diffuse) or
                (self.t > 0 and self.kfilter.univariate_filter[self.t] == 0 and self.kfilter.univariate_filter[self.t-1] == 1)):
            t = self.t
            if self._smooth_method & SMOOTH_CONVENTIONAL:
                zsmoothed_estimators_time_univariate(self, self.kfilter, self.model)
            if self._smooth_method & SMOOTH_ALTERNATIVE:
                self.t = self.t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                zsmoothed_estimators_time_univariate(self, self.kfilter, self.model)
                self.t = t
            elif self._smooth_method & SMOOTH_CLASSICAL:
                self.t = t - 1
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                zsmoothed_estimators_measurement_classical(self, self.kfilter, self.model)
                self.t = t
                self.initialize_statespace_object_pointers()
                self.initialize_filter_object_pointers()
                self.initialize_smoother_object_pointers()
                self.initialize_function_pointers()
                zsmoothed_estimators_time_univariate(self, self.kfilter, self.model)

        # Note: the above diffuse handling for alternative and classical
        # handling modifies the pointers, and should be the last part of this
        # function

        # Advance the smoother
        self.t -= 1

    cdef void initialize_statespace_object_pointers(self) except *:
        cdef:
            int transform_diagonalize = 0
            int transform_generalized_collapse = 0
            int collapse_occurred = 0

        # Determine which transformations (would) need to be made
        transform_generalized_collapse = self.kfilter.filter_method & FILTER_COLLAPSED
        if not transform_generalized_collapse:
            transform_diagonalize = self.kfilter.univariate_filter[self.t]

        # Initialize object-level pointers to statespace arrays
        # Note: does not matter what transformations were required for the
        #       filter; we do not need to perform them for the smoother
        # TODO  actually we do, to get _design, _obs_cov, etc. However we do not
        #       need it to recalculate the selected_obs and loglikelihood, so
        #       need to decouple those parts from the generalized collapse
        self.model.seek(self.t, transform_diagonalize, transform_generalized_collapse)

        # Initialize object-level pointers to statespace arrays
        # self._design = self.model._design
        # self._obs_cov = self.model._obs_cov
        # self._transition = self.model._transition
        # self._selection = self.model._selection
        # self._state_cov = self.model._state_cov

        # A collapse would not actually occur in a given iteration, even with
        # the FILTER_COLLAPSED flag, in the case that there was enough missing
        # data that k_endog - nmissing <= k_states
        # collapse_occurred = (
        #     transform_generalized_collapse and
        #     self.model.k_endog - self.model._nmissing > self.model.k_states
        # )

        # If a collapse should have occurred, the dimensions need to be
        # adjusted (because we did not tell the model about the collapse in the
        # seek() call above)
        # if collapse_occurred:
        #     self.model.set_dimensions(self.model.k_states,
        #                               self.model.k_states,
        #                               self.model.k_posdef)

    cdef void initialize_filter_object_pointers(self):
        # cdef:
        #     int t = self.t
        #     int inc = 1

        # # Initialize object-level pointers to output arrays
        # self._predicted_state = &self.kfilter.predicted_state[0, t]
        # self._predicted_state_cov = &self.kfilter.predicted_state_cov[0, 0, t]
        # self._kalman_gain = &self.kfilter.kalman_gain[0, 0, t]

        # # Initialize object-level pointers to named temporary arrays
        # self._tmp1 = &self.kfilter.tmp1[0, 0, t]
        # self._tmp2 = &self.kfilter.tmp2[0, t]
        # self._tmp3 = &self.kfilter.tmp3[0, 0, t]
        # self._tmp4 = &self.kfilter.tmp4[0, 0, t]

        self.kfilter.seek(self.t, False)
        self.kfilter.initialize_filter_object_pointers()

    cdef void initialize_smoother_object_pointers(self) except *:
        cdef:
            int t = self.t
            int inc = 1
            int diffuse = self.t < self.kfilter.nobs_diffuse

        # Initialize object-level pointers to output arrays
        if diffuse or self._smooth_method & (SMOOTH_CONVENTIONAL | SMOOTH_CLASSICAL | SMOOTH_UNIVARIATE):
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t+1]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
        else:  # if self._smooth_method & SMOOTH_ALTERNATIVE
            self._input_scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t]
            self._input_scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t]
            self._scaled_smoothed_estimator = &self.scaled_smoothed_estimator[0, t-1]
            self._scaled_smoothed_estimator_cov = &self.scaled_smoothed_estimator_cov[0, 0, t-1]

        self._smoothing_error = &self.smoothing_error[0, t]
        self._smoothed_state = &self.smoothed_state[0, t]
        self._smoothed_state_cov = &self.smoothed_state_cov[0, 0, t]
        self._smoothed_measurement_disturbance = &self.smoothed_measurement_disturbance[0, t]
        self._smoothed_state_disturbance = &self.smoothed_state_disturbance[0, t]
        self._smoothed_measurement_disturbance_cov = &self.smoothed_measurement_disturbance_cov[0, 0, t]
        self._smoothed_state_disturbance_cov = &self.smoothed_state_disturbance_cov[0, 0, t]

        self._innovations_transition = &self.innovations_transition[0, 0, t]
        self._smoothed_state_autocov = &self.smoothed_state_autocov[0, 0, t]

        # Diffuse
        if diffuse:
            self._input_scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t+1]
            self._input_scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t+1]
            self._input_scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t+1]
            self._scaled_smoothed_diffuse_estimator = &self.scaled_smoothed_diffuse_estimator[0, t]
            self._scaled_smoothed_diffuse1_estimator_cov = &self.scaled_smoothed_diffuse1_estimator_cov[0, 0, t]
            self._scaled_smoothed_diffuse2_estimator_cov = &self.scaled_smoothed_diffuse2_estimator_cov[0, 0, t]


    cdef void initialize_function_pointers(self) except *:
        cdef int diffuse = self.t < self.kfilter.nobs_diffuse
        # Diffuse smoother
        if diffuse:
            self.smooth_estimators_measurement = zsmoothed_estimators_measurement_univariate_diffuse
            self.smooth_estimators_time = zsmoothed_estimators_time_univariate_diffuse
            self.smooth_state = zsmoothed_state_univariate_diffuse
            self.smooth_disturbances = zsmoothed_disturbances_univariate_diffuse
        # Univariate (modified Bryson-Frazier) smoother
        elif (self._smooth_method & SMOOTH_UNIVARIATE) or self.kfilter.univariate_filter[self.t]:
            self.smooth_estimators_measurement = zsmoothed_estimators_measurement_univariate
            self.smooth_estimators_time = zsmoothed_estimators_time_univariate
            self.smooth_state = zsmoothed_state_conventional
            self.smooth_disturbances = zsmoothed_disturbances_univariate
        # Multivariate modified Bryson-Frazier smoother
        elif self._smooth_method & SMOOTH_ALTERNATIVE:
            self.smooth_estimators_measurement = zsmoothed_estimators_measurement_alternative
            self.smooth_estimators_time = zsmoothed_estimators_time_alternative
            self.smooth_state = zsmoothed_state_alternative
            self.smooth_disturbances = zsmoothed_disturbances_alternative
        # Multivariate classical (Anderson and Moore) smoother
        elif self._smooth_method & SMOOTH_CLASSICAL:
            self.smooth_estimators_measurement = zsmoothed_estimators_measurement_classical
            self.smooth_estimators_time = zsmoothed_estimators_time_classical
            self.smooth_state = zsmoothed_state_classical
            self.smooth_disturbances = zsmoothed_disturbances_conventional
        # Multivariate conventional (Durbin and Koopman) smoother
        elif self._smooth_method & SMOOTH_CONVENTIONAL:
            self.smooth_estimators_measurement = zsmoothed_estimators_measurement_conventional
            self.smooth_estimators_time = zsmoothed_estimators_time_conventional
            self.smooth_state = zsmoothed_state_conventional
            self.smooth_disturbances = zsmoothed_disturbances_conventional
        else:
            raise NotImplementedError("Smoother method not available.")

        # Handle completely missing data
        # (All methods except the conventional method can use the same routines in this case)
        # This is essentially just an application of the smoothed_estimators_time_* step.
        if not diffuse and self._smooth_method & SMOOTH_CONVENTIONAL and self.model._nmissing == self.model.k_endog:
            # Change the smoothing functions to take into account a missing observation
            self.smooth_estimators_measurement = zsmoothed_estimators_missing_conventional
            # (no need to change the state smoothing recursion)
            # self.smooth_state = zsmoothed_state_missing_conventional
            self.smooth_disturbances = zsmoothed_disturbances_missing_conventional
