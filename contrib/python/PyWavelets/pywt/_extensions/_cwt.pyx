#cython: boundscheck=False, wraparound=False
from . cimport common
from . cimport c_wt
from .common cimport pywt_index_t, MODE
from ._pywt cimport _check_dtype

cimport numpy as np
import numpy as np

np.import_array()


cpdef cwt_psi_single(data_t[::1] data, ContinuousWavelet wavelet, size_t output_len):
    cdef np.ndarray psi, psi_r, psi_i
    cdef size_t data_size = data.size
    cdef int family_number = 0
    cdef double bandwidth_frequency
    cdef double center_frequency
    cdef int fbsp_order
    if output_len < 1:
        raise RuntimeError("Invalid output length.")

    #if data_t is np.float64_t:
        # TODO: Don't think these have to be 0-initialized
        # TODO: Check other methods of allocating (e.g. Cython/CPython arrays)
    if data_t is np.float64_t:
        if wavelet.short_family_name == "gaus":
            psi = np.zeros(output_len, np.float64)
            family_number = wavelet.family_number
            with nogil:
                c_wt.double_gaus(&data[0], <double *>psi.data, data_size, family_number)
            return psi
        elif wavelet.short_family_name == "mexh":
            psi = np.zeros(output_len, np.float64)
            with nogil:
                c_wt.double_mexh(&data[0], <double *>psi.data, data_size)
            return psi
        elif wavelet.short_family_name == "morl":
            psi = np.zeros(output_len, np.float64)
            with nogil:
                c_wt.double_morl(&data[0], <double *>psi.data, data_size)
            return psi
        elif wavelet.short_family_name == "cgau":
            psi_r = np.zeros(output_len, np.float64)
            psi_i = np.zeros(output_len, np.float64)
            family_number = wavelet.family_number
            with nogil:
                c_wt.double_cgau(&data[0], <double *>psi_r.data, <double *>psi_i.data, data_size, family_number)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "shan":
            psi_r = np.zeros(output_len, np.float64)
            psi_i = np.zeros(output_len, np.float64)
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.double_shan(&data[0], <double *>psi_r.data, <double *>psi_i.data, data_size, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "fbsp":
            psi_r = np.zeros(output_len, np.float64)
            psi_i = np.zeros(output_len, np.float64)
            fbsp_order = wavelet.fbsp_order
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.double_fbsp(&data[0], <double *>psi_r.data, <double *>psi_i.data, data_size, fbsp_order, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "cmor":
            psi_r = np.zeros(output_len, np.float64)
            psi_i = np.zeros(output_len, np.float64)
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.double_cmor(&data[0], <double *>psi_r.data, <double *>psi_i.data, data_size, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)

    elif data_t is np.float32_t:
        if wavelet.short_family_name == "gaus":
            psi = np.zeros(output_len, np.float32)
            family_number = wavelet.family_number
            with nogil:
                c_wt.float_gaus(&data[0], <float *>psi.data, data_size, family_number)
            return psi
        elif wavelet.short_family_name == "mexh":
            psi = np.zeros(output_len, np.float32)
            with nogil:
                c_wt.float_mexh(&data[0], <float *>psi.data, data_size)
            return psi
        elif wavelet.short_family_name == "morl":
            psi = np.zeros(output_len, np.float32)
            with nogil:
                c_wt.float_morl(&data[0], <float *>psi.data, data_size)
            return psi
        elif wavelet.short_family_name == "cgau":
            psi_r = np.zeros(output_len, np.float32)
            psi_i = np.zeros(output_len, np.float32)
            family_number = wavelet.family_number
            with nogil:
                c_wt.float_cgau(&data[0], <float *>psi_r.data, <float *>psi_i.data, data_size, family_number)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "shan":
            psi_r = np.zeros(output_len, np.float32)
            psi_i = np.zeros(output_len, np.float32)
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.float_shan(&data[0], <float *>psi_r.data, <float *>psi_i.data, data_size, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "fbsp":
            psi_r = np.zeros(output_len, np.float32)
            psi_i = np.zeros(output_len, np.float32)
            fbsp_order = wavelet.fbsp_order
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.float_fbsp(&data[0], <float *>psi_r.data, <float *>psi_i.data, data_size, fbsp_order, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)
        elif wavelet.short_family_name == "cmor":
            psi_r = np.zeros(output_len, np.float32)
            psi_i = np.zeros(output_len, np.float32)
            bandwidth_frequency = wavelet.bandwidth_frequency
            center_frequency = wavelet.center_frequency
            with nogil:
                c_wt.float_cmor(&data[0], <float *>psi_r.data, <float *>psi_i.data, data_size, bandwidth_frequency, center_frequency)
            return (psi_r, psi_i)
