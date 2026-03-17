#cython: boundscheck=False, wraparound=False
from . cimport common
from . cimport c_wt
from .common cimport pywt_index_t, MODE
from ._pywt cimport _check_dtype

cimport numpy as np
import numpy as np

include "config.pxi"

np.import_array()

cpdef dwt_max_level(size_t data_len, size_t filter_len):
    return common.dwt_max_level(data_len, filter_len)


cpdef dwt_coeff_len(size_t data_len, size_t filter_len, MODE mode):
    if data_len < 1:
        raise ValueError("Value of data_len must be greater than zero.")
    if filter_len < 1:
        raise ValueError("Value of filter_len must be greater than zero.")

    return common.dwt_buffer_length(data_len, filter_len, mode)

cpdef dwt_single(cdata_t[::1] data, Wavelet wavelet, MODE mode):
    cdef size_t output_len = dwt_coeff_len(data.size, wavelet.dec_len, mode)
    cdef np.ndarray cA, cD
    cdef int retval_a, retval_d
    cdef size_t data_size = data.size
    if output_len < 1:
        raise RuntimeError("Invalid output length.")

    if data_size == 1 and (mode == MODE.MODE_REFLECT or mode == MODE.MODE_ANTIREFLECT):
        raise ValueError("Input data length must be greater than 1 for [anti]reflect mode.")

    if cdata_t is np.float64_t:
        # TODO: Don't think these have to be 0-initialized
        # TODO: Check other methods of allocating (e.g. Cython/CPython arrays)
        cA = np.zeros(output_len, np.float64)
        cD = np.zeros(output_len, np.float64)
        with nogil:
            retval_a = c_wt.double_dec_a(&data[0], data_size, wavelet.w,
                                <double *>cA.data, output_len, mode)
            retval_d = c_wt.double_dec_d(&data[0], data_size, wavelet.w,
                            <double *>cD.data, output_len, mode)
        if ( retval_a < 0 or retval_d < 0):
            raise RuntimeError("C dwt failed.")
    elif cdata_t is np.float32_t:
        cA = np.zeros(output_len, np.float32)
        cD = np.zeros(output_len, np.float32)
        with nogil:
            retval_a = c_wt.float_dec_a(&data[0], data_size, wavelet.w,
                               <float *>cA.data, output_len, mode)
            retval_d = c_wt.float_dec_d(&data[0], data_size, wavelet.w,
                           <float *>cD.data, output_len, mode)
        if ( retval_a < 0 or retval_d < 0):
            raise RuntimeError("C dwt failed.")

    IF HAVE_C99_CPLX:
        if cdata_t is np.complex128_t:
            cA = np.zeros(output_len, np.complex128)
            cD = np.zeros(output_len, np.complex128)
            with nogil:
                retval_a = c_wt.double_complex_dec_a(&data[0], data_size, wavelet.w,
                                    <double complex *>cA.data, output_len, mode)
                retval_d = c_wt.double_complex_dec_d(&data[0], data_size, wavelet.w,
                                <double complex *>cD.data, output_len, mode)
            if ( retval_a < 0 or retval_d < 0):
                raise RuntimeError("C dwt failed.")
        elif cdata_t is np.complex64_t:
            cA = np.zeros(output_len, np.complex64)
            cD = np.zeros(output_len, np.complex64)
            with nogil:
                retval_a = c_wt.float_complex_dec_a(&data[0], data_size, wavelet.w,
                                   <float complex *>cA.data, output_len, mode)
                retval_d = c_wt.float_complex_dec_d(&data[0], data_size, wavelet.w,
                               <float complex *>cD.data, output_len, mode)
            if ( retval_a < 0 or retval_d < 0):
                raise RuntimeError("C dwt failed.")

    return (cA, cD)


cpdef dwt_axis(np.ndarray data, Wavelet wavelet, MODE mode, unsigned int axis=0):
    # memory-views do not support n-dimensional arrays, use np.ndarray instead
    cdef common.ArrayInfo data_info, output_info
    cdef np.ndarray cD, cA
    # Explicit input_shape necessary to prevent memory leak
    cdef size_t[::1] input_shape, output_shape
    cdef int retval = -5


    if data.shape[axis] == 1 and (mode == MODE.MODE_REFLECT or mode == MODE.MODE_ANTIREFLECT):
        raise ValueError("Input data length must be greater than 1 for [anti]reflect mode along the transformed axis.")

    data = data.astype(_check_dtype(data), copy=False)

    input_shape = <size_t [:data.ndim]> <size_t *> data.shape
    output_shape = input_shape.copy()
    output_shape[axis] = dwt_coeff_len(data.shape[axis], wavelet.dec_len, mode)

    cA = np.empty(output_shape, data.dtype)
    cD = np.empty(output_shape, data.dtype)

    data_info.ndim = data.ndim
    data_info.strides = <pywt_index_t *> data.strides
    data_info.shape = <size_t *> data.shape

    output_info.ndim = cA.ndim
    output_info.strides = <pywt_index_t *> cA.strides
    output_info.shape = <size_t *> cA.shape

    if data.dtype == np.float64:
        with nogil:
            retval = c_wt.double_downcoef_axis(<double *> data.data, data_info,
                                         <double *> cA.data, output_info,
                                         wavelet.w, axis, common.COEF_APPROX, mode,
                                         0, common.DWT_TRANSFORM)
        if retval:
            raise RuntimeError("C wavelet transform failed")
        with nogil:
            retval = c_wt.double_downcoef_axis(<double *> data.data, data_info,
                                     <double *> cD.data, output_info,
                                     wavelet.w, axis, common.COEF_DETAIL, mode,
                                     0, common.DWT_TRANSFORM)
        if retval:
            raise RuntimeError("C wavelet transform failed")
    elif data.dtype == np.float32:
        with nogil:
            retval = c_wt.float_downcoef_axis(<float *> data.data, data_info,
                                    <float *> cA.data, output_info,
                                    wavelet.w, axis, common.COEF_APPROX, mode,
                                    0, common.DWT_TRANSFORM)
        if retval:
            raise RuntimeError("C wavelet transform failed")
        with nogil:
            retval = c_wt.float_downcoef_axis(<float *> data.data, data_info,
                                    <float *> cD.data, output_info,
                                    wavelet.w, axis, common.COEF_DETAIL, mode,
                                    0, common.DWT_TRANSFORM)
        if retval:
            raise RuntimeError("C wavelet transform failed")
    IF HAVE_C99_CPLX:
        if data.dtype == np.complex64:
            with nogil:
                retval = c_wt.float_complex_downcoef_axis(<float complex *> data.data, data_info,
                                        <float complex *> cA.data, output_info,
                                        wavelet.w, axis, common.COEF_APPROX, mode,
                                        0, common.DWT_TRANSFORM)
            if retval:
                raise RuntimeError("C wavelet transform failed")
            with nogil:
                retval = c_wt.float_complex_downcoef_axis(<float complex *> data.data, data_info,
                                        <float complex *> cD.data, output_info,
                                        wavelet.w, axis, common.COEF_DETAIL, mode,
                                        0, common.DWT_TRANSFORM)
            if retval:
                raise RuntimeError("C wavelet transform failed")
        elif data.dtype == np.complex128:
            with nogil:
                retval = c_wt.double_complex_downcoef_axis(<double complex *> data.data, data_info,
                                             <double complex *> cA.data, output_info,
                                             wavelet.w, axis, common.COEF_APPROX, mode,
                                             0, common.DWT_TRANSFORM)
            if retval:
                raise RuntimeError("C wavelet transform failed")
            with nogil:
                retval = c_wt.double_complex_downcoef_axis(<double complex *> data.data, data_info,
                                         <double complex *> cD.data, output_info,
                                         wavelet.w, axis, common.COEF_DETAIL, mode,
                                         0, common.DWT_TRANSFORM)
            if retval:
                raise RuntimeError("C wavelet transform failed")

    if retval == -5:
        raise TypeError("Array must be floating point, not {}"
                        .format(data.dtype))
    return (cA, cD)


cpdef idwt_single(np.ndarray cA, np.ndarray cD, Wavelet wavelet, MODE mode):
    cdef size_t input_len, rec_len
    cdef int retval
    cdef np.ndarray rec

    # check for size difference between arrays
    if cA.size != cD.size:
        raise ValueError("Coefficients arrays must have the same size.")
    else:
        input_len = cA.size

    if cA.dtype != cD.dtype:
        raise ValueError("Coefficients arrays must have the same dtype.")

    # find reconstruction buffer length
    rec_len = common.idwt_buffer_length(input_len, wavelet.rec_len, mode)
    if rec_len < 1:
        msg = ("Invalid coefficient arrays length for specified wavelet. "
               "Wavelet and mode must be the same as used for decomposition.")
        raise ValueError(msg)

        # call idwt func.  one of cA/cD can be None, then only
    # reconstruction of non-null part will be performed
    if cA.dtype == np.float64:
        rec = np.zeros(rec_len, dtype=np.float64)
        with nogil:
            retval = c_wt.double_idwt(<double *>cA.data, input_len,
                            <double *>cD.data, input_len,
                            <double *>rec.data, rec_len,
                            wavelet.w, mode)
        if retval < 0:
            raise RuntimeError("C idwt failed.")
    elif cA.dtype == np.float32:
        rec = np.zeros(rec_len, dtype=np.float32)
        with nogil:
            retval = c_wt.float_idwt(<float *>cA.data, input_len,
                           <float *>cD.data, input_len,
                           <float *>rec.data, rec_len,
                           wavelet.w, mode)
        if retval < 0:
            raise RuntimeError("C idwt failed.")
    IF HAVE_C99_CPLX:
        if cA.dtype == np.complex128:
            rec = np.zeros(rec_len, dtype=np.complex128)
            with nogil:
                retval = c_wt.double_complex_idwt(<double complex *>cA.data, input_len,
                                <double complex *>cD.data, input_len,
                                <double complex *>rec.data, rec_len,
                                wavelet.w, mode)
            if retval < 0:
                raise RuntimeError("C idwt failed.")
        elif cA.dtype == np.complex64:
            rec = np.zeros(rec_len, dtype=np.complex64)
            with nogil:
                retval = c_wt.float_complex_idwt(<float complex *>cA.data, input_len,
                               <float complex *>cD.data, input_len,
                               <float complex *>rec.data, rec_len,
                               wavelet.w, mode)
            if retval < 0:
                raise RuntimeError("C idwt failed.")

    return rec


cpdef idwt_axis(np.ndarray coefs_a, np.ndarray coefs_d,
                Wavelet wavelet, MODE mode, unsigned int axis=0):
    cdef common.ArrayInfo a_info, d_info, output_info
    cdef common.ArrayInfo *a_info_p = NULL
    cdef common.ArrayInfo *d_info_p = NULL
    cdef np.ndarray output
    cdef np.dtype output_dtype
    cdef void *data_a = NULL
    cdef void *data_d = NULL
    # Explicit input_shape necessary to prevent memory leak
    cdef size_t[::1] input_shape, output_shape
    cdef int retval = -5

    if coefs_a is not None:
        if coefs_d is not None and coefs_d.dtype.itemsize > coefs_a.dtype.itemsize:
            coefs_a = coefs_a.astype(_check_dtype(coefs_d), copy=False)
        else:
            coefs_a = coefs_a.astype(_check_dtype(coefs_a), copy=False)
        a_info.ndim = coefs_a.ndim
        a_info.strides = <pywt_index_t *> coefs_a.strides
        a_info.shape = <size_t *> coefs_a.shape
        a_info_p = &a_info
        data_a = <void *> coefs_a.data
    if coefs_d is not None:
        if coefs_a is not None and coefs_a.dtype.itemsize > coefs_d.dtype.itemsize:
            coefs_d = coefs_d.astype(_check_dtype(coefs_a), copy=False)
        else:
            coefs_d = coefs_d.astype(_check_dtype(coefs_d), copy=False)
        d_info.ndim = coefs_d.ndim
        d_info.strides = <pywt_index_t *> coefs_d.strides
        d_info.shape = <size_t *> coefs_d.shape
        d_info_p = &d_info
        data_d = <void *> coefs_d.data

    if coefs_a is not None:
        input_shape = <size_t [:coefs_a.ndim]> <size_t *> coefs_a.shape
        output_dtype = coefs_a.dtype
    elif coefs_d is not None:
        input_shape = <size_t [:coefs_d.ndim]> <size_t *> coefs_d.shape
        output_dtype = coefs_d.dtype
    else:
        return None

    output_shape = input_shape.copy()
    output_shape[axis] = common.idwt_buffer_length(input_shape[axis],
                                                   wavelet.rec_len, mode)
    output = np.empty(output_shape, output_dtype)

    output_info.ndim = output.ndim
    output_info.strides = <pywt_index_t *> output.strides
    output_info.shape = <size_t *> output.shape

    if output.dtype == np.float64:
        with nogil:
            retval = c_wt.double_idwt_axis(<double *> data_a, a_info_p,
                                 <double *> data_d, d_info_p,
                                 <double *> output.data, output_info,
                                 wavelet.w, axis, mode)
        if retval:
            raise RuntimeError("C inverse wavelet transform failed")
    elif output.dtype == np.float32:
        with nogil:
            retval = c_wt.float_idwt_axis(<float *> data_a, a_info_p,
                                <float *> data_d, d_info_p,
                                <float *> output.data, output_info,
                                wavelet.w, axis, mode)
        if retval:
            raise RuntimeError("C inverse wavelet transform failed")
    IF HAVE_C99_CPLX:
        if output.dtype == np.complex128:
            with nogil:
                retval = c_wt.double_complex_idwt_axis(<double complex *> data_a, a_info_p,
                                     <double complex *> data_d, d_info_p,
                                     <double complex *> output.data, output_info,
                                     wavelet.w, axis, mode)
            if retval:
                raise RuntimeError("C inverse wavelet transform failed")
        elif output.dtype == np.complex64:
            with nogil:
                retval = c_wt.float_complex_idwt_axis(<float complex *> data_a, a_info_p,
                                    <float complex *> data_d, d_info_p,
                                    <float complex *> output.data, output_info,
                                    wavelet.w, axis, mode)
            if retval:
                raise RuntimeError("C inverse wavelet transform failed")

    if retval == -5:
        raise TypeError("Array must be floating point, not {}"
                        .format(output.dtype))

    return output


cpdef upcoef(bint do_rec_a, cdata_t[::1] coeffs, Wavelet wavelet, int level,
             size_t take):
    cdef cdata_t[::1] rec
    cdef int i, retval
    cdef size_t rec_len, left_bound, right_bound, coeffs_size

    rec_len = 0

    if level < 1:
        raise ValueError("Value of level must be greater than 0.")

    for i in range(level):
        coeffs_size = coeffs.size
        # output len
        rec_len = common.reconstruction_buffer_length(coeffs.size, wavelet.dec_len)
        if rec_len < 1:
            raise RuntimeError("Invalid output length.")

        # To mirror multi-level wavelet reconstruction behaviour, when detail
        # reconstruction is requested, the dec_d variant is only called at the
        # first level to generate the approximation coefficients at the second
        # level.  Subsequent levels apply the reconstruction filter.
        if cdata_t is np.float64_t:
            rec = np.zeros(rec_len, dtype=np.float64)
            if do_rec_a or i > 0:
                with nogil:
                    retval = c_wt.double_rec_a(&coeffs[0], coeffs_size, wavelet.w,
                                     &rec[0], rec_len)
                if retval < 0:
                    raise RuntimeError("C rec_a failed.")
            else:
                with nogil:
                    retval = c_wt.double_rec_d(&coeffs[0], coeffs_size, wavelet.w,
                                     &rec[0], rec_len)
                if retval < 0:
                    raise RuntimeError("C rec_d failed.")
        elif cdata_t is np.float32_t:
            rec = np.zeros(rec_len, dtype=np.float32)
            if do_rec_a or i > 0:
                with nogil:
                    retval = c_wt.float_rec_a(&coeffs[0], coeffs_size, wavelet.w,
                                    &rec[0], rec_len)
                if retval < 0:
                    raise RuntimeError("C rec_a failed.")
            else:
                with nogil:
                    retval = c_wt.float_rec_d(&coeffs[0], coeffs_size, wavelet.w,
                                    &rec[0], rec_len)
                if retval < 0:
                    raise RuntimeError("C rec_d failed.")
        IF HAVE_C99_CPLX:
            if cdata_t is np.complex128_t:
                rec = np.zeros(rec_len, dtype=np.complex128)
                if do_rec_a or i > 0:
                    with nogil:
                        retval = c_wt.double_complex_rec_a(&coeffs[0], coeffs_size, wavelet.w,
                                         &rec[0], rec_len)
                    if retval < 0:
                        raise RuntimeError("C rec_a failed.")
                else:
                    with nogil:
                        retval = c_wt.double_complex_rec_d(&coeffs[0], coeffs_size, wavelet.w,
                                         &rec[0], rec_len)
                    if retval < 0:
                        raise RuntimeError("C rec_d failed.")
            elif cdata_t is np.complex64_t:
                rec = np.zeros(rec_len, dtype=np.complex64)
                if do_rec_a or i > 0:
                    with nogil:
                        retval = c_wt.float_complex_rec_a(&coeffs[0], coeffs_size, wavelet.w,
                                        &rec[0], rec_len)
                    if retval < 0:
                        raise RuntimeError("C rec_a failed.")
                else:
                    with nogil:
                        retval = c_wt.float_complex_rec_d(&coeffs[0], coeffs_size, wavelet.w,
                                        &rec[0], rec_len)
                    if retval < 0:
                        raise RuntimeError("C rec_d failed.")
        # TODO: this algorithm needs some explaining
        coeffs = rec

    if take > 0 and take < rec_len:
        left_bound = right_bound = (rec_len-take) // 2
        if (rec_len-take) % 2:
            # right_bound must never be zero for indexing to work
            right_bound = right_bound + 1

        return rec[left_bound:-right_bound]

    return rec


cpdef downcoef(bint do_dec_a, cdata_t[::1] data, Wavelet wavelet, MODE mode, int level):
    cdef cdata_t[::1] coeffs
    cdef int i, retval
    cdef size_t output_len, data_size

    if level < 1:
        raise ValueError("Value of level must be greater than 0.")

    for i in range(level):
        data_size = data.size
        output_len = common.dwt_buffer_length(data.size, wavelet.dec_len, mode)
        if output_len < 1:
            raise RuntimeError("Invalid output length.")

        # To mirror multi-level wavelet decomposition behaviour, when detail
        # coefficients are requested, the dec_d variant is only called at the
        # final level.  All prior levels use dec_a.  In other words, the detail
        # coefficients at level n are those produced via the operation of the
        # detail filter on the approximation coefficients of level n-1.
        if cdata_t is np.float64_t:
            coeffs = np.zeros(output_len, dtype=np.float64)
            if do_dec_a or (i < level - 1):
                with nogil:
                    retval = c_wt.double_dec_a(&data[0], data_size, wavelet.w,
                                     &coeffs[0], output_len, mode)
                if retval < 0:
                    raise RuntimeError("C dec_a failed.")
            else:
                with nogil:
                    retval = c_wt.double_dec_d(&data[0], data_size, wavelet.w,
                                     &coeffs[0], output_len, mode)
                if retval < 0:
                    raise RuntimeError("C dec_d failed.")
        elif cdata_t is np.float32_t:
            coeffs = np.zeros(output_len, dtype=np.float32)
            if do_dec_a or (i < level - 1):
                with nogil:
                    retval = c_wt.float_dec_a(&data[0], data_size, wavelet.w,
                                    &coeffs[0], output_len, mode)
                if retval < 0:
                    raise RuntimeError("C dec_a failed.")
            else:
                with nogil:
                    retval = c_wt.float_dec_d(&data[0], data_size, wavelet.w,
                                    &coeffs[0], output_len, mode)
                if retval < 0:
                    raise RuntimeError("C dec_d failed.")
        IF HAVE_C99_CPLX:
            if cdata_t is np.complex128_t:
                coeffs = np.zeros(output_len, dtype=np.complex128)
                if do_dec_a or (i < level - 1):
                    with nogil:
                        retval = c_wt.double_complex_dec_a(&data[0], data_size, wavelet.w,
                                         &coeffs[0], output_len, mode)
                    if retval < 0:
                        raise RuntimeError("C dec_a failed.")
                else:
                    with nogil:
                        retval = c_wt.double_complex_dec_d(&data[0], data_size, wavelet.w,
                                         &coeffs[0], output_len, mode)
                    if retval < 0:
                        raise RuntimeError("C dec_d failed.")
            elif cdata_t is np.complex64_t:
                coeffs = np.zeros(output_len, dtype=np.complex64)
                if do_dec_a or (i < level - 1):
                    with nogil:
                        retval = c_wt.float_complex_dec_a(&data[0], data_size, wavelet.w,
                                        &coeffs[0], output_len, mode)
                    if retval < 0:
                        raise RuntimeError("C dec_a failed.")
                else:
                    with nogil:
                        retval = c_wt.float_complex_dec_d(&data[0], data_size, wavelet.w,
                                        &coeffs[0], output_len, mode)
                    if retval < 0:
                        raise RuntimeError("C dec_d failed.")
        data = coeffs

    return coeffs
