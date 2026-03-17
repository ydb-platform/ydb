from ._pywt cimport ContinuousWavelet, data_t


cpdef cwt_psi_single(data_t[::1] data, ContinuousWavelet wavelet, size_t output_len)
