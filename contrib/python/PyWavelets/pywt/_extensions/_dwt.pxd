from ._pywt cimport Wavelet, cdata_t


cpdef upcoef(bint do_rec_a, cdata_t[::1] coeffs, Wavelet wavelet, int level,
             size_t take)
