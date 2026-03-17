from .common cimport pywt_index_t

cdef extern from "c/wavelets.h":
    ctypedef enum SYMMETRY:
        ASYMMETRIC
        NEAR_SYMMETRIC
        SYMMETRIC
        ANTI_SYMMETRIC


    ctypedef enum WAVELET_NAME:
        HAAR
        RBIO
        DB
        SYM
        COIF
        BIOR
        DMEY
        GAUS
        MEXH
        MORL
        CGAU
        SHAN
        FBSP
        CMOR

    ctypedef struct BaseWavelet:
        pywt_index_t support_width

        unsigned int orthogonal
        unsigned int biorthogonal
        unsigned int compact_support

        SYMMETRY symmetry


        int _builtin
        char* family_name
        char* short_name

    ctypedef struct DiscreteWavelet:
        double* dec_hi_double      # highpass decomposition
        double* dec_lo_double      # lowpass   decomposition
        double* rec_hi_double      # highpass reconstruction
        double* rec_lo_double      # lowpass   reconstruction

        float* dec_hi_float
        float* dec_lo_float
        float* rec_hi_float
        float* rec_lo_float
        size_t dec_len         # length of decomposition filter
        size_t rec_len         # length of reconstruction filter

        int vanishing_moments_psi
        int vanishing_moments_phi
        BaseWavelet base


    ctypedef struct ContinuousWavelet:

        BaseWavelet base
        float lower_bound
        float upper_bound
        float center_frequency
        float bandwidth_frequency
        unsigned int fbsp_order
        int complex_cwt


    cdef int is_discrete_wavelet(WAVELET_NAME name)
    cdef DiscreteWavelet* discrete_wavelet(WAVELET_NAME name, int type)
    cdef DiscreteWavelet* blank_discrete_wavelet(size_t filter_length)
    cdef void free_discrete_wavelet(DiscreteWavelet* wavelet)

    cdef ContinuousWavelet* continuous_wavelet(WAVELET_NAME name, int type)
    cdef ContinuousWavelet* blank_continuous_wavelet()
    cdef void free_continuous_wavelet(ContinuousWavelet* wavelet)
