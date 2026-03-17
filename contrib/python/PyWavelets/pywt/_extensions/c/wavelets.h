/* Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
 * Copyright (c) 2012-2016 The PyWavelets Developers
 *                         <https://github.com/PyWavelets/pywt>
 * See COPYING for license details.
 */

#pragma once

#include "common.h"

/* Wavelet symmetry properties */
typedef enum {
    UNKNOWN = -1,
    ASYMMETRIC = 0,
    NEAR_SYMMETRIC = 1,
    SYMMETRIC = 2,
    ANTI_SYMMETRIC = 3
} SYMMETRY;

/* Wavelet name */
typedef enum {
    HAAR,
    RBIO,
    DB,
    SYM,
    COIF,
    BIOR,
    DMEY,
    GAUS,
    MEXH,
    MORL,
    CGAU,
    SHAN,
    FBSP,
    CMOR
} WAVELET_NAME;


/* Wavelet structure holding pointers to filter arrays and property attributes */
typedef struct  {
    /* Wavelet properties */

    pywt_index_t support_width;

    SYMMETRY symmetry;

    unsigned int orthogonal:1;
    unsigned int biorthogonal:1;
    unsigned int compact_support:1;

    int _builtin;
    char* family_name;
    char* short_name;


} BaseWavelet;

typedef struct {
    BaseWavelet base;
    double* dec_hi_double;  /* highpass decomposition */
    double* dec_lo_double;  /* lowpass decomposition */
    double* rec_hi_double;  /* highpass reconstruction */
    double* rec_lo_double;  /* lowpass reconstruction */
    float* dec_hi_float;
    float* dec_lo_float;
    float* rec_hi_float;
    float* rec_lo_float;
    size_t dec_len;   /* length of decomposition filter */
    size_t rec_len;   /* length of reconstruction filter */

    int vanishing_moments_psi;
    int vanishing_moments_phi;

} DiscreteWavelet;

typedef struct {

    BaseWavelet base;
    float lower_bound;
    float upper_bound;
    /* Parameters for shan, fbsp, cmor*/
    int complex_cwt;
    float center_frequency;
    float bandwidth_frequency;
    unsigned int fbsp_order;

} ContinuousWavelet;


int is_discrete_wavelet(WAVELET_NAME name);

/*
 * Allocate Wavelet struct and set its attributes
 * name - (currently) a character codename of a wavelet family
 * order - order of the wavelet (ie. coif3 has order 3)
 */
DiscreteWavelet* discrete_wavelet(WAVELET_NAME name, unsigned int order);
ContinuousWavelet* continuous_wavelet(WAVELET_NAME name, unsigned int order);
/*
 * Allocate blank Discrete Wavelet with zero-filled filters of given length
 */
DiscreteWavelet* blank_discrete_wavelet(size_t filters_length);

ContinuousWavelet* blank_continuous_wavelet(void);

/* Deep copy Discrete Wavelet */
DiscreteWavelet* copy_discrete_wavelet(DiscreteWavelet* base);

/*
 * Free wavelet struct. Use this to free Wavelet allocated with
 * wavelet(...) or blank_wavelet(...) functions.
 */
void free_discrete_wavelet(DiscreteWavelet *wavelet);

void free_continuous_wavelet(ContinuousWavelet *wavelet);
