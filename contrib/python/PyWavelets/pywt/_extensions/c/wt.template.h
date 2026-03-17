/* Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
 * Copyright (c) 2012-2016 The PyWavelets Developers
 *                         <https://github.com/PyWavelets/pywt>
 * See COPYING for license details.
 */

/* Wavelet transforms using convolution functions defined in convolution.h */

#include "templating.h"

#ifndef TYPE
#error TYPE must be defined here.
#else

#include "wt.h"

#if defined _MSC_VER
#define restrict __restrict
#elif defined __GNUC__
#define restrict __restrict__
#endif

/* _a suffix - wavelet transform approximations */
/* _d suffix - wavelet transform details */

int CAT(TYPE, _downcoef_axis)(const TYPE * const restrict input, const ArrayInfo input_info,
                              TYPE * const restrict output, const ArrayInfo output_info,
                              const DiscreteWavelet * const restrict wavelet, const size_t axis,
                              const Coefficient detail, const MODE dwt_mode,
                              const size_t swt_level,
                              const DiscreteTransformType transform);

// a_info and d_info are pointers, as they may be NULL
int CAT(TYPE, _idwt_axis)(const TYPE * const restrict coefs_a, const ArrayInfo * a_info,
                          const TYPE * const restrict coefs_d, const ArrayInfo * d_info,
                          TYPE * const restrict output, const ArrayInfo output_info,
                          const DiscreteWavelet * const restrict wavelet,
                          const size_t axis, const MODE mode);

/* Single level decomposition */
int CAT(TYPE, _dec_a)(const TYPE * const restrict input, const size_t input_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, const size_t output_len,
                      const MODE mode);

int CAT(TYPE, _dec_d)(const TYPE * const restrict input, const size_t input_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, const size_t output_len,
                      const MODE mode);

/* Single level reconstruction */
int CAT(TYPE, _rec_a)(const TYPE * const restrict coeffs_a, const size_t coeffs_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, const size_t output_len);

int CAT(TYPE, _rec_d)(const TYPE * const restrict coeffs_d, const size_t coeffs_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, const size_t output_len);

/* Single level IDWT reconstruction */
int CAT(TYPE, _idwt)(const TYPE * const restrict coeffs_a, const size_t coeffs_a_len,
                     const TYPE * const restrict coeffs_d, const size_t coeffs_d_len,
                     TYPE * const restrict output, const size_t output_len,
                     const DiscreteWavelet * const restrict wavelet, const MODE mode);

/* SWT decomposition at given level */
int CAT(TYPE, _swt_a)(const TYPE * const restrict input, pywt_index_t input_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, pywt_index_t output_len,
                      unsigned int level);

int CAT(TYPE, _swt_d)(const TYPE * const restrict input, pywt_index_t input_len,
                      const DiscreteWavelet * const restrict wavelet,
                      TYPE * const restrict output, pywt_index_t output_len,
                      unsigned int level);

int CAT(TYPE, _swt_axis)(const TYPE * const restrict input, const ArrayInfo input_info,
                         TYPE * const restrict output, const ArrayInfo output_info,
                         const DiscreteWavelet * const restrict wavelet, const size_t axis,
                         const Coefficient detail, unsigned int level);

#endif /* TYPE */
#undef restrict
