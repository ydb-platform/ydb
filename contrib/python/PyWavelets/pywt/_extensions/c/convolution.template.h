/* Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
 * Copyright (c) 2012-2016 The PyWavelets Developers
 *                         <https://github.com/PyWavelets/pywt>
 * See COPYING for license details.
 */

#include "templating.h"


#ifndef REAL_TYPE
#error REAL_TYPE must be defined here.
#else

#ifndef TYPE
#error TYPE must be defined here.
#else

#include "common.h"

#if defined _MSC_VER
#define restrict __restrict
#elif defined __GNUC__
#define restrict __restrict__
#endif

/* Performs convolution of input with filter and downsamples by taking every
 * step-th element from the result.
 *
 * input    - input data
 * N        - input data length
 * filter   - filter data
 * F        - filter data length
 * output   - output data
 * step     - decimation step
 * mode     - signal extension mode
 */

/* memory efficient version */

int CAT(TYPE, _downsampling_convolution)(const TYPE * const restrict input, const size_t N,
                                         const REAL_TYPE * const restrict filter, const size_t F,
                                         TYPE * const restrict output, const size_t step,
                                         MODE mode);


/* downsampling convolution routine specific to periodization mode.
 *
 * input    - input data
 * N        - input data length
 * filter   - filter data
 * F        - filter data length
 * output   - output data
 * step     - decimation step
 * fstep    - step size between non-zero entries in filter
 *            (used to improve performance for the multilevel swt)
 */
int CAT(TYPE, _downsampling_convolution_periodization)(
    const TYPE * const restrict input, const size_t N,
    const REAL_TYPE * const restrict filter, const size_t F,
    TYPE * const restrict output, const size_t step,
    const size_t fstep);

/*
 * Performs normal (full) convolution of "upsampled" input coeffs array with
 * filter Requires zero-filled output buffer (adds values instead of
 * overwriting - can be called many times with the same output).
 *
 * input    - input data
 * N        - input data length
 * filter   - filter data
 * F        - filter data length
 * output   - output data
 * O        - output length (currently not used)
 * mode     - signal extension mode
 */

int CAT(TYPE, _upsampling_convolution_full)(const TYPE * const restrict input, const size_t N,
                                            const REAL_TYPE * const restrict filter, const size_t F,
                                            TYPE * const restrict output, const size_t O);

/* Performs valid convolution (signals must overlap)
 * Extends (virtually) input for MODE_PERIODIZATION.
 */

int CAT(TYPE, _upsampling_convolution_valid_sf)(const TYPE * const restrict input, const size_t N,
                                                const REAL_TYPE * const restrict filter, const size_t F,
                                                TYPE * const restrict output, const size_t O,
                                                MODE mode);

/* TODO
 * for SWT
 * int upsampled_filter_convolution(const TYPE * const restrict input, const int N,
 *                                  const REAL_TYPE * const restrict filter, const int F,
 *                                  TYPE * const restrict output, int step, int mode);
 */



#undef restrict
#endif /* REAL_TYPE */
#endif /* TYPE */
