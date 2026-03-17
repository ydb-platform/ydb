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

#include "convolution.h"

#if defined _MSC_VER
#define restrict __restrict
#elif defined __GNUC__
#define restrict __restrict__
#endif

/* This file contains several functions for computing the convolution of a
 * signal with a filter. The general scheme is:
 *   output[o] = sum(filter[j] * input[i-j] for j = [0..F) and i = [0..N))
 * where 'o', 'i' and 'j' may progress at different rates.
 *
 * Most of the code deals with different edge extension modes. Values are
 * computed on-demand, in four steps:
 * 1. Filter extends past signal on the left.
 * 2. Filter completely contained within signal (no extension).
 * 3. Filter extends past signal on both sides (only if F > N).
 * 4. Filter extends past signal on the right.
 *
 * MODE_PERIODIZATION produces different output lengths to other modes, so is
 * implemented as a separate function for each case.
 *
 * See 'common.h' for descriptions of the extension modes.
 */

int CAT(TYPE, _downsampling_convolution_periodization)(const TYPE * const restrict input, const size_t N,
                                                       const REAL_TYPE * const restrict filter, const size_t F,
                                                       TYPE * const restrict output, const size_t step,
                                                       const size_t fstep)
{
    size_t i = F/2, o = 0;
    const size_t padding = (step - (N % step)) % step;

    for (; i < F && i < N; i += step, ++o) {
        TYPE sum = 0;
        size_t j;
        size_t k_start = 0;
        for (j = 0; j <= i; j += fstep)
            sum += filter[j] * input[i-j];
        if (fstep > 1)
            k_start = j - (i + 1);
        while (j < F){
            size_t k;
            for (k = k_start; k < padding && j < F; k += fstep, j += fstep)
                sum += filter[j] * input[N-1];
            for (k = k_start; k < N && j < F; k += fstep, j += fstep)
                sum += filter[j] * input[N-1-k];
        }
        output[o] = sum;
    }

    for(; i < N; i+=step, ++o){
        TYPE sum = 0;
        size_t j;
        for(j = 0; j < F; j += fstep)
            sum += input[i-j]*filter[j];
        output[o] = sum;
    }

    for (; i < F && i < N + F/2; i += step, ++o) {
        TYPE sum = 0;
        size_t j = 0;
        size_t k_start = 0;
        while (i-j >= N){
            size_t k;
            // for simplicity, not using fstep here
            for (k = 0; k < padding && i-j >= N; ++k, ++j)
                sum += filter[i-N-j] * input[N-1];
            for (k = 0; k < N && i-j >= N; ++k, ++j)
                sum += filter[i-N-j] * input[k];
        }
        if (fstep > 1)
            j += (fstep - j % fstep) % fstep;  // move to next non-zero entry
        for (; j <= i; j += fstep)
            sum += filter[j] * input[i-j];
        if (fstep > 1)
            k_start = j - (i + 1);
        while (j < F){
            size_t k;
            for (k = k_start; k < padding && j < F; k += fstep, j += fstep)
                sum += filter[j] * input[N-1];
            for (k = k_start; k < N && j < F; k += fstep, j += fstep)
                sum += filter[j] * input[N-1-k];
        }
        output[o] = sum;
    }

    for(; i < N + F/2; i += step, ++o){
        TYPE sum = 0;
        size_t j = 0;
        while (i-j >= N){
            // for simplicity, not using fstep here
            size_t k;
            for (k = 0; k < padding && i-j >= N; ++k, ++j)
                sum += filter[i-N-j] * input[N-1];
            for (k = 0; k < N && i-j >= N; ++k, ++j)
                sum += filter[i-N-j] * input[k];
        }
        if (fstep > 1)
            j += (fstep - j % fstep) % fstep;  // move to next non-zero entry
        for (; j < F; j += fstep)
            sum += filter[j] * input[i-j];
        output[o] = sum;
    }
    return 0;
}


int CAT(TYPE, _downsampling_convolution)(const TYPE * const restrict input, const size_t N,
                                         const REAL_TYPE * const restrict filter, const size_t F,
                                         TYPE * const restrict output,
                                         const size_t step, MODE mode)
{
    /* This convolution performs efficient downsampling by computing every
     * step'th element of normal convolution (currently tested only for step=1
     * and step=2).
     */

    size_t i = step - 1, o = 0;

    if(mode == MODE_PERIODIZATION)
        return CAT(TYPE, _downsampling_convolution_periodization)(input, N, filter, F, output, step, 1);

    if (mode == MODE_SMOOTH && N < 2)
        mode = MODE_CONSTANT_EDGE;

    // left boundary overhang
    for(; i < F && i < N; i+=step, ++o){
        TYPE sum = 0;
        size_t j;
        for(j = 0; j <= i; ++j)
            sum += filter[j]*input[i-j];

        switch(mode) {
        case MODE_SYMMETRIC:
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++j, ++k)
                    sum += filter[j]*input[k];
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ANTISYMMETRIC:
            // half-sample anti-symmetric
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++j, ++k)
                    sum -= filter[j]*input[k];
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_REFLECT:
            while (j < F){
                size_t k;
                for(k = 1; k < N && j < F; ++j, ++k)
                    sum += filter[j]*input[k];
                for(k = 1; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ANTIREFLECT:{
            // whole-sample anti-symmetric
            size_t k;
            TYPE le = input[0];    // current left edge value
            TYPE tmp = 0;
            while (j < F) {
                for(k = 1; k < N && j < F; ++j, ++k){
                    tmp = le - (input[k] - input[0]);
                    sum += filter[j]*tmp;
                }
                le = tmp;
                for(k = 1; k < N && j < F; ++j, ++k){
                    tmp = le + (input[N-1-k] - input[N-1]);
                    sum += filter[j]*tmp;
                }
                le = tmp;
            }
            break;
        }
        case MODE_CONSTANT_EDGE:
            for(; j < F; ++j)
                sum += filter[j]*input[0];
            break;
        case MODE_SMOOTH:{
            size_t k;
            for(k = 1; j < F; ++j, ++k)
                sum += filter[j]*(input[0] + k * (input[0] - input[1]));
            break;
        }
        case MODE_PERIODIC:
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ZEROPAD:
        default:
            break;
        }
        output[o] = sum;
    }

    // center (if input equal or wider than filter: N >= F)
    for(; i < N; i+=step, ++o){
        TYPE sum = 0;
        size_t j;
        for(j = 0; j < F; ++j)
            sum += input[i-j]*filter[j];
        output[o] = sum;
    }

    // center (if filter is wider than input: F > N)
    for(; i < F; i+=step, ++o){
        TYPE sum = 0;
        size_t j = 0;

        switch(mode) {
        case MODE_SYMMETRIC:
            // Included from original: TODO: j < F-_offset
            /* Iterate over filter in reverse to process elements away from
             * data. This gives a known first input element to process (N-1)
             */
            while (i - j >= N){
                size_t k;
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[N-1-k];
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ANTISYMMETRIC:
            // half-sample anti-symmetric
            while (i - j >= N){
                size_t k;
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum -= filter[i-N-j]*input[N-1-k];
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_REFLECT:
            while (i - j >= N){
                size_t k;
                for(k = 1; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[N-1-k];
                for(k = 1; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ANTIREFLECT:{
            // whole-sample anti-symmetric
            size_t k;
            TYPE re = input[N-1];    // current right edge value
            TYPE tmp = 0;
            while (i - j >= N) {
                for(k = 1; k < N && i-j >= N; ++j, ++k){
                    tmp = re - (input[N-1-k] - input[N-1]);
                    sum += filter[i-N-j]*tmp;
                }
                re = tmp;
                for(k = 1; k < N && i-j >= N; ++j, ++k){
                    tmp = re + (input[k] - input[0]);
                    sum += filter[i-N-j]*tmp;
                }
                re = tmp;
            }
            break;
        }
        case MODE_CONSTANT_EDGE:
            for(; i-j >= N; ++j)
                sum += filter[j]*input[N-1];
            break;
        case MODE_SMOOTH:{
            size_t k;
            for(k = i - N + 1; i-j >= N; ++j, --k)
                sum += filter[j]*(input[N-1] + k * (input[N-1] - input[N-2]));
            break;
        }
        case MODE_PERIODIC:
            while (i-j >= N){
                size_t k;
                for (k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ZEROPAD:
        default:
            j = i - N + 1;
            break;
        }

        for(; j <= i; ++j)
            sum += filter[j]*input[i-j];

        switch(mode) {
        case MODE_SYMMETRIC:
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++j, ++k)
                    sum += filter[j]*input[k];
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ANTISYMMETRIC:
            // half-sample anti-symmetric
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++j, ++k)
                    sum -= filter[j]*input[k];
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_REFLECT:
            while (j < F){
                size_t k;
                for(k = 1; k < N && j < F; ++j, ++k)
                    sum += filter[j]*input[k];
                for(k = 1; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ANTIREFLECT:{
            // whole-sample anti-symmetric
            size_t k;
            TYPE le = input[0];    // current left edge value
            TYPE tmp = 0;
            while (j < F) {
                for(k = 1; k < N && j < F; ++j, ++k){
                    tmp = le - (input[k] - input[0]);
                    sum += filter[j]*tmp;
                }
                le = tmp;
                for(k = 1; k < N && j < F; ++j, ++k){
                    tmp = le + (input[N-1-k] - input[N-1]);
                    sum += filter[j]*tmp;
                }
                le = tmp;
            }
            break;
            }
        case MODE_CONSTANT_EDGE:
            for(; j < F; ++j)
                sum += filter[j]*input[0];
            break;
        case MODE_SMOOTH:{
            size_t k;
            for(k = 1; j < F; ++j, ++k)
                sum += filter[j]*(input[0] + k * (input[0] - input[1]));
            break;
        }
        case MODE_PERIODIC:
            while (j < F){
                size_t k;
                for(k = 0; k < N && j < F; ++k, ++j)
                    sum += filter[j]*input[N-1-k];
            }
            break;
        case MODE_ZEROPAD:
        default:
            break;
        }
        output[o] = sum;
    }

    // right boundary overhang
    for(; i < N+F-1; i += step, ++o){
        TYPE sum = 0;
        size_t j = 0;
        switch(mode) {
        case MODE_SYMMETRIC:
            // Included from original: TODO: j < F-_offset
            while (i - j >= N){
                size_t k;
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[N-1-k];
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ANTISYMMETRIC:
            // half-sample anti-symmetric
            while (i - j >= N){
                size_t k;
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum -= filter[i-N-j]*input[N-1-k];
                for(k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_REFLECT:
            while (i - j >= N){
                size_t k;
                for(k = 1; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[N-1-k];
                for(k = 1; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ANTIREFLECT:{
            // whole-sample anti-symmetric
            size_t k;
            TYPE re = input[N-1];    //current right edge value
            TYPE tmp = 0;
            while (i - j >= N) {
                //first reflection
                for(k = 1; k < N && i-j >= N; ++j, ++k){
                    tmp = re - (input[N-1-k] - input[N-1]);
                    sum += filter[i-N-j]*tmp;
                }
                re = tmp;
                //second reflection
                for(k = 1; k < N && i-j >= N; ++j, ++k){
                    tmp = re + (input[k] - input[0]);
                    sum += filter[i-N-j]*tmp;
                }
                re = tmp;
            }
            break;
            }
        case MODE_CONSTANT_EDGE:
            for(; i-j >= N; ++j)
                sum += filter[j]*input[N-1];
            break;
        case MODE_SMOOTH:{
            size_t k;
            for(k = i - N + 1; i-j >= N; ++j, --k)
                sum += filter[j]*(input[N-1] + k * (input[N-1] - input[N-2]));
            break;
        }
        case MODE_PERIODIC:
            while (i-j >= N){
                size_t k;
                for (k = 0; k < N && i-j >= N; ++j, ++k)
                    sum += filter[i-N-j]*input[k];
            }
            break;
        case MODE_ZEROPAD:
        default:
            j = i - N + 1;
            break;
        }
        for(; j < F; ++j)
            sum += filter[j]*input[i-j];
        output[o] = sum;
    }
    return 0;
}

int CAT(TYPE, _upsampling_convolution_full)(const TYPE * const restrict input, const size_t N,
                                            const REAL_TYPE * const restrict filter, const size_t F,
                                            TYPE * const restrict output, const size_t O)
{
    /* Performs a zero-padded convolution, using each input element for two
     * consecutive filter elements. This simulates an upsampled input.
     *
     * In contrast to downsampling_convolution, this adds to the output. This
     * allows multiple runs with different inputs and the same output to be used
     * for idwt.
     */

    // If check omitted, this function would be a no-op for F<2
    size_t i = 0, o = 0;

    if(F<2)
        return -1;
    if(F%2)
        return -3;

    for(; i < N && i < F/2; ++i, o += 2){
        size_t j;
        for(j = 0; j <= i; ++j){
            output[o] += filter[j*2] * input[i-j];
            output[o+1] += filter[j*2+1] * input[i-j];
        }
    }

    for(; i < N; ++i, o += 2){
        size_t j;
        for(j = 0; j < F/2; ++j){
            output[o] += filter[j*2] * input[i-j];
            output[o+1] += filter[j*2+1] * input[i-j];
        }
    }

    for(; i < F/2; ++i, o += 2){
        size_t j;
        for(j = i-(N-1); j <= i; ++j){
            output[o] += filter[j*2] * input[i-j];
            output[o+1] += filter[j*2+1] * input[i-j];
        }
    }

    for(; i < N+F/2; ++i, o += 2){
        size_t j;
        for(j = i-(N-1); j < F/2; ++j){
            output[o] += filter[j*2] * input[i-j];
            output[o+1] += filter[j*2+1] * input[i-j];
        }
    }
    return 0;
}


static int CAT(TYPE, _upsampling_convolution_valid_sf_periodization)(const TYPE * const restrict input, const size_t N,
                                                                     const REAL_TYPE * const restrict filter, const size_t F,
                                                                     TYPE * const restrict output, const size_t O)
{
    // TODO? Allow for non-2 step

    size_t const start = F/4;
    size_t i = start;
    size_t const end = N + start - (((F/2)%2) ? 0 : 1);
    size_t o = 0;

    if(F%2) return -3; /* Filter must have even-length. */

    if ((F/2)%2 == 0){
        // Shift output one element right. This is necessary for perfect reconstruction.

        // i = N-1; even element goes to output[O-1], odd element goes to output[0]
        size_t j = 0;
        while(j <= start-1){
            size_t k;
            for (k = 0; k < N && j <= start-1; ++k, ++j){
                output[2*N-1] += filter[2*(start-1-j)] * input[k];
                output[0] += filter[2*(start-1-j)+1] * input[k];
            }
        }
        for (; j <= N+start-1 && j < F/2; ++j){
            output[2*N-1] += filter[2*j] * input[N+start-1-j];
            output[0] += filter[2*j+1] * input[N+start-1-j];
        }
        while (j < F / 2){
            size_t k;
            for (k = 0; k < N && j < F/2; ++k, ++j){
                output[2*N-1] += filter[2*j] * input[N-1-k];
                output[0] += filter[2*j+1] * input[N-1-k];
            }
        }

        o += 1;
    }

    for (; i < F/2 && i < N; ++i, o += 2){
        size_t j = 0;
        for(; j <= i; ++j){
            output[o] += filter[2*j] * input[i-j];
            output[o+1] += filter[2*j+1] * input[i-j];
        }
        while (j < F/2){
            size_t k;
            for(k = 0; k < N && j < F/2; ++k, ++j){
                output[o] += filter[2*j] * input[N-1-k];
                output[o+1] += filter[2*j+1] * input[N-1-k];
            }
        }
    }

    for (; i < N; ++i, o += 2){
        size_t j;
        for(j = 0; j < F/2; ++j){
            output[o] += filter[2*j] * input[i-j];
            output[o+1] += filter[2*j+1] * input[i-j];
        }
    }

    for (; i < F/2 && i < end; ++i, o += 2){
        size_t j = 0;
        while(i-j >= N){
            size_t k;
            for (k = 0; k < N && i-j >= N; ++k, ++j){
                output[o] += filter[2*(i-N-j)] * input[k];
                output[o+1] += filter[2*(i-N-j)+1] * input[k];
            }
        }
        for (; j <= i && j < F/2; ++j){
            output[o] += filter[2*j] * input[i-j];
            output[o+1] += filter[2*j+1] * input[i-j];
        }
        while (j < F / 2){
            size_t k;
            for (k = 0; k < N && j < F/2; ++k, ++j){
                output[o] += filter[2*j] * input[N-1-k];
                output[o+1] += filter[2*j+1] * input[N-1-k];
            }
        }
    }

    for (; i < end; ++i, o += 2){
        size_t j = 0;
        while(i-j >= N){
            size_t k;
            for (k = 0; k < N && i-j >= N; ++k, ++j){
                output[o] += filter[2*(i-N-j)] * input[k];
                output[o+1] += filter[2*(i-N-j)+1] * input[k];
            }
        }
        for (; j <= i && j < F/2; ++j){
            output[o] += filter[2*j] * input[i-j];
            output[o+1] += filter[2*j+1] * input[i-j];
        }
    }

    return 0;
}


/*
 * performs IDWT for all modes
 *
 * The upsampling is performed by splitting filters to even and odd elements
 * and performing 2 convolutions.  After refactoring the PERIODIZATION mode
 * case to separate function this looks much clearer now.
 */

int CAT(TYPE, _upsampling_convolution_valid_sf)(const TYPE * const restrict input, const size_t N,
                                                const REAL_TYPE * const restrict filter, const size_t F,
                                                TYPE * const restrict output, const size_t O,
                                                MODE mode)
{
    // TODO: Allow non-2 step?

    if(mode == MODE_PERIODIZATION)
        return CAT(TYPE, _upsampling_convolution_valid_sf_periodization)(input, N, filter, F, output, O);

    if((F%2) || (N < F/2))
        return -1;

    // Perform only stage 2 - all elements in the filter overlap an input element.
    {
        size_t o, i;
        for(o = 0, i = F/2 - 1; i < N; ++i, o += 2){
            TYPE sum_even = 0;
            TYPE sum_odd = 0;
            size_t j;
            for(j = 0; j < F/2; ++j){
                sum_even += filter[j*2] * input[i-j];
                sum_odd += filter[j*2+1] * input[i-j];
            }
            output[o] += sum_even;
            output[o+1] += sum_odd;
        }
    }
    return 0;
}

/* -> swt - todo */
int CAT(TYPE, _upsampled_filter_convolution)(const TYPE * const restrict input, const size_t N,
                                             const REAL_TYPE * const restrict filter, const size_t F,
                                             TYPE * const restrict output,
                                             const size_t step, MODE mode)
{
    return -1;
}

#undef restrict
#endif /* REAL_TYPE */
#endif /* TYPE */
