/* Copyright (c) 2006-2012 Filip Wasilewski <http://en.ig.ma/>
 * Copyright (c) 2012-2016 The PyWavelets Developers
 *                         <https://github.com/PyWavelets/pywt>
 * See COPYING for license details.
 */

#include "common.h"
#include <limits.h> // for U*_MAX

// MSVC <= 2008 does not have stdint.h, but defines SIZE_MAX in limits.h
#if (!defined(_MSC_VER)) || (_MSC_VER > 1500)
#include <stdint.h> // for SIZE_MAX
#endif /* _MSC_VER */

/* Returns the floor of the base-2 log of it's input
 *
 * Undefined for x = 0
 */
unsigned char size_log2(size_t x){
#if defined(_MSC_VER)
    unsigned long i;
#if SIZE_MAX == 0xFFFFFFFF
    _BitScanReverse(&i, x);
#elif SIZE_MAX == 0xFFFFFFFFFFFFFFFF
    _BitScanReverse64(&i, x);
#else
#error "Unrecognized SIZE_MAX"
#endif /* SIZE_MAX */
    return i;
#else
    // GCC and clang
    // Safe cast: 0 <= clzl < arch_bits (64) where result is defined
#if SIZE_MAX == UINT_MAX
    unsigned char leading_zeros = (unsigned char) __builtin_clz(x);
#elif SIZE_MAX == ULONG_MAX
    unsigned char leading_zeros = (unsigned char) __builtin_clzl(x);
#elif SIZE_MAX == ULLONG_MAX
    unsigned char leading_zeros = (unsigned char) __builtin_clzll(x);
#else
#error "Unrecognized SIZE_MAX"
#endif /* SIZE_MAX */
    return sizeof(size_t) * 8 - leading_zeros - 1;
#endif /* _MSC_VER */
}

/* buffers and max levels params */

size_t dwt_buffer_length(size_t input_len, size_t filter_len, MODE mode){
    if(input_len < 1 || filter_len < 1)
        return 0;

    switch(mode){
            case MODE_PERIODIZATION:
                return input_len / 2 + ((input_len%2) ? 1 : 0);
            default:
                return (input_len + filter_len - 1) / 2;
    }
}

size_t reconstruction_buffer_length(size_t coeffs_len, size_t filter_len){
    if(coeffs_len < 1 || filter_len < 1)
        return 0;

    return 2*coeffs_len+filter_len-2;
}

size_t idwt_buffer_length(size_t coeffs_len, size_t filter_len, MODE mode){
    switch(mode){
            case MODE_PERIODIZATION:
                return 2*coeffs_len;
            default:
                return 2*coeffs_len-filter_len+2;
    }
}

size_t swt_buffer_length(size_t input_len){
    return input_len;
}

unsigned char dwt_max_level(size_t input_len, size_t filter_len){
    if(filter_len <= 1 || input_len < (filter_len-1))
        return 0;

    return size_log2(input_len/(filter_len-1));
}

unsigned char swt_max_level(size_t input_len){
    /* check how many times input_len is divisible by 2 */
    unsigned char j = 0;
    while (input_len > 0){
        if (input_len % 2)
            return j;
        input_len /= 2;
        j++;
    }
    return j;
}
