#ifndef AWS_COMMON_MATH_MSVC_INL
#define AWS_COMMON_MATH_MSVC_INL

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*
 * This header is already included, but include it again to make editor
 * highlighting happier.
 */
#include <aws/common/common.h>
#include <aws/common/math.h>

#include <immintrin.h>
#include <intrin.h>

AWS_EXTERN_C_BEGIN
/**
 * Multiplies a * b. If the result overflows, returns 2^64 - 1.
 */
AWS_STATIC_IMPL uint64_t aws_mul_u64_saturating(uint64_t a, uint64_t b) {
    uint64_t out;
    uint64_t ret_val = _umul128(a, b, &out);
    return (out == 0) ? ret_val : UINT64_MAX;
}

/**
 * If a * b overflows, returns AWS_OP_ERR; otherwise multiplies
 * a * b, returns the result in *r, and returns AWS_OP_SUCCESS.
 */
AWS_STATIC_IMPL int aws_mul_u64_checked(uint64_t a, uint64_t b, uint64_t *r) {
    uint64_t out;
    *r = _umul128(a, b, &out);

    if (out != 0) {
        return aws_raise_error(AWS_ERROR_OVERFLOW_DETECTED);
    }
    return AWS_OP_SUCCESS;
}

/**
 * Multiplies a * b. If the result overflows, returns 2^32 - 1.
 */
AWS_STATIC_IMPL uint32_t aws_mul_u32_saturating(uint32_t a, uint32_t b) {
    uint32_t out;
    uint32_t ret_val = _mulx_u32(a, b, &out);
    return (out == 0) ? ret_val : UINT32_MAX;
}

/**
 * If a * b overflows, returns AWS_OP_ERR; otherwise multiplies
 * a * b, returns the result in *r, and returns AWS_OP_SUCCESS.
 */
AWS_STATIC_IMPL int aws_mul_u32_checked(uint32_t a, uint32_t b, uint32_t *r) {
    uint32_t out;
    *r = _mulx_u32(a, b, &out);

    if (out != 0) {
        return aws_raise_error(AWS_ERROR_OVERFLOW_DETECTED);
    }
    return AWS_OP_SUCCESS;
}

/**
 * If a + b overflows, returns AWS_OP_ERR; otherwise adds
 * a + b, returns the result in *r, and returns AWS_OP_SUCCESS.
 */
AWS_STATIC_IMPL int aws_add_u64_checked(uint32_t a, uint32_t b, uint32_t *r) {
    if (_addcarry_u64(0, a, b, *r)) {
        return aws_raise_error(AWS_ERROR_OVERFLOW_DETECTED);
    }
    return AWS_OP_SUCCESS;
}

/**
 * Adds a + b. If the result overflows, returns 2^64 - 1.
 */
AWS_STATIC_IMPL uint64_t aws_add_u64_saturating(uint32_t a, uint32_t b) {
    uint32_t res;

    if (_addcarry_u64(0, a, b, &res)) {
        res = UINT64_MAX;
    }

    return res;
}

/**
 * If a + b overflows, returns AWS_OP_ERR; otherwise adds
 * a + b, returns the result in *r, and returns AWS_OP_SUCCESS.
 */
AWS_STATIC_IMPL int aws_add_u32_checked(uint32_t a, uint32_t b, uint32_t *r) {
  if(_addcarry_u32(0, a, b, *r){
        return aws_raise_error(AWS_ERROR_OVERFLOW_DETECTED);
    }
    return AWS_OP_SUCCESS;
}

/**
 * Adds a + b. If the result overflows, returns 2^32 - 1.
 */
AWS_STATIC_IMPL uint64_t aws_add_u32_saturating(uint32_t a, uint32_t b) {
    uint32_t res;

    if (_addcarry_u32(0, a, b, &res)) {
        res = UINT32_MAX;
    }

    return res;
}

/**
 * Search from the MSB to LSB, looking for a 1
 */
AWS_STATIC_IMPL size_t aws_clz_u32(uint32_t n) {
    unsigned long idx = 0;
    _BitScanReverse(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_clz_i32(int32_t n) {
    unsigned long idx = 0;
    _BitScanReverse(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_clz_u64(uint64_t n) {
    unsigned long idx = 0;
    _BitScanReverse64(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_clz_i64(int64_t n) {
    unsigned long idx = 0;
    _BitScanReverse64(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_clz_size(size_t n) {
#if SIZE_BITS == 64
    return aws_clz_u64(n);
#else
    return aws_clz_u32(n);
#endif
}

/**
 * Search from the LSB to MSB, looking for a 1
 */
AWS_STATIC_IMPL size_t aws_ctz_u32(uint32_t n) {
    unsigned long idx = 0;
    _BitScanForward(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_ctz_i32(int32_t n) {
    unsigned long idx = 0;
    _BitScanForward(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_ctz_u64(uint64_t n) {
    unsigned long idx = 0;
    _BitScanForward64(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_ctz_i64(int64_t n) {
    unsigned long idx = 0;
    _BitScanForward64(&idx, n);
    return idx;
}

AWS_STATIC_IMPL size_t aws_ctz_size(size_t n) {
#if SIZE_BITS == 64
    return aws_ctz_u64(n);
#else
    return aws_ctz_u32(n);
#endif
}

AWS_EXTERN_C_END
#endif /* WS_COMMON_MATH_MSVC_INL */
