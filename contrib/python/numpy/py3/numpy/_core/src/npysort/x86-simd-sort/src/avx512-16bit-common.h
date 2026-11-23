/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_16BIT_COMMON
#define AVX512_16BIT_COMMON

struct avx512_16bit_swizzle_ops {
    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t swap_n(typename vtype::reg_t reg)
    {
        __m512i v = vtype::cast_to(reg);

        if constexpr (scale == 2) {
            constexpr static uint16_t arr[]
                    = {1,  0,  3,  2,  5,  4,  7,  6,  9,  8,  11,
                       10, 13, 12, 15, 14, 17, 16, 19, 18, 21, 20,
                       23, 22, 25, 24, 27, 26, 29, 28, 31, 30};
            __m512i mask = _mm512_loadu_si512(arr);
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 4) {
            v = _mm512_shuffle_epi32(v, (_MM_PERM_ENUM)0b10110001);
        }
        else if constexpr (scale == 8) {
            v = _mm512_shuffle_epi32(v, (_MM_PERM_ENUM)0b01001110);
        }
        else if constexpr (scale == 16) {
            v = _mm512_shuffle_i64x2(v, v, 0b10110001);
        }
        else if constexpr (scale == 32) {
            v = _mm512_shuffle_i64x2(v, v, 0b01001110);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v);
    }

    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t
    reverse_n(typename vtype::reg_t reg)
    {
        __m512i v = vtype::cast_to(reg);

        if constexpr (scale == 2) { return swap_n<vtype, 2>(reg); }
        else if constexpr (scale == 4) {
            constexpr static uint16_t arr[]
                    = {3,  2,  1,  0,  7,  6,  5,  4,  11, 10, 9,
                       8,  15, 14, 13, 12, 19, 18, 17, 16, 23, 22,
                       21, 20, 27, 26, 25, 24, 31, 30, 29, 28};
            __m512i mask = _mm512_loadu_si512(arr);
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 8) {
            constexpr static int16_t arr[]
                    = {7,  6,  5,  4,  3,  2,  1,  0,  15, 14, 13,
                       12, 11, 10, 9,  8,  23, 22, 21, 20, 19, 18,
                       17, 16, 31, 30, 29, 28, 27, 26, 25, 24};
            __m512i mask = _mm512_loadu_si512(arr);
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 16) {
            constexpr static uint16_t arr[]
                    = {15, 14, 13, 12, 11, 10, 9,  8,  7,  6,  5,
                       4,  3,  2,  1,  0,  31, 30, 29, 28, 27, 26,
                       25, 24, 23, 22, 21, 20, 19, 18, 17, 16};
            __m512i mask = _mm512_loadu_si512(arr);
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 32) {
            return vtype::reverse(reg);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v);
    }

    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t
    merge_n(typename vtype::reg_t reg, typename vtype::reg_t other)
    {
        __m512i v1 = vtype::cast_to(reg);
        __m512i v2 = vtype::cast_to(other);

        if constexpr (scale == 2) {
            v1 = _mm512_mask_blend_epi16(
                    0b01010101010101010101010101010101, v1, v2);
        }
        else if constexpr (scale == 4) {
            v1 = _mm512_mask_blend_epi16(
                    0b00110011001100110011001100110011, v1, v2);
        }
        else if constexpr (scale == 8) {
            v1 = _mm512_mask_blend_epi16(
                    0b00001111000011110000111100001111, v1, v2);
        }
        else if constexpr (scale == 16) {
            v1 = _mm512_mask_blend_epi16(
                    0b00000000111111110000000011111111, v1, v2);
        }
        else if constexpr (scale == 32) {
            v1 = _mm512_mask_blend_epi16(
                    0b00000000000000001111111111111111, v1, v2);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v1);
    }
};

#endif // AVX512_16BIT_COMMON
