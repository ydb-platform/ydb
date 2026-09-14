/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_16BIT_COMMON
#define AVX512_16BIT_COMMON

/*
 * Constants used in sorting 32 elements in a ZMM registers. Based on Bitonic
 * sorting network (see
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg)
 */
// ZMM register: 31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0
static const uint16_t network[6][32]
        = {{7,  6,  5,  4,  3,  2,  1,  0,  15, 14, 13, 12, 11, 10, 9,  8,
            23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24},
           {15, 14, 13, 12, 11, 10, 9,  8,  7,  6,  5,  4,  3,  2,  1,  0,
            31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16},
           {4,  5,  6,  7,  0,  1,  2,  3,  12, 13, 14, 15, 8,  9,  10, 11,
            20, 21, 22, 23, 16, 17, 18, 19, 28, 29, 30, 31, 24, 25, 26, 27},
           {31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16,
            15, 14, 13, 12, 11, 10, 9,  8,  7,  6,  5,  4,  3,  2,  1,  0},
           {8,  9,  10, 11, 12, 13, 14, 15, 0,  1,  2,  3,  4,  5,  6,  7,
            24, 25, 26, 27, 28, 29, 30, 31, 16, 17, 18, 19, 20, 21, 22, 23},
           {16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15}};

/*
 * Assumes zmm is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_zmm_16bit(reg_t zmm)
{
    // Level 1
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(2, 3, 0, 1)>(zmm),
            0xAAAAAAAA);
    // Level 2
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(0, 1, 2, 3)>(zmm),
            0xCCCCCCCC);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(2, 3, 0, 1)>(zmm),
            0xAAAAAAAA);
    // Level 3
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(1), zmm), 0xF0F0F0F0);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(1, 0, 3, 2)>(zmm),
            0xCCCCCCCC);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(2, 3, 0, 1)>(zmm),
            0xAAAAAAAA);
    // Level 4
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(2), zmm), 0xFF00FF00);
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(3), zmm), 0xF0F0F0F0);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(1, 0, 3, 2)>(zmm),
            0xCCCCCCCC);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(2, 3, 0, 1)>(zmm),
            0xAAAAAAAA);
    // Level 5
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(4), zmm), 0xFFFF0000);
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(5), zmm), 0xFF00FF00);
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::get_network(3), zmm), 0xF0F0F0F0);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(1, 0, 3, 2)>(zmm),
            0xCCCCCCCC);
    zmm = cmp_merge<vtype>(
            zmm,
            vtype::template shuffle<SHUFFLE_MASK(2, 3, 0, 1)>(zmm),
            0xAAAAAAAA);
    return zmm;
}

struct avx512_16bit_swizzle_ops {
    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t swap_n(typename vtype::reg_t reg)
    {
        __m512i v = vtype::cast_to(reg);

        if constexpr (scale == 2) {
            std::vector<uint16_t> arr
                    = {1,  0,  3,  2,  5,  4,  7,  6,  9,  8,  11,
                       10, 13, 12, 15, 14, 17, 16, 19, 18, 21, 20,
                       23, 22, 25, 24, 27, 26, 29, 28, 31, 30};
            __m512i mask = _mm512_loadu_si512(arr.data());
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
            std::vector<uint16_t> arr
                    = {3,  2,  1,  0,  7,  6,  5,  4,  11, 10, 9,
                       8,  15, 14, 13, 12, 19, 18, 17, 16, 23, 22,
                       21, 20, 27, 26, 25, 24, 31, 30, 29, 28};
            __m512i mask = _mm512_loadu_si512(arr.data());
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 8) {
            std::vector<uint16_t> arr
                    = {7,  6,  5,  4,  3,  2,  1,  0,  15, 14, 13,
                       12, 11, 10, 9,  8,  23, 22, 21, 20, 19, 18,
                       17, 16, 31, 30, 29, 28, 27, 26, 25, 24};
            __m512i mask = _mm512_loadu_si512(arr.data());
            v = _mm512_permutexvar_epi16(mask, v);
        }
        else if constexpr (scale == 16) {
            std::vector<uint16_t> arr
                    = {15, 14, 13, 12, 11, 10, 9,  8,  7,  6,  5,
                       4,  3,  2,  1,  0,  31, 30, 29, 28, 27, 26,
                       25, 24, 23, 22, 21, 20, 19, 18, 17, 16};
            __m512i mask = _mm512_loadu_si512(arr.data());
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
