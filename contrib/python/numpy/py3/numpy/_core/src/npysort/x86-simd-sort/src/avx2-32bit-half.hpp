/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX2_HALF_32BIT
#define AVX2_HALF_32BIT

#include "avx2-emu-funcs.hpp"

/*
 * Constants used in sorting 8 elements in a ymm registers. Based on Bitonic
 * sorting network (see
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg)
 */

// ymm                  7, 6, 5, 4, 3, 2, 1, 0
#define NETWORK_32BIT_AVX2_1 4, 5, 6, 7, 0, 1, 2, 3
#define NETWORK_32BIT_AVX2_2 0, 1, 2, 3, 4, 5, 6, 7
#define NETWORK_32BIT_AVX2_3 5, 4, 7, 6, 1, 0, 3, 2
#define NETWORK_32BIT_AVX2_4 3, 2, 1, 0, 7, 6, 5, 4

/*
 * Assumes ymm is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_ymm_32bit_half(reg_t ymm)
{
    using swizzle = typename vtype::swizzle_ops;

    const typename vtype::opmask_t oxAA = vtype::seti(-1, 0, -1, 0);
    const typename vtype::opmask_t oxCC = vtype::seti(-1, -1, 0, 0);

    ymm = cmp_merge<vtype>(ymm, swizzle::template swap_n<vtype, 2>(ymm), oxAA);
    ymm = cmp_merge<vtype>(ymm, vtype::reverse(ymm), oxCC);
    ymm = cmp_merge<vtype>(ymm, swizzle::template swap_n<vtype, 2>(ymm), oxAA);
    return ymm;
}

struct avx2_32bit_half_swizzle_ops;

template <>
struct avx2_half_vector<int32_t> {
    using type_t = int32_t;
    using reg_t = __m128i;
    using regi_t = __m128i;
    using opmask_t = __m128i;
    static const uint8_t numlanes = 4;
    static constexpr simd_type vec_type = simd_type::AVX2;

    using swizzle_ops = avx2_32bit_half_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_INT32;
    }
    static type_t type_min()
    {
        return X86_SIMD_SORT_MIN_INT32;
    }
    static reg_t zmm_max()
    {
        return _mm_set1_epi32(type_max());
    } // TODO: this should broadcast bits as is?
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        auto mask = ((0x1ull << num_to_read) - 0x1ull);
        return convert_int_to_avx2_mask_half(mask);
    }
    static regi_t seti(int v1, int v2, int v3, int v4)
    {
        return _mm_set_epi32(v1, v2, v3, v4);
    }
    static reg_t set(int v1, int v2, int v3, int v4)
    {
        return _mm_set_epi32(v1, v2, v3, v4);
    }
    static opmask_t kxor_opmask(opmask_t x, opmask_t y)
    {
        return _mm_xor_si128(x, y);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        opmask_t equal = eq(x, y);
        opmask_t greater = _mm_cmpgt_epi32(x, y);
        return _mm_or_si128(equal, greater);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm_cmpeq_epi32(x, y);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mask_i64gather_epi32(
                src, (const int *)base, index, mask, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m128i index, void const *base)
    {
        return _mm_mask_i32gather_epi32(
                src, (const int *)base, index, mask, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[3]], arr[ind[2]], arr[ind[1]], arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm_loadu_si128((reg_t const *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm_max_epi32(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return avx2_emu_mask_compressstoreu32_half<type_t>(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm_maskload_epi32((const int *)mem, mask);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        reg_t dst = _mm_maskload_epi32((type_t *)mem, mask);
        return mask_mov(x, mask, dst);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm_castps_si128(_mm_blendv_ps(_mm_castsi128_ps(x),
                                              _mm_castsi128_ps(y),
                                              _mm_castsi128_ps(mask)));
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm_maskstore_epi32((type_t *)mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm_min_epi32(x, y);
    }
    static reg_t permutexvar(__m128i idx, reg_t ymm)
    {
        return _mm_castps_si128(_mm_permutevar_ps(_mm_castsi128_ps(ymm), idx));
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m128i rev_index = _mm_set_epi32(0, 1, 2, 3);
        return permutexvar(rev_index, ymm);
    }
    static type_t reducemax(reg_t v)
    {
        return avx2_emu_reduce_max32_half<type_t>(v);
    }
    static type_t reducemin(reg_t v)
    {
        return avx2_emu_reduce_min32_half<type_t>(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm_set1_epi32(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t ymm)
    {
        return _mm_shuffle_epi32(ymm, mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm_storeu_si128((__m128i *)mem, x);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_ymm_32bit_half<avx2_half_vector<type_t>>(x);
    }
    static reg_t cast_from(__m128i v)
    {
        return v;
    }
    static __m128i cast_to(reg_t v)
    {
        return v;
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx2_double_compressstore32_half<type_t>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct avx2_half_vector<uint32_t> {
    using type_t = uint32_t;
    using reg_t = __m128i;
    using regi_t = __m128i;
    using opmask_t = __m128i;
    static const uint8_t numlanes = 4;
    static constexpr simd_type vec_type = simd_type::AVX2;

    using swizzle_ops = avx2_32bit_half_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_UINT32;
    }
    static type_t type_min()
    {
        return 0;
    }
    static reg_t zmm_max()
    {
        return _mm_set1_epi32(type_max());
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        auto mask = ((0x1ull << num_to_read) - 0x1ull);
        return convert_int_to_avx2_mask_half(mask);
    }
    static regi_t seti(int v1, int v2, int v3, int v4)
    {
        return _mm_set_epi32(v1, v2, v3, v4);
    }
    static reg_t set(int v1, int v2, int v3, int v4)
    {
        return _mm_set_epi32(v1, v2, v3, v4);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mask_i64gather_epi32(
                src, (const int *)base, index, mask, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m128i index, void const *base)
    {
        return _mm_mask_i32gather_epi32(
                src, (const int *)base, index, mask, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[3]], arr[ind[2]], arr[ind[1]], arr[ind[0]]);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        reg_t maxi = max(x, y);
        return eq(maxi, x);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm_cmpeq_epi32(x, y);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm_loadu_si128((reg_t const *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm_max_epu32(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return avx2_emu_mask_compressstoreu32_half<type_t>(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        reg_t dst = _mm_maskload_epi32((const int *)mem, mask);
        return mask_mov(x, mask, dst);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm_castps_si128(_mm_blendv_ps(_mm_castsi128_ps(x),
                                              _mm_castsi128_ps(y),
                                              _mm_castsi128_ps(mask)));
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm_maskstore_epi32((int *)mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm_min_epu32(x, y);
    }
    static reg_t permutexvar(__m128i idx, reg_t ymm)
    {
        return _mm_castps_si128(_mm_permutevar_ps(_mm_castsi128_ps(ymm), idx));
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m128i rev_index = _mm_set_epi32(0, 1, 2, 3);
        return permutexvar(rev_index, ymm);
    }
    static type_t reducemax(reg_t v)
    {
        return avx2_emu_reduce_max32_half<type_t>(v);
    }
    static type_t reducemin(reg_t v)
    {
        return avx2_emu_reduce_min32_half<type_t>(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm_set1_epi32(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t ymm)
    {
        return _mm_shuffle_epi32(ymm, mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm_storeu_si128((__m128i *)mem, x);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_ymm_32bit_half<avx2_half_vector<type_t>>(x);
    }
    static reg_t cast_from(__m128i v)
    {
        return v;
    }
    static __m128i cast_to(reg_t v)
    {
        return v;
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx2_double_compressstore32_half<type_t>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct avx2_half_vector<float> {
    using type_t = float;
    using reg_t = __m128;
    using regi_t = __m128i;
    using opmask_t = __m128i;
    static const uint8_t numlanes = 4;
    static constexpr simd_type vec_type = simd_type::AVX2;

    using swizzle_ops = avx2_32bit_half_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_INFINITYF;
    }
    static type_t type_min()
    {
        return -X86_SIMD_SORT_INFINITYF;
    }
    static reg_t zmm_max()
    {
        return _mm_set1_ps(type_max());
    }

    static regi_t seti(int v1, int v2, int v3, int v4)
    {
        return _mm_set_epi32(v1, v2, v3, v4);
    }
    static reg_t set(float v1, float v2, float v3, float v4)
    {
        return _mm_set_ps(v1, v2, v3, v4);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm_maskload_ps((const float *)mem, mask);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm_castps_si128(_mm_cmp_ps(x, y, _CMP_GE_OQ));
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm_castps_si128(_mm_cmp_ps(x, y, _CMP_EQ_OQ));
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        auto mask = ((0x1ull << num_to_read) - 0x1ull);
        return convert_int_to_avx2_mask_half(mask);
    }
    static int32_t convert_mask_to_int(opmask_t mask)
    {
        return convert_avx2_mask_to_int_half(mask);
    }
    template <int type>
    static opmask_t fpclass(reg_t x)
    {
        if constexpr (type == (0x01 | 0x80)) {
            return _mm_castps_si128(_mm_cmp_ps(x, x, _CMP_UNORD_Q));
        }
        else {
            static_assert(type == (0x01 | 0x80), "should not reach here");
        }
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mask_i64gather_ps(
                src, (const float *)base, index, _mm_castsi128_ps(mask), scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m128i index, void const *base)
    {
        return _mm_mask_i32gather_ps(
                src, (const float *)base, index, _mm_castsi128_ps(mask), scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[3]], arr[ind[2]], arr[ind[1]], arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm_loadu_ps((float const *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm_max_ps(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return avx2_emu_mask_compressstoreu32_half<type_t>(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        reg_t dst = _mm_maskload_ps((type_t *)mem, mask);
        return mask_mov(x, mask, dst);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm_blendv_ps(x, y, _mm_castsi128_ps(mask));
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm_maskstore_ps((type_t *)mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm_min_ps(x, y);
    }
    static reg_t permutexvar(__m128i idx, reg_t ymm)
    {
        return _mm_permutevar_ps(ymm, idx);
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m128i rev_index = _mm_set_epi32(0, 1, 2, 3);
        return permutexvar(rev_index, ymm);
    }
    static type_t reducemax(reg_t v)
    {
        return avx2_emu_reduce_max32_half<type_t>(v);
    }
    static type_t reducemin(reg_t v)
    {
        return avx2_emu_reduce_min32_half<type_t>(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm_set1_ps(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t ymm)
    {
        return _mm_castsi128_ps(_mm_shuffle_epi32(_mm_castps_si128(ymm), mask));
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm_storeu_ps((float *)mem, x);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_ymm_32bit_half<avx2_half_vector<type_t>>(x);
    }
    static reg_t cast_from(__m128i v)
    {
        return _mm_castsi128_ps(v);
    }
    static __m128i cast_to(reg_t v)
    {
        return _mm_castps_si128(v);
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx2_double_compressstore32_half<type_t>(
                left_addr, right_addr, k, reg);
    }
};

struct avx2_32bit_half_swizzle_ops {
    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t swap_n(typename vtype::reg_t reg)
    {
        if constexpr (scale == 2) {
            return vtype::template shuffle<0b10110001>(reg);
        }
        else if constexpr (scale == 4) {
            return vtype::template shuffle<0b01001110>(reg);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }
    }

    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t
    reverse_n(typename vtype::reg_t reg)
    {
        __m128i v = vtype::cast_to(reg);

        if constexpr (scale == 2) { return swap_n<vtype, 2>(reg); }
        else if constexpr (scale == 4) {
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
        __m128i v1 = vtype::cast_to(reg);
        __m128i v2 = vtype::cast_to(other);

        if constexpr (scale == 2) { v1 = _mm_blend_epi32(v1, v2, 0b0101); }
        else if constexpr (scale == 4) {
            v1 = _mm_blend_epi32(v1, v2, 0b0011);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v1);
    }
};

#endif // AVX2_HALF_32BIT
