/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_64BIT_COMMON
#define AVX512_64BIT_COMMON

#include "avx2-32bit-qsort.hpp"

/*
 * Constants used in sorting 8 elements in a ZMM registers. Based on Bitonic
 * sorting network (see
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg)
 */
// ZMM                  7, 6, 5, 4, 3, 2, 1, 0
#define NETWORK_64BIT_1 4, 5, 6, 7, 0, 1, 2, 3
#define NETWORK_64BIT_2 0, 1, 2, 3, 4, 5, 6, 7
#define NETWORK_64BIT_3 5, 4, 7, 6, 1, 0, 3, 2
#define NETWORK_64BIT_4 3, 2, 1, 0, 7, 6, 5, 4

template <typename vtype, typename reg_t>
X86_SIMD_SORT_INLINE reg_t sort_zmm_64bit(reg_t zmm);

struct avx512_64bit_swizzle_ops;
struct avx512_ymm_64bit_swizzle_ops;

template <>
struct ymm_vector<float> {
    using type_t = float;
    using reg_t = __m256;
    using regi_t = __m256i;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_ymm_64bit_swizzle_ops;

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
        return _mm256_set1_ps(type_max());
    }
    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm256_set_ps(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static opmask_t kxor_opmask(opmask_t x, opmask_t y)
    {
        return _kxor_mask8(x, y);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t le(reg_t x, reg_t y)
    {
        return _mm256_cmp_ps_mask(x, y, _CMP_LE_OQ);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm256_cmp_ps_mask(x, y, _CMP_GE_OQ);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm256_cmp_ps_mask(x, y, _CMP_EQ_OQ);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static int32_t convert_mask_to_int(opmask_t mask)
    {
        return mask;
    }
    template <int type>
    static opmask_t fpclass(reg_t x)
    {
        return _mm256_fpclass_ps_mask(x, type);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_ps(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mmask_i32gather_ps(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm256_loadu_ps((float *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm256_max_ps(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_compressstoreu_ps(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm256_maskz_loadu_ps(mask, mem);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm256_mask_loadu_ps(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm256_mask_mov_ps(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_storeu_ps(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm256_min_ps(x, y);
    }
    static reg_t permutexvar(__m256i idx, reg_t zmm)
    {
        return _mm256_permutexvar_ps(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        __m128 v128 = _mm_max_ps(_mm256_castps256_ps128(v),
                                 _mm256_extractf32x4_ps(v, 1));
        __m128 v64 = _mm_max_ps(
                v128, _mm_shuffle_ps(v128, v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128 v32 = _mm_max_ps(
                v64, _mm_shuffle_ps(v64, v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return _mm_cvtss_f32(v32);
    }
    static type_t reducemin(reg_t v)
    {
        __m128 v128 = _mm_min_ps(_mm256_castps256_ps128(v),
                                 _mm256_extractf32x4_ps(v, 1));
        __m128 v64 = _mm_min_ps(
                v128, _mm_shuffle_ps(v128, v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128 v32 = _mm_min_ps(
                v64, _mm_shuffle_ps(v64, v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return _mm_cvtss_f32(v32);
    }
    static reg_t set1(type_t v)
    {
        return _mm256_set1_ps(v);
    }
    template <uint8_t mask, bool = (mask == 0b01010101)>
    static reg_t shuffle(reg_t zmm)
    {
        /* Hack!: have to make shuffles within 128-bit lanes work for both
         * 32-bit and 64-bit */
        return _mm256_shuffle_ps(zmm, zmm, 0b10110001);
        //if constexpr (mask == 0b01010101) {
        //}
        //else {
        //    /* Not used, so far */
        //    return _mm256_shuffle_ps(zmm, zmm, mask);
        //}
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<ymm_vector<type_t>>(x);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm256_storeu_ps((float *)mem, x);
    }
    static reg_t cast_from(__m256i v)
    {
        return _mm256_castsi256_ps(v);
    }
    static __m256i cast_to(reg_t v)
    {
        return _mm256_castps_si256(v);
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m256i rev_index = _mm256_set_epi32(NETWORK_32BIT_AVX2_2);
        return permutexvar(rev_index, ymm);
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<ymm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct ymm_vector<uint32_t> {
    using type_t = uint32_t;
    using reg_t = __m256i;
    using regi_t = __m256i;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_ymm_64bit_swizzle_ops;

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
        return _mm256_set1_epi32(type_max());
    }

    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static opmask_t kxor_opmask(opmask_t x, opmask_t y)
    {
        return _kxor_mask8(x, y);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t le(reg_t x, reg_t y)
    {
        return _mm256_cmp_epu32_mask(x, y, _MM_CMPINT_LE);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm256_cmp_epu32_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm256_cmp_epu32_mask(x, y, _MM_CMPINT_EQ);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_epi32(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mmask_i32gather_epi32(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm256_loadu_si256((__m256i *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm256_max_epu32(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_compressstoreu_epi32(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm256_maskz_loadu_epi32(mask, mem);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm256_mask_loadu_epi32(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm256_mask_mov_epi32(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_storeu_epi32(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm256_min_epu32(x, y);
    }
    static reg_t permutexvar(__m256i idx, reg_t zmm)
    {
        return _mm256_permutexvar_epi32(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        __m128i v128 = _mm_max_epu32(_mm256_castsi256_si128(v),
                                     _mm256_extracti128_si256(v, 1));
        __m128i v64 = _mm_max_epu32(
                v128, _mm_shuffle_epi32(v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128i v32 = _mm_max_epu32(
                v64, _mm_shuffle_epi32(v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return (type_t)_mm_cvtsi128_si32(v32);
    }
    static type_t reducemin(reg_t v)
    {
        __m128i v128 = _mm_min_epu32(_mm256_castsi256_si128(v),
                                     _mm256_extracti128_si256(v, 1));
        __m128i v64 = _mm_min_epu32(
                v128, _mm_shuffle_epi32(v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128i v32 = _mm_min_epu32(
                v64, _mm_shuffle_epi32(v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return (type_t)_mm_cvtsi128_si32(v32);
    }
    static reg_t set1(type_t v)
    {
        return _mm256_set1_epi32(v);
    }
    template <uint8_t mask, bool = (mask == 0b01010101)>
    static reg_t shuffle(reg_t zmm)
    {
        /* Hack!: have to make shuffles within 128-bit lanes work for both
         * 32-bit and 64-bit */
        return _mm256_shuffle_epi32(zmm, 0b10110001);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<ymm_vector<type_t>>(x);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm256_storeu_si256((__m256i *)mem, x);
    }
    static reg_t cast_from(__m256i v)
    {
        return v;
    }
    static __m256i cast_to(reg_t v)
    {
        return v;
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m256i rev_index = _mm256_set_epi32(NETWORK_32BIT_AVX2_2);
        return permutexvar(rev_index, ymm);
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<ymm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct ymm_vector<int32_t> {
    using type_t = int32_t;
    using reg_t = __m256i;
    using regi_t = __m256i;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_ymm_64bit_swizzle_ops;

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
        return _mm256_set1_epi32(type_max());
    } // TODO: this should broadcast bits as is?

    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm256_set_epi32(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static opmask_t kxor_opmask(opmask_t x, opmask_t y)
    {
        return _kxor_mask8(x, y);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t le(reg_t x, reg_t y)
    {
        return _mm256_cmp_epi32_mask(x, y, _MM_CMPINT_LE);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm256_cmp_epi32_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm256_cmp_epi32_mask(x, y, _MM_CMPINT_EQ);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_epi32(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm256_mmask_i32gather_epi32(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm256_loadu_si256((__m256i *)mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm256_max_epi32(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_compressstoreu_epi32(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm256_maskz_loadu_epi32(mask, mem);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm256_mask_loadu_epi32(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm256_mask_mov_epi32(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm256_mask_storeu_epi32(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm256_min_epi32(x, y);
    }
    static reg_t permutexvar(__m256i idx, reg_t zmm)
    {
        return _mm256_permutexvar_epi32(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        __m128i v128 = _mm_max_epi32(_mm256_castsi256_si128(v),
                                     _mm256_extracti128_si256(v, 1));
        __m128i v64 = _mm_max_epi32(
                v128, _mm_shuffle_epi32(v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128i v32 = _mm_max_epi32(
                v64, _mm_shuffle_epi32(v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return (type_t)_mm_cvtsi128_si32(v32);
    }
    static type_t reducemin(reg_t v)
    {
        __m128i v128 = _mm_min_epi32(_mm256_castsi256_si128(v),
                                     _mm256_extracti128_si256(v, 1));
        __m128i v64 = _mm_min_epi32(
                v128, _mm_shuffle_epi32(v128, _MM_SHUFFLE(1, 0, 3, 2)));
        __m128i v32 = _mm_min_epi32(
                v64, _mm_shuffle_epi32(v64, _MM_SHUFFLE(0, 0, 0, 1)));
        return (type_t)_mm_cvtsi128_si32(v32);
    }
    static reg_t set1(type_t v)
    {
        return _mm256_set1_epi32(v);
    }
    template <uint8_t mask, bool = (mask == 0b01010101)>
    static reg_t shuffle(reg_t zmm)
    {
        /* Hack!: have to make shuffles within 128-bit lanes work for both
         * 32-bit and 64-bit */
        return _mm256_shuffle_epi32(zmm, 0b10110001);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<ymm_vector<type_t>>(x);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm256_storeu_si256((__m256i *)mem, x);
    }
    static reg_t cast_from(__m256i v)
    {
        return v;
    }
    static __m256i cast_to(reg_t v)
    {
        return v;
    }
    static reg_t reverse(reg_t ymm)
    {
        const __m256i rev_index = _mm256_set_epi32(NETWORK_32BIT_AVX2_2);
        return permutexvar(rev_index, ymm);
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<ymm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct zmm_vector<int64_t> {
    using type_t = int64_t;
    using reg_t = __m512i;
    using regi_t = __m512i;
    using halfreg_t = __m512i;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 256;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_64bit_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_INT64;
    }
    static type_t type_min()
    {
        return X86_SIMD_SORT_MIN_INT64;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_epi64(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_epi64(type_min());
    }

    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm512_set_epi64(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm512_set_epi64(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static opmask_t kxor_opmask(opmask_t x, opmask_t y)
    {
        return _kxor_mask8(x, y);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t le(reg_t x, reg_t y)
    {
        return _mm512_cmp_epi64_mask(x, y, _MM_CMPINT_LE);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_epi64_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmp_epi64_mask(x, y, _MM_CMPINT_EQ);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_epi64(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm512_mask_i32gather_epi64(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_si512(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_epi64(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_compressstoreu_epi64(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm512_maskz_loadu_epi64(mask, mem);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm512_mask_loadu_epi64(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_epi64(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi64(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_epi64(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_epi64(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        return _mm512_reduce_max_epi64(v);
    }
    static type_t reducemin(reg_t v)
    {
        return _mm512_reduce_min_epi64(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_epi64(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        __m512d temp = _mm512_castsi512_pd(zmm);
        return _mm512_castpd_si512(
                _mm512_shuffle_pd(temp, temp, (_MM_PERM_ENUM)mask));
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm512_storeu_si512(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const regi_t rev_index = seti(NETWORK_64BIT_2);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<zmm_vector<type_t>>(x);
    }
    static reg_t cast_from(__m512i v)
    {
        return v;
    }
    static __m512i cast_to(reg_t v)
    {
        return v;
    }
    static bool all_false(opmask_t k)
    {
        return k == 0;
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<zmm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};
template <>
struct zmm_vector<uint64_t> {
    using type_t = uint64_t;
    using reg_t = __m512i;
    using regi_t = __m512i;
    using halfreg_t = __m512i;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 256;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_64bit_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_UINT64;
    }
    static type_t type_min()
    {
        return 0;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_epi64(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_epi64(type_min());
    }

    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm512_set_epi64(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm512_set_epi64(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_epi64(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm512_mask_i32gather_epi64(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_epu64_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmp_epu64_mask(x, y, _MM_CMPINT_EQ);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_si512(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_epu64(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_compressstoreu_epi64(mem, mask, x);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm512_maskz_loadu_epi64(mask, mem);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm512_mask_loadu_epi64(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_epi64(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi64(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_epu64(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_epi64(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        return _mm512_reduce_max_epu64(v);
    }
    static type_t reducemin(reg_t v)
    {
        return _mm512_reduce_min_epu64(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_epi64(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        __m512d temp = _mm512_castsi512_pd(zmm);
        return _mm512_castpd_si512(
                _mm512_shuffle_pd(temp, temp, (_MM_PERM_ENUM)mask));
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm512_storeu_si512(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const regi_t rev_index = seti(NETWORK_64BIT_2);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<zmm_vector<type_t>>(x);
    }
    static reg_t cast_from(__m512i v)
    {
        return v;
    }
    static __m512i cast_to(reg_t v)
    {
        return v;
    }
    static bool all_false(opmask_t k)
    {
        return k == 0;
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<zmm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};

/*
 * workaround on 64-bit macOS and OpenBSD which both define size_t as unsigned
 * long and define uint64_t as unsigned long long, both of which are 8 bytes
 */
#if (defined(__APPLE__) || defined(__OpenBSD__)) && defined(__x86_64__)
static_assert(sizeof(size_t) == sizeof(uint64_t),
              "Size of size_t and uint64_t are not the same");
template <>
struct zmm_vector<size_t> : public zmm_vector<uint64_t> {
};
#endif

template <>
struct zmm_vector<double> {
    using type_t = double;
    using reg_t = __m512d;
    using regi_t = __m512i;
    using halfreg_t = __m512d;
    using opmask_t = __mmask8;
    static const uint8_t numlanes = 8;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 256;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_64bit_swizzle_ops;

    static type_t type_max()
    {
        return X86_SIMD_SORT_INFINITY;
    }
    static type_t type_min()
    {
        return -X86_SIMD_SORT_INFINITY;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_pd(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_pd(type_min());
    }
    static regi_t
    seti(int v1, int v2, int v3, int v4, int v5, int v6, int v7, int v8)
    {
        return _mm512_set_epi64(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t set(type_t v1,
                     type_t v2,
                     type_t v3,
                     type_t v4,
                     type_t v5,
                     type_t v6,
                     type_t v7,
                     type_t v8)
    {
        return _mm512_set_pd(v1, v2, v3, v4, v5, v6, v7, v8);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm512_maskz_loadu_pd(mask, mem);
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask8(x);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_pd_mask(x, y, _CMP_GE_OQ);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmp_pd_mask(x, y, _CMP_EQ_OQ);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static int32_t convert_mask_to_int(opmask_t mask)
    {
        return mask;
    }
    template <int type>
    static opmask_t fpclass(reg_t x)
    {
        return _mm512_fpclass_pd_mask(x, type);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m512i index, void const *base)
    {
        return _mm512_mask_i64gather_pd(src, mask, index, base, scale);
    }
    template <int scale>
    static reg_t
    mask_i64gather(reg_t src, opmask_t mask, __m256i index, void const *base)
    {
        return _mm512_mask_i32gather_pd(src, mask, index, base, scale);
    }
    static reg_t i64gather(type_t *arr, arrsize_t *ind)
    {
        return set(arr[ind[7]],
                   arr[ind[6]],
                   arr[ind[5]],
                   arr[ind[4]],
                   arr[ind[3]],
                   arr[ind[2]],
                   arr[ind[1]],
                   arr[ind[0]]);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_pd(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_pd(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_compressstoreu_pd(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm512_mask_loadu_pd(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_pd(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_pd(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_pd(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_pd(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        return _mm512_reduce_max_pd(v);
    }
    static type_t reducemin(reg_t v)
    {
        return _mm512_reduce_min_pd(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_pd(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        return _mm512_shuffle_pd(zmm, zmm, (_MM_PERM_ENUM)mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        _mm512_storeu_pd(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const regi_t rev_index = seti(NETWORK_64BIT_2);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_64bit<zmm_vector<type_t>>(x);
    }
    static reg_t cast_from(__m512i v)
    {
        return _mm512_castsi512_pd(v);
    }
    static __m512i cast_to(reg_t v)
    {
        return _mm512_castpd_si512(v);
    }
    static bool all_false(opmask_t k)
    {
        return k == 0;
    }
    static int double_compressstore(type_t *left_addr,
                                    type_t *right_addr,
                                    opmask_t k,
                                    reg_t reg)
    {
        return avx512_double_compressstore<zmm_vector<type_t>>(
                left_addr, right_addr, k, reg);
    }
};

/*
 * Assumes zmm is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_zmm_64bit(reg_t zmm)
{
    const typename vtype::regi_t rev_index = vtype::seti(NETWORK_64BIT_2);
    zmm = cmp_merge<vtype>(
            zmm, vtype::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(zmm), 0xAA);
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::seti(NETWORK_64BIT_1), zmm), 0xCC);
    zmm = cmp_merge<vtype>(
            zmm, vtype::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(zmm), 0xAA);
    zmm = cmp_merge<vtype>(zmm, vtype::permutexvar(rev_index, zmm), 0xF0);
    zmm = cmp_merge<vtype>(
            zmm, vtype::permutexvar(vtype::seti(NETWORK_64BIT_3), zmm), 0xCC);
    zmm = cmp_merge<vtype>(
            zmm, vtype::template shuffle<SHUFFLE_MASK(1, 1, 1, 1)>(zmm), 0xAA);
    return zmm;
}

struct avx512_64bit_swizzle_ops {
    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t swap_n(typename vtype::reg_t reg)
    {
        __m512i v = vtype::cast_to(reg);

        if constexpr (scale == 2) {
            v = _mm512_shuffle_epi32(v, (_MM_PERM_ENUM)0b01001110);
        }
        else if constexpr (scale == 4) {
            v = _mm512_shuffle_i64x2(v, v, 0b10110001);
        }
        else if constexpr (scale == 8) {
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
            constexpr uint64_t mask = 0b00011011;
            v = _mm512_permutex_epi64(v, mask);
        }
        else if constexpr (scale == 8) {
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
            v1 = _mm512_mask_blend_epi64(0b01010101, v1, v2);
        }
        else if constexpr (scale == 4) {
            v1 = _mm512_mask_blend_epi64(0b00110011, v1, v2);
        }
        else if constexpr (scale == 8) {
            v1 = _mm512_mask_blend_epi64(0b00001111, v1, v2);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v1);
    }
};

struct avx512_ymm_64bit_swizzle_ops {
    template <typename vtype, int scale>
    X86_SIMD_SORT_INLINE typename vtype::reg_t swap_n(typename vtype::reg_t reg)
    {
        __m256i v = vtype::cast_to(reg);

        if constexpr (scale == 2) {
            __m256 vf = _mm256_castsi256_ps(v);
            vf = _mm256_permute_ps(vf, 0b10110001);
            v = _mm256_castps_si256(vf);
        }
        else if constexpr (scale == 4) {
            __m256 vf = _mm256_castsi256_ps(v);
            vf = _mm256_permute_ps(vf, 0b01001110);
            v = _mm256_castps_si256(vf);
        }
        else if constexpr (scale == 8) {
            v = _mm256_permute2x128_si256(v, v, 0b00000001);
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
        __m256i v = vtype::cast_to(reg);

        if constexpr (scale == 2) { return swap_n<vtype, 2>(reg); }
        else if constexpr (scale == 4) {
            constexpr uint64_t mask = 0b00011011;
            __m256 vf = _mm256_castsi256_ps(v);
            vf = _mm256_permute_ps(vf, mask);
            v = _mm256_castps_si256(vf);
        }
        else if constexpr (scale == 8) {
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
        __m256i v1 = vtype::cast_to(reg);
        __m256i v2 = vtype::cast_to(other);

        if constexpr (scale == 2) {
            v1 = _mm256_blend_epi32(v1, v2, 0b01010101);
        }
        else if constexpr (scale == 4) {
            v1 = _mm256_blend_epi32(v1, v2, 0b00110011);
        }
        else if constexpr (scale == 8) {
            v1 = _mm256_blend_epi32(v1, v2, 0b00001111);
        }
        else {
            static_assert(scale == -1, "should not be reached");
        }

        return vtype::cast_from(v1);
    }
};

#endif
