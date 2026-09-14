/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_QSORT_16BIT
#define AVX512_QSORT_16BIT

#include "avx512-16bit-common.h"

struct float16 {
    uint16_t val;
};

template <>
struct zmm_vector<float16> {
    using type_t = uint16_t;
    using reg_t = __m512i;
    using halfreg_t = __m256i;
    using opmask_t = __mmask32;
    static const uint8_t numlanes = 32;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 512;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_16bit_swizzle_ops;

    static reg_t get_network(int index)
    {
        return _mm512_loadu_si512(&network[index - 1][0]);
    }
    static type_t type_max()
    {
        return X86_SIMD_SORT_INFINITYH;
    }
    static type_t type_min()
    {
        return X86_SIMD_SORT_NEGINFINITYH;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_epi16(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_epi16(type_min());
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask32(x);
    }

    static opmask_t ge(reg_t x, reg_t y)
    {
        reg_t sign_x = _mm512_and_si512(x, _mm512_set1_epi16(0x8000));
        reg_t sign_y = _mm512_and_si512(y, _mm512_set1_epi16(0x8000));
        reg_t exp_x = _mm512_and_si512(x, _mm512_set1_epi16(0x7c00));
        reg_t exp_y = _mm512_and_si512(y, _mm512_set1_epi16(0x7c00));
        reg_t mant_x = _mm512_and_si512(x, _mm512_set1_epi16(0x3ff));
        reg_t mant_y = _mm512_and_si512(y, _mm512_set1_epi16(0x3ff));

        __mmask32 mask_ge = _mm512_cmp_epu16_mask(
                sign_x, sign_y, _MM_CMPINT_LT); // only greater than
        __mmask32 sign_eq = _mm512_cmpeq_epu16_mask(sign_x, sign_y);
        __mmask32 neg = _mm512_mask_cmpeq_epu16_mask(
                sign_eq,
                sign_x,
                _mm512_set1_epi16(0x8000)); // both numbers are -ve

        // compare exponents only if signs are equal:
        mask_ge = mask_ge
                | _mm512_mask_cmp_epu16_mask(
                          sign_eq, exp_x, exp_y, _MM_CMPINT_NLE);
        // get mask for elements for which both sign and exponents are equal:
        __mmask32 exp_eq = _mm512_mask_cmpeq_epu16_mask(sign_eq, exp_x, exp_y);

        // compare mantissa for elements for which both sign and expponent are equal:
        mask_ge = mask_ge
                | _mm512_mask_cmp_epu16_mask(
                          exp_eq, mant_x, mant_y, _MM_CMPINT_NLT);
        return _kxor_mask32(mask_ge, neg);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmpeq_epu16_mask(x, y);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static int32_t convert_mask_to_int(opmask_t mask)
    {
        return mask;
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_si512(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_mask_mov_epi16(y, ge(x, y), x);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        // AVX512_VBMI2
        return _mm512_mask_compressstoreu_epi16(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        // AVX512BW
        return _mm512_mask_loadu_epi16(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_epi16(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi16(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_mask_mov_epi16(x, ge(x, y), y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_epi16(idx, zmm);
    }
    // Apparently this is a terrible for perf, npy_half_to_float seems to work
    // better
    //static float uint16_to_float(uint16_t val)
    //{
    //    // Ideally use _mm_loadu_si16, but its only gcc > 11.x
    //    // TODO: use inline ASM? https://godbolt.org/z/aGYvh7fMM
    //    __m128i xmm = _mm_maskz_loadu_epi16(0x01, &val);
    //    __m128 xmm2 = _mm_cvtph_ps(xmm);
    //    return _mm_cvtss_f32(xmm2);
    //}
    static type_t float_to_uint16(float val)
    {
        __m128 xmm = _mm_load_ss(&val);
        __m128i xmm2 = _mm_cvtps_ph(xmm, _MM_FROUND_NO_EXC);
        return _mm_extract_epi16(xmm2, 0);
    }
    static type_t reducemax(reg_t v)
    {
        __m512 lo = _mm512_cvtph_ps(_mm512_extracti64x4_epi64(v, 0));
        __m512 hi = _mm512_cvtph_ps(_mm512_extracti64x4_epi64(v, 1));
        float lo_max = _mm512_reduce_max_ps(lo);
        float hi_max = _mm512_reduce_max_ps(hi);
        return float_to_uint16(std::max(lo_max, hi_max));
    }
    static type_t reducemin(reg_t v)
    {
        __m512 lo = _mm512_cvtph_ps(_mm512_extracti64x4_epi64(v, 0));
        __m512 hi = _mm512_cvtph_ps(_mm512_extracti64x4_epi64(v, 1));
        float lo_max = _mm512_reduce_min_ps(lo);
        float hi_max = _mm512_reduce_min_ps(hi);
        return float_to_uint16(std::min(lo_max, hi_max));
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_epi16(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        zmm = _mm512_shufflehi_epi16(zmm, (_MM_PERM_ENUM)mask);
        return _mm512_shufflelo_epi16(zmm, (_MM_PERM_ENUM)mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        return _mm512_storeu_si512(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const auto rev_index = get_network(4);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_16bit<zmm_vector<float16>>(x);
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
        return avx512_double_compressstore<zmm_vector<float16>>(
                left_addr, right_addr, k, reg);
    }
};

template <>
struct zmm_vector<int16_t> {
    using type_t = int16_t;
    using reg_t = __m512i;
    using halfreg_t = __m256i;
    using opmask_t = __mmask32;
    static const uint8_t numlanes = 32;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 512;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_16bit_swizzle_ops;

    static reg_t get_network(int index)
    {
        return _mm512_loadu_si512(&network[index - 1][0]);
    }
    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_INT16;
    }
    static type_t type_min()
    {
        return X86_SIMD_SORT_MIN_INT16;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_epi16(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_epi16(type_min());
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask32(x);
    }

    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_epi16_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmpeq_epi16_mask(x, y);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_si512(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_epi16(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        // AVX512_VBMI2
        return _mm512_mask_compressstoreu_epi16(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        // AVX512BW
        return _mm512_mask_loadu_epi16(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_epi16(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi16(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_epi16(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_epi16(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        reg_t lo = _mm512_cvtepi16_epi32(_mm512_extracti64x4_epi64(v, 0));
        reg_t hi = _mm512_cvtepi16_epi32(_mm512_extracti64x4_epi64(v, 1));
        type_t lo_max = (type_t)_mm512_reduce_max_epi32(lo);
        type_t hi_max = (type_t)_mm512_reduce_max_epi32(hi);
        return std::max(lo_max, hi_max);
    }
    static type_t reducemin(reg_t v)
    {
        reg_t lo = _mm512_cvtepi16_epi32(_mm512_extracti64x4_epi64(v, 0));
        reg_t hi = _mm512_cvtepi16_epi32(_mm512_extracti64x4_epi64(v, 1));
        type_t lo_min = (type_t)_mm512_reduce_min_epi32(lo);
        type_t hi_min = (type_t)_mm512_reduce_min_epi32(hi);
        return std::min(lo_min, hi_min);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_epi16(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        zmm = _mm512_shufflehi_epi16(zmm, (_MM_PERM_ENUM)mask);
        return _mm512_shufflelo_epi16(zmm, (_MM_PERM_ENUM)mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        return _mm512_storeu_si512(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const auto rev_index = get_network(4);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_16bit<zmm_vector<type_t>>(x);
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
struct zmm_vector<uint16_t> {
    using type_t = uint16_t;
    using reg_t = __m512i;
    using halfreg_t = __m256i;
    using opmask_t = __mmask32;
    static const uint8_t numlanes = 32;
#ifdef XSS_MINIMAL_NETWORK_SORT
    static constexpr int network_sort_threshold = numlanes;
#else
    static constexpr int network_sort_threshold = 512;
#endif
    static constexpr int partition_unroll_factor = 8;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_16bit_swizzle_ops;

    static reg_t get_network(int index)
    {
        return _mm512_loadu_si512(&network[index - 1][0]);
    }
    static type_t type_max()
    {
        return X86_SIMD_SORT_MAX_UINT16;
    }
    static type_t type_min()
    {
        return 0;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_epi16(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_epi16(type_min());
    }

    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask32(x);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_epu16_mask(x, y, _MM_CMPINT_NLT);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmpeq_epu16_mask(x, y);
    }
    static opmask_t get_partial_loadmask(uint64_t num_to_read)
    {
        return ((0x1ull << num_to_read) - 0x1ull);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_si512(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_epu16(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_compressstoreu_epi16(mem, mask, x);
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        return _mm512_mask_loadu_epi16(x, mask, mem);
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_mask_mov_epi16(x, mask, y);
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi16(mem, mask, x);
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_epu16(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_epi16(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        reg_t lo = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(v, 0));
        reg_t hi = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(v, 1));
        type_t lo_max = (type_t)_mm512_reduce_max_epi32(lo);
        type_t hi_max = (type_t)_mm512_reduce_max_epi32(hi);
        return std::max(lo_max, hi_max);
    }
    static type_t reducemin(reg_t v)
    {
        reg_t lo = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(v, 0));
        reg_t hi = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(v, 1));
        type_t lo_min = (type_t)_mm512_reduce_min_epi32(lo);
        type_t hi_min = (type_t)_mm512_reduce_min_epi32(hi);
        return std::min(lo_min, hi_min);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_epi16(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        zmm = _mm512_shufflehi_epi16(zmm, (_MM_PERM_ENUM)mask);
        return _mm512_shufflelo_epi16(zmm, (_MM_PERM_ENUM)mask);
    }
    static void storeu(void *mem, reg_t x)
    {
        return _mm512_storeu_si512(mem, x);
    }
    static reg_t reverse(reg_t zmm)
    {
        const auto rev_index = get_network(4);
        return permutexvar(rev_index, zmm);
    }
    static reg_t sort_vec(reg_t x)
    {
        return sort_zmm_16bit<zmm_vector<type_t>>(x);
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
X86_SIMD_SORT_INLINE_ONLY bool
comparison_func<zmm_vector<float16>>(const uint16_t &a, const uint16_t &b)
{
    uint16_t signa = a & 0x8000, signb = b & 0x8000;
    uint16_t expa = a & 0x7c00, expb = b & 0x7c00;
    uint16_t manta = a & 0x3ff, mantb = b & 0x3ff;
    if (signa != signb) {
        // opposite signs
        return a > b;
    }
    else if (signa > 0) {
        // both -ve
        if (expa != expb) { return expa > expb; }
        else {
            return manta > mantb;
        }
    }
    else {
        // both +ve
        if (expa != expb) { return expa < expb; }
        else {
            return manta < mantb;
        }
    }

    //return npy_half_to_float(a) < npy_half_to_float(b);
}

template <>
X86_SIMD_SORT_INLINE_ONLY arrsize_t
replace_nan_with_inf<zmm_vector<float16>>(uint16_t *arr, arrsize_t arrsize)
{
    arrsize_t nan_count = 0;
    __mmask16 loadmask = 0xFFFF;
    for (arrsize_t ii = 0; ii < arrsize;
         ii = ii + zmm_vector<float16>::numlanes / 2) {
        if (arrsize - ii < 16) {
            loadmask = (0x0001 << (arrsize - ii)) - 0x0001;
        }
        __m256i in_zmm = _mm256_maskz_loadu_epi16(loadmask, arr);
        __m512 in_zmm_asfloat = _mm512_cvtph_ps(in_zmm);
        __mmask16 nanmask = _mm512_cmp_ps_mask(
                in_zmm_asfloat, in_zmm_asfloat, _CMP_NEQ_UQ);
        nan_count += _mm_popcnt_u32((int32_t)nanmask);
        _mm256_mask_storeu_epi16(arr, nanmask, YMM_MAX_HALF);
        arr += 16;
    }
    return nan_count;
}

template <>
X86_SIMD_SORT_INLINE_ONLY bool is_a_nan<uint16_t>(uint16_t elem)
{
    return ((elem & 0x7c00u) == 0x7c00u) && ((elem & 0x03ffu) != 0);
}

[[maybe_unused]] X86_SIMD_SORT_INLINE void
avx512_qsort_fp16(uint16_t *arr,
                  arrsize_t arrsize,
                  bool hasnan = false,
                  bool descending = false)
{
    using vtype = zmm_vector<float16>;

    if (arrsize > 1) {
        arrsize_t nan_count = 0;
        if (UNLIKELY(hasnan)) {
            nan_count = replace_nan_with_inf<zmm_vector<float16>, uint16_t>(
                    arr, arrsize);
        }
        if (descending) {
            qsort_<vtype, Comparator<vtype, true>, uint16_t>(
                    arr, 0, arrsize - 1, 2 * (arrsize_t)log2(arrsize));
        }
        else {
            qsort_<vtype, Comparator<vtype, false>, uint16_t>(
                    arr, 0, arrsize - 1, 2 * (arrsize_t)log2(arrsize));
        }
        replace_inf_with_nan(arr, arrsize, nan_count, descending);
    }
}

[[maybe_unused]] X86_SIMD_SORT_INLINE void
avx512_qselect_fp16(uint16_t *arr,
                    arrsize_t k,
                    arrsize_t arrsize,
                    bool hasnan = false,
                    bool descending = false)
{
    using vtype = zmm_vector<float16>;

    arrsize_t indx_last_elem = arrsize - 1;
    if (UNLIKELY(hasnan)) {
        indx_last_elem = move_nans_to_end_of_array(arr, arrsize);
    }
    if (indx_last_elem >= k) {
        if (descending) {
            qselect_<vtype, Comparator<vtype, true>, uint16_t>(
                    arr,
                    k,
                    0,
                    indx_last_elem,
                    2 * (arrsize_t)log2(indx_last_elem));
        }
        else {
            qselect_<vtype, Comparator<vtype, false>, uint16_t>(
                    arr,
                    k,
                    0,
                    indx_last_elem,
                    2 * (arrsize_t)log2(indx_last_elem));
        }
    }
}

[[maybe_unused]] X86_SIMD_SORT_INLINE void
avx512_partial_qsort_fp16(uint16_t *arr,
                          arrsize_t k,
                          arrsize_t arrsize,
                          bool hasnan = false,
                          bool descending = false)
{
    avx512_qselect_fp16(arr, k - 1, arrsize, hasnan, descending);
    avx512_qsort_fp16(arr, k - 1, descending);
}
#endif // AVX512_QSORT_16BIT
