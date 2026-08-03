/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512FP16_QSORT_16BIT
#define AVX512FP16_QSORT_16BIT

#include "avx512-16bit-common.h"

typedef union {
    _Float16 f_;
    uint16_t i_;
} Fp16Bits;

template <>
struct zmm_vector<_Float16> {
    using type_t = _Float16;
    using reg_t = __m512h;
    using halfreg_t = __m256h;
    using opmask_t = __mmask32;
    static const uint8_t numlanes = 32;
    static constexpr int network_sort_threshold = 128;
    static constexpr int partition_unroll_factor = 0;
    static constexpr simd_type vec_type = simd_type::AVX512;

    using swizzle_ops = avx512_16bit_swizzle_ops;

    static __m512i get_network(int index)
    {
        return _mm512_loadu_si512(&network[index - 1][0]);
    }
    static type_t type_max()
    {
        Fp16Bits val;
        val.i_ = X86_SIMD_SORT_INFINITYH;
        return val.f_;
    }
    static type_t type_min()
    {
        Fp16Bits val;
        val.i_ = X86_SIMD_SORT_NEGINFINITYH;
        return val.f_;
    }
    static reg_t zmm_max()
    {
        return _mm512_set1_ph(type_max());
    }
    static reg_t zmm_min()
    {
        return _mm512_set1_ph(type_min());
    }
    static opmask_t knot_opmask(opmask_t x)
    {
        return _knot_mask32(x);
    }
    static opmask_t ge(reg_t x, reg_t y)
    {
        return _mm512_cmp_ph_mask(x, y, _CMP_GE_OQ);
    }
    static opmask_t eq(reg_t x, reg_t y)
    {
        return _mm512_cmp_ph_mask(x, y, _CMP_EQ_OQ);
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
        return _mm512_fpclass_ph_mask(x, type);
    }
    static reg_t loadu(void const *mem)
    {
        return _mm512_loadu_ph(mem);
    }
    static reg_t max(reg_t x, reg_t y)
    {
        return _mm512_max_ph(x, y);
    }
    static void mask_compressstoreu(void *mem, opmask_t mask, reg_t x)
    {
        __m512i temp = _mm512_castph_si512(x);
        // AVX512_VBMI2
        return _mm512_mask_compressstoreu_epi16(mem, mask, temp);
    }
    static reg_t maskz_loadu(opmask_t mask, void const *mem)
    {
        return _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, mem));
    }
    static reg_t mask_loadu(reg_t x, opmask_t mask, void const *mem)
    {
        // AVX512BW
        return _mm512_castsi512_ph(
                _mm512_mask_loadu_epi16(_mm512_castph_si512(x), mask, mem));
    }
    static reg_t mask_mov(reg_t x, opmask_t mask, reg_t y)
    {
        return _mm512_castsi512_ph(_mm512_mask_mov_epi16(
                _mm512_castph_si512(x), mask, _mm512_castph_si512(y)));
    }
    static void mask_storeu(void *mem, opmask_t mask, reg_t x)
    {
        return _mm512_mask_storeu_epi16(mem, mask, _mm512_castph_si512(x));
    }
    static reg_t min(reg_t x, reg_t y)
    {
        return _mm512_min_ph(x, y);
    }
    static reg_t permutexvar(__m512i idx, reg_t zmm)
    {
        return _mm512_permutexvar_ph(idx, zmm);
    }
    static type_t reducemax(reg_t v)
    {
        return _mm512_reduce_max_ph(v);
    }
    static type_t reducemin(reg_t v)
    {
        return _mm512_reduce_min_ph(v);
    }
    static reg_t set1(type_t v)
    {
        return _mm512_set1_ph(v);
    }
    template <uint8_t mask>
    static reg_t shuffle(reg_t zmm)
    {
        __m512i temp = _mm512_shufflehi_epi16(_mm512_castph_si512(zmm),
                                              (_MM_PERM_ENUM)mask);
        return _mm512_castsi512_ph(
                _mm512_shufflelo_epi16(temp, (_MM_PERM_ENUM)mask));
    }
    static void storeu(void *mem, reg_t x)
    {
        return _mm512_storeu_ph(mem, x);
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
        return _mm512_castsi512_ph(v);
    }
    static __m512i cast_to(reg_t v)
    {
        return _mm512_castph_si512(v);
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
X86_SIMD_SORT_INLINE_ONLY bool is_a_nan<_Float16>(_Float16 elem)
{
    return elem != elem;
}

template <>
X86_SIMD_SORT_INLINE_ONLY void replace_inf_with_nan(_Float16 *arr,
                                                    arrsize_t size,
                                                    arrsize_t nan_count,
                                                    bool descending)
{
    Fp16Bits val;
    val.i_ = 0x7c01;

    if (descending) {
        for (arrsize_t ii = 0; nan_count > 0; ++ii) {
            arr[ii] = val.f_;
            nan_count -= 1;
        }
    }
    else {
        for (arrsize_t ii = size - 1; nan_count > 0; --ii) {
            arr[ii] = val.f_;
            nan_count -= 1;
        }
    }
}
#endif // AVX512FP16_QSORT_16BIT
