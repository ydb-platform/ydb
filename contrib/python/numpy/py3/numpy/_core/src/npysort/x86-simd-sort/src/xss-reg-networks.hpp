#ifndef XSS_REG_NETWORKS
#define XSS_REG_NETWORKS

#include "xss-common-includes.h"

template <typename vtype, typename maskType>
typename vtype::opmask_t convert_int_to_mask(maskType mask);

template <typename vtype, typename reg_t, typename opmask_t>
X86_SIMD_SORT_INLINE reg_t cmp_merge(reg_t in1, reg_t in2, opmask_t mask);

template <typename vtype1,
          typename vtype2,
          typename reg_t1,
          typename reg_t2,
          typename opmask_t>
X86_SIMD_SORT_INLINE reg_t1 cmp_merge(reg_t1 in1,
                                      reg_t1 in2,
                                      reg_t2 &indexes1,
                                      reg_t2 indexes2,
                                      opmask_t mask);

// Single vector functions

/*
 * Assumes reg is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_4lanes(reg_t reg)
{
    using swizzle = typename vtype::swizzle_ops;

    const typename vtype::opmask_t oxA = convert_int_to_mask<vtype>(0xA);
    const typename vtype::opmask_t oxC = convert_int_to_mask<vtype>(0xC);

    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 2>(reg), oxA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 4>(reg), oxC);
    reg = cmp_merge<vtype>(reg, swizzle::template swap_n<vtype, 2>(reg), oxA);
    return reg;
}

/*
 * Assumes reg is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_8lanes(reg_t reg)
{
    using swizzle = typename vtype::swizzle_ops;

    const typename vtype::opmask_t oxAA = convert_int_to_mask<vtype>(0xAA);
    const typename vtype::opmask_t oxCC = convert_int_to_mask<vtype>(0xCC);
    const typename vtype::opmask_t oxF0 = convert_int_to_mask<vtype>(0xF0);

    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 2>(reg), oxAA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 4>(reg), oxCC);
    reg = cmp_merge<vtype>(reg, swizzle::template swap_n<vtype, 2>(reg), oxAA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 8>(reg), oxF0);
    reg = cmp_merge<vtype>(reg, swizzle::template swap_n<vtype, 4>(reg), oxCC);
    reg = cmp_merge<vtype>(reg, swizzle::template swap_n<vtype, 2>(reg), oxAA);
    return reg;
}

/*
 * Assumes reg is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_16lanes(reg_t reg)
{
    using swizzle = typename vtype::swizzle_ops;

    const typename vtype::opmask_t oxAAAA = convert_int_to_mask<vtype>(0xAAAA);
    const typename vtype::opmask_t oxCCCC = convert_int_to_mask<vtype>(0xCCCC);
    const typename vtype::opmask_t oxF0F0 = convert_int_to_mask<vtype>(0xF0F0);
    const typename vtype::opmask_t oxFF00 = convert_int_to_mask<vtype>(0xFF00);

    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 2>(reg), oxAAAA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 4>(reg), oxCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 8>(reg), oxF0F0);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 4>(reg), oxCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAA);
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 16>(reg), oxFF00);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 8>(reg), oxF0F0);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 4>(reg), oxCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAA);
    return reg;
}

/*
 * Assumes reg is random and performs a full sorting network defined in
 * https://en.wikipedia.org/wiki/Bitonic_sorter#/media/File:BitonicSort.svg
 */
template <typename vtype, typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_32lanes(reg_t reg)
{
    using swizzle = typename vtype::swizzle_ops;

    const typename vtype::opmask_t oxAAAAAAAA
            = convert_int_to_mask<vtype>(0xAAAAAAAA);
    const typename vtype::opmask_t oxCCCCCCCC
            = convert_int_to_mask<vtype>(0xCCCCCCCC);
    const typename vtype::opmask_t oxF0F0F0F0
            = convert_int_to_mask<vtype>(0xF0F0F0F0);
    const typename vtype::opmask_t oxFF00FF00
            = convert_int_to_mask<vtype>(0xFF00FF00);
    const typename vtype::opmask_t oxFFFF0000
            = convert_int_to_mask<vtype>(0xFFFF0000);

    // Level 1
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 2>(reg), oxAAAAAAAA);
    // Level 2
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 4>(reg), oxCCCCCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAAAAAA);
    // Level 3
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 8>(reg), oxF0F0F0F0);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 4>(reg), oxCCCCCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAAAAAA);
    // Level 4
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 16>(reg), oxFF00FF00);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 8>(reg), oxF0F0F0F0);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 4>(reg), oxCCCCCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAAAAAA);
    // Level 5
    reg = cmp_merge<vtype>(
            reg, swizzle::template reverse_n<vtype, 32>(reg), oxFFFF0000);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 16>(reg), oxFF00FF00);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 8>(reg), oxF0F0F0F0);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 4>(reg), oxCCCCCCCC);
    reg = cmp_merge<vtype>(
            reg, swizzle::template swap_n<vtype, 2>(reg), oxAAAAAAAA);
    return reg;
}

// Key-index functions for kv-sort

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_4lanes(reg_t key_reg, index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const typename vtype1::opmask_t oxA = convert_int_to_mask<vtype1>(0xA);
    const typename vtype1::opmask_t oxC = convert_int_to_mask<vtype1>(0xC);

    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 2>(index_reg),
            oxA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 4>(index_reg),
            oxC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxA);
    return key_reg;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_8lanes(reg_t key_reg, index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAA = convert_int_to_mask<vtype1>(0xAA);
    const auto oxCC = convert_int_to_mask<vtype1>(0xCC);
    const auto oxF0 = convert_int_to_mask<vtype1>(0xF0);

    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 2>(index_reg),
            oxAA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 4>(index_reg),
            oxCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 8>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 8>(index_reg),
            oxF0);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAA);
    return key_reg;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_16lanes(reg_t key_reg,
                                            index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAAAA = convert_int_to_mask<vtype1>(0xAAAA);
    const auto oxCCCC = convert_int_to_mask<vtype1>(0xCCCC);
    const auto oxFF00 = convert_int_to_mask<vtype1>(0xFF00);
    const auto oxF0F0 = convert_int_to_mask<vtype1>(0xF0F0);

    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 2>(index_reg),
            oxAAAA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 4>(index_reg),
            oxCCCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAAAA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 8>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 8>(index_reg),
            oxF0F0);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxCCCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAAAA);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template reverse_n<vtype1, 16>(key_reg),
            index_reg,
            index_swizzle::template reverse_n<vtype2, 16>(index_reg),
            oxFF00);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 8>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 8>(index_reg),
            oxF0F0);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxCCCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAAAA);
    return key_reg;
}

// Assumes reg is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_reg_4lanes(reg_t key_reg,
                                                    index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const typename vtype1::opmask_t oxA = convert_int_to_mask<vtype1>(0xA);
    const typename vtype1::opmask_t oxC = convert_int_to_mask<vtype1>(0xC);

    // 2) half_cleaner[4]
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxC);
    // 3) half_cleaner[1]
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxA);
    return key_reg;
}

// Assumes reg is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_reg_8lanes(reg_t key_reg,
                                                    index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAA = convert_int_to_mask<vtype1>(0xAA);
    const auto oxCC = convert_int_to_mask<vtype1>(0xCC);
    const auto oxF0 = convert_int_to_mask<vtype1>(0xF0);

    // 1) half_cleaner[8]: compare 0-4, 1-5, 2-6, 3-7
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 8>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 8>(index_reg),
            oxF0);
    // 2) half_cleaner[4]
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxCC);
    // 3) half_cleaner[1]
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAA);
    return key_reg;
}

// Assumes reg is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_reg_16lanes(reg_t key_reg,
                                                     index_type &index_reg)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAAAA = convert_int_to_mask<vtype1>(0xAAAA);
    const auto oxCCCC = convert_int_to_mask<vtype1>(0xCCCC);
    const auto oxFF00 = convert_int_to_mask<vtype1>(0xFF00);
    const auto oxF0F0 = convert_int_to_mask<vtype1>(0xF0F0);

    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 16>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 16>(index_reg),
            oxFF00);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 8>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 8>(index_reg),
            oxF0F0);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 4>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 4>(index_reg),
            oxCCCC);
    key_reg = cmp_merge<vtype1, vtype2>(
            key_reg,
            key_swizzle::template swap_n<vtype1, 2>(key_reg),
            index_reg,
            index_swizzle::template swap_n<vtype2, 2>(index_reg),
            oxAAAA);
    return key_reg;
}

#endif // XSS_REG_NETWORKS