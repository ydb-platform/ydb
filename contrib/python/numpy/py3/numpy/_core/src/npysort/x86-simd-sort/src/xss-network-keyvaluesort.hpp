#ifndef XSS_KEYVALUE_NETWORKS
#define XSS_KEYVALUE_NETWORKS

#include "xss-common-includes.h"

template <typename vtype, typename maskType>
typename vtype::opmask_t convert_int_to_mask(maskType mask)
{
    if constexpr (vtype::vec_type == simd_type::AVX512) { return mask; }
    else if constexpr (vtype::vec_type == simd_type::AVX2) {
        return vtype::convert_int_to_mask(mask);
    }
    else {
        static_assert(always_false<maskType>,
                      "Error in func convert_int_to_mask");
    }
}

template <typename keyType, typename valueType>
typename valueType::opmask_t resize_mask(typename keyType::opmask_t mask)
{
    using inT = typename keyType::opmask_t;
    using outT = typename valueType::opmask_t;

    if constexpr (sizeof(inT) == sizeof(outT)) { //std::is_same_v<inT, outT>) {
        return mask;
    }
    /* convert __m256i to __m128i */
    else if constexpr (sizeof(inT) == 32 && sizeof(outT) == 16) {
        return _mm_castps_si128(_mm256_cvtpd_ps(_mm256_castsi256_pd(mask)));
    }
    /* convert __m128i to __m256i */
    else if constexpr (sizeof(inT) == 16 && sizeof(outT) == 32) {
        return _mm256_cvtepi32_epi64(mask);
    }
    else {
        static_assert(always_false<keyType>, "Error in func resize_mask");
    }
}

template <typename vtype1,
          typename vtype2,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE void
COEX(reg_t1 &key1, reg_t1 &key2, reg_t2 &index1, reg_t2 &index2)
{
    reg_t1 key_t1 = vtype1::min(key1, key2);
    reg_t1 key_t2 = vtype1::max(key1, key2);

    auto eqMask = resize_mask<vtype1, vtype2>(vtype1::eq(key_t1, key1));

    reg_t2 index_t1 = vtype2::mask_mov(index2, eqMask, index1);
    reg_t2 index_t2 = vtype2::mask_mov(index1, eqMask, index2);

    key1 = key_t1;
    key2 = key_t2;
    index1 = index_t1;
    index2 = index_t2;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t,
          typename opmask_t = typename vtype1::opmask_t>
X86_SIMD_SORT_INLINE reg_t1 cmp_merge(reg_t1 in1,
                                      reg_t1 in2,
                                      reg_t2 &indexes1,
                                      reg_t2 indexes2,
                                      opmask_t mask)
{
    reg_t1 tmp_keys = cmp_merge<vtype1>(in1, in2, mask);
    indexes1 = vtype2::mask_mov(
            indexes2,
            resize_mask<vtype1, vtype2>(vtype1::eq(tmp_keys, in1)),
            indexes1);
    return tmp_keys; // 0 -> min, 1 -> max
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_16lanes(reg_t key_zmm,
                                            index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAAAA = convert_int_to_mask<vtype1>(0xAAAA);
    const auto oxCCCC = convert_int_to_mask<vtype1>(0xCCCC);
    const auto oxFF00 = convert_int_to_mask<vtype1>(0xFF00);
    const auto oxF0F0 = convert_int_to_mask<vtype1>(0xF0F0);

    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 2>(index_zmm),
            oxAAAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 4>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 4>(index_zmm),
            oxCCCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 2>(index_zmm),
            oxAAAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_32BIT_3), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_32BIT_3), index_zmm),
            oxF0F0);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 4>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 4>(index_zmm),
            oxCCCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 2>(index_zmm),
            oxAAAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_32BIT_5), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_32BIT_5), index_zmm),
            oxFF00);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_32BIT_6), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_32BIT_6), index_zmm),
            oxF0F0);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 4>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 4>(index_zmm),
            oxCCCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 2>(index_zmm),
            oxAAAA);
    return key_zmm;
}

// Assumes zmm is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_reg_16lanes(reg_t key_zmm,
                                                     index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAAAA = convert_int_to_mask<vtype1>(0xAAAA);
    const auto oxCCCC = convert_int_to_mask<vtype1>(0xCCCC);
    const auto oxFF00 = convert_int_to_mask<vtype1>(0xFF00);
    const auto oxF0F0 = convert_int_to_mask<vtype1>(0xF0F0);

    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_32BIT_7), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_32BIT_7), index_zmm),
            oxFF00);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_32BIT_6), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_32BIT_6), index_zmm),
            oxF0F0);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 4>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 4>(index_zmm),
            oxCCCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template reverse_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template reverse_n<vtype2, 2>(index_zmm),
            oxAAAA);
    return key_zmm;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_reg_8lanes(reg_t key_zmm, index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAA = convert_int_to_mask<vtype1>(0xAA);
    const auto oxCC = convert_int_to_mask<vtype1>(0xCC);
    const auto oxF0 = convert_int_to_mask<vtype1>(0xF0);

    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_1), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_1), index_zmm),
            oxCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    key_zmm = cmp_merge<vtype1, vtype2>(key_zmm,
                                        vtype1::reverse(key_zmm),
                                        index_zmm,
                                        vtype2::reverse(index_zmm),
                                        oxF0);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_3), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_3), index_zmm),
            oxCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    return key_zmm;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t sort_ymm_64bit(reg_t key_zmm, index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const typename vtype1::opmask_t oxAA = vtype1::seti(-1, 0, -1, 0);
    const typename vtype1::opmask_t oxCC = vtype1::seti(-1, -1, 0, 0);

    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    key_zmm = cmp_merge<vtype1, vtype2>(key_zmm,
                                        vtype1::reverse(key_zmm),
                                        index_zmm,
                                        vtype2::reverse(index_zmm),
                                        oxCC);
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    return key_zmm;
}

// Assumes zmm is bitonic and performs a recursive half cleaner
template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_reg_8lanes(reg_t key_zmm,
                                                    index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const auto oxAA = convert_int_to_mask<vtype1>(0xAA);
    const auto oxCC = convert_int_to_mask<vtype1>(0xCC);
    const auto oxF0 = convert_int_to_mask<vtype1>(0xF0);

    // 1) half_cleaner[8]: compare 0-4, 1-5, 2-6, 3-7
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_4), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_4), index_zmm),
            oxF0);
    // 2) half_cleaner[4]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            vtype1::permutexvar(vtype1::seti(NETWORK_64BIT_3), key_zmm),
            index_zmm,
            vtype2::permutexvar(vtype2::seti(NETWORK_64BIT_3), index_zmm),
            oxCC);
    // 3) half_cleaner[1]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    return key_zmm;
}

template <typename vtype1,
          typename vtype2,
          typename reg_t = typename vtype1::reg_t,
          typename index_type = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE reg_t bitonic_merge_ymm_64bit(reg_t key_zmm,
                                                   index_type &index_zmm)
{
    using key_swizzle = typename vtype1::swizzle_ops;
    using index_swizzle = typename vtype2::swizzle_ops;

    const typename vtype1::opmask_t oxAA = vtype1::seti(-1, 0, -1, 0);
    const typename vtype1::opmask_t oxCC = vtype1::seti(-1, -1, 0, 0);

    // 2) half_cleaner[4]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 4>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 4>(index_zmm),
            oxCC);
    // 3) half_cleaner[1]
    key_zmm = cmp_merge<vtype1, vtype2>(
            key_zmm,
            key_swizzle::template swap_n<vtype1, 2>(key_zmm),
            index_zmm,
            index_swizzle::template swap_n<vtype2, 2>(index_zmm),
            oxAA);
    return key_zmm;
}

template <typename keyType, typename valueType>
X86_SIMD_SORT_INLINE void
bitonic_merge_dispatch(typename keyType::reg_t &key,
                       typename valueType::reg_t &value)
{
    constexpr int numlanes = keyType::numlanes;
    if constexpr (numlanes == 8) {
        key = bitonic_merge_reg_8lanes<keyType, valueType>(key, value);
    }
    else if constexpr (numlanes == 16) {
        key = bitonic_merge_reg_16lanes<keyType, valueType>(key, value);
    }
    else if constexpr (numlanes == 4) {
        key = bitonic_merge_ymm_64bit<keyType, valueType>(key, value);
    }
    else {
        static_assert(always_false<keyType>,
                      "bitonic_merge_dispatch: No implementation");
        UNUSED(key);
        UNUSED(value);
    }
}

template <typename keyType, typename valueType>
X86_SIMD_SORT_INLINE void sort_vec_dispatch(typename keyType::reg_t &key,
                                            typename valueType::reg_t &value)
{
    constexpr int numlanes = keyType::numlanes;
    if constexpr (numlanes == 8) {
        key = sort_reg_8lanes<keyType, valueType>(key, value);
    }
    else if constexpr (numlanes == 16) {
        key = sort_reg_16lanes<keyType, valueType>(key, value);
    }
    else if constexpr (numlanes == 4) {
        key = sort_ymm_64bit<keyType, valueType>(key, value);
    }
    else {
        static_assert(always_false<keyType>,
                      "sort_vec_dispatch: No implementation");
        UNUSED(key);
        UNUSED(value);
    }
}

template <typename keyType, typename valueType, int numVecs>
X86_SIMD_SORT_INLINE void bitonic_clean_n_vec(typename keyType::reg_t *keys,
                                              typename valueType::reg_t *values)
{
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int num = numVecs / 2; num >= 2; num /= 2) {
        X86_SIMD_SORT_UNROLL_LOOP(64)
        for (int j = 0; j < numVecs; j += num) {
            X86_SIMD_SORT_UNROLL_LOOP(64)
            for (int i = 0; i < num / 2; i++) {
                arrsize_t index1 = i + j;
                arrsize_t index2 = i + j + num / 2;
                COEX<keyType, valueType>(keys[index1],
                                         keys[index2],
                                         values[index1],
                                         values[index2]);
            }
        }
    }
}

template <typename keyType, typename valueType, int numVecs>
X86_SIMD_SORT_INLINE void bitonic_merge_n_vec(typename keyType::reg_t *keys,
                                              typename valueType::reg_t *values)
{
    // Do the reverse part
    if constexpr (numVecs == 2) {
        keys[1] = keyType::reverse(keys[1]);
        values[1] = valueType::reverse(values[1]);
        COEX<keyType, valueType>(keys[0], keys[1], values[0], values[1]);
        keys[1] = keyType::reverse(keys[1]);
        values[1] = valueType::reverse(values[1]);
    }
    else if constexpr (numVecs > 2) {
        // Reverse upper half
        X86_SIMD_SORT_UNROLL_LOOP(64)
        for (int i = 0; i < numVecs / 2; i++) {
            keys[numVecs - i - 1] = keyType::reverse(keys[numVecs - i - 1]);
            values[numVecs - i - 1]
                    = valueType::reverse(values[numVecs - i - 1]);

            COEX<keyType, valueType>(keys[i],
                                     keys[numVecs - i - 1],
                                     values[i],
                                     values[numVecs - i - 1]);

            keys[numVecs - i - 1] = keyType::reverse(keys[numVecs - i - 1]);
            values[numVecs - i - 1]
                    = valueType::reverse(values[numVecs - i - 1]);
        }
    }

    // Call cleaner
    bitonic_clean_n_vec<keyType, valueType, numVecs>(keys, values);

    // Now do bitonic_merge
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs; i++) {
        bitonic_merge_dispatch<keyType, valueType>(keys[i], values[i]);
    }
}

template <typename keyType, typename valueType, int numVecs, int numPer = 2>
X86_SIMD_SORT_INLINE void
bitonic_fullmerge_n_vec(typename keyType::reg_t *keys,
                        typename valueType::reg_t *values)
{
    if constexpr (numPer > numVecs) {
        UNUSED(keys);
        UNUSED(values);
        return;
    }
    else {
        X86_SIMD_SORT_UNROLL_LOOP(64)
        for (int i = 0; i < numVecs / numPer; i++) {
            bitonic_merge_n_vec<keyType, valueType, numPer>(
                    keys + i * numPer, values + i * numPer);
        }
        bitonic_fullmerge_n_vec<keyType, valueType, numVecs, numPer * 2>(
                keys, values);
    }
}

template <typename keyType, typename indexType, int numVecs>
X86_SIMD_SORT_INLINE void
argsort_n_vec(typename keyType::type_t *keys, arrsize_t *indices, int N)
{
    using kreg_t = typename keyType::reg_t;
    using ireg_t = typename indexType::reg_t;

    static_assert(numVecs > 0, "numVecs should be > 0");
    if constexpr (numVecs > 1) {
        if (N * 2 <= numVecs * keyType::numlanes) {
            argsort_n_vec<keyType, indexType, numVecs / 2>(keys, indices, N);
            return;
        }
    }

    kreg_t keyVecs[numVecs];
    ireg_t indexVecs[numVecs];

    // Generate masks for loading and storing
    typename keyType::opmask_t ioMasks[numVecs - numVecs / 2];
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2, j = 0; i < numVecs; i++, j++) {
        uint64_t num_to_read
                = std::min((uint64_t)std::max(0, N - i * keyType::numlanes),
                           (uint64_t)keyType::numlanes);
        ioMasks[j] = keyType::get_partial_loadmask(num_to_read);
    }

    // Unmasked part of the load
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs / 2; i++) {
        indexVecs[i] = indexType::loadu(indices + i * indexType::numlanes);
        keyVecs[i]
                = keyType::i64gather(keys, indices + i * indexType::numlanes);
    }
    // Masked part of the load
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2; i < numVecs; i++) {
        indexVecs[i] = indexType::mask_loadu(
                indexType::zmm_max(),
                resize_mask<keyType, indexType>(ioMasks[i - numVecs / 2]),
                indices + i * indexType::numlanes);

        keyVecs[i] = keyType::template mask_i64gather<sizeof(
                typename keyType::type_t)>(keyType::zmm_max(),
                                           ioMasks[i - numVecs / 2],
                                           indexVecs[i],
                                           keys);
    }

    // Sort each loaded vector
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs; i++) {
        sort_vec_dispatch<keyType, indexType>(keyVecs[i], indexVecs[i]);
    }

    // Run the full merger
    bitonic_fullmerge_n_vec<keyType, indexType, numVecs>(keyVecs, indexVecs);

    // Unmasked part of the store
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs / 2; i++) {
        indexType::storeu(indices + i * indexType::numlanes, indexVecs[i]);
    }
    // Masked part of the store
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2, j = 0; i < numVecs; i++, j++) {
        indexType::mask_storeu(
                indices + i * indexType::numlanes,
                resize_mask<keyType, indexType>(ioMasks[i - numVecs / 2]),
                indexVecs[i]);
    }
}

template <typename keyType, typename valueType, int numVecs>
X86_SIMD_SORT_INLINE void kvsort_n_vec(typename keyType::type_t *keys,
                                       typename valueType::type_t *values,
                                       int N)
{
    using kreg_t = typename keyType::reg_t;
    using vreg_t = typename valueType::reg_t;

    static_assert(numVecs > 0, "numVecs should be > 0");
    if constexpr (numVecs > 1) {
        if (N * 2 <= numVecs * keyType::numlanes) {
            kvsort_n_vec<keyType, valueType, numVecs / 2>(keys, values, N);
            return;
        }
    }

    kreg_t keyVecs[numVecs];
    vreg_t valueVecs[numVecs];

    // Generate masks for loading and storing
    typename keyType::opmask_t ioMasks[numVecs - numVecs / 2];
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2, j = 0; i < numVecs; i++, j++) {
        uint64_t num_to_read
                = std::min((uint64_t)std::max(0, N - i * keyType::numlanes),
                           (uint64_t)keyType::numlanes);
        ioMasks[j] = keyType::get_partial_loadmask(num_to_read);
    }

    // Unmasked part of the load
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs / 2; i++) {
        keyVecs[i] = keyType::loadu(keys + i * keyType::numlanes);
        valueVecs[i] = valueType::loadu(values + i * valueType::numlanes);
    }
    // Masked part of the load
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2, j = 0; i < numVecs; i++, j++) {
        keyVecs[i] = keyType::mask_loadu(
                keyType::zmm_max(), ioMasks[j], keys + i * keyType::numlanes);
        valueVecs[i] = valueType::mask_loadu(
                valueType::zmm_max(),
                resize_mask<keyType, valueType>(ioMasks[j]),
                values + i * valueType::numlanes);
    }

    // Sort each loaded vector
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs; i++) {
        sort_vec_dispatch<keyType, valueType>(keyVecs[i], valueVecs[i]);
    }

    // Run the full merger
    bitonic_fullmerge_n_vec<keyType, valueType, numVecs>(keyVecs, valueVecs);

    // Unmasked part of the store
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = 0; i < numVecs / 2; i++) {
        keyType::storeu(keys + i * keyType::numlanes, keyVecs[i]);
        valueType::storeu(values + i * valueType::numlanes, valueVecs[i]);
    }
    // Masked part of the store
    X86_SIMD_SORT_UNROLL_LOOP(64)
    for (int i = numVecs / 2, j = 0; i < numVecs; i++, j++) {
        keyType::mask_storeu(
                keys + i * keyType::numlanes, ioMasks[j], keyVecs[i]);
        valueType::mask_storeu(values + i * valueType::numlanes,
                               resize_mask<keyType, valueType>(ioMasks[j]),
                               valueVecs[i]);
    }
}

template <typename keyType, typename indexType, int maxN>
X86_SIMD_SORT_INLINE void
argsort_n(typename keyType::type_t *keys, arrsize_t *indices, int N)
{
    static_assert(keyType::numlanes == indexType::numlanes,
                  "invalid pairing of value/index types");

    constexpr int numVecs = maxN / keyType::numlanes;
    constexpr bool isMultiple = (maxN == (keyType::numlanes * numVecs));
    constexpr bool powerOfTwo = (numVecs != 0 && !(numVecs & (numVecs - 1)));
    static_assert(powerOfTwo == true && isMultiple == true,
                  "maxN must be keyType::numlanes times a power of 2");

    argsort_n_vec<keyType, indexType, numVecs>(keys, indices, N);
}

template <typename keyType, typename valueType, int maxN>
X86_SIMD_SORT_INLINE void kvsort_n(typename keyType::type_t *keys,
                                   typename valueType::type_t *values,
                                   int N)
{
    static_assert(keyType::numlanes == valueType::numlanes,
                  "invalid pairing of key/value types");

    constexpr int numVecs = maxN / keyType::numlanes;
    constexpr bool isMultiple = (maxN == (keyType::numlanes * numVecs));
    constexpr bool powerOfTwo = (numVecs != 0 && !(numVecs & (numVecs - 1)));
    static_assert(powerOfTwo == true && isMultiple == true,
                  "maxN must be keyType::numlanes times a power of 2");

    kvsort_n_vec<keyType, valueType, numVecs>(keys, values, N);
}

#endif
