/***************************************************************************
 * Copyright (c) Johan Mabille, Sylvain Corlay, Wolf Vollprecht and         *
 * Martin Renou                                                             *
 * Copyright (c) QuantStack                                                 *
 * Copyright (c) Serge Guelton                                              *
 *                                                                          *
 * Distributed under the terms of the BSD 3-Clause License.                 *
 *                                                                          *
 * The full license is in the file LICENSE, distributed with this software. *
 ****************************************************************************/

#ifndef XSIMD_AVX512DQ_REGISTER_HPP
#define XSIMD_AVX512DQ_REGISTER_HPP

#include "./xsimd_avx512cd_register.hpp"

namespace xsimd
{

    /**
     * @ingroup architectures
     *
     * AVX512DQ instructions
     */
    struct avx512dq : avx512cd
    {
        static constexpr bool supported() noexcept { return XSIMD_WITH_AVX512DQ; }
        static constexpr bool available() noexcept { return true; }
        static constexpr unsigned version() noexcept { return generic::version(3, 3, 0); }
        static constexpr char const* name() noexcept { return "avx512dq"; }
    };

#if XSIMD_WITH_AVX512DQ

    namespace types
    {
        template <class T>
        struct get_bool_simd_register<T, avx512dq>
        {
            using type = simd_avx512_bool_register<T>;
        };

        XSIMD_DECLARE_SIMD_REGISTER_ALIAS(avx512dq, avx512cd);

    }
#endif
}
#endif
