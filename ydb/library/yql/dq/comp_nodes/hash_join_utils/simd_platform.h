#pragma once

#if defined(__x86_64__) && defined(__linux__)
    #define YDB_HASH_JOIN_SIMD_ENABLED 1
#else
    #define YDB_HASH_JOIN_SIMD_ENABLED 0
#endif

// Always include fallback implementation for all platforms
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/simd_fallback.h>

#if YDB_HASH_JOIN_SIMD_ENABLED
    #include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/simd_avx2.h>
    #include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/simd_sse42.h>
    #include <util/system/cpu_id.h>
#endif

// Define SIMD traits namespace and types
namespace NSimd {

template<int RegisterSize, typename TBaseRegister, template<typename> typename TSimd>
struct TSimdTraits {
    using TRegister = TBaseRegister;
    template<typename T>
    using TSimd8 = TSimd<T>;
    using TSimdI8 = TSimd8<i8>;
    static constexpr int Size = RegisterSize;
};

using TSimdFallbackTraits = TSimdTraits<8, ui64, NSimd::NFallback::TSimd8>;

#if YDB_HASH_JOIN_SIMD_ENABLED
using TSimdAVX2Traits = TSimdTraits<32, __m256i, NSimd::NAVX2::TSimd8>;
using TSimdSSE42Traits = TSimdTraits<16, __m128i, NSimd::NSSE42::TSimd8>;
#endif

}

