#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernel.h>
#include <type_traits>

#ifdef WIN32
#define INLINE inline
#else
#define INLINE __attribute__((always_inline))
#endif

namespace NKikimr::NKernels {

template<typename T>
INLINE void FastIntSwap(T& lhs, T& rhs) {
    lhs ^= rhs;
    rhs ^= lhs;
    lhs ^= rhs;
 }

struct TGreatestCommonDivisor {

    static constexpr const char * Name = "gcd";

    template <typename TRes, typename TArg0, typename TArg1>
    static constexpr TRes Call(arrow::compute::KernelContext*, TArg0 lhs, TArg1 rhs, arrow::Status*) {
        static_assert(std::is_integral_v<TRes>, "");
        static_assert(std::is_integral_v<TArg0>, "");
        static_assert(std::is_integral_v<TArg1>, "");
        while (rhs != 0) {
            lhs %= rhs;
            FastIntSwap(lhs, rhs);
        }
        return lhs;
    }
};

}

#undef INLINE
