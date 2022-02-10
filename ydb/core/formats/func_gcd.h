#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernel.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include "func_common.h"
#include <cstdlib>
#include <type_traits>

namespace NKikimr::NArrow { 

template<typename T>
__attribute__((always_inline)) void FastIntSwap(T& lhs, T& rhs) {
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
