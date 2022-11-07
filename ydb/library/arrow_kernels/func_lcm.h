#pragma once
#include "func_common.h"
#include "func_mul.h"
#include "func_gcd.h"
#include "clickhouse_type_traits.h"

namespace NKikimr::NKernels {

struct TLeastCommonMultiple {

    static constexpr const char * Name = "lcm";

    template <typename TRes, typename TArg0, typename TArg1>
    static constexpr TRes Call(arrow::compute::KernelContext* ctx, TArg0 lhs, TArg1 rhs, arrow::Status* st) {
        static_assert(std::is_integral_v<TRes>, "");
        static_assert(std::is_integral_v<TArg0>, "");
        static_assert(std::is_integral_v<TArg1>, "");
        auto gcd = TGreatestCommonDivisor::Call<TRes, TArg0, TArg1>(ctx, lhs, rhs, st);
        if (ARROW_PREDICT_FALSE(gcd == 0)) {
            *st = arrow::Status::Invalid("divide by zero");
            return 0;
        }
        return TMultiply::Call<TRes, TArg0, TArg1>(ctx, lhs, rhs, st) / gcd;
    }
};

}
