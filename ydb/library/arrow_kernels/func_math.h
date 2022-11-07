#pragma once
#include "func_common.h"
#include <cmath>

namespace NKikimr::NKernels {

struct TAcosh {

    static constexpr const char * Name = "acosh";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::acosh(arg);
    }
};

struct TAtanh {

    static constexpr const char * Name = "atanh";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::atanh(arg);
    }
};

struct TCbrt {

    static constexpr const char * Name = "cbrt";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::cbrt(arg);
    }
};

struct TCosh {

    static constexpr const char * Name = "cosh";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::cosh(arg);
    }
};

struct TE {

    static constexpr const char * Name = "e";
    static constexpr double value = 2.7182818284590452353602874713526624977572470;

    template <typename TRes>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*) {
        return value;
    }
};

struct TErf {

    static constexpr const char * Name = "erf";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::erf(arg);
    }
};

struct TErfc {

    static constexpr const char * Name = "erfc";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::erfc(arg);
    }
};

struct TExp {

    static constexpr const char * Name = "exp";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::exp(arg);
    }
};

struct TExp2 {

    static constexpr const char * Name = "exp2";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::exp2(arg);
    }
};

#if 0
// Temporarily disable function because it doesn't compile on Windows.
struct TExp10 {

    static constexpr const char * Name = "exp10";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return exp10(arg);
    }
};
#endif

struct THypot {

    static constexpr const char * Name = "hypot";

    template <typename TRes, typename TArg0, typename TArg1>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg0 lhs, TArg1 rhs, arrow::Status*) {
        return std::hypot(lhs, rhs);
    }
};

struct TLgamma {

    static constexpr const char * Name = "lgamma";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::lgamma(arg);
    }
};

struct TPi {

    static constexpr const char * Name = "pi";
    static constexpr double value = 3.1415926535897932384626433832795028841971693;

    template <typename TRes>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*) {
        return value;
    }
};

struct TSinh {

    static constexpr const char * Name = "sinh";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::sinh(arg);
    }
};

struct TSqrt {

    static constexpr const char * Name = "sqrt";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::sqrt(arg);
    }
};

struct TTgamma {

    static constexpr const char * Name = "tgamma";

    template <typename TRes, typename TArg>
    static constexpr EnableIfFloat64<TRes> Call(arrow::compute::KernelContext*, TArg arg, arrow::Status*) {
        return std::tgamma(arg);
    }
};

}
