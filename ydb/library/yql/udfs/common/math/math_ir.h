#pragma once

#include <ydb/library/yql/udfs/common/math/lib/round.h>
#include <ydb/library/yql/udfs/common/math/lib/erfinv.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/ymath.h>
#include <util/system/compiler.h>

#include <math.h>

namespace NYql {
namespace NUdf {

#define CONST_FUNCS(XX)                             \
    XX(Pi, M_PI)                                    \
    XX(E, M_E)                                      \
    XX(Eps, std::numeric_limits<double>::epsilon()) \
    XX(RoundDownward, 0) \
    XX(RoundToNearest, 1) \
    XX(RoundTowardZero, 2) \
    XX(RoundUpward, 3)

#define SINGLE_ARG_FUNCS(XX)            \
    XX(Abs, Abs)                        \
    XX(Acos, acos)                      \
    XX(Asin, asin)                      \
    XX(Asinh, asin)                     \
    XX(Atan, atan)                      \
    XX(Cbrt, cbrt)                      \
    XX(Ceil, ceil)                      \
    XX(Cos, cos)                        \
    XX(Cosh, cosh)                      \
    XX(Erf, Erf)                        \
    XX(Exp, exp)                        \
    XX(Exp2, Exp2)                      \
    XX(Fabs, fabs)                      \
    XX(Floor, std::floor)               \
    XX(Lgamma, LogGamma)                \
    XX(Rint, rint)                      \
    XX(Sin, sin)                        \
    XX(Sinh, sinh)                      \
    XX(Sqrt, sqrt)                      \
    XX(Tan, tan)                        \
    XX(Tanh, tanh)                      \
    XX(Tgamma, tgamma)                  \
    XX(Trunc, trunc)                    \
    XX(IsFinite, std::isfinite)         \
    XX(IsInf, std::isinf)               \
    XX(IsNaN, std::isnan)

#define TWO_ARGS_FUNCS(XX)              \
    XX(Atan2, atan2, double)            \
    XX(Fmod, fmod, double)              \
    XX(Hypot, hypot, double)            \
    XX(Remainder, remainder, double)    \
    XX(Pow, pow, double)                \
    XX(Ldexp, ldexp, int)

#define POSITIVE_SINGLE_ARG_FUNCS(XX)   \
    XX(Log, log)                        \
    XX(Log2, Log2)                      \
    XX(Log10, log10)


#define CONST_IMPL(name, cnst)                                                                                                                      \
    extern "C" UDF_ALWAYS_INLINE                                                                                                                    \
    void name##IR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* /*args*/) {\
        *result = TUnboxedValuePod(cnst);                                                                                                           \
    }

#define SINGLE_ARG_IMPL(name, func)                                                                                                                 \
    extern "C" UDF_ALWAYS_INLINE                                                                                                                    \
    void name##IR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {    \
        *result = TUnboxedValuePod(func(args[0].Get<double>()));                                                                                    \
    }

#define TWO_ARGS_IMPL(name, func, secondType)                                                                                                       \
    extern "C" UDF_ALWAYS_INLINE                                                                                                                    \
    void name##IR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {    \
        *result = TUnboxedValuePod(func(args[0].Get<double>(), args[1].Get<secondType>()));                                                         \
    }

#define POSITIVE_SINGLE_ARG_IMPL(name, func)                                                                                                        \
    extern "C" UDF_ALWAYS_INLINE                                                                                                                    \
    void name##IR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {    \
        double input = args[0].Get<double>();                                                                                                       \
        if (input > 0) {                                                                                                                            \
            *result = TUnboxedValuePod(func(input));                                                                                                \
        } else {                                                                                                                                    \
            *result = TUnboxedValuePod(static_cast<double>(NAN));                                                                                   \
        }                                                                                                                                           \
    }

CONST_FUNCS(CONST_IMPL)
SINGLE_ARG_FUNCS(SINGLE_ARG_IMPL)
TWO_ARGS_FUNCS(TWO_ARGS_IMPL)
POSITIVE_SINGLE_ARG_FUNCS(POSITIVE_SINGLE_ARG_IMPL)

extern "C" UDF_ALWAYS_INLINE
void SigmoidIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    *result = TUnboxedValuePod(1. / (1. + exp(-args[0].Get<double>())));
}

extern "C" UDF_ALWAYS_INLINE
void FuzzyEqualsIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    if (!args[2]) {
        *result = TUnboxedValuePod(FuzzyEquals(args[0].Get<double>(), args[1].Get<double>()));
    } else {
        const double eps = args[2].Get<double>();
        *result = TUnboxedValuePod(FuzzyEquals(args[0].Get<double>(), args[1].Get<double>(), eps));
    }
}

extern "C" UDF_ALWAYS_INLINE
void RoundIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    const double val = NMathUdf::RoundToDecimal<long double>(args[0].Get<double>(), args[1].GetOrDefault<int>(0));
    *result = TUnboxedValuePod(val);
}

extern "C" UDF_ALWAYS_INLINE
void ErfInvIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    *result = TUnboxedValuePod(NMathUdf::ErfInv(args[0].Get<double>()));
}

extern "C" UDF_ALWAYS_INLINE
void ErfcInvIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    *result = TUnboxedValuePod(NMathUdf::ErfInv(1. - args[0].Get<double>()));
}

extern "C" UDF_ALWAYS_INLINE
void ModIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    const auto val = NMathUdf::Mod(args[0].Get<i64>(), args[1].Get<i64>());
    *result = val ? TUnboxedValuePod(*val) : TUnboxedValuePod();
}

extern "C" UDF_ALWAYS_INLINE
void RemIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    const auto val = NMathUdf::Rem(args[0].Get<i64>(), args[1].Get<i64>());
    *result = val ? TUnboxedValuePod(*val) : TUnboxedValuePod();
}

extern "C" UDF_ALWAYS_INLINE
void NearbyIntIR(const IBoxedValue* /*pThis*/, TUnboxedValuePod* result, const IValueBuilder* /*valueBuilder*/, const TUnboxedValuePod* args) {
    const auto val = NMathUdf::NearbyInt(args[0].Get<double>(), args[1].Get<ui32>());
    *result = val ? TUnboxedValuePod(*val) : TUnboxedValuePod();
}

} // NUdf
} // NYql
