#include "math_ir.h"


#include <ydb/library/yql/public/udf/udf_helpers.h>

extern const char TagRoundingMode[] = "MathRoundingMode";
using TTaggedRoundingMode = NYql::NUdf::TTagged<ui32, TagRoundingMode>;

#define MATH_UDF_MAP(XX, XXL)                                                                       \
    XX(Pi, double(), 0)                                                                             \
    XX(E, double(), 0)                                                                              \
    XX(Eps, double(), 0)                                                                            \
    XX(RoundDownward, TTaggedRoundingMode(), 0)                                                     \
    XX(RoundToNearest, TTaggedRoundingMode(), 0)                                                    \
    XX(RoundTowardZero, TTaggedRoundingMode(), 0)                                                   \
    XX(RoundUpward, TTaggedRoundingMode(), 0)                                                       \
    XX(Abs, double(TAutoMap<double>), 0)                                                            \
    XX(Acos, double(TAutoMap<double>), 0)                                                           \
    XX(Asin, double(TAutoMap<double>), 0)                                                           \
    XX(Asinh, double(TAutoMap<double>), 0)                                                          \
    XX(Atan, double(TAutoMap<double>), 0)                                                           \
    XX(Cbrt, double(TAutoMap<double>), 0)                                                           \
    XX(Ceil, double(TAutoMap<double>), 0)                                                           \
    XX(Cos, double(TAutoMap<double>), 0)                                                            \
    XX(Cosh, double(TAutoMap<double>), 0)                                                           \
    XX(Erf, double(TAutoMap<double>), 0)                                                            \
    XX(ErfInv, double(TAutoMap<double>), 0)                                                         \
    XX(ErfcInv, double(TAutoMap<double>), 0)                                                        \
    XX(Exp, double(TAutoMap<double>), 0)                                                            \
    XX(Exp2, double(TAutoMap<double>), 0)                                                           \
    XX(Fabs, double(TAutoMap<double>), 0)                                                           \
    XX(Floor, double(TAutoMap<double>), 0)                                                          \
    XX(Lgamma, double(TAutoMap<double>), 0)                                                         \
    XX(Rint, double(TAutoMap<double>), 0)                                                           \
    XX(Sin, double(TAutoMap<double>), 0)                                                            \
    XX(Sinh, double(TAutoMap<double>), 0)                                                           \
    XX(Sqrt, double(TAutoMap<double>), 0)                                                           \
    XX(Tan, double(TAutoMap<double>), 0)                                                            \
    XX(Tanh, double(TAutoMap<double>), 0)                                                           \
    XX(Tgamma, double(TAutoMap<double>), 0)                                                         \
    XX(Trunc, double(TAutoMap<double>), 0)                                                          \
    XX(Log, double(TAutoMap<double>), 0)                                                            \
    XX(Log2, double(TAutoMap<double>), 0)                                                           \
    XX(Log10, double(TAutoMap<double>), 0)                                                          \
    XX(Atan2, double(TAutoMap<double>, TAutoMap<double>), 0)                                        \
    XX(Fmod, double(TAutoMap<double>, TAutoMap<double>), 0)                                         \
    XX(Hypot, double(TAutoMap<double>, TAutoMap<double>), 0)                                        \
    XX(Remainder, double(TAutoMap<double>, TAutoMap<double>), 0)                                    \
    XX(Pow, double(TAutoMap<double>, TAutoMap<double>), 0)                                          \
    XX(Ldexp, double(TAutoMap<double>, TAutoMap<int>), 0)                                           \
    XX(IsFinite, bool(TAutoMap<double>), 0)                                                         \
    XX(IsInf, bool(TAutoMap<double>), 0)                                                            \
    XX(IsNaN, bool(TAutoMap<double>), 0)                                                            \
    XX(Sigmoid, double(TAutoMap<double>), 0)                                                        \
    XX(FuzzyEquals, bool(TAutoMap<double>, TAutoMap<double>, TEpsilon), 1)                          \
    XX(Mod, TOptional<i64>(TAutoMap<i64>, i64), 0)                                                  \
    XX(Rem, TOptional<i64>(TAutoMap<i64>, i64), 0)                                                  \
    XXL(Round, double(TAutoMap<double>, TPrecision), 1)

#define MATH_UDF_MAP_WITHOUT_IR(XX)                                                                 \
    XX(NearbyInt, TOptional<i64>(TAutoMap<double>, TTaggedRoundingMode), 0)

#define MATH_STRICT_UDF(name, signature, optionalArgsCount)                                                       \
    SIMPLE_STRICT_UDF_WITH_IR(T##name, signature, optionalArgsCount, "/llvm_bc/Math", #name "IR") {               \
        TUnboxedValuePod res;                                                                                     \
        name##IR(this, &res, valueBuilder, args);                                                                 \
        return res;                                                                                               \
    }

#define MATH_STRICT_UDF_WITHOUT_IR(name, signature, optionalArgsCount)                                            \
    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(T##name, signature, optionalArgsCount) {                                                    \
        TUnboxedValuePod res;                                                                                     \
        name##IR(this, &res, valueBuilder, args);                                                                 \
        return res;                                                                                               \
    }

#define REGISTER_MATH_UDF(udfName, ...)         T##udfName,
#define REGISTER_MATH_UDF_LAST(udfName, ...)    T##udfName

using namespace NKikimr;
using namespace NUdf;

namespace {
    extern const char epsilon[] = "Epsilon";
    using TEpsilon = TNamedArg<double, epsilon>;

    extern const char precision[] = "Precision";
    using TPrecision = TNamedArg<int, precision>;

    MATH_UDF_MAP(MATH_STRICT_UDF, MATH_STRICT_UDF)

    MATH_UDF_MAP_WITHOUT_IR(MATH_STRICT_UDF_WITHOUT_IR)

    SIMPLE_MODULE(TMathModule,
        MATH_UDF_MAP_WITHOUT_IR(REGISTER_MATH_UDF)
        MATH_UDF_MAP(REGISTER_MATH_UDF, REGISTER_MATH_UDF_LAST))
}

REGISTER_MODULES(TMathModule)
