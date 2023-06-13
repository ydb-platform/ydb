#include "math_ir.h"


#include <ydb/library/yql/public/udf/udf_helpers.h>

#define MATH_UDF_MAP(XX, XXL)                                                                       \
    XX(Pi, double(), ;)                                                                             \
    XX(E, double(), ;)                                                                              \
    XX(Eps, double(), ;)                                                                            \
    XX(Abs, double(TAutoMap<double>), ;)                                                            \
    XX(Acos, double(TAutoMap<double>), ;)                                                           \
    XX(Asin, double(TAutoMap<double>), ;)                                                           \
    XX(Asinh, double(TAutoMap<double>), ;)                                                          \
    XX(Atan, double(TAutoMap<double>), ;)                                                           \
    XX(Cbrt, double(TAutoMap<double>), ;)                                                           \
    XX(Ceil, double(TAutoMap<double>), ;)                                                           \
    XX(Cos, double(TAutoMap<double>), ;)                                                            \
    XX(Cosh, double(TAutoMap<double>), ;)                                                           \
    XX(Erf, double(TAutoMap<double>), ;)                                                            \
    XX(ErfInv, double(TAutoMap<double>), ;)                                                         \
    XX(ErfcInv, double(TAutoMap<double>), ;)                                                        \
    XX(Exp, double(TAutoMap<double>), ;)                                                            \
    XX(Exp2, double(TAutoMap<double>), ;)                                                           \
    XX(Fabs, double(TAutoMap<double>), ;)                                                           \
    XX(Floor, double(TAutoMap<double>), ;)                                                          \
    XX(Lgamma, double(TAutoMap<double>), ;)                                                         \
    XX(Rint, double(TAutoMap<double>), ;)                                                           \
    XX(Sin, double(TAutoMap<double>), ;)                                                            \
    XX(Sinh, double(TAutoMap<double>), ;)                                                           \
    XX(Sqrt, double(TAutoMap<double>), ;)                                                           \
    XX(Tan, double(TAutoMap<double>), ;)                                                            \
    XX(Tanh, double(TAutoMap<double>), ;)                                                           \
    XX(Tgamma, double(TAutoMap<double>), ;)                                                         \
    XX(Trunc, double(TAutoMap<double>), ;)                                                          \
    XX(Log, double(TAutoMap<double>), ;)                                                            \
    XX(Log2, double(TAutoMap<double>), ;)                                                           \
    XX(Log10, double(TAutoMap<double>), ;)                                                          \
    XX(Atan2, double(TAutoMap<double>, TAutoMap<double>), ;)                                        \
    XX(Fmod, double(TAutoMap<double>, TAutoMap<double>), ;)                                         \
    XX(Hypot, double(TAutoMap<double>, TAutoMap<double>), ;)                                        \
    XX(Remainder, double(TAutoMap<double>, TAutoMap<double>), ;)                                    \
    XX(Pow, double(TAutoMap<double>, TAutoMap<double>), ;)                                          \
    XX(Ldexp, double(TAutoMap<double>, TAutoMap<int>), ;)                                           \
    XX(IsFinite, bool(TAutoMap<double>), ;)                                                         \
    XX(IsInf, bool(TAutoMap<double>), ;)                                                            \
    XX(IsNaN, bool(TAutoMap<double>), ;)                                                            \
    XX(Sigmoid, double(TAutoMap<double>), ;)                                                        \
    XX(FuzzyEquals, bool(TAutoMap<double>, TAutoMap<double>, TEpsilon), builder.OptionalArgs(1))    \
    XX(Mod, TOptional<i64>(TAutoMap<i64>, i64), ;)                                                  \
    XX(Rem, TOptional<i64>(TAutoMap<i64>, i64), ;)                                                  \
    XXL(Round, double(TAutoMap<double>, TPrecision), builder.OptionalArgs(1))

#define MATH_UDF_IMPL(name, signature, options)                                                                   \
    UDF_IMPL(T##name, builder.SimpleSignature<signature>(); options;, ;, ;, "/llvm_bc/Math", #name "IR", void) {  \
        TUnboxedValuePod res;                                                                                     \
        name##IR(this, &res, valueBuilder, args);                                                                 \
        return res;                                                                                               \
    }

#define MATH_STRICT_UDF_IMPL(name, signature, options)                                                                       \
    UDF_IMPL(T##name, builder.SimpleSignature<signature>().IsStrict(); options;, ;, ;, "/llvm_bc/Math", #name "IR", void) {  \
        TUnboxedValuePod res;                                                                                                \
        name##IR(this, &res, valueBuilder, args);                                                                            \
        return res;                                                                                                          \
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

    MATH_UDF_MAP(MATH_STRICT_UDF_IMPL, MATH_STRICT_UDF_IMPL)

    SIMPLE_MODULE(TMathModule,
        MATH_UDF_MAP(REGISTER_MATH_UDF, REGISTER_MATH_UDF_LAST))
}

REGISTER_MODULES(TMathModule)
