#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB_CHDB
{
namespace
{
    struct SinhName
    {
        static constexpr auto name = "sinh";
    };
    using FunctionSinh = FunctionMathUnary<UnaryFunctionVectorized<SinhName, sinh>>;

}

REGISTER_FUNCTION(Sinh)
{
    factory.registerFunction<FunctionSinh>();
}

}
