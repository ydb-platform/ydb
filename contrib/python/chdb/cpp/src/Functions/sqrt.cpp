#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{
namespace
{

struct SqrtName { static constexpr auto name = "sqrt"; };
using FunctionSqrt = FunctionMathUnary<UnaryFunctionVectorized<SqrtName, sqrt>>;

}

REGISTER_FUNCTION(Sqrt)
{
    factory.registerFunction<FunctionSqrt>({}, FunctionFactory::Case::Insensitive);
}

}
