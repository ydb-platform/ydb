#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB_CHDB
{
namespace
{

struct CoshName
{
    static constexpr auto name = "cosh";
};
using FunctionCosh = FunctionMathUnary<UnaryFunctionVectorized<CoshName, cosh>>;

}

REGISTER_FUNCTION(Cosh)
{
    factory.registerFunction<FunctionCosh>();
}

}
