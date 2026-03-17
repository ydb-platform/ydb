#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{
namespace
{

struct CosName { static constexpr auto name = "cos"; };
using FunctionCos = FunctionMathUnary<UnaryFunctionVectorized<CosName, cos>>;

}

REGISTER_FUNCTION(Cos)
{
    factory.registerFunction<FunctionCos>({}, FunctionFactory::Case::Insensitive);
}

}
