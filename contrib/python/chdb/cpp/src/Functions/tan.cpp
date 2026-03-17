#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{
namespace
{

struct TanName { static constexpr auto name = "tan"; };
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, tan>>;

}

REGISTER_FUNCTION(Tan)
{
    factory.registerFunction<FunctionTan>({}, FunctionFactory::Case::Insensitive);
}

}
