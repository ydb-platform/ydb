#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{
namespace
{

struct ErfcName { static constexpr auto name = "erfc"; };
using FunctionErfc = FunctionMathUnary<UnaryFunctionVectorized<ErfcName, std::erfc>>;

}

REGISTER_FUNCTION(Erfc)
{
    factory.registerFunction<FunctionErfc>();
}

}
