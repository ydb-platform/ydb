#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB_CHDB
{
namespace
{
    struct HypotName
    {
        static constexpr auto name = "hypot";
    };
    using FunctionHypot = FunctionMathBinaryFloat64<BinaryFunctionVectorized<HypotName, hypot>>;

}

REGISTER_FUNCTION(Hypot)
{
    factory.registerFunction<FunctionHypot>({}, FunctionFactory::Case::Insensitive);
}

}
