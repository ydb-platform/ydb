#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace NDB
{

void registerFunctionFixedString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToFixedString>();
}

}
