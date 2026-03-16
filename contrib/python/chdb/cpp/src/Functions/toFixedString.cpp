#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(FixedString)
{
    factory.registerFunction<FunctionToFixedString>();
}

}
