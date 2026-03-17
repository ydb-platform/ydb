#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(Sleep)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>();
}

}
