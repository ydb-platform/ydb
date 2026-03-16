#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(SleepEachRow)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
}

}
