#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(RunningDifference)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<true>>();
}

}
