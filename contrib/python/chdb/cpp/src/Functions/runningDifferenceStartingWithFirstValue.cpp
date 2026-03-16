#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(RunningDifferenceStartingWithFirstValue)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<false>>();
}

}
