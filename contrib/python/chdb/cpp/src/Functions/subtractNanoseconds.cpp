#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionSubtractNanoseconds = FunctionDateOrDateTimeAddInterval<SubtractNanosecondsImpl>;

REGISTER_FUNCTION(SubtractNanoseconds)
{
    factory.registerFunction<FunctionSubtractNanoseconds>();
}

}


