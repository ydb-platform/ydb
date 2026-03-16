#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionSubtractMicroseconds = FunctionDateOrDateTimeAddInterval<SubtractMicrosecondsImpl>;
REGISTER_FUNCTION(SubtractMicroseconds)
{
    factory.registerFunction<FunctionSubtractMicroseconds>();
}

}


