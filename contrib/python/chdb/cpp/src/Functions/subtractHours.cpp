#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;

REGISTER_FUNCTION(SubtractHours)
{
    factory.registerFunction<FunctionSubtractHours>();
}

}


