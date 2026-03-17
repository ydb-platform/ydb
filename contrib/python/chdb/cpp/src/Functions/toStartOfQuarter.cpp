#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB_CHDB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToDateOrDate32<ToStartOfQuarterImpl>;

REGISTER_FUNCTION(ToStartOfQuarter)
{
    factory.registerFunction<FunctionToStartOfQuarter>();
}

}


