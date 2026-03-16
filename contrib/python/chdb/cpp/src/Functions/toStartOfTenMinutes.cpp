#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB_CHDB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfTenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfTenMinutes)
{
    factory.registerFunction<FunctionToStartOfTenMinutes>();
}

}


