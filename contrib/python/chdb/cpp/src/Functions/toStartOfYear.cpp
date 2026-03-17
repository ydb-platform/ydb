#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB_CHDB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfYearImpl>;

REGISTER_FUNCTION(ToStartOfYear)
{
    factory.registerFunction<FunctionToStartOfYear>();
}

}


