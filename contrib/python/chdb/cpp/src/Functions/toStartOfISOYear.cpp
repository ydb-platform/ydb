#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB_CHDB
{

using FunctionToStartOfISOYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfISOYearImpl>;

REGISTER_FUNCTION(ToStartOfISOYear)
{
    factory.registerFunction<FunctionToStartOfISOYear>();
}

}


