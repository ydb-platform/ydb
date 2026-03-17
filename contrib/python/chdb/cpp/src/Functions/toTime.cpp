#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB_CHDB
{

using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;

REGISTER_FUNCTION(ToTime)
{
    factory.registerFunction<FunctionToTime>();
}

}


