#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;

REGISTER_FUNCTION(AddMinutes)
{
    factory.registerFunction<FunctionAddMinutes>();
}

}


