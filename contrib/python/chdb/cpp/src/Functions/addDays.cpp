#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;

REGISTER_FUNCTION(AddDays)
{
    factory.registerFunction<FunctionAddDays>();
}

}


