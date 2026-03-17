#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;

REGISTER_FUNCTION(AddWeeks)
{
    factory.registerFunction<FunctionAddWeeks>();
}

}


