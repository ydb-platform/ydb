#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;

REGISTER_FUNCTION(AddMonths)
{
    factory.registerFunction<FunctionAddMonths>();
}

}


