#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB_CHDB
{

using FunctionAddMicroseconds = FunctionDateOrDateTimeAddInterval<AddMicrosecondsImpl>;

REGISTER_FUNCTION(AddMicroseconds)
{
    factory.registerFunction<FunctionAddMicroseconds>();
}

}


