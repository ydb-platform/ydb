#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB_CHDB
{

using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMinuteImpl>;

REGISTER_FUNCTION(ToMinute)
{
    factory.registerFunction<FunctionToMinute>();

    /// MySQL compatibility alias.
    factory.registerAlias("MINUTE", "toMinute", FunctionFactory::Case::Insensitive);
}

}
