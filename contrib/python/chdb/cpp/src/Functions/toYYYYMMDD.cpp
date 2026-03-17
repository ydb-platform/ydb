#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB_CHDB
{

using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;

REGISTER_FUNCTION(ToYYYYMMDD)
{
    factory.registerFunction<FunctionToYYYYMMDD>();
}

}


