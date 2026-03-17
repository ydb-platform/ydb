#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(IndexHint)
{
    factory.registerFunction<FunctionIndexHint>();
}

}
