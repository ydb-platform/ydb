#include <Functions/array/arrayMap.h>
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{

REGISTER_FUNCTION(ArrayMap)
{
    factory.registerFunction<FunctionArrayMap>();
}

}
