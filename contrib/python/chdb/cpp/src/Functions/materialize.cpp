#include <Functions/FunctionFactory.h>
#include <Functions/materialize.h>


namespace DB_CHDB
{

REGISTER_FUNCTION(Materialize)
{
    factory.registerFunction<FunctionMaterialize>();
}

}
