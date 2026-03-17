#include "like.h"
#include "FunctionFactory.h"


namespace DB_CHDB
{

REGISTER_FUNCTION(Like)
{
    factory.registerFunction<FunctionLike>();
}

}
