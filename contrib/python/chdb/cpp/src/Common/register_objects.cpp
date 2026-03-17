#include <Common/register_objects.h>

namespace DB_CHDB
{

FunctionRegisterMap & FunctionRegisterMap::instance()
{
    static FunctionRegisterMap map;
    return map;
}

}
