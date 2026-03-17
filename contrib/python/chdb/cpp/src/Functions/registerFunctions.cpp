#include "clickhouse_config.h"

#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    for (const auto & [_, reg] : FunctionRegisterMap::instance())
        reg(factory);
}

}
