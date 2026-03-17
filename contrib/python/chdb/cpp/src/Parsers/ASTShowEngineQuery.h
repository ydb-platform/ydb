#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB_CHDB
{

struct ASTShowEngineAndQueryNames
{
    static constexpr auto ID = "ShowEngineQuery";
    static constexpr auto Query = "SHOW ENGINES";
};

using ASTShowEnginesQuery = ASTQueryWithOutputImpl<ASTShowEngineAndQueryNames>;

}
