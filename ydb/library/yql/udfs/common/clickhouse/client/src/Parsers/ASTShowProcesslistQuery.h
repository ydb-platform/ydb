#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace NDB
{

struct ASTShowProcesslistIDAndQueryNames
{
    static constexpr auto ID = "ShowProcesslistQuery";
    static constexpr auto Query = "SHOW PROCESSLIST";
};

using ASTShowProcesslistQuery = ASTQueryWithOutputImpl<ASTShowProcesslistIDAndQueryNames>;

}
