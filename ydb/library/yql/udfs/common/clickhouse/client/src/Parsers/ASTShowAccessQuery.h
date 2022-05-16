#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace NDB
{

struct ASTShowAccessQueryNames
{
    static constexpr auto ID = "ShowAccessQuery";
    static constexpr auto Query = "SHOW ACCESS";
};

using ASTShowAccessQuery = ASTQueryWithOutputImpl<ASTShowAccessQueryNames>;

}
