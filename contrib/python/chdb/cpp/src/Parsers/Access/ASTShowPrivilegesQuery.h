#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB_CHDB
{

struct ASTShowPrivilegesIDAndQueryName
{
    static constexpr auto ID = "ShowPrivilegesQuery";
    static constexpr auto Query = "SHOW PRIVILEGES";
};

using ASTShowPrivilegesQuery = ASTQueryWithOutputImpl<ASTShowPrivilegesIDAndQueryName>;

}
