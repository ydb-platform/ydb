#pragma once

#include <Parsers/IAST.h>

namespace DB_CHDB
{
    String queryToString(const ASTPtr & query);
    String queryToString(const IAST & query);
}
