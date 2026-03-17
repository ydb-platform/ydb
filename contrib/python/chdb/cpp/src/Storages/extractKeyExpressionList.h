#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB_CHDB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);
}
