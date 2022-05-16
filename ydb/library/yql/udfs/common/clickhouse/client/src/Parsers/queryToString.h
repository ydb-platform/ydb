#pragma once

#include <Parsers/IAST.h>

namespace NDB
{
    String queryToString(const ASTPtr & query);
    String queryToString(const IAST & query);
}
