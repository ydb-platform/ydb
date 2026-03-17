#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>

namespace DB_CHDB
{

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
