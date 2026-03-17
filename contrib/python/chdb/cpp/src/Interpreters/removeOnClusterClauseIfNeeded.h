#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST_fwd.h>

namespace DB_CHDB
{

ASTPtr removeOnClusterClauseIfNeeded(const ASTPtr & query_ptr, ContextPtr context, const WithoutOnClusterASTRewriteParams & params = {});

}
