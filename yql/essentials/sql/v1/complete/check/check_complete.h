#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    bool CheckComplete(
        TStringBuf query,
        NYql::TExprNode::TPtr root,
        NYql::TExprContext& ctx,
        NYql::TIssues& issues);

    bool CheckComplete(TStringBuf query, NYql::TAstNode& root, NYql::TIssues& issues);

} // namespace NSQLComplete
