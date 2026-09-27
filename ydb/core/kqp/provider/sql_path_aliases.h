#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

bool RewriteSqlPathAliases(TExprNode::TPtr& query, TExprContext& ctx, TStringBuf localCluster,
    const std::function<TString(TStringBuf)>& normalizePath);

}
