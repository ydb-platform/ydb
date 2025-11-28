#pragma once
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {
    TVector<TVector<TString>> ExtractLayersFromExpr(const TExprNode::TPtr& node);
}
