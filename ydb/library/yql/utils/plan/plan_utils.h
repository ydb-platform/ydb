#pragma once
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/string.h>

namespace NYql::NPlanUtils {

struct TPredicate {
    TVector<TString> Args;
    TString Body;
};

TPredicate ExtractPredicate(const NNodes::TCoLambda& expr);

TString PrettyExprStr(const NNodes::TExprBase& expr);

} // namespace NYql::NPlanUtils
