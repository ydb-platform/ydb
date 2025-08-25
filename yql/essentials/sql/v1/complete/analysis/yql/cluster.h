#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

    TMaybe<TString> ToCluster(const NYql::TExprNode& node);

    THashSet<TString> CollectClusters(const NYql::TExprNode& root);

} // namespace NSQLComplete
