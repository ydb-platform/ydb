#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node);

    THashMap<TString, THashSet<TString>> CollectTablesByCluster(const NYql::TExprNode& node);

} // namespace NSQLComplete
