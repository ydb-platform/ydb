#pragma once
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>
#include <ydb/library/yql/providers/common/pushdown/settings.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NYql::NPushdown {

// Collects subpredicate that we can then push down
void CollectPredicates(const NNodes::TExprBase& predicate, TPredicateNode& predicateTree,
                       const TExprNode* lambdaArg, const NNodes::TExprBase& lambdaBody,
                       const TSettings& settings);

} // namespace NYql::NPushdown
