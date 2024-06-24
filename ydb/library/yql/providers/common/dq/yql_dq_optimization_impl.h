#pragma once

#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>

namespace NYql {

class TDqOptimizationBase: public IDqOptimization {
public:
    TExprNode::TPtr RewriteRead(const TExprNode::TPtr& read, TExprContext& ctx) override;
    TExprNode::TPtr RewriteLookupRead(const TExprNode::TPtr& read, TExprContext& ctx) override;
    TExprNode::TPtr ApplyExtractMembers(const TExprNode::TPtr& read, const TExprNode::TPtr& members, TExprContext& ctx) override;
    TExprNode::TPtr ApplyTakeOrSkip(const TExprNode::TPtr& read, const TExprNode::TPtr& countBase, TExprContext& ctx) override;
    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& read, TExprContext& ctx) override;
    TExprNode::TListType ApplyExtend(const TExprNode::TListType& reads, bool ordered, TExprContext& ctx) override;
};

} // namespace NYql
