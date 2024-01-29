#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpRelOptimizerNode : public NYql::TRelOptimizerNode {
    const NYql::TExprNode::TPtr Node;

    TKqpRelOptimizerNode(TString label, std::shared_ptr<NYql::TOptimizerStatistics> stats, const NYql::TExprNode::TPtr node) : 
        TRelOptimizerNode(label, stats), Node(node) { }
};

struct TKqpProviderContext : public NYql::IProviderContext {
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx) : KqpCtx(kqpCtx) {}

    virtual bool IsJoinApplicable(const std::shared_ptr<NYql::IBaseOptimizerNode>& left, 
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right, 
        const std::set<std::pair<NYql::NDq::TJoinColumn, NYql::NDq::TJoinColumn>>& joinConditions,
        NYql::EJoinImplType joinImpl);

    virtual double ComputeJoinCost(const NYql::TOptimizerStatistics& leftStats, const NYql::TOptimizerStatistics& rightStats, NYql::EJoinImplType joinImpl) const;

    const TKqpOptimizeContext& KqpCtx;
};

}