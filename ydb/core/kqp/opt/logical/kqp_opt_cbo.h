#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

/**
 * KQP specific Rel node, includes a pointer to ExprNode
*/
struct TKqpRelOptimizerNode : public TRelOptimizerNode {
    const NYql::TExprNode::TPtr Node;

    TKqpRelOptimizerNode(TString label, TOptimizerStatistics stats, const NYql::TExprNode::TPtr node) :
        TRelOptimizerNode(label, std::move(stats)), Node(node) { }
};

/**
 * KQP Specific cost function and join applicability cost function
*/
struct TKqpProviderContext : public TBaseProviderContext {
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool blockJoinEnabled) : KqpCtx(kqpCtx), OptLevel(optLevel), BlockJoinEnabled(blockJoinEnabled) {}

    virtual bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override;

    virtual double ComputeJoinCost(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo
    ) const override;

    const TKqpOptimizeContext& KqpCtx;
    int OptLevel;
    bool BlockJoinEnabled;
};

}
